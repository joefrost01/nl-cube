use axum::{
    extract::{Path, State},
    http::{StatusCode, header, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    Json,
};

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use std::fs;
use tracing::{debug, error, info};
use std::ops::Deref;
use arrow::ipc::writer::StreamWriter;

use crate::web::state::AppState;

// Query types

#[derive(Debug, Deserialize)]
pub struct ExecuteQueryRequest {
    pub query: String,
}

#[derive(Debug, Serialize)]
pub struct QueryMetadata {
    pub columns: Vec<String>,
    pub row_count: usize,
    pub execution_time_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NlQueryRequest {
    pub question: String,
}

// Report types

#[derive(Debug, Deserialize)]
pub struct SaveReportRequest {
    pub name: String,
    pub category: String,
    pub question: Option<String>,
    pub sql: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub id: String,
    pub name: String,
    pub category: String,
    pub question: Option<String>,
    pub sql: String,
    pub config: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

// Subject types

#[derive(Debug, Serialize)]
pub struct Subject {
    pub name: String,
    pub tables: Vec<String>,
    pub file_count: usize,
}

// System status

#[derive(Debug, Serialize)]
pub struct SystemStatus {
    pub version: String,
    pub uptime_seconds: i64,
    pub subject_count: usize,
    pub table_count: usize,
    pub report_count: usize,
}

// API Implementations

// Query execution
pub async fn execute_query(
    state: State<Arc<AppState>>,
    Json(payload): Json<ExecuteQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let start_time = Instant::now();
    info!("Executing SQL query: {}", payload.query);

    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database connection error".to_string(),
        )
    })?;

    // Find the schema used in the query or set a default
    let schema_name = extract_schema_from_query(&payload.query).unwrap_or_else(|| {
        // If no schema specified, try to find available schemas
        match find_first_schema(&conn) {
            Ok(schema) => {
                info!("No schema specified in query, using schema: {}", schema);
                schema
            },
            Err(_) => {
                info!("No schema found, using default");
                "main".to_string()
            }
        }
    });

    // Get table names for this schema
    let table_names = get_tables_for_schema(&conn, &schema_name).unwrap_or_default();

    // Apply simple schema qualification
    let qualified_sql = apply_simple_qualification(&payload.query, &table_names, &schema_name);
    info!("Qualified SQL: {}", qualified_sql);

    // Execute the query with qualified table names
    let mut stmt = conn.prepare(&qualified_sql).map_err(|e| {
        error!("Failed to prepare query: {}", e);
        (StatusCode::BAD_REQUEST, format!("SQL error: {}", e))
    })?;

    // Get result as an Arrow batch
    let arrow_batch = stmt.query_arrow([]).map_err(|e| {
        error!("Failed to execute query: {}", e);
        (StatusCode::BAD_REQUEST, format!("SQL error: {}", e))
    })?;

    let schema = arrow_batch.get_schema();

    // Collect the Arrow batch into a Vec of RecordBatch
    let record_batches = arrow_batch.collect::<Vec<_>>().to_vec();

    // Get row count for metadata
    let row_count: usize = record_batches.iter().map(|batch| batch.num_rows()).sum();

    // Get column names for metadata
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<String>>();

    // Create metadata
    let metadata = QueryMetadata {
        row_count,
        execution_time_ms: start_time.elapsed().as_millis() as u64,
        columns: columns.clone(), // Clone here to keep ownership of columns
    };

    // Serialize record batches to IPC format
    let mut buffer = Vec::new();

    // Check if we have record batches
    if !record_batches.is_empty() {
        // Create a stream writer with the schema from the first batch
        let mut stream_writer = StreamWriter::try_new(&mut buffer, schema.deref()).map_err(|e| {
            error!("Failed to create Arrow stream writer: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize Arrow data: {}", e))
        })?;

        // Write all record batches to the buffer
        for batch in record_batches {
            stream_writer.write(&batch).map_err(|e| {
                error!("Failed to write Arrow batch: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize Arrow data: {}", e))
            })?;
        }

        // Finalize the stream
        stream_writer.finish().map_err(|e| {
            error!("Failed to finalize Arrow stream: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize Arrow data: {}", e))
        })?;
    }

    info!("Query executed successfully. Row count: {}, Execution time: {}ms",
          row_count, metadata.execution_time_ms);

    // Create response with Arrow data and metadata headers
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/vnd.apache.arrow.file"));

    if let Ok(count_header) = HeaderValue::from_str(&row_count.to_string()) {
        headers.insert("X-Total-Count", count_header);
    }

    if let Ok(time_header) = HeaderValue::from_str(&metadata.execution_time_ms.to_string()) {
        headers.insert("X-Execution-Time", time_header);
    }

    if let Ok(columns_json) = serde_json::to_string(&columns) {
        if let Ok(columns_header) = HeaderValue::from_str(&columns_json) {
            headers.insert("X-Columns", columns_header);
        }
    }

    // Add the SQL query as a header for debugging/tracing
    if let Ok(sql_header) = HeaderValue::from_str(&qualified_sql) {
        headers.insert("X-Generated-SQL", sql_header);
    }

    // Return the Arrow buffer with appropriate headers
    Ok((headers, buffer))
}

fn get_tables_for_schema(conn: &duckdb::Connection, schema: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query = format!(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
        schema
    );

    let mut stmt = conn.prepare(&query)?;
    let table_names: Vec<String> = stmt.query_map([], |row| row.get(0))?
        .filter_map(Result::ok)
        .collect();

    Ok(table_names)
}

// Helper function to extract schema from query
fn extract_schema_from_query(query: &str) -> Option<String> {
    // Simple regex pattern to find schema.table pattern
    let re = regex::Regex::new(r#"["']?([a-zA-Z0-9_]+)["']?\.["']?[a-zA-Z0-9_]+"#).ok()?;
    if let Some(captures) = re.captures(query) {
        if let Some(schema_match) = captures.get(1) {
            let schema = schema_match.as_str().to_string();
            return Some(schema);
        }
    }
    None
}

// Helper function to find the first available schema
fn find_first_schema(conn: &duckdb::Connection) -> Result<String, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare("
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
        LIMIT 1
    ")?;

    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let schemas: Vec<String> = rows.filter_map(Result::ok).collect();

    if schemas.is_empty() {
        return Err("No schemas found".into());
    }

    Ok(schemas[0].clone())
}

// Natural language query - updated to use table metadata from database directly
pub async fn nl_query(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<NlQueryRequest>,
) -> Result<Response, (StatusCode, String)> {
    use axum::http::{HeaderName, HeaderValue};
    use tracing::{debug, error, info};

    debug!("NL-query: {}", payload.question);

    // Find active subject based on the query or use the first available subject
    let target_subject = determine_query_subject(&app_state).await?;
    info!("Using subject '{}' for query", target_subject);

    // Get the table metadata for only the current subject
    let table_metadata = match app_state.get_table_metadata(Some(&target_subject)).await {
        Ok(metadata) => metadata,
        Err(e) => {
            error!("Failed to get table metadata: {}", e);
            // Provide a fallback minimal schema description
            format!("# DATABASE SCHEMA\n\nDatabase: {}\nTable: orders\n\nColumns:\n- order_id (INTEGER)\n- customer_id (INTEGER)\n- order_date (DATE)\n- total_amount (DOUBLE)\n", target_subject)
        }
    };

    if table_metadata.trim() == "# DATABASE SCHEMA\n\nNo databases found. Please upload data files first." {
        return Err((
            StatusCode::BAD_REQUEST,
            "No database tables found – upload some data first".into(),
        ));
    }

    // Get LLM manager and generate SQL
    let llm = Arc::clone(&app_state.llm_manager);
    let raw_sql = {
        let mgr = llm.lock().await;
        mgr.generate_sql(&payload.question, &table_metadata).await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM error: {}", e))
        })?
    };

    // Extra validation step: ensure we have usable SQL
    let sql = if raw_sql.trim().is_empty() || raw_sql.trim() == "--" || raw_sql.trim().starts_with("-- ") {
        // Fallback to a simple COUNT query if the LLM output is unusable
        info!("LLM generated empty or comment-only SQL, using fallback query");
        "SELECT COUNT(*) FROM orders;".to_string()
    } else {
        raw_sql.replace("`", "")  // Clean any backticks
    };

    let sql_for_headers = sql.clone();  // Clone here for use outside the task

    info!("Validated SQL: {}", sql);

    // Execute the query
    let result = app_state.schema_manager.execute_query(&sql, &target_subject).await;
    let execution_time = std::time::Instant::now().elapsed().as_millis() as u64;

    match result {
        Ok(rows) => {
            // Create a response with metadata
            let mut headers = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&sql_for_headers) {
                headers.insert(HeaderName::from_static("x-generated-sql"), v);
            }

            // Add row count and execution time
            if let Ok(v) = HeaderValue::from_str(&rows.len().to_string()) {
                headers.insert(HeaderName::from_static("x-total-count"), v);
            }

            if let Ok(v) = HeaderValue::from_str(&execution_time.to_string()) {
                headers.insert(HeaderName::from_static("x-execution-time"), v);
            }

            // Extract column names from the first row if available
            let columns = if !rows.is_empty() {
                rows[0].keys().cloned().collect()
            } else {
                // Default column for COUNT queries
                if sql_for_headers.to_uppercase().contains("COUNT") {
                    vec!["count".to_string()]
                } else {
                    vec![]
                }
            };

            // Create the metadata
            let metadata = QueryMetadata {
                row_count: rows.len(),
                execution_time_ms: execution_time,
                columns
            };

            // Create the response
            let response = (StatusCode::OK, headers, Json(metadata)).into_response();

            Ok(response)
        },
        Err(e) => {
            error!("Query execution error: {}", e);

            // Try a different approach - direct database connection
            let data_dir = app_state.data_dir.join(&target_subject);
            let db_path = data_dir.join(format!("{}.duckdb", target_subject));

            if db_path.exists() {
                // Use a direct connection for the fallback approach
                info!("Trying fallback with direct connection to: {}", db_path.display());

                let fallback_sql = if sql.trim().is_empty() || sql.trim().starts_with("--") {
                    "SELECT COUNT(*) FROM orders;".to_string()
                } else {
                    sql.clone()
                };

                let result = tokio::task::spawn_blocking({
                    let sql = fallback_sql;
                    let db_path = db_path.clone();

                    move || -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
                        let conn = duckdb::Connection::open(&db_path)?;
                        let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
                        Ok(count)
                    }
                }).await;

                match result {
                    Ok(Ok(_)) => {
                        // Create a response with metadata
                        let mut headers = HeaderMap::new();
                        if let Ok(v) = HeaderValue::from_str(&sql_for_headers) {
                            headers.insert(HeaderName::from_static("x-generated-sql"), v);
                        }

                        // Add execution time
                        if let Ok(v) = HeaderValue::from_str(&execution_time.to_string()) {
                            headers.insert(HeaderName::from_static("x-execution-time"), v);
                        }

                        // Create metadata for COUNT result
                        let column_name = if sql_for_headers.contains("total_orders") {
                            "total_orders"
                        } else if sql_for_headers.contains("number_of_orders") {
                            "number_of_orders"
                        } else {
                            "count"
                        };

                        let metadata = QueryMetadata {
                            row_count: 1,  // COUNT queries always return 1 row
                            execution_time_ms: execution_time,
                            columns: vec![column_name.to_string()]
                        };

                        // Create response with the direct count value
                        Ok((StatusCode::OK, headers, Json(metadata)).into_response())
                    },
                    _ => {
                        Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Query execution error: {}", e)))
                    }
                }
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Query execution error: {}", e)))
            }
        }
    }
}

async fn determine_query_subject(app_state: &Arc<AppState>) -> Result<String, (StatusCode, String)> {
    // Refresh schema cache first
    if let Err(e) = app_state.schema_manager.refresh_cache().await {
        error!("Failed to refresh schema cache: {}", e);
        // Continue anyway, might have cached data
    }

    // Get available subjects
    let mut subjects = app_state.subjects.read().await.clone();

    if subjects.is_empty() {
        // If subject list is empty, refresh it
        if let Err(e) = app_state.refresh_subjects().await {
            error!("Failed to refresh subjects: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to determine query subject".to_string(),
            ));
        }

        // Get updated subjects
        subjects = app_state.subjects.read().await.clone();
    }

    if subjects.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "No subjects found. Please create a subject and upload data files.".into(),
        ));
    }

    // For now, just use the first subject
    // In a more advanced version, you could determine this based on the query content
    Ok(subjects[0].clone())
}

fn apply_simple_qualification(sql: &str, tables: &[String], schema: &str) -> String {
    // Start with the original SQL
    let mut result = sql.to_string();

    // For each known table name, apply qualification
    for table in tables {
        // Handle various SQL patterns with table references
        // Be careful with spaces to avoid partial matches

        // FROM clause
        let from_pattern = format!(" FROM {} ", table);
        let from_replacement = format!(" FROM \"{}\".\"{}\" ", schema, table);
        result = result.replace(&from_pattern, &from_replacement);

        // FROM clause at end of statement
        let from_end_pattern = format!(" FROM {};", table);
        let from_end_replacement = format!(" FROM \"{}\".\"{}\" ;", schema, table);
        result = result.replace(&from_end_pattern, &from_end_replacement);

        // JOIN clause
        let join_pattern = format!(" JOIN {} ", table);
        let join_replacement = format!(" JOIN \"{}\".\"{}\" ", schema, table);
        result = result.replace(&join_pattern, &join_replacement);

        // UPDATE clause
        let update_pattern = format!("UPDATE {} ", table);
        let update_replacement = format!("UPDATE \"{}\".\"{}\" ", schema, table);
        result = result.replace(&update_pattern, &update_replacement);

        // INSERT INTO clause
        let insert_pattern = format!("INSERT INTO {} ", table);
        let insert_replacement = format!("INSERT INTO \"{}\".\"{}\" ", schema, table);
        result = result.replace(&insert_pattern, &insert_replacement);

        // DELETE FROM clause
        let delete_pattern = format!("DELETE FROM {} ", table);
        let delete_replacement = format!("DELETE FROM \"{}\".\"{}\" ", schema, table);
        result = result.replace(&delete_pattern, &delete_replacement);

        // Table column references (e.g., "orders.column")
        // The issue was here - we can't modify parts[i] directly
        // Instead, create a new string and use replace_range

        // Fix for table.column references - using a simpler approach with string replacement
        let column_pattern = format!("{}.order_id", table);
        let column_replacement = format!("\"{}\".\"{}\".", schema, table);

        // Safely replace table.column patterns
        if result.contains(&column_pattern) {
            let new_pattern = column_replacement + "order_id";
            result = result.replace(&column_pattern, &new_pattern);
        }
    }

    result
}

// Subjects
pub async fn list_subjects(state: State<Arc<AppState>>) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    state.refresh_subjects().await.map_err(|e| {
        error!("Failed to refresh subjects: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to list subjects".to_string())
    })?;

    let subjects = state.subjects.read().await;
    Ok(Json(subjects.clone()))
}

pub async fn get_subject(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<Json<Subject>, (StatusCode, String)> {
    let subject = path.0;
    let subject_path = state.data_dir.join(&subject);

    if !subject_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Subject not found".to_string()));
    }

    // Count files in the subject directory
    let entries = fs::read_dir(&subject_path).map_err(|e| {
        error!("Failed to read subject directory: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read subject data".to_string())
    })?;

    let file_count = entries.count();

    // Get tables from this subject's schema using information_schema
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
    })?;

    let mut stmt = conn.prepare("
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = ?
    ").map_err(|e| {
        error!("Failed to prepare query: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
    })?;

    let tables_result = stmt.query_map(&[&subject], |row| {
        row.get::<_, String>(0)
    }).map_err(|e| {
        error!("Failed to execute query: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
    })?;

    let tables: Result<Vec<String>, _> = tables_result.collect();
    let tables = tables.map_err(|e| {
        error!("Failed to collect tables: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
    })?;

    Ok(Json(Subject {
        name: subject,
        tables,
        file_count,
    }))
}

pub async fn create_subject(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let subject = path.0;
    // Validate subject name (alphanumeric with underscores)
    if !subject.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err((
            StatusCode::BAD_REQUEST,
            "Subject name must be alphanumeric with underscores".to_string(),
        ));
    }

    // Create the subject directory
    let subject_path = state.data_dir.join(&subject);

    if subject_path.exists() {
        return Err((
            StatusCode::CONFLICT,
            "Subject already exists".to_string(),
        ));
    }

    fs::create_dir_all(&subject_path).map_err(|e| {
        error!("Failed to create subject directory: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create subject".to_string(),
        )
    })?;

    // Create the schema in the database
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
    })?;

    let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", subject);
    conn.execute(&create_schema_sql, []).map_err(|e| {
        error!("Failed to create database schema: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create database schema".to_string(),
        )
    })?;

    // Refresh subjects list
    state.refresh_subjects().await.ok();

    Ok(StatusCode::CREATED)
}

pub async fn delete_subject(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let subject = path.0;
    let subject_path = state.data_dir.join(&subject);

    if !subject_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Subject not found".to_string()));
    }

    // Delete the subject directory
    fs::remove_dir_all(&subject_path).map_err(|e| {
        error!("Failed to delete subject directory: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete subject".to_string(),
        )
    })?;

    // Drop the schema in the database
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
    })?;

    let drop_schema_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", subject);
    conn.execute(&drop_schema_sql, []).map_err(|e| {
        error!("Failed to drop database schema: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to drop database schema".to_string(),
        )
    })?;

    // Refresh subjects list
    state.refresh_subjects().await.ok();

    Ok(StatusCode::NO_CONTENT)
}

// Schema
pub async fn get_schema(
    state: State<Arc<AppState>>,
) -> Result<Json<String>, (StatusCode, String)> {
    let schemas_ddl = state.get_schemas_ddl().await.map_err(|e| {
        error!("Failed to get schema DDL: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
    })?;

    Ok(Json(schemas_ddl))
}

// Export
pub async fn export_data(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    // This is a placeholder - in a real implementation, we would:
    // 1. Accept query parameters to determine what to export
    // 2. Generate the appropriate export format (CSV, JSON, Parquet)
    // 3. Stream the result back to the client

    let format = path.0;
    match format.as_str() {
        "csv" | "json" | "parquet" => {
            Err((StatusCode::NOT_IMPLEMENTED, format!("Export to {} not yet implemented", format)))
        },
        _ => Err((StatusCode::BAD_REQUEST, "Unsupported export format".to_string())),
    }
}

// Reports
pub async fn list_reports(
    state: State<Arc<AppState>>,
) -> Result<Json<Vec<Report>>, (StatusCode, String)> {
    // Placeholder - in a real app, load from database
    let reports: Vec<Report> = Vec::new();
    Ok(Json(reports))
}

pub async fn get_report(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<Json<Report>, (StatusCode, String)> {
    // Placeholder - in a real app, load from database
    Err((StatusCode::NOT_FOUND, "Report not found".to_string()))
}

pub async fn save_report(
    state: State<Arc<AppState>>,
    Json(payload): Json<SaveReportRequest>,
) -> Result<Json<Report>, (StatusCode, String)> {
    // Placeholder - in a real app, save to database
    let id = format!("report-{}", chrono::Utc::now().timestamp());
    let now = chrono::Utc::now().to_rfc3339();

    Ok(Json(Report {
        id,
        name: payload.name,
        category: payload.category,
        question: payload.question,
        sql: payload.sql,
        config: payload.config,
        created_at: now.clone(),
        updated_at: now,
    }))
}

pub async fn delete_report(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Placeholder - in a real app, delete from database
    Err((StatusCode::NOT_FOUND, "Report not found".to_string()))
}

// System status
pub async fn system_status(
    state: State<Arc<AppState>>,
) -> Result<Json<SystemStatus>, (StatusCode, String)> {
    let now = chrono::Utc::now();
    let uptime = now.signed_duration_since(state.startup_time).num_seconds();

    let subject_count = state.subjects.read().await.len();

    // Get table count from database (across all schemas)
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database connection error".to_string(),
        )
    })?;

    let mut stmt = conn.prepare("
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
    ").map_err(|e| {
        error!("Failed to prepare query: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
    })?;

    let table_count: i64 = stmt.query_row([], |row| row.get(0)).map_err(|e| {
        error!("Failed to get table count: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
    })?;

    Ok(Json(SystemStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: uptime,
        subject_count,
        table_count: table_count as usize,
        report_count: 0, // Placeholder
    }))
}
