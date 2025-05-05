use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};

use serde::{Deserialize, Serialize};
use std::fs;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

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

    // Always use the currently selected subject for direct table queries
    let subject_name = match state.current_subject.read().await.clone() {
        Some(subject) => subject,
        None => extract_schema_from_query(&payload.query)
            .or_else(|| {
                if let Some(first_subject) = state.subjects.try_read().ok().and_then(|s| s.first().cloned()) {
                    Some(first_subject)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "main".to_string())
    };

    info!("Using subject '{}' for direct query", subject_name);

    // Build the path to the subject database
    let subject_dir = state.data_dir.join(&subject_name);
    let db_path = subject_dir.join(format!("{}.duckdb", subject_name));

    info!("Using subject database at: {}", db_path.display());

    // Make sure subject directory exists
    if !subject_dir.exists() {
        error!("Subject directory does not exist: {}", subject_dir.display());
        return Err((
            StatusCode::NOT_FOUND,
            format!("Subject '{}' not found", subject_name)
        ));
    }

    // Create the database file if it doesn't exist yet
    if !db_path.exists() {
        info!("Creating new database file at: {}", db_path.display());
        std::fs::create_dir_all(&subject_dir).map_err(|e| {
            error!("Failed to create subject directory: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create subject directory: {}", e)
            )
        })?;
    }

    // Make a direct connection to the subject database
    let conn = match duckdb::Connection::open(&db_path) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to open database at {}: {}", db_path.display(), e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database connection error: {}", e)
            ));
        }
    };

    // Simplify the query - remove schema qualifiers as they aren't needed when connecting directly
    let simplified_sql = simplify_query_for_direct_connection(&payload.query);
    info!("Qualified SQL: {}", simplified_sql);

    // Execute the query with the direct connection
    let mut stmt = match conn.prepare(&simplified_sql) {
        Ok(stmt) => stmt,
        Err(e) => {
            error!("Failed to prepare query: {}", e);
            return Err((StatusCode::BAD_REQUEST, format!("SQL error: {}", e)));
        }
    };

    // Get result as an Arrow batch
    let arrow_batch = match stmt.query_arrow([]) {
        Ok(batch) => batch,
        Err(e) => {
            error!("Failed to execute query: {}", e);
            return Err((StatusCode::BAD_REQUEST, format!("SQL error: {}", e)));
        }
    };

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

    // Serialize record batches to IPC format
    let mut buffer = Vec::new();

    // Create a file writer with the schema
    let mut file_writer = match arrow::ipc::writer::FileWriter::try_new(&mut buffer, schema.deref()) {
        Ok(writer) => writer,
        Err(e) => {
            error!("Failed to create Arrow file writer: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize Arrow data: {}", e)));
        }
    };

    // Write all record batches to the buffer
    for batch in &record_batches {
        if let Err(e) = file_writer.write(batch) {
            error!("Failed to write Arrow batch: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize Arrow data: {}", e)));
        }
    }

    // Finalize the stream
    if let Err(e) = file_writer.finish() {
        error!("Failed to finalize Arrow file: {}", e);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize Arrow data: {}", e)));
    }

    info!("Query executed successfully. Row count: {}, Execution time: {}ms",
          row_count, start_time.elapsed().as_millis());

    // Create response with Arrow data and metadata headers
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/vnd.apache.arrow.file"));

    if let Ok(count_header) = HeaderValue::from_str(&row_count.to_string()) {
        headers.insert("X-Total-Count", count_header);
    }

    if let Ok(time_header) = HeaderValue::from_str(&start_time.elapsed().as_millis().to_string()) {
        headers.insert("X-Execution-Time", time_header);
    }

    if let Ok(columns_json) = serde_json::to_string(&columns) {
        if let Ok(columns_header) = HeaderValue::from_str(&columns_json) {
            headers.insert("X-Columns", columns_header);
        }
    }

    // Add the SQL query as a header for debugging/tracing
    if let Ok(sql_header) = HeaderValue::from_str(&simplified_sql) {
        headers.insert("X-Generated-SQL", sql_header);
    }

    // Return the Arrow buffer with appropriate headers
    Ok((headers, buffer))
}

fn simplify_query_for_direct_connection(query: &str) -> String {
    // If the query has qualified references like "db1"."orders", simplify to just "orders"
    let re = regex::Regex::new(r#"["']([a-zA-Z0-9_]+)["']\s*\.\s*["']([a-zA-Z0-9_]+)["']"#).unwrap();
    let simplified = re.replace_all(query, "\"$2\"");

    simplified.to_string()
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

    // Get the table metadata for the current subject
    let table_metadata = match app_state.get_table_metadata(Some(&target_subject)).await {
        Ok(metadata) => metadata,
        Err(e) => {
            error!("Failed to get table metadata: {}", e);
            format!("")
        }
    };

    if table_metadata.trim() == "No databases found. Please upload data files first." {
        return Err((
            StatusCode::BAD_REQUEST,
            "No database tables found – upload some data first".into(),
        ));
    }

    // Generate SQL using LLM
    let llm = Arc::clone(&app_state.llm_manager);
    let raw_sql = {
        let mgr = llm.lock().await;
        mgr.generate_sql(&payload.question, &table_metadata).await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM error: {}", e))
        })?
    };

    // Validate SQL
    let sql = raw_sql.replace("`", "");

    let sql_for_headers = sql.clone();
    info!("Validated SQL: {}", sql);

    // Build the path to the subject database
    let subject_dir = app_state.data_dir.join(&target_subject);
    let db_path = subject_dir.join(format!("{}.duckdb", target_subject));
    debug!("Using database at path: {}", db_path.display());

    // Clone for use in the blocking task
    let db_path_string = db_path.to_string_lossy().to_string();
    let sql_to_execute = sql.clone();

    // Execute the query and get Arrow data in a blocking task
    let blocking_task = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, usize, Vec<String>, u64), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();

        // Connect to the database
        let conn = match duckdb::Connection::open(&db_path_string) {
            Ok(conn) => conn,
            Err(e) => return Err(Box::new(e))
        };

        // Prepare the statement
        let mut stmt = match conn.prepare(&sql_to_execute) {
            Ok(stmt) => stmt,
            Err(e) => return Err(Box::new(e))
        };

        // Execute and get Arrow results
        let arrow_batch = match stmt.query_arrow([]) {
            Ok(batch) => batch,
            Err(e) => return Err(Box::new(e))
        };

        let schema = arrow_batch.get_schema();

        // Get column names
        let columns = schema.fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<String>>();

        // Collect the Arrow batch into a Vec of RecordBatch
        let record_batches = arrow_batch.collect::<Vec<_>>().to_vec();

        // Get row count
        let row_count: usize = record_batches.iter().map(|batch| batch.num_rows()).sum();

        // Serialize record batches to IPC format
        let mut buffer = Vec::new();

        // Create a stream writer with the schema
        let mut stream_writer = match arrow::ipc::writer::FileWriter::try_new(&mut buffer, schema.deref()) {
            Ok(writer) => writer,
            Err(e) => return Err(Box::new(e))
        };

        // Write all record batches to the buffer
        for batch in &record_batches {
            if let Err(e) = stream_writer.write(batch) {
                return Err(Box::new(e));
            }
        }

        // Finalize the stream
        if let Err(e) = stream_writer.finish() {
            return Err(Box::new(e));
        }

        let execution_time = start_time.elapsed().as_millis() as u64;

        Ok((buffer, row_count, columns, execution_time))
    });

    // Properly handle the JoinError
    let join_result = blocking_task.await;
    let task_result = match join_result {
        Ok(result) => result,
        Err(join_err) => {
            error!("Task join error: {}", join_err);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database task execution failed: {}", join_err)
            ));
        }
    };

    // Handle the actual task result
    let (arrow_buffer, row_count, columns, execution_time) = match task_result {
        Ok(result) => result,
        Err(err) => {
            error!("Database query error: {}", err);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database query failed: {}", err)
            ));
        }
    };

    // Create the response with headers
    let mut headers = HeaderMap::new();

    // Set content type for Arrow data
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.file"),
    );

    // Add metadata headers
    if let Ok(v) = HeaderValue::from_str(&sql_for_headers) {
        headers.insert(HeaderName::from_static("x-generated-sql"), v);
    }

    if let Ok(v) = HeaderValue::from_str(&row_count.to_string()) {
        headers.insert(HeaderName::from_static("x-total-count"), v);
    }

    if let Ok(v) = HeaderValue::from_str(&execution_time.to_string()) {
        headers.insert(HeaderName::from_static("x-execution-time"), v);
    }

    if let Ok(columns_json) = serde_json::to_string(&columns) {
        if let Ok(v) = HeaderValue::from_str(&columns_json) {
            headers.insert(HeaderName::from_static("x-columns"), v);
        }
    }

    // Return the Arrow data with headers
    Ok((StatusCode::OK, headers, arrow_buffer).into_response())
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

    // Use the currently selected subject if available
    if let Some(current_subject) = app_state.current_subject.read().await.as_ref() {
        // Verify the subject still exists
        if subjects.contains(current_subject) {
            info!("Using currently selected subject '{}'", current_subject);
            return Ok(current_subject.clone());
        }
    }

    // If no subject is currently selected, use the first available one
    // This is a fallback and should only happen if the UI hasn't set a subject yet
    info!("No subject currently selected, using '{}' as default", subjects[0]);
    Ok(subjects[0].clone())
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

    // Build the path to the subject database
    let db_path = subject_path.join(format!("{}.duckdb", subject));

    // Connect directly to the subject database instead of using the pool
    let conn = match duckdb::Connection::open(&db_path) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to open subject database at {}: {}", db_path.display(), e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database connection error".to_string()
            ));
        }
    };

    // Try different approaches to get table names
    let tables = match get_tables_from_database(&conn) {
        Ok(tables) => tables,
        Err(e) => {
            error!("Failed to get tables for subject {}: {}", subject, e);
            vec![] // Return empty list rather than failing
        }
    };

    Ok(Json(Subject {
        name: subject,
        tables,
        file_count,
    }))
}

pub async fn select_subject(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let subject = path.0;
    let subject_path = state.data_dir.join(&subject);

    if !subject_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Subject not found".to_string()));
    }

    // Pass a reference to the subject
    match state.set_current_subject(&subject).await {
        Ok(_) => {
            info!("Selected subject: {}", subject);
            Ok(StatusCode::OK)
        }
        Err(e) => {
            error!("Failed to set current subject: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to set current subject: {}", e)
            ))
        }
    }
}

// Helper function to get tables from a database connection using multiple approaches
fn get_tables_from_database(conn: &duckdb::Connection) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut tables = Vec::new();

    // First try with sqlite_master (most reliable for DuckDB)
    let query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'duck_%' AND name NOT LIKE 'pg_%'";

    match conn.prepare(query) {
        Ok(mut stmt) => {
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            for row in rows {
                if let Ok(table_name) = row {
                    tables.push(table_name);
                }
            }
        }
        Err(e) => {
            error!("Error preparing sqlite_master query: {}", e);

            // Second attempt: try SHOW TABLES if first attempt fails
            match conn.prepare("SHOW TABLES") {
                Ok(mut show_stmt) => {
                    let rows = show_stmt.query_map([], |row| row.get::<_, String>(0))?;
                    for row in rows {
                        if let Ok(table_name) = row {
                            tables.push(table_name);
                        }
                    }
                }
                Err(e) => {
                    error!("Error preparing SHOW TABLES query: {}", e);

                    // Third attempt: try with information_schema
                    match conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'") {
                        Ok(mut info_stmt) => {
                            let rows = info_stmt.query_map([], |row| row.get::<_, String>(0))?;
                            for row in rows {
                                if let Ok(table_name) = row {
                                    tables.push(table_name);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error preparing information_schema query: {}", e);
                        }
                    }
                }
            }
        }
    }

    // If we still don't have tables, try a fourth approach with PRAGMA
    if tables.is_empty() {
        match conn.prepare("PRAGMA table_list") {
            Ok(mut pragma_stmt) => {
                let rows = pragma_stmt.query_map([], |row| row.get::<_, String>(1))?; // 1 is the name column
                for row in rows {
                    if let Ok(table_name) = row {
                        // Skip internal tables
                        if !table_name.starts_with("sqlite_") &&
                            !table_name.starts_with("duck_") &&
                            !table_name.starts_with("pg_") {
                            tables.push(table_name);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error preparing PRAGMA table_list query: {}", e);
            }
        }
    }

    Ok(tables)
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
#[allow(unused)]
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
        }
        _ => Err((StatusCode::BAD_REQUEST, "Unsupported export format".to_string())),
    }
}

// Reports
#[allow(unused)]
pub async fn list_reports(
    state: State<Arc<AppState>>,
) -> Result<Json<Vec<Report>>, (StatusCode, String)> {
    // Placeholder - in a real app, load from database
    let reports: Vec<Report> = Vec::new();
    Ok(Json(reports))
}

#[allow(unused)]
pub async fn get_report(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<Json<Report>, (StatusCode, String)> {
    // Placeholder - in a real app, load from database
    Err((StatusCode::NOT_FOUND, "Report not found".to_string()))
}

#[allow(unused)]
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

pub async fn delete_report() -> Result<StatusCode, (StatusCode, String)> {
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
