use axum::{
    extract::{Multipart, Path, State},
    http::{StatusCode, header, HeaderValue, HeaderName},
    response::{IntoResponse, Response},
    Json,
};
use duckdb::Row;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info};
use std::ops::Deref;
use arrow::ipc::writer::StreamWriter;

use crate::llm::models::{NlQuery, SqlGenerationInput};
use crate::web::state::AppState;
use crate::ingest::IngestManager;

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
// Query execution
pub async fn execute_query(
    state: State<Arc<AppState>>,
    Json(payload): Json<ExecuteQueryRequest>,
) -> Result<(StatusCode, Json<QueryMetadata>), (StatusCode, String)> {
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

    // Set the search path to include this schema
    let search_path_sql = format!("SET search_path = '{}', 'main'", schema_name);
    match conn.execute(&search_path_sql, []) {
        Ok(_) => info!("Set search_path to {}", schema_name),
        Err(e) => error!("Failed to set search_path: {}", e),
    }

    // Execute the query
    let mut stmt = conn.prepare(&payload.query).map_err(|e| {
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
    let record_batch = arrow_batch.collect::<Vec<_>>().to_vec();

    // Get row count for metadata
    let row_count = record_batch.iter().map(|batch| batch.num_rows()).sum();

    // If there are no record batches (e.g., empty table), return an empty response
    if record_batch.is_empty() {
        let metadata = QueryMetadata {
            row_count: 0,
            execution_time_ms: start_time.elapsed().as_millis() as u64,
            columns: Vec::new(),
        };

        // We need to return the same type as the non-empty case
        info!("Query returned empty result set");
        return Ok((StatusCode::OK, Json(metadata)));
    }

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
        columns,
    };

    info!("Query executed successfully. Row count: {}, Execution time: {}ms",
          row_count, metadata.execution_time_ms);

    // Return the metadata
    Ok((StatusCode::OK, Json(metadata)))
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

    // Get the table metadata directly from the database
    let table_metadata = app_state.get_table_metadata().await.map_err(|e| {
        error!("Failed to get table metadata: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
    })?;

    if table_metadata.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "No database tables found – upload some data first".into(),
        ));
    }

    // ------------------------------------------------------------------------
    // Grab cheap clones *before* any await that might move across threads.
    // The Arc<Mutex<_>> is Send, so the outer future is Send.
    let llm     = Arc::clone(&app_state.llm_manager);
    let prompt  = payload.question.clone();
    let schema  = table_metadata.clone();

    // Call LLM inside a tokio task; we must lock before calling generate_sql.
    let raw_sql = tokio::spawn(async move {
        let mut mgr = llm.lock().await;           // 🔒 acquire the mutex
        mgr.generate_sql(&prompt, &schema).await  //   run the model
    })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM task failed: {e}")))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM error: {e}")))?;

    if raw_sql.trim().is_empty() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "The model produced empty SQL".into(),
        ));
    }

    info!("Generated raw SQL: {}", raw_sql);

    // Validate and clean the SQL
    let sql = validate_and_fix_sql(&raw_sql, &table_metadata);
    info!("Validated SQL: {}", sql);

    // Get a connection from the pool to determine the schema
    let conn = app_state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database connection error".to_string(),
        )
    })?;

    // Find available schemas and their tables
    let mut schemas_with_tables: Vec<(String, Vec<String>)> = Vec::new();

    // Query for all user schemas
    let mut schema_stmt = conn.prepare("
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
    ").map_err(|e| {
        error!("Failed to prepare schema query: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
    })?;

    let schema_rows = schema_stmt.query_map([], |row| row.get::<_, String>(0)).map_err(|e| {
        error!("Failed to execute schema query: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
    })?;

    let schemas: Vec<String> = schema_rows.filter_map(Result::ok).collect();

    // For each schema, get its tables
    for schema_name in &schemas {
        let table_query = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
            schema_name
        );

        let mut table_stmt = conn.prepare(&table_query).map_err(|e| {
            error!("Failed to prepare table query for schema {}: {}", schema_name, e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
        })?;

        let table_rows = table_stmt.query_map([], |row| row.get::<_, String>(0)).map_err(|e| {
            error!("Failed to execute table query for schema {}: {}", schema_name, e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
        })?;

        let tables: Vec<String> = table_rows.filter_map(Result::ok).collect();

        if !tables.is_empty() {
            schemas_with_tables.push((schema_name.clone(), tables));
        }
    }


    // Determine which schema to use based on the SQL query
    // Default to the first schema with tables if available
    let target_schema = if !schemas_with_tables.is_empty() {
        let first_schema = &schemas_with_tables[0];

        // Check if any table from the schema is mentioned in the SQL
        // This is a simple heuristic - it could be enhanced
        schemas_with_tables.iter()
            .find(|(_, tables)| {
                tables.iter().any(|table| sql.to_lowercase().contains(&table.to_lowercase()))
            })
            .map(|(schema_name, _)| schema_name.clone())
            .unwrap_or_else(|| first_schema.0.clone())
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "No tables found in any schema".into(),
        ));
    };

    info!("Using schema '{}' for query execution", target_schema);

    // Set the search_path to include the target schema
    let set_search_path_sql = format!("SET search_path = '{}', 'main'", target_schema);
    conn.execute(&set_search_path_sql, []).map_err(|e| {
        error!("Failed to set search_path: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e))
    })?;

    // Now execute the SQL query with the search_path set
    let start_time = Instant::now();

    let mut stmt = conn.prepare(&sql).map_err(|e| {
        error!("Failed to prepare query: {}", e);
        (StatusCode::BAD_REQUEST, format!("SQL error: {}", e))
    })?;

    // Get result as an Arrow batch
    let arrow_batch = stmt.query_arrow([]).map_err(|e| {
        error!("Failed to execute query: {}", e);
        (StatusCode::BAD_REQUEST, format!("SQL error: {}", e))
    })?;

    let schema = arrow_batch.get_schema();

    // Create metadata
    let start_time = Instant::now();
    let row_count = arrow_batch.collect::<Vec<_>>().iter().map(|batch| batch.num_rows()).sum();
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<String>>();

    let metadata = QueryMetadata {
        row_count,
        execution_time_ms: start_time.elapsed().as_millis() as u64,
        columns,
    };

    // Build the response and attach the generated SQL header
    // Create the response with metadata
    let mut resp = (StatusCode::OK, Json(metadata)).into_response();

    // Add the generated SQL as a header
    if let Ok(v) = HeaderValue::from_str(&sql) {
        resp.headers_mut().insert(HeaderName::from_static("x-generated-sql"), v);
    }

    info!("NL query response created successfully");
    Ok(resp)
}

// Get Arrow data directly from a table
pub async fn get_table_arrow(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<(StatusCode, Json<QueryMetadata>), (StatusCode, String)> {
    let table_name = path.0;
    let start_time = Instant::now();

    // Get a connection from the pool
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
    })?;

    // Prepare and execute the SELECT query
    let query = format!("SELECT * FROM \"{}\"", table_name);
    let mut stmt = conn.prepare(&query).map_err(|e| {
        error!("Error preparing query for {}: {}", table_name, e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Error preparing query".to_string())
    })?;

    // Get result as an Arrow batch
    let arrow_batch = stmt.query_arrow([]).map_err(|e| {
        error!("Error executing query_arrow for {}: {}", table_name, e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Error executing query".to_string())
    })?;

    let schema = arrow_batch.get_schema();

    // Collect the Arrow batch into a Vec of RecordBatch
    let record_batch = arrow_batch.collect::<Vec<_>>().to_vec();

    // Get row count for metadata
    let row_count = record_batch.iter().map(|batch| batch.num_rows()).sum();

    // Serialize the RecordBatch to an Arrow IPC stream
    let mut buffer = Vec::new();

    // If there are no record batches (e.g., empty table), return empty metadata
    if record_batch.is_empty() {
        let metadata = QueryMetadata {
            row_count: 0,
            execution_time_ms: start_time.elapsed().as_millis() as u64,
            columns: Vec::new(),
        };

        return Ok((StatusCode::OK, Json(metadata)));
    }

    let mut stream_writer = StreamWriter::try_new(&mut buffer, schema.deref()).map_err(|e| {
        error!("Error creating StreamWriter for {}: {}", table_name, e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Error creating Arrow stream".to_string())
    })?;

    for batch in record_batch {
        stream_writer.write(&batch).map_err(|e| {
            error!("Error writing batch for {}: {}", table_name, e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Error writing Arrow batch".to_string())
        })?;
    }

    stream_writer.finish().map_err(|e| {
        error!("Error finishing Arrow stream for {}: {}", table_name, e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Error finishing Arrow stream".to_string())
    })?;

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
        columns,
    };

    // Return the tuple format
    Ok((
        StatusCode::OK,
        Json(metadata)
    ))
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

// File upload - updated to use schema-based table creation
pub async fn upload_file(
    state: State<Arc<AppState>>,
    path: Path<String>,
    mut multipart: Multipart,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let subject = path.0;
    let subject_path = state.data_dir.join(&subject);

    if !subject_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Subject not found".to_string()));
    }

    // Process each part of the multipart form
    let mut file_paths: Vec<PathBuf> = Vec::new();

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        error!("Failed to process multipart form: {}", e);
        (StatusCode::BAD_REQUEST, "Failed to process upload".to_string())
    })? {
        let file_name = match field.file_name() {
            Some(name) => name.to_string(),
            None => continue, // Skip fields without a filename
        };

        info!("Processing uploaded file: {}", file_name);

        // Sanitize filename
        let safe_name = file_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '.' || c == '-' || c == '_' { c } else { '_' })
            .collect::<String>();

        let file_path = subject_path.join(&safe_name);
        file_paths.push(file_path.clone());

        // Save file to disk
        let content = field.bytes().await.map_err(|e| {
            error!("Failed to read file content: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read upload".to_string())
        })?;

        let mut file = File::create(&file_path).await.map_err(|e| {
            error!("Failed to create file: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to save file".to_string())
        })?;

        file.write_all(&content).await.map_err(|e| {
            error!("Failed to write file: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to save file".to_string())
        })?;
    }

    // Process all files after saving them
    let mut uploaded_files: Vec<String> = Vec::new();
    let ingest_manager = IngestManager::with_connection_string(state.config.database.connection_string.clone());

    for file_path in file_paths {
        // Generate a table name based on file name only (not including subject prefix)
        let table_name = file_path.file_stem().unwrap_or_default().to_string_lossy().to_string();

        info!("Ingesting file to DuckDB. Subject: {}, Table: {}, File: {}",
              subject, table_name, file_path.display());

        // Use the ingest manager to create the table in the appropriate schema
        match ingest_manager.ingest_file(&file_path, &table_name, &subject) {
            Ok(_) => {
                info!("Successfully ingested table {}.{}", subject, table_name);
                uploaded_files.push(table_name);
            },
            Err(e) => {
                error!("Failed to ingest file {}: {}", file_path.display(), e);
                // Continue with other files even if one fails
            }
        }
    }

    // Wait a moment for DuckDB to complete any background tasks
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Run direct database diagnostic to check table existence
    {
        info!("Running database diagnostic...");

        // Get a direct database connection
        let conn = state.db_pool.get().map_err(|e| {
            error!("Failed to get DB connection: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
        })?;

        // Check tables in the specific schema
        let check_sql = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
            subject
        );

        match conn.prepare(&check_sql) {
            Ok(mut stmt) => {
                match stmt.query_map([], |row| row.get::<_, String>(0)) {
                    Ok(rows) => {
                        let tables: Vec<String> = rows.filter_map(Result::ok).collect();
                        info!("Found {} tables in schema {}: {:?}", tables.len(), subject, tables);
                    }
                    Err(e) => {
                        error!("Error executing schema tables query: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error preparing schema tables query: {}", e);
            }
        }
    }

    // Return the list of successfully uploaded and ingested files
    Ok(Json(uploaded_files))
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

// Diagnostic endpoint
pub async fn diagnostic_query(
    state: State<Arc<AppState>>,
) -> impl IntoResponse {
    // First get a connection from the pool
    let conn = match state.db_pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get DB connection: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string()).into_response();
        }
    };

    // Try various queries to check the database state
    let mut output = String::new();
    output.push_str("Database Diagnostic Results:\n\n");

    // Check connection string
    output.push_str(&format!("Connection string: {}\n\n", state.config.database.connection_string));

    // List schemas
    match conn.prepare("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')") {
        Ok(mut stmt) => {
            match stmt.query_map([], |row| row.get::<_, String>(0)) {
                Ok(rows) => {
                    let schemas: Vec<String> = rows.filter_map(Result::ok).collect();
                    output.push_str(&format!("Schemas in database: {:?}\n\n", schemas));

                    // For each schema, list tables
                    for schema in &schemas {
                        let query = format!(
                            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
                            schema
                        );

                        match conn.prepare(&query) {
                            Ok(mut tables_stmt) => {
                                match tables_stmt.query_map([], |row| row.get::<_, String>(0)) {
                                    Ok(table_rows) => {
                                        let tables: Vec<String> = table_rows.filter_map(Result::ok).collect();
                                        output.push_str(&format!("Tables in schema {}: {:?}\n", schema, tables));

                                        // For each table, show a sample of columns
                                        for table in &tables {
                                            let col_query = format!(
                                                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'",
                                                schema, table
                                            );

                                            match conn.prepare(&col_query) {
                                                Ok(mut cols_stmt) => {
                                                    match cols_stmt.query_map([], |row| {
                                                        Ok((
                                                            row.get::<_, String>(0).unwrap_or_default(),
                                                            row.get::<_, String>(1).unwrap_or_default()
                                                        ))
                                                    }) {
                                                        Ok(col_rows) => {
                                                            let columns: Vec<(String, String)> = col_rows.filter_map(Result::ok).collect();
                                                            output.push_str(&format!("  Columns for {}.{}: {:?}\n", schema, table, columns));
                                                        }
                                                        Err(e) => {
                                                            output.push_str(&format!("  Error fetching columns: {}\n", e));
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    output.push_str(&format!("  Error preparing column query: {}\n", e));
                                                }
                                            }
                                        }

                                        output.push_str("\n");
                                    }
                                    Err(e) => {
                                        output.push_str(&format!("Error listing tables for schema {}: {}\n", schema, e));
                                    }
                                }
                            }
                            Err(e) => {
                                output.push_str(&format!("Error preparing tables query for schema {}: {}\n", schema, e));
                            }
                        }
                    }
                }
                Err(e) => {
                    output.push_str(&format!("Error listing schemas: {}\n", e));
                }
            }
        }
        Err(e) => {
            output.push_str(&format!("Error preparing schemas query: {}\n", e));
        }
    };

    // Test the search_path functionality
    output.push_str("\nTesting search_path functionality:\n");

    // Get a list of schemas
    let schemas_result: Result<Vec<String>, _> = conn
        .prepare("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')")
        .and_then(|mut stmt| {
            stmt.query_map([], |row| row.get::<_, String>(0))
                .map(|rows| rows.filter_map(Result::ok).collect())
        });

    match schemas_result {
        Ok(schemas) => {
            if !schemas.is_empty() {
                // Test with the first user schema
                let test_schema = &schemas[0];

                // Try to set the search_path
                match conn.execute(&format!("SET search_path = '{}', 'main'", test_schema), []) {
                    Ok(_) => {
                        output.push_str(&format!("Successfully set search_path to {}\n", test_schema));

                        // Try to query a table without schema prefix
                        match conn.prepare("SHOW TABLES") {
                            Ok(mut stmt) => {
                                match stmt.query_map([], |row| row.get::<_, String>(0)) {
                                    Ok(rows) => {
                                        let tables: Vec<String> = rows.filter_map(Result::ok).collect();

                                        if !tables.is_empty() {
                                            let test_table = &tables[0];
                                            output.push_str(&format!("Tables in current search_path: {:?}\n", tables));

                                            // Try to query the table without schema prefix
                                            match conn.prepare(&format!("SELECT * FROM \"{}\" LIMIT 1", test_table)) {
                                                Ok(_) => {
                                                    output.push_str(&format!("Successfully prepared query for table {} without schema prefix\n", test_table));
                                                }
                                                Err(e) => {
                                                    output.push_str(&format!("Error querying table without schema prefix: {}\n", e));
                                                }
                                            }
                                        } else {
                                            output.push_str("No tables found in schema after setting search_path\n");
                                        }
                                    }
                                    Err(e) => {
                                        output.push_str(&format!("Error listing tables after setting search_path: {}\n", e));
                                    }
                                }
                            }
                            Err(e) => {
                                output.push_str(&format!("Error preparing SHOW TABLES after setting search_path: {}\n", e));
                            }
                        }
                    }
                    Err(e) => {
                        output.push_str(&format!("Error setting search_path: {}\n", e));
                    }
                }
            } else {
                output.push_str("No user schemas found to test search_path\n");
            }
        }
        Err(e) => {
            output.push_str(&format!("Error getting schemas for search_path test: {}\n", e));
        }
    }

    // Return the diagnostic output
    (StatusCode::OK, output).into_response()
}

// Helper function to validate and fix SQL
fn validate_and_fix_sql(sql: &str, schema_info: &str) -> String {
    use tracing::{info, warn};

    // 1. Extract column names from schema info
    let column_re = regex::Regex::new(r"\|\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+\|").unwrap();
    let mut column_names = Vec::new();

    for cap in column_re.captures_iter(schema_info) {
        if let Some(column) = cap.get(1) {
            column_names.push(column.as_str().to_string());
        }
    }

    // Create a map of lowercase column names to their correct case
    let mut column_case_map = std::collections::HashMap::new();
    for column in column_names {
        column_case_map.insert(column.to_lowercase(), column.clone());
    }

    // 2. Fix SQL keywords and column names
    let mut result = String::new();
    let sql_keywords = [
        "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
        "LIMIT", "JOIN", "INNER", "OUTER", "LEFT", "RIGHT", "FULL",
        "ON", "AS", "AND", "OR", "NOT", "EXISTS", "IN", "BETWEEN",
        "IS", "NULL", "LIKE", "CASE", "WHEN", "THEN", "ELSE", "END",
        "COUNT", "SUM", "AVG", "MIN", "MAX", "CAST", "COALESCE"
    ];

    // Create a word list from the SQL
    let word_re = regex::Regex::new(r"[a-zA-Z_][a-zA-Z0-9_]*").unwrap();
    let mut word_positions = Vec::new();

    for word_match in word_re.find_iter(sql) {
        word_positions.push((word_match.start(), word_match.end(), word_match.as_str().to_string()));
    }

    // Process the SQL in order
    let mut last_pos = 0;
    for (start, end, word) in word_positions {
        // Add any non-word characters before this word
        result.push_str(&sql[last_pos..start]);

        // Process the word
        let word_upper = word.to_uppercase();
        let word_lower = word.to_lowercase();

        // Check if it's a SQL keyword
        if sql_keywords.contains(&word_upper.as_str()) {
            result.push_str(&word_lower);
        }
        // Check if it's a column name that needs to be fixed
        else if let Some(correct_case) = column_case_map.get(&word_lower) {
            if &word != correct_case {
                warn!("Found column name with incorrect case: {} - Fixing to: {}", word, correct_case);
                result.push_str(correct_case);
            } else {
                result.push_str(&word);
            }
        }
        // Check for potential typos in column names
        else {
            let mut fixed = false;

            for (correct_lower, correct_case) in &column_case_map {
                // Simple fuzzy matching - edit distance <= 2 for short words, 3 for longer ones
                let max_distance = if correct_lower.len() > 6 { 3 } else { 2 };
                let distance = levenshtein_distance(&word_lower, correct_lower);

                if distance <= max_distance && distance > 0 {
                    warn!("Possible typo in column name: {} - Fixing to: {}", word, correct_case);
                    result.push_str(correct_case);
                    fixed = true;
                    break;
                }
            }

            if !fixed {
                result.push_str(&word);
            }
        }

        last_pos = end;
    }

    // Add any remaining text
    result.push_str(&sql[last_pos..]);

    // 3. Ensure the SQL ends with a semicolon
    let trimmed = result.trim();
    if !trimmed.ends_with(';') {
        format!("{};", trimmed)
    } else {
        trimmed.to_string()
    }
}

// Simple Levenshtein distance calculation for typo detection
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();

    let m = s1_chars.len();
    let n = s2_chars.len();

    // Create a matrix of size (m+1)x(n+1)
    let mut matrix = vec![vec![0; n + 1]; m + 1];

    // Initialize the matrix
    for i in 0..=m {
        matrix[i][0] = i;
    }
    for j in 0..=n {
        matrix[0][j] = j;
    }

    // Fill the matrix
    for i in 1..=m {
        for j in 1..=n {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };

            matrix[i][j] = std::cmp::min(
                matrix[i - 1][j] + 1,      // deletion
                std::cmp::min(
                    matrix[i][j - 1] + 1,  // insertion
                    matrix[i - 1][j - 1] + cost  // substitution
                )
            );
        }
    }

    matrix[m][n]
}