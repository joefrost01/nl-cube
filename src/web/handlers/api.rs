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

#[derive(Debug, Deserialize)]
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
) -> Result<(StatusCode, Json<QueryMetadata>), (StatusCode, String)> {
    let start_time = Instant::now();

    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database connection error".to_string(),
        )
    })?;

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

    // Serialize the RecordBatch to an Arrow IPC stream
    let mut buffer = Vec::new();

    // If there are no record batches (e.g., empty table), return an empty response
    if record_batch.is_empty() {
        let metadata = QueryMetadata {
            row_count: 0,
            execution_time_ms: start_time.elapsed().as_millis() as u64,
            columns: Vec::new(),
        };

        // We need to return the same type as the non-empty case
        return Ok((StatusCode::OK, Json(metadata)));
    }

    let mut stream_writer = StreamWriter::try_new(&mut buffer, schema.deref()).map_err(|e| {
        error!("Error creating StreamWriter: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Error creating Arrow stream".to_string())
    })?;

    for batch in record_batch {
        stream_writer.write(&batch).map_err(|e| {
            error!("Error writing batch: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Error writing Arrow batch".to_string())
        })?;
    }

    stream_writer.finish().map_err(|e| {
        error!("Error finishing Arrow stream: {}", e);
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

    // Return a tuple of (StatusCode, Headers, Body) to match the return type
    Ok((
        StatusCode::OK,
        Json(metadata)
    ))
}

// Natural language query
pub async fn nl_query(
    State(app_state): State<Arc<AppState>>,
    Json(payload):    Json<NlQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    use axum::http::{HeaderName, HeaderValue};
    use tracing::{debug, error, info};

    debug!("NL-query: {}", payload.question);

    // -- keep the schema refresh exactly as before ---------------------------
    app_state.refresh_schemas().await.ok();
    let schema_ddl = app_state.get_schemas_ddl().await;

    if schema_ddl.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "No database schema loaded – upload some data first".into(),
        ));
    }

    // ------------------------------------------------------------------------
    // Grab cheap clones *before* any await that might move across threads.
    // The Arc<Mutex<_>> is Send, so the outer future is Send.
    let llm     = Arc::clone(&app_state.llm_manager);
    let prompt  = payload.question.clone();
    let schema  = schema_ddl.clone();

    // Call LLM inside a tokio task; we must lock before calling generate_sql.
    let sql = tokio::spawn(async move {
        let mut mgr = llm.lock().await;           // 🔒 acquire the mutex
        mgr.generate_sql(&prompt, &schema).await  //   run the model
    })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM task failed: {e}")))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("LLM error: {e}")))?;

    if sql.trim().is_empty() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "The model produced empty SQL".into(),
        ));
    }

    // ------------------------------------------------------------------------
    // Execute the SQL via the existing handler
    let (status, meta) = execute_query(
        State(app_state.clone()),
        Json(ExecuteQueryRequest { query: sql.clone() }),
    )
        .await?;

    // Build the response and attach the generated SQL header
    let mut resp = (status, meta).into_response();
    if let Ok(v) = HeaderValue::from_str(&sql) {
        resp.headers_mut()
            .insert(HeaderName::from_static("x-generated-sql"), v);
    }

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

    // Get tables from this subject
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
    })?;

    let mut stmt = conn.prepare("
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name LIKE ?
    ").map_err(|e| {
        error!("Failed to prepare query: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
    })?;

    let pattern = format!("{}_%", subject);
    let tables_result = stmt.query_map(&[&pattern], |row| {
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

    // Refresh subjects list
    state.refresh_subjects().await.ok();

    Ok(StatusCode::NO_CONTENT)
}

// File upload
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

    // Process all files after saving them - DIRECTLY USING THE CONNECTION POOL
    let mut uploaded_files: Vec<String> = Vec::new();

    // Get a direct database connection for creating tables
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Database connection error".to_string())
    })?;

    for file_path in file_paths {
        // Generate a table name based on subject and file name
        let table_name = format!("{}_{}", subject, file_path.file_stem().unwrap_or_default().to_string_lossy());

        info!("Creating table {} from file {}", table_name, file_path.display());

        // Get the absolute path
        let absolute_path = file_path.canonicalize().map_err(|e| {
            error!("Failed to get absolute path: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Path error: {}", e))
        })?;

        // Drop table if exists
        match conn.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table_name), []) {
            Ok(_) => info!("Successfully dropped table if it existed"),
            Err(e) => error!("Error dropping table: {}", e),
        }

        // Create table directly from CSV
        let create_sql = format!(
            "CREATE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}', HEADER=true)",
            table_name,
            absolute_path.to_string_lossy()
        );

        info!("Executing SQL: {}", create_sql);

        match conn.execute(&create_sql, []) {
            Ok(_) => {
                info!("Successfully created table {}", table_name);
                uploaded_files.push(table_name);
            },
            Err(e) => {
                error!("Failed to create table {}: {}", table_name, e);
                // Continue with other files even if one fails
            }
        }
    }

    // Wait a moment for DuckDB to complete any background tasks
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Run direct database diagnostic to check table existence
    {
        info!("Running database diagnostic...");
        info!("Connection string from config: {}", state.config.database.connection_string);

        // Try various ways to list tables
        info!("Trying sqlite_master:");
        match conn.prepare("SELECT name FROM sqlite_master WHERE type='table'") {
            Ok(mut stmt) => {
                match stmt.query_map([], |row| row.get::<_, String>(0)) {
                    Ok(rows) => {
                        let tables: Vec<String> = rows.filter_map(Result::ok).collect();
                        info!("Found {} tables in sqlite_master: {:?}", tables.len(), tables);
                    }
                    Err(e) => {
                        error!("Error executing sqlite_master query: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error preparing sqlite_master query: {}", e);
            }
        }

        // Try SHOW TABLES
        info!("Trying SHOW TABLES:");
        match conn.prepare("SHOW TABLES") {
            Ok(mut stmt) => {
                match stmt.query_map([], |row| row.get::<_, String>(0)) {
                    Ok(rows) => {
                        let tables: Vec<String> = rows.filter_map(Result::ok).collect();
                        info!("Found {} tables with SHOW TABLES: {:?}", tables.len(), tables);
                    }
                    Err(e) => {
                        error!("Error executing SHOW TABLES: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error preparing SHOW TABLES: {}", e);
            }
        }
    }

    // Explicitly refresh the schema cache after ingestion
    match state.refresh_schemas().await {
        Ok(_) => info!("Successfully refreshed schemas after file upload"),
        Err(e) => error!("Failed to refresh schemas after upload: {}", e),
    }

    // Double-check that we have schemas after refresh
    {
        let schemas = state.schemas.read().await;
        info!("After refresh: Found {} schemas", schemas.len());
        if !schemas.is_empty() {
            debug!("First schema after refresh: {}", schemas[0]);
        }
    }

    // Return the list of successfully uploaded and ingested files
    Ok(Json(uploaded_files))
}

// Schema
pub async fn get_schema(
    state: State<Arc<AppState>>,
) -> Result<Json<String>, (StatusCode, String)> {
    let schemas = state.get_schemas_ddl().await;
    Ok(Json(schemas))
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

    // Get table count from database
    let conn = state.db_pool.get().map_err(|e| {
        error!("Failed to get DB connection: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database connection error".to_string(),
        )
    })?;

    let mut stmt = conn.prepare("
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
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

// Add this to src/web/handlers/api.rs

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

    // Try listing tables using sqlite_master
    match conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'") {
        Ok(mut stmt) => {
            match stmt.query_map([], |row| row.get::<_, String>(0)) {
                Ok(rows) => {
                    let tables: Vec<String> = rows.filter_map(Result::ok).collect();
                    output.push_str(&format!("Tables in sqlite_master: {:?}\n", tables));

                    // For each table, try to get column info
                    for table in &tables {
                        output.push_str(&format!("\nColumns for table {}:\n", table));
                        match conn.prepare(&format!("PRAGMA table_info(\"{}\")", table)) {
                            Ok(mut col_stmt) => {
                                match col_stmt.query_map([], |row| {
                                    Ok((
                                        row.get::<_, i32>(0).unwrap_or(-1),  // cid
                                        row.get::<_, String>(1).unwrap_or_else(|_| "unknown".to_string()), // name
                                        row.get::<_, String>(2).unwrap_or_else(|_| "unknown".to_string()), // type
                                    ))
                                }) {
                                    Ok(col_rows) => {
                                        let cols: Vec<_> = col_rows.filter_map(Result::ok).collect();
                                        output.push_str(&format!("{:?}\n", cols));
                                    }
                                    Err(e) => {
                                        output.push_str(&format!("Error getting columns: {}\n", e));
                                    }
                                }
                            }
                            Err(e) => {
                                output.push_str(&format!("Error preparing PRAGMA table_info: {}\n", e));
                            }
                        }
                    }
                }
                Err(e) => {
                    output.push_str(&format!("Error executing sqlite_master query: {}\n", e));
                }
            }
        }
        Err(e) => {
            output.push_str(&format!("Error preparing sqlite_master query: {}\n", e));
        }
    };

    // Force schema refresh and check results
    match state.refresh_schemas().await {
        Ok(_) => {
            let schemas = state.schemas.read().await;
            output.push_str(&format!("\nAfter refresh: Found {} schemas\n", schemas.len()));
            if !schemas.is_empty() {
                output.push_str(&format!("First schema: {}\n", schemas[0]));
            }
        }
        Err(e) => {
            output.push_str(&format!("\nSchema refresh error: {}\n", e));
        }
    }

    // Return the diagnostic output
    (StatusCode::OK, output).into_response()
}