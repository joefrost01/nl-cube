use axum::{
    extract::{Multipart, Path, State},
    http::{StatusCode, header, HeaderValue},
    response::IntoResponse,
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
) -> Result<impl IntoResponse, (StatusCode, String)> {
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

        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Json(metadata),
        ));
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

    // Set headers with metadata
    let metadata_header = serde_json::to_string(&metadata).unwrap_or_default();

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/vnd.apache.arrow.stream"),
            ("X-Query-Metadata", HeaderValue::from_str(&metadata_header).unwrap_or_default()),
        ],
        buffer,
    ))
}

// Natural language query
pub async fn nl_query(
    state: State<Arc<AppState>>,
    Json(payload): Json<NlQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    debug!("Received NL query: {}", payload.question);

    // Get the DB schema
    let schema = state.get_schemas_ddl().await;
    if schema.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "No database schema available. Please upload some data first.".to_string()
        ));
    }

    // Generate SQL using LLM
    let sql = state.llm_manager.generate_sql(&payload.question, &schema).await
        .map_err(|e| {
            error!("Failed to generate SQL: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to generate SQL: {}", e))
        })?;

    debug!("Generated SQL: {}", sql);

    // Now execute the generated SQL
    let execute_request = ExecuteQueryRequest { query: sql.clone() };

    // Include the SQL in the response headers
    let result = execute_query(state, Json(execute_request)).await?;

    // We need to extract the response parts to add the SQL header
    let (status, mut headers, body) = result.into_response().into_parts();

    // Add the SQL as a header
    headers.insert("X-Generated-SQL", HeaderValue::from_str(&sql).unwrap_or_default());

    Ok((status, headers, body).into_response())
}

// Get Arrow data directly from a table
pub async fn get_table_arrow(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
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

        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Json(metadata),
        ));
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

    // Set headers with metadata
    let metadata_header = serde_json::to_string(&metadata).unwrap_or_default();

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/vnd.apache.arrow.stream"),
            ("X-Query-Metadata", HeaderValue::from_str(&metadata_header).unwrap_or_default()),
        ],
        buffer,
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

    let mut uploaded_files = Vec::new();
    let ingest_manager = IngestManager::new();

    // Process each part of the multipart form
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

        // Ingest the file into DuckDB
        let table_name = format!("{}_{}", subject, file_path.file_stem().unwrap().to_string_lossy());
        match ingest_manager.ingest_file(&file_path, &table_name) {
            Ok(schema) => {
                info!("Ingested file {} as table {}", safe_name, table_name);
                uploaded_files.push(table_name);
            },
            Err(e) => {
                error!("Failed to ingest file {}: {}", safe_name, e);
                // Continue with other files even if one fails
            }
        }
    }

    // Refresh schemas after ingestion
    state.refresh_schemas().await.ok();

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
    Ok(Json(Vec::new()))
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