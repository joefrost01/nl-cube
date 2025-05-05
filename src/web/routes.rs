use axum::{
    routing::{get, post, delete},
    Router,
    extract::{Multipart, Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use axum::response::IntoResponse;
use crate::web::handlers::api::NlQueryRequest;
use super::handlers;
use super::static_files::static_handler;
use super::state::AppState;
use tracing::{error, info, warn, debug};

// This is a special handler that spawns a new task to handle file uploads
// This avoids Send/Sync issues with DuckDB
async fn sync_upload_handler(
    state: State<Arc<AppState>>,
    path: Path<String>,
    multipart: Multipart
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    info!("Starting file upload to subject: {}", path.0);

    // Create a oneshot channel for the result
    let (tx, rx) = oneshot::channel();

    // Clone state since we need to move it into the new task
    let state_clone = Arc::clone(&state);
    let path_str = path.0.clone();

    // Process the multipart form in the current thread
    let mut multipart_data = multipart;

    // Add boundaries and debug information to multipart extraction
    info!("Extracting files from multipart form");
    let result = try_extract_multipart(&mut multipart_data).await;

    match result {
        Ok(extracted_files) => {
            if extracted_files.is_empty() {
                return Err((StatusCode::BAD_REQUEST, "No valid files found in upload".to_string()));
            }

            info!("Successfully extracted {} files from multipart form", extracted_files.len());

            // Spawn a blocking task to handle the upload with the extracted files
            tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();

                // Process the files in the blocking task
                let result = rt.block_on(async {
                    // Create a temporary directory to store the files
                    let temp_dir = std::env::temp_dir().join("nl-cube-uploads");
                    if let Err(e) = std::fs::create_dir_all(&temp_dir) {
                        error!("Failed to create temp directory: {}", e);
                        return Err((StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("Failed to create temporary directory: {}", e)));
                    }
                    info!("Created temporary directory at {}", temp_dir.display());

                    let mut file_paths = Vec::new();

                    // Save files to disk
                    for (file_name, content) in &extracted_files {
                        let file_path = temp_dir.join(file_name);
                        info!("Saving file to {}", file_path.display());

                        match tokio::fs::write(&file_path, content).await {
                            Ok(_) => {
                                info!("Successfully saved file: {}", file_name);
                                file_paths.push(file_path);
                            },
                            Err(e) => {
                                error!("Failed to save file {}: {}", file_name, e);
                                return Err((StatusCode::INTERNAL_SERVER_ERROR,
                                            format!("Failed to save file {}: {}", file_name, e)));
                            }
                        }
                    }

                    if file_paths.is_empty() {
                        return Err((StatusCode::BAD_REQUEST, "No files were saved".to_string()));
                    }

                    // Now call the API handler with the saved files
                    info!("Processing {} saved files", file_paths.len());
                    let uploaded_files = match process_uploaded_files(state_clone, &path_str, &file_paths).await {
                        Ok(files) => files,
                        Err(e) => {
                            error!("Failed to process uploaded files: {:?}", e);
                            return Err(e);
                        }
                    };

                    // Clean up temp files
                    for path in file_paths {
                        if let Err(e) = tokio::fs::remove_file(&path).await {
                            warn!("Failed to remove temporary file {}: {}", path.display(), e);
                        } else {
                            debug!("Removed temporary file: {}", path.display());
                        }
                    }

                    info!("Upload processing completed successfully");
                    Ok(Json(uploaded_files))
                });

                // Send the result back through the channel
                let _ = tx.send(result);
            });

            // Wait for the result from the channel
            match rx.await {
                Ok(result) => result,
                Err(e) => {
                    error!("Channel error during upload processing: {}", e);
                    Err((StatusCode::INTERNAL_SERVER_ERROR,
                         "Failed to process upload: channel error".to_string()))
                }
            }
        },
        Err(e) => {
            error!("Failed to extract multipart form: {}", e);
            Err((StatusCode::BAD_REQUEST, format!("Failed to parse upload: {}", e)))
        }
    }
}

async fn try_extract_multipart(multipart: &mut Multipart) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut files = Vec::new();

    // Process each field in the multipart form
    while let Some(field) = multipart.next_field().await? {
        // Log the field name and content-type for debugging
        let name = field.name().unwrap_or("unnamed").to_string();
        let content_type = field.content_type().unwrap_or("").to_string();
        debug!("Processing field: {}, content-type: {}", name, content_type);

        let file_name = match field.file_name() {
            Some(name) => name.to_string(),
            None => {
                debug!("Skipping field without filename: {}", name);
                continue
            }
        };

        // Sanitize filename
        let safe_name = file_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '.' || c == '-' || c == '_' { c } else { '_' })
            .collect::<String>();

        debug!("Reading content for file: {}", safe_name);

        // Read the field content
        let content = match field.bytes().await {
            Ok(bytes) => {
                debug!("Successfully read {} bytes for file {}", bytes.len(), safe_name);
                bytes.to_vec()
            },
            Err(e) => {
                error!("Error reading field bytes for {}: {}", safe_name, e);
                return Err(Box::new(e));
            }
        };

        // Store the file data
        files.push((safe_name, content));
    }

    if files.is_empty() {
        debug!("No files extracted from multipart form");
    } else {
        debug!("Extracted {} files from multipart form", files.len());
    }

    Ok(files)
}

async fn process_uploaded_files(
    state: Arc<AppState>,
    subject: &str,
    file_paths: &[std::path::PathBuf]
) -> Result<Vec<String>, (StatusCode, String)> {
    use tracing::{error, info};

    // Verify the subject exists
    let subject_path = state.data_dir.join(subject);
    if !subject_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Subject not found".to_string()));
    }

    // Process all files
    let mut uploaded_files: Vec<String> = Vec::new();
    let ingest_manager = crate::ingest::IngestManager::with_connection_string(state.config.database.connection_string.clone());

    for file_path in file_paths {
        // Generate a table name based on file name only (not including subject prefix)
        let table_name = file_path.file_stem().unwrap_or_default().to_string_lossy().to_string();

        // Copy to the destination in the subject directory
        let dest_path = subject_path.join(file_path.file_name().unwrap_or_default());

        // Copy the file to the subject directory
        if let Err(e) = tokio::fs::copy(file_path, &dest_path).await {
            error!("Failed to copy file to subject directory: {}", e);
            continue;
        }

        info!("Ingesting file to DuckDB. Subject: {}, Table: {}, File: {}",
              subject, table_name, dest_path.display());

        // Use the ingest manager to create the table in the appropriate schema
        match ingest_manager.ingest_file(&dest_path, &table_name, &subject) {
            Ok(_) => {
                info!("Successfully ingested table {}.{}", subject, table_name);
                uploaded_files.push(table_name);
            },
            Err(e) => {
                error!("Failed to ingest file {}: {}", dest_path.display(), e);
                // Continue with other files even if one fails
            }
        }
    }

    // Add a significant delay before running any diagnostics to allow DuckDB to stabilize
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

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

    // Refresh the schema cache to make sure new tables are detected
    if let Err(e) = state.schema_manager.refresh_cache().await {
        error!("Error refreshing schema cache: {}", e);
    }

    // Return the list of successfully uploaded and ingested files
    Ok(uploaded_files)
}

// This is a special handler that spawns a blocking task to handle NL queries
// This avoids Send/Sync issues with DuckDB connections
async fn sync_nl_query_handler(
    state: State<Arc<AppState>>,
    payload: Json<NlQueryRequest>
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Create a oneshot channel for the result
    let (tx, rx) = oneshot::channel();

    // Clone state since we need to move it into the new task
    let state_clone = Arc::clone(&state);
    let payload_clone = payload.0.clone();

    // Spawn a blocking task to handle the query
    // This avoids thread safety issues with DuckDB
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();

        // Run the nl_query handler in the blocking task
        let result = rt.block_on(async {
            handlers::api::nl_query(State(state_clone), Json(payload_clone)).await
        });

        // Send the result back through the channel
        let _ = tx.send(result);
    });

    // Wait for the result from the channel and convert to appropriate response
    match rx.await {
        Ok(result) => result,
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to process natural language query".to_string()
        ))
    }
}

// UI Routes - web interface
pub fn ui_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(handlers::ui::index_handler))
        .route("/static/{*path}", get(static_handler))
}

// API Routes - REST API for programmatic access
pub fn api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .nest(
            "/api",
            Router::new()
                // Query endpoints
                .route("/query", post(handlers::api::execute_query))
                // Use the sync handler for nl-query
                .route("/nl-query", post(sync_nl_query_handler))

                // Data management
                .route("/subjects", get(handlers::api::list_subjects))
                .route("/subjects/{subject}", get(handlers::api::get_subject))
                .route("/subjects/{subject}", post(handlers::api::create_subject))
                .route("/subjects/{subject}", delete(handlers::api::delete_subject))

                // File upload and processing - using sync handler to avoid send issues
                .route("/upload/{subject}", post(sync_upload_handler))

                // Schema management
                .route("/schema", get(handlers::api::get_schema))

                // Data export
                .route("/export/{format}", get(handlers::api::export_data))

                // Saved queries and reports
                .route("/reports", get(handlers::api::list_reports))
                .route("/reports/{id}", get(handlers::api::get_report))
                .route("/reports", post(handlers::api::save_report))
                .route("/reports/{id}", delete(handlers::api::delete_report))

                // System status
                .route("/status", get(handlers::api::system_status))
        )
}