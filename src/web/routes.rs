use axum::{
    routing::{get, post, delete},
    Router,
    extract::{Multipart, Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use tokio::sync::oneshot;

use super::handlers;
use super::static_files::static_handler;
use super::state::AppState;

// This is a special handler that spawns a new task to handle file uploads
// This avoids Send/Sync issues with DuckDB
async fn sync_upload_handler(
    state: State<Arc<AppState>>,
    path: Path<String>,
    multipart: Multipart
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    // Create a oneshot channel for the result
    let (tx, rx) = oneshot::channel();

    // Clone state since we need to move it into the new task
    let state_clone = Arc::clone(&state);
    let path_str = path.0.clone();

    // Spawn a blocking task to handle the upload
    // This avoids thread safety issues with DuckDB
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();

        // Run the upload handler in the blocking task
        let result = rt.block_on(async {
            handlers::api::upload_file(State(state_clone), Path(path_str), multipart).await
        });

        // Send the result back through the channel
        let _ = tx.send(result);
    });

    // Wait for the result from the channel
    rx.await.unwrap_or(Err((StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to process upload".to_string())))
}

// UI Routes - web interface
pub fn ui_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(handlers::ui::index_handler))
        .route("/static/*path", get(static_handler))
}

// API Routes - REST API for programmatic access
pub fn api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .nest(
            "/api",
            Router::new()
                // Query endpoints
                .route("/query", post(handlers::api::execute_query))
                .route("/nl-query", post(handlers::api::nl_query))

                // Data management
                .route("/subjects", get(handlers::api::list_subjects))
                .route("/subjects/:subject", get(handlers::api::get_subject))
                .route("/subjects/:subject", post(handlers::api::create_subject))
                .route("/subjects/:subject", delete(handlers::api::delete_subject))

                // File upload and processing - using sync handler to avoid send issues
                .route("/upload/:subject", post(sync_upload_handler))


                // Schema management
                .route("/schema", get(handlers::api::get_schema))

                // Data export
                .route("/export/:format", get(handlers::api::export_data))

                // Saved queries and reports
                .route("/reports", get(handlers::api::list_reports))
                .route("/reports/:id", get(handlers::api::get_report))
                .route("/reports", post(handlers::api::save_report))
                .route("/reports/:id", delete(handlers::api::delete_report))

                // System status
                .route("/status", get(handlers::api::system_status))
        )
}