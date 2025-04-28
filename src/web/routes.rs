use axum::{
    routing::{get, post, delete},
    Router,
};
use std::sync::Arc;

use super::handlers;
use super::static_files::static_handler;
use super::state::AppState;

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

                // File upload and processing
                .route("/upload/:subject", post(handlers::api::upload_file))

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