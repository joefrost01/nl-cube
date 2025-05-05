pub mod handlers;
pub mod routes;
pub mod templates;
pub mod static_files;
pub mod state;


use crate::config::WebConfig;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

use self::routes::api_routes;
use self::routes::ui_routes;
use self::state::AppState;

pub async fn run_server(config: WebConfig, app_state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // Create a specific multipart configuration with larger limits
    let multipart_config = axum::extract::DefaultBodyLimit::max(250 * 1024 * 1024); // 250 MB limit

    // Build the router with increased body limit for multipart forms
    let app = Router::new()
        .merge(ui_routes())
        .merge(api_routes())
        .fallback(fallback_handler)
        .with_state(app_state)
        .layer(tower_http::limit::RequestBodyLimitLayer::new(100 * 1024 * 1024)) // 100 MB global limit
        .layer(multipart_config);

    // Parse the socket address
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("Failed to parse socket address");

    // Start the server
    info!("Starting web server at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// Fallback handler for unmatched routes
async fn fallback_handler() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        "The requested resource was not found",
    )
}