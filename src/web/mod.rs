// This is where the Axum start-up code goes as well as the handlers
// we'll need a post-handler for when the user drops in a file via the UI
// the post-handler will need to be multipart/streaming and stream to a temp file on disk
// which can then be uploaded via DuckDB.

// At some point we'll likely do a Tauri version but just by bundling Axum and having the browser
// part talk via loopback to minimise the code changes.

// We'll stream arrow results directly out of DuckDB for the queries as we'll be passing that
// to FINOS perspective in the UI and that can handle Arrow as input
pub mod handlers;
pub mod routes;
pub mod templates;
pub mod static_files;
pub mod state;


use crate::config::WebConfig;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, info};

use self::routes::api_routes;
use self::routes::ui_routes;
use self::state::AppState;

pub async fn run_server(config: WebConfig, app_state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Setup CORS with reasonable defaults
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers(Any);

    // Build the middleware stack
    let middleware = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(
            SetResponseHeaderLayer::if_not_present(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("no-cache, no-store"),
            ),
        )
        .layer(cors)
        .layer(CompressionLayer::new())
        .timeout(Duration::from_secs(30));

    // Build the router
    let app = Router::new()
        .merge(ui_routes())
        .merge(api_routes())
        .fallback(fallback_handler)
        .with_state(app_state);

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