use axum::{
    response::{Html, IntoResponse},
};

use crate::web::static_files::get_embedded_file;

// Main UI entry point
pub async fn index_handler() -> impl IntoResponse {
    // For embedded files, we'll use the static file embed system
    match get_embedded_file("index.html") {
        Some(content) => Html(content).into_response(),
        None => {
            // Fallback using template
            Html("<html><body><h1>NL-Cube</h1><p>Error: index.html not found</p></body></html>").into_response()
        }
    }
}