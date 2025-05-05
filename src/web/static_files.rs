use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;
use std::sync::Arc;

use crate::web::state::AppState;

#[derive(RustEmbed)]
#[folder = "static/"]
struct StaticAssets;

pub async fn static_handler(
    Path(path): Path<String>,
    State(_state): State<Arc<AppState>>,
) -> impl IntoResponse {
    serve_static_file(path)
}

// Embedded static file handler
pub fn serve_static_file(path: String) -> Response {
    let path = path.trim_start_matches('/');

    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();

            (
                [(header::CONTENT_TYPE, mime.as_ref())],
                content.data.to_vec(),
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}

pub fn get_embedded_file(path: &str) -> Option<String> {
    StaticAssets::get(path).map(|content| {
        let bytes = content.data.to_vec();
        String::from_utf8_lossy(&bytes).to_string()
    })
}