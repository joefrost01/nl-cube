use tracing::info;
use crate::util::logging::init_tracing;

mod llm;
mod db;
mod server;
mod ingest;
mod config;
mod util;
mod web;

// Main, keep it compact and light
fn main() {
    init_tracing();
    info!("Hello, world!");
}
