use clap::Parser;
use r2d2::Pool;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use crate::db::multi_db_pool::MultiDbConnectionManager;

mod config;
mod db;
mod ingest;
mod llm;
mod util;
mod web;

use crate::config::{AppConfig, CliArgs};
use crate::db::db_pool::DuckDBConnectionManager;
use crate::llm::LlmManager;
use crate::util::logging::init_tracing;
use crate::web::state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    init_tracing();

    // Parse command line arguments
    let args = CliArgs::parse();

    // Load configuration
    let config = match AppConfig::new(&args) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e.into());
        }
    };

    // Ensure data directory exists
    let data_dir = PathBuf::from(&config.data_dir);
    if !data_dir.exists() {
        info!("Creating data directory: {}", config.data_dir);
        std::fs::create_dir_all(&data_dir)?;
    }

    info!("Initializing DuckDB connection pool with multi-db support");
    let main_db_path = config.database.connection_string.clone();
    let db_manager = DuckDBConnectionManager::new(main_db_path);
    let pool = Pool::builder()
        .max_size(config.database.pool_size as u32)
        .build(db_manager)?;

    // Create the multi-db connection manager
    let multi_db_manager = Arc::new(MultiDbConnectionManager::new(
        config.database.connection_string.clone(),
        data_dir.clone()
    ));

    // Initialize LLM manager
    info!("Initializing LLM manager with backend: {}", config.llm.backend);
    let llm_manager = LlmManager::new(&config.llm)?;

    // Create application state with the multi-db manager
    let app_state = Arc::new(AppState::new_with_multi_db(
        config.clone(),
        pool,
        multi_db_manager.clone(),
        llm_manager,
        data_dir.clone()
    ));

    // Initialize schema cache
    info!("Initializing schema cache");
    if let Err(e) = app_state.schema_manager.refresh_cache().await {
        error!("Failed to initialize schema cache: {}", e);
        // Continue anyway, it will be refreshed later
    }

    // Initialize subjects
    info!("Initializing subjects");
    if let Err(e) = app_state.refresh_subjects().await {
        error!("Failed to initialize subjects: {}", e);
        // Continue anyway, it will be refreshed later
    }

    // Start the web server
    info!("Starting NL-Cube server on {}:{}", config.web.host, config.web.port);
    match web::run_server(config.web, app_state).await {
        Ok(_) => info!("Server stopped gracefully"),
        Err(e) => {
            error!("Server error: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn std::error::Error>);
        }
    }

    Ok(())
}