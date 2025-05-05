use clap::Parser;
use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub connection_string: String,
    pub pool_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LlmConfig {
    pub backend: String, // "local", "remote", or "ollama"
    pub model: String,   // Model name
    pub api_key: Option<String>,
    pub api_url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub web: WebConfig,
    pub llm: LlmConfig,
    pub data_dir: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CliArgs {
    /// Path to configuration file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Host to bind to
    #[arg(long)]
    pub host: Option<String>,

    /// Port to bind to
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Directory for data storage
    #[arg(long)]
    pub data_dir: Option<String>,
}

impl AppConfig {
    pub fn new(args: &CliArgs) -> Result<Self, ConfigError> {
        // Start with default configuration
        let mut config_builder = Config::builder();

        // Add configuration from file if specified
        if let Some(config_path) = &args.config {
            config_builder = config_builder.add_source(File::from(config_path.as_path()));
        } else {
            // Check for config in default locations
            let default_locations = vec![
                "config.toml",
                "config/config.toml",
                "/etc/nl-cube/config.toml",
            ];

            for location in default_locations {
                if Path::new(location).exists() {
                    config_builder =
                        config_builder.add_source(File::new(location, config::FileFormat::Toml));
                    break;
                }
            }
        }

        // Build the config
        let mut config: AppConfig = config_builder.build()?.try_deserialize()?;

        // Override with command line args if provided
        if let Some(host) = &args.host {
            config.web.host = host.clone();
        }
        if let Some(port) = args.port {
            config.web.port = port;
        }
        if let Some(data_dir) = &args.data_dir {
            config.data_dir = data_dir.clone();
        }

        Ok(config)
    }
}

// Default implementation
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                connection_string: "nl-cube.db".to_string(),
                pool_size: 5,
            },
            web: WebConfig {
                host: "127.0.0.1".to_string(),
                port: 3000,
            },
            llm: LlmConfig {
                backend: "local".to_string(),
                model: "sqlcoder".to_string(),
                api_key: None,
                api_url: None,
            },
            data_dir: "data".to_string(),
        }
    }
}
