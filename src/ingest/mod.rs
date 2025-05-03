// this is where our ingest from file code will go
// we'll need to infer the schema across many rows for CSVs

// probably need a separate admin connection for loading data on
pub mod csv;
pub mod parquet;
pub mod schema;

use std::path::Path;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum IngestError {
    IoError(std::io::Error),
    ParsingError(String),
    DatabaseError(String),
    UnsupportedFileType(String),
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngestError::IoError(err) => write!(f, "IO error: {}", err),
            IngestError::ParsingError(msg) => write!(f, "Parsing error: {}", msg),
            IngestError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            IngestError::UnsupportedFileType(ext) => write!(f, "Unsupported file type: {}", ext),
        }
    }
}

impl Error for IngestError {}

impl From<std::io::Error> for IngestError {
    fn from(err: std::io::Error) -> Self {
        IngestError::IoError(err)
    }
}

pub trait FileIngestor: Send + Sync {
    // Updated to include subject parameter
    fn ingest(&self, path: &Path, table_name: &str, subject: &str) -> Result<schema::TableSchema, IngestError>;
}

pub struct IngestManager {
    csv_ingestor: csv::CsvIngestor,
    parquet_ingestor: parquet::ParquetIngestor,
    connection_string: String,
}

impl IngestManager {
    pub fn new() -> Self {
        Self {
            csv_ingestor: csv::CsvIngestor::new("nl-cube.db".to_string()),
            parquet_ingestor: parquet::ParquetIngestor::new(),
            connection_string: "nl-cube.db".to_string(),
        }
    }

    pub fn with_connection_string(connection_string: String) -> Self {
        Self {
            csv_ingestor: csv::CsvIngestor::new(connection_string.clone()),
            parquet_ingestor: parquet::ParquetIngestor::new(),
            connection_string,
        }
    }

    // Updated to include subject parameter and use schema-based table access
    pub fn ingest_file(&self, path: &Path, table_name: &str, subject: &str) -> Result<schema::TableSchema, IngestError> {
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .ok_or_else(|| IngestError::UnsupportedFileType("No extension".to_string()))?;

        // First, create the schema if it doesn't exist
        let conn = duckdb::Connection::open(&self.connection_string)
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Create the schema with proper error handling
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", subject);
        match conn.execute(&create_schema_sql, []) {
            Ok(_) => {
                tracing::info!("Created or ensured schema '{}' exists", subject);
            },
            Err(e) => {
                tracing::error!("Failed to create schema '{}': {}", subject, e);
                return Err(IngestError::DatabaseError(format!("Failed to create schema: {}", e)));
            }
        }

        // Proceed with ingestion based on file type
        match extension.to_lowercase().as_str() {
            "csv" => self.csv_ingestor.ingest(path, table_name, subject),
            "parquet" => self.parquet_ingestor.ingest(path, table_name, subject),
            _ => Err(IngestError::UnsupportedFileType(extension.to_string())),
        }
    }

    // Verify that a table was properly ingested
    pub fn verify_ingestion(&self, subject: &str, table_name: &str) -> Result<bool, IngestError> {
        // Connect to the database
        let conn = duckdb::Connection::open(&self.connection_string)
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Set search path to the subject schema
        let search_path_sql = format!("SET search_path = '{}'", subject);
        match conn.execute(&search_path_sql, []) {
            Ok(_) => {
                tracing::debug!("Set search_path to '{}'", subject);
            },
            Err(e) => {
                tracing::warn!("Failed to set search_path: {} - will use fully qualified names", e);
            }
        }

        // Try to query the table using fully qualified name
        let verify_sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", subject, table_name);
        match conn.query_row(&verify_sql, [], |row| row.get::<_, i64>(0)) {
            Ok(count) => {
                tracing::info!("Verified table {}.{} exists with {} rows", subject, table_name, count);
                Ok(true)
            },
            Err(e) => {
                tracing::error!("Table verification failed: {}", e);
                Ok(false)
            }
        }
    }
}

impl Default for IngestManager {
    fn default() -> Self {
        Self::new()
    }
}