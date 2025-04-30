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

        match extension.to_lowercase().as_str() {
            "csv" => self.csv_ingestor.ingest(path, table_name, subject),
            "parquet" => self.parquet_ingestor.ingest(path, table_name, subject),
            _ => Err(IngestError::UnsupportedFileType(extension.to_string())),
        }
    }
}

impl Default for IngestManager {
    fn default() -> Self {
        Self::new()
    }
}