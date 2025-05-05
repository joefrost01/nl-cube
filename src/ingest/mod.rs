pub mod csv;
pub mod parquet;
pub mod schema;

use std::path::Path;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum IngestError {
    IoError(std::io::Error),
    DatabaseError(String),
    UnsupportedFileType(String),
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngestError::IoError(err) => write!(f, "IO error: {}", err),
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
}

impl IngestManager {
    pub fn new() -> Self {
        Self {
            csv_ingestor: csv::CsvIngestor::new(),
            parquet_ingestor: parquet::ParquetIngestor::new(),
        }
    }

    pub fn with_connection_string() -> Self {
        Self {
            csv_ingestor: csv::CsvIngestor::new(),
            parquet_ingestor: parquet::ParquetIngestor::new(),
        }
    }

    // Updated to include subject parameter and use schema-based table access
    pub fn ingest_file(&self, path: &Path, table_name: &str, subject: &str) -> Result<schema::TableSchema, IngestError> {
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .ok_or_else(|| IngestError::UnsupportedFileType("No extension".to_string()))?;

        // Ensure the subject directory exists
        let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());
        let subject_dir = Path::new(&data_dir).join(subject);
        if !subject_dir.exists() {
            std::fs::create_dir_all(&subject_dir)
                .map_err(|e| IngestError::IoError(e))?;
        }

        // Log that we've ensured the subject directory exists
        tracing::info!("Created or ensured schema '{}' exists", subject);

        // Proceed with ingestion based on file type
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