use std::fs::File;
use std::path::Path;
use std::io::{BufReader, Read};
use duckdb::Connection;
use crate::ingest::{FileIngestor, IngestError};
use crate::ingest::schema::{TableSchema, ColumnSchema, DataType};

pub struct CsvIngestor {
    sample_size: usize,
}

impl CsvIngestor {
    pub fn new() -> Self {
        Self {
            sample_size: 1000, // Default sample size for schema inference
        }
    }

    pub fn with_sample_size(sample_size: usize) -> Self {
        Self { sample_size }
    }

    fn infer_schema(&self, path: &Path) -> Result<TableSchema, IngestError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read a sample of the file for schema inference
        let mut sample = String::new();
        reader.take(self.sample_size as u64).read_to_string(&mut sample)?;

        // Create a temporary in-memory DuckDB connection for schema inference
        let conn = Connection::open_in_memory()
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Get the file name without extension to use as table name if not specified
        let file_stem = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("data")
            .to_string();

        // Use DuckDB's schema inference capabilities
        conn.execute(&format!(
            "CREATE TABLE temp_schema AS SELECT * FROM read_csv_auto('{}', SAMPLE_SIZE={})",
            path.to_string_lossy(),
            self.sample_size
        ), [])
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Query the schema information
        let mut stmt = conn.prepare("PRAGMA table_info(temp_schema)")
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        let column_iter = stmt.query_map([], |row| {
            Ok(ColumnSchema {
                name: row.get(1)?,
                data_type: match row.get::<_, String>(2)?.to_lowercase().as_str() {
                    "integer" => DataType::Integer,
                    "bigint" => DataType::BigInt,
                    "double" => DataType::Double,
                    "varchar" | "text" => DataType::String,
                    "boolean" => DataType::Boolean,
                    "date" => DataType::Date,
                    "timestamp" => DataType::Timestamp,
                    other => DataType::Unknown(other.to_string()),
                },
                nullable: row.get::<_, i32>(3)? == 0, // 'notnull' column is 0 if nullable
            })
        })
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        let columns: Result<Vec<ColumnSchema>, _> = column_iter.collect();
        let columns = columns.map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Make sure to finalize the statement and close the connection
        drop(stmt);
        drop(conn);

        Ok(TableSchema {
            name: file_stem,
            columns,
        })
    }
}

// Implement Send + Sync safely
unsafe impl Send for CsvIngestor {}
unsafe impl Sync for CsvIngestor {}

impl FileIngestor for CsvIngestor {
    fn ingest(&self, path: &Path, table_name: &str) -> Result<TableSchema, IngestError> {
        // First infer the schema
        let mut schema = self.infer_schema(path)?;
        schema.name = table_name.to_string();

        // In a real implementation, we would now load the data into DuckDB
        // using the inferred schema and the specified table name

        Ok(schema)
    }
}