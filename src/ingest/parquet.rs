use std::path::Path;
use duckdb::Connection;
use crate::ingest::{FileIngestor, IngestError};
use crate::ingest::schema::{TableSchema, ColumnSchema, DataType};

pub struct ParquetIngestor {
    // Configuration options if needed
}

impl ParquetIngestor {
    pub fn new() -> Self {
        Self {}
    }

    fn infer_schema(&self, path: &Path) -> Result<TableSchema, IngestError> {
        // Create a temporary in-memory DuckDB connection for schema inference
        let conn = Connection::open_in_memory()
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Get the file name without extension to use as table name if not specified
        let file_stem = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("data")
            .to_string();

        // Use DuckDB's schema inference for Parquet
        conn.execute(&format!(
            "CREATE TABLE temp_schema AS SELECT * FROM read_parquet('{}') LIMIT 0",
            path.to_string_lossy()
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

        Ok(TableSchema {
            name: file_stem,
            columns,
        })
    }
}

impl FileIngestor for ParquetIngestor {
    fn ingest(&self, path: &Path, table_name: &str) -> Result<TableSchema, IngestError> {
        // First infer the schema
        let mut schema = self.infer_schema(path)?;
        schema.name = table_name.to_string();

        // In a real implementation, we would now load the data into DuckDB
        // For Parquet, DuckDB can efficiently query directly from the files

        Ok(schema)
    }
}