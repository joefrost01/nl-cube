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
            // Try to handle the notnull column value which can be bool or int in different DuckDB versions
            let is_not_null = match row.get::<_, bool>(3) {
                Ok(value) => value,
                Err(_) => match row.get::<_, i32>(3) {
                    Ok(value) => value != 0,
                    Err(e) => return Err(e)
                }
            };

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
                nullable: !is_not_null, // If is_not_null is true, then the column is not nullable
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
unsafe impl Send for ParquetIngestor {}
unsafe impl Sync for ParquetIngestor {}

impl FileIngestor for ParquetIngestor {
    // Updated to include subject parameter and implement schema-based approach
    fn ingest(&self, path: &Path, table_name: &str, subject: &str) -> Result<TableSchema, IngestError> {
        // First infer the schema
        let mut schema = self.infer_schema(path)?;
        schema.name = table_name.to_string();

        // Get the absolute path to the Parquet file for DuckDB
        let absolute_path = path.canonicalize()
            .map_err(|e| IngestError::IoError(e))?;

        // Use the environment DATABASE_URL variable to determine which file to open
        let db_file = std::env::var("DATABASE_URL").unwrap_or_else(|_| "nl-cube.db".to_string());
        tracing::info!("Opening database connection to: {}", db_file);

        // Connect to the actual DuckDB database
        let conn = Connection::open(&db_file)
            .map_err(|e| IngestError::DatabaseError(e.to_string()))?;

        // Create the schema if it doesn't exist
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", subject);
        conn.execute(&create_schema_sql, [])
            .map_err(|e| IngestError::DatabaseError(format!("Failed to create schema: {}", e)))?;

        // Log database and table info
        tracing::info!("Ingesting Parquet file to DuckDB. Schema: {}, Table: {}, File: {}",
                       subject, table_name, absolute_path.display());

        // Create a more robust create table statement with explicit DROP IF EXISTS
        let drop_sql = format!("DROP TABLE IF EXISTS \"{}\".\"{}\"", subject, table_name);

        // First drop the table if it exists
        conn.execute(&drop_sql, [])
            .map_err(|e| IngestError::DatabaseError(format!("Failed to drop existing table: {}", e)))?;

        // Now use DuckDB's Parquet reading to create the table directly using the schema.table format
        let create_sql = format!(
            "CREATE TABLE \"{}\".\"{}\" AS SELECT * FROM read_parquet('{}') ",
            subject,
            table_name,
            absolute_path.to_string_lossy()
        );

        tracing::info!("Executing SQL: {}", create_sql);

        conn.execute(&create_sql, [])
            .map_err(|e| IngestError::DatabaseError(format!("Failed to create table: {}", e)))?;

        // Verify table was created
        let verify_sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", subject, table_name);

        match conn.query_row(&verify_sql, [], |row| row.get::<_, i64>(0)) {
            Ok(count) => {
                tracing::info!("Successfully created table {}.{} with {} rows", subject, table_name, count);
            }
            Err(e) => {
                tracing::error!("Table creation verification failed: {}", e);
                return Err(IngestError::DatabaseError(format!("Table verification failed: {}", e)));
            }
        }

        Ok(schema)
    }
}