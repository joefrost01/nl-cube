use crate::db::db_utils::execute_stmt;
use duckdb::{Connection, Result as DuckResult, Statement};
use r2d2::{Pool, PooledConnection};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::db::db_pool::DuckDBConnectionManager;

/// A struct to cache and manage database schema information
pub struct SchemaManager {
    /// The database connection pool
    db_pool: Pool<DuckDBConnectionManager>,
    /// Cache of schemas and their tables
    schema_cache: RwLock<HashMap<String, Vec<String>>>,
    /// Last refresh timestamp
    last_refresh: RwLock<chrono::DateTime<chrono::Utc>>,
}

impl SchemaManager {
    /// Create a new SchemaManager
    pub fn new(db_pool: Pool<DuckDBConnectionManager>) -> Self {
        Self {
            db_pool,
            schema_cache: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(chrono::Utc::now()),
        }
    }

    /// Refresh the schema cache
    pub async fn refresh_cache(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Refreshing schema cache");

        // Get a connection from the pool
        let conn = self.db_pool.get()?;

        // Use a blocking task to execute the query
        let schemas = tokio::task::spawn_blocking(move || -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
            let mut schema_map = HashMap::new();

            // Get all schemas except system schemas
            let mut stmt = conn.prepare("
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
            ")?;

            let schema_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let schemas: Vec<String> = schema_iter.filter_map(Result::ok).collect();

            for schema_name in schemas {
                debug!("Found schema: {}", schema_name);

                // Get tables for this schema
                let table_query = format!(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
                    schema_name
                );

                let mut tables_stmt = conn.prepare(&table_query)?;
                let table_iter = tables_stmt.query_map([], |row| row.get::<_, String>(0))?;
                let tables: Vec<String> = table_iter.filter_map(Result::ok).collect();

                schema_map.insert(schema_name, tables);
            }

            Ok(schema_map)
        }).await??;

        // Update the cache
        let mut cache = self.schema_cache.write().await;
        *cache = schemas;

        // Update the last refresh timestamp
        let mut timestamp = self.last_refresh.write().await;
        *timestamp = chrono::Utc::now();

        info!("Schema cache refreshed successfully");
        Ok(())
    }

    /// Get all schemas
    pub async fn get_schemas(&self) -> Vec<String> {
        let cache = self.schema_cache.read().await;
        cache.keys().cloned().collect()
    }

    /// Get tables for a specific schema
    pub async fn get_tables(&self, schema: &str) -> Option<Vec<String>> {
        let cache = self.schema_cache.read().await;
        cache.get(schema).cloned()
    }

    /// Check if a schema exists
    pub async fn schema_exists(&self, schema: &str) -> bool {
        let cache = self.schema_cache.read().await;
        cache.contains_key(schema)
    }

    /// Check if a table exists in a schema
    pub async fn table_exists(&self, schema: &str, table: &str) -> bool {
        let cache = self.schema_cache.read().await;
        match cache.get(schema) {
            Some(tables) => tables.contains(&table.to_string()),
            None => false,
        }
    }

    /// Set search_path for a connection - explicitly handling errors
    pub fn set_search_path(&self, conn: &Connection, schema: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First ensure the schema exists by creating it if needed
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema);
        match conn.execute(&create_schema_sql, []) {
            Ok(_) => {
                debug!("Schema '{}' created or exists", schema);
            },
            Err(e) => {
                warn!("Error creating schema '{}': {}", schema, e);
                // Continue anyway, as the schema might already exist
            }
        }

        // Try to set search_path
        let search_path_sql = format!("SET search_path = '{}'", schema);
        match conn.execute(&search_path_sql, []) {
            Ok(_) => {
                debug!("Successfully set search_path to '{}'", schema);
                Ok(())
            },
            Err(e) => {
                warn!("Failed to set search_path to '{}': {}", schema, e);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to set search_path: {}", e)
                )))
            }
        }
    }

    /// Prepare a fully-qualified SQL query with schema prefix for all table names
    pub async fn qualify_sql(&self, sql: &str, schema: &str) -> String {
        let cache = self.schema_cache.read().await;
        let tables = match cache.get(schema) {
            Some(t) => t,
            None => return sql.to_string(), // Return original SQL if schema not found
        };

        // Replace table names with schema-qualified names
        let mut result = sql.to_string();

        for table in tables {
            // Common SQL patterns for table references
            let patterns = [
                (format!(" FROM {} ", table), format!(" FROM \"{}\".\"{}\" ", schema, table)),
                (format!(" JOIN {} ", table), format!(" JOIN \"{}\".\"{}\" ", schema, table)),
                (format!("{}.order_id", table), format!("\"{}\".\"{}\".", schema, table)),
                (format!(" UPDATE {} ", table), format!(" UPDATE \"{}\".\"{}\" ", schema, table)),
                (format!(" INTO {} ", table), format!(" INTO \"{}\".\"{}\" ", schema, table)),
            ];

            // Apply each pattern
            for (pattern, replacement) in &patterns {
                result = result.replace(pattern, replacement);
            }
        }

        result
    }

    /// Get a validated and qualified SQL query ready for execution
    pub async fn prepare_sql(&self, sql: &str, schema: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        if !self.schema_exists(schema).await {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Schema '{}' does not exist", schema)
            )));
        }

        // Qualify the SQL with schema name
        let qualified_sql = self.qualify_sql(sql, schema).await;
        debug!("Qualified SQL: {}", qualified_sql);

        Ok(qualified_sql)
    }

    /// Execute a SQL query with proper schema handling
    pub async fn execute_query(&self, sql: &str, schema: &str) -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error + Send + Sync>> {
        // Get a connection from the pool
        let conn = self.db_pool.get()?;

        // Prepare the SQL with schema qualification
        let qualified_sql = self.prepare_sql(sql, schema).await?;

        // Try to set search_path
        let set_search_path_result = self.set_search_path(&conn, schema);
        if let Err(e) = set_search_path_result {
            debug!("Could not set search_path: {}. Will rely on fully qualified table names.", e);
        }

        // Use a clone of the qualified SQL to move into the task
        let sql_to_execute = qualified_sql.clone();

        // Execute the query in a blocking task
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error + Send + Sync>> {
            // Prepare the statement
            let mut stmt = conn.prepare(&sql_to_execute)?;

            // Get column names and count BEFORE starting the query
            let column_count = stmt.column_count();
            let mut column_names = Vec::new();

            for i in 0..column_count {
                let name = stmt.column_name(i)?;
                column_names.push(name.to_string());
            }

            // NOW execute the query
            let mut rows = stmt.query([])?;

            let mut results = Vec::new();

            // Process each row
            while let Some(row) = rows.next()? {
                let mut row_map = HashMap::new();

                for (i, name) in column_names.iter().enumerate() {
                    // Handle different data types using pattern matching
                    let value = match row.get_ref(i)? {
                        // Updated for the exact enum variants in DuckDB 1.1.1
                        duckdb::types::ValueRef::Null => "NULL".to_string(),
                        duckdb::types::ValueRef::Boolean(v) => v.to_string(),
                        duckdb::types::ValueRef::TinyInt(v) => v.to_string(),
                        duckdb::types::ValueRef::SmallInt(v) => v.to_string(),
                        duckdb::types::ValueRef::Int(v) => v.to_string(),
                        duckdb::types::ValueRef::BigInt(v) => v.to_string(),
                        duckdb::types::ValueRef::HugeInt(v) => v.to_string(),
                        duckdb::types::ValueRef::UTinyInt(v) => v.to_string(),
                        duckdb::types::ValueRef::USmallInt(v) => v.to_string(),
                        duckdb::types::ValueRef::UInt(v) => v.to_string(),
                        duckdb::types::ValueRef::UBigInt(v) => v.to_string(),
                        duckdb::types::ValueRef::Float(v) => v.to_string(),
                        duckdb::types::ValueRef::Double(v) => v.to_string(),
                        duckdb::types::ValueRef::Date32(v) => v.to_string(),
                        duckdb::types::ValueRef::Time64(_, v) => v.to_string(), // Properly handling tuple
                        duckdb::types::ValueRef::Timestamp(_, v) => v.to_string(), // Properly handling tuple
                        duckdb::types::ValueRef::Interval { months, days, nanos } =>
                            format!("{}m {}d {}n", months, days, nanos),
                        duckdb::types::ValueRef::Blob(v) => format!("[BLOB: {} bytes]", v.len()),
                        duckdb::types::ValueRef::Text(v) => String::from_utf8_lossy(v).to_string(),
                        duckdb::types::ValueRef::Decimal(v) => v.to_string(),
                        // Catch-all for any other types or future variants
                        _ => "[UNKNOWN TYPE]".to_string(),
                    };

                    row_map.insert(name.clone(), value);
                }

                results.push(row_map);
            }

            Ok(results)
        }).await??;

        Ok(result)
    }

    /// Add a schema (create if not exists)
    pub async fn add_schema(&self, schema: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db_pool.get()?;

        // Clone schema for the blocking task
        let schema_clone = schema.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema_clone);
            conn.execute(&create_schema_sql, [])?;
            Ok(())
        }).await??;

        // Refresh cache to reflect changes
        self.refresh_cache().await?;

        Ok(result)
    }

    /// Drop a schema
    pub async fn drop_schema(&self, schema: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db_pool.get()?;

        // Clone schema for the blocking task
        let schema_clone = schema.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let drop_schema_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_clone);
            conn.execute(&drop_schema_sql, [])?;
            Ok(())
        }).await??;

        // Refresh cache to reflect changes
        self.refresh_cache().await?;

        Ok(result)
    }
}