// src/db/schema_manager.rs
use crate::db::db_utils::execute_stmt;
use crate::db::multi_db_pool::MultiDbConnectionManager;
use crate::db::db_pool::DuckDBConnectionManager;
use duckdb::{Connection, Result as DuckResult, Statement};
use r2d2::{Pool, PooledConnection};
use std::sync::Arc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

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
    /// Create a new SchemaManager with the standard connection manager
    pub fn new(db_pool: Pool<DuckDBConnectionManager>) -> Self {
        Self {
            db_pool,
            schema_cache: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(chrono::Utc::now()),
        }
    }

    /// Create a new SchemaManager with the multi-database connection manager
    /// Note: This constructor is here for future use but isn't being used yet
    pub fn with_multi_db(
        db_pool: Pool<MultiDbConnectionManager>,
        _conn_manager: Arc<MultiDbConnectionManager>,
        _data_dir: PathBuf
    ) -> Self {
        // We can't currently use a MultiDbConnectionManager pool directly,
        // but this constructor is here for future use
        unimplemented!("Multi-database connection manager not yet fully implemented");
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

    /// Execute a query with proper schema handling
    // from src/db/schema_manager.rs
    pub async fn execute_query(&self, sql: &str, schema: &str) -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error + Send + Sync>> {
        // Get a connection from the pool
        let conn = self.db_pool.get()?;

        // Try to attach the subject database if it exists
        let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());
        let subject_dir = std::path::Path::new(&data_dir).join(schema);
        let db_path = subject_dir.join(format!("{}.duckdb", schema));

        if db_path.exists() {
            // Try to attach the database
            let attach_sql = format!("ATTACH DATABASE '{}' AS {}", db_path.to_string_lossy(), schema);
            match conn.execute(&attach_sql, []) {
                Ok(_) => debug!("Successfully attached database for subject: {}", schema),
                Err(e) => {
                    // If the database is already attached, that's fine
                    if !e.to_string().contains("already attached") {
                        warn!("Failed to attach database: {}", e);
                        // Continue anyway as it might be a different issue
                    }
                }
            }
        }

        // Try to set the search path to the schema
        let set_search_path = conn.execute(&format!("SET search_path = '{}'", schema), []);
        if let Err(e) = set_search_path {
            warn!("Failed to set search_path: {}", e);
        }

        // Prepare the SQL with schema qualification
        let qualified_sql = self.qualify_sql(sql, schema).await;

        // Use a clone of the qualified SQL to move into the task
        let sql_to_execute = qualified_sql.clone();
        // Clone the schema to avoid lifetime issues
        let schema_clone = schema.to_string();

        // Execute the query in a blocking task
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error + Send + Sync>> {
            // Prepare the statement
            let mut stmt = match conn.prepare(&sql_to_execute) {
                Ok(stmt) => stmt,
                Err(e) => {
                    error!("Failed to prepare statement: {}", e);

                    // Additional diagnostic query to check schema existence
                    match conn.prepare("SELECT schema_name FROM information_schema.schemata") {
                        Ok(mut diag_stmt) => {
                            let schemas: Vec<String> = match diag_stmt.query_map([], |row| row.get::<_, String>(0)) {
                                Ok(rows) => rows.filter_map(Result::ok).collect(),
                                Err(_) => Vec::new(),
                            };
                            debug!("Available schemas: {:?}", schemas);
                        },
                        Err(diag_err) => {
                            debug!("Failed to run schema diagnostic: {}", diag_err);
                        }
                    }

                    // Try to explicitly attach the database again
                    let subject_dir = Path::new("data").join(&schema_clone);
                    let db_path = subject_dir.join(format!("{}.duckdb", schema_clone));

                    if db_path.exists() {
                        debug!("Subject database exists at {}", db_path.display());

                        let attach_sql = format!("ATTACH DATABASE '{}' AS {}", db_path.to_str().unwrap(), schema_clone);
                        match conn.execute(&attach_sql, []) {
                            Ok(_) => {
                                debug!("Explicitly attached database for subject: {}", schema_clone);
                                // Try to prepare the statement again
                                match conn.prepare(&sql_to_execute) {
                                    Ok(new_stmt) => new_stmt,
                                    Err(retry_err) => {
                                        error!("Still failed to prepare statement after explicit attach: {}", retry_err);
                                        return Err(Box::new(retry_err));
                                    }
                                }
                            },
                            Err(attach_err) => {
                                debug!("Failed to explicitly attach database: {}", attach_err);
                                return Err(Box::new(e));
                            }
                        }
                    } else {
                        debug!("Subject database does not exist at {}", db_path.display());
                        return Err(Box::new(e));
                    }
                }
            };

            // Get column names and count
            let column_count = stmt.column_count();
            let mut column_names = Vec::new();

            for i in 0..column_count {
                match stmt.column_name(i) {
                    Ok(name) => column_names.push(name.to_string()),
                    Err(e) => {
                        error!("Failed to get column name for index {}: {}", i, e);
                        column_names.push(format!("column_{}", i));
                    }
                }
            }

            // Execute the query
            let mut rows = match stmt.query([]) {
                Ok(rows) => rows,
                Err(e) => {
                    error!("Failed to execute query: {}", e);
                    return Err(Box::new(e));
                }
            };

            let mut results = Vec::new();

            // Process each row
            while let Some(row) = match rows.next() {
                Ok(row_opt) => row_opt,
                Err(e) => {
                    error!("Error fetching next row: {}", e);
                    return Err(Box::new(e));
                }
            } {
                let mut row_map = HashMap::new();

                for (i, name) in column_names.iter().enumerate() {
                    // Handle different data types using pattern matching
                    let value = match row.get_ref(i) {
                        Ok(val_ref) => match val_ref {
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
                            duckdb::types::ValueRef::Time64(_, v) => v.to_string(),
                            duckdb::types::ValueRef::Timestamp(_, v) => v.to_string(),
                            duckdb::types::ValueRef::Interval { months, days, nanos } =>
                                format!("{}m {}d {}n", months, days, nanos),
                            duckdb::types::ValueRef::Blob(v) => format!("[BLOB: {} bytes]", v.len()),
                            duckdb::types::ValueRef::Text(v) => String::from_utf8_lossy(v).to_string(),
                            duckdb::types::ValueRef::Decimal(v) => v.to_string(),
                            // Catch-all for any other types or future variants
                            _ => "[UNKNOWN TYPE]".to_string(),
                        },
                        Err(e) => {
                            error!("Error getting value for column {}: {}", name, e);
                            "[ERROR]".to_string()
                        }
                    };

                    row_map.insert(name.clone(), value);
                }

                results.push(row_map);
            }

            Ok(results)
        }).await??;

        Ok(result)
    }

    // Rest of the methods remain the same...
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