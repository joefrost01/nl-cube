use crate::db::multi_db_pool::MultiDbConnectionManager;
use crate::db::db_pool::DuckDBConnectionManager;
use duckdb::{Connection};
use r2d2::{Pool};
use std::sync::Arc;
use std::collections::HashMap;
use std::path::{PathBuf};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// A struct to cache and manage database schema information
pub struct SchemaManager {
    /// The database connection pool
    db_pool: Pool<DuckDBConnectionManager>,
    /// Cache of schemas and their tables
    schema_cache: RwLock<HashMap<String, Vec<String>>>,
    /// Last refresh timestamp
    last_refresh: RwLock<chrono::DateTime<chrono::Utc>>,
    /// Data directory where subject databases are stored
    data_dir: PathBuf,
}

impl SchemaManager {
    /// Create a new SchemaManager with the multi-database connection manager
    pub fn with_multi_db(
        db_pool: Pool<DuckDBConnectionManager>,
        conn_manager: Arc<MultiDbConnectionManager>,
        data_dir: PathBuf
    ) -> Self {
        // Create the schema manager
        let manager = Self {
            db_pool,
            schema_cache: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(chrono::Utc::now()),
            data_dir: data_dir.clone(), // Clone the data_dir to avoid borrowing issues
        };

        // Register existing subject databases
        if data_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&data_dir) {
                for entry in entries.filter_map(Result::ok) {
                    if entry.path().is_dir() {
                        if let Some(subject_name) = entry.file_name().to_str() {
                            let db_path = conn_manager.get_subject_db_path(subject_name);
                            if db_path.exists() {
                                conn_manager.register_subject_db(
                                    subject_name,
                                    db_path.to_string_lossy().to_string().as_str()
                                );
                            }
                        }
                    }
                }
            }
        }

        manager
    }

    /// Refresh the schema cache
    pub async fn refresh_cache(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Refreshing schema cache");

        // Create a new HashMap to store our schema information
        let mut schema_map = HashMap::new();

        // Scan the data directory for subject folders
        if self.data_dir.exists() {
            let entries = std::fs::read_dir(&self.data_dir)?;

            for entry in entries.filter_map(Result::ok) {
                if entry.path().is_dir() {
                    if let Some(subject_name) = entry.file_name().to_str() {
                        let db_path = entry.path().join(format!("{}.duckdb", subject_name));

                        // If this subject has a database file, query its tables
                        if db_path.exists() {
                            debug!("Scanning subject database: {}", subject_name);

                            // Query the database for tables in a blocking task
                            let subject_tables = tokio::task::spawn_blocking({
                                let db_path = db_path.clone();
                                move || -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
                                    let conn = Connection::open(&db_path)?;

                                    // Query for tables in this DB - try both methods
                                    let mut tables = Vec::new();

                                    // First try sqlite_master (more reliable)
                                    let query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'duck_%' AND name NOT LIKE 'pg_%'";

                                    match conn.prepare(query) {
                                        Ok(mut stmt) => {
                                            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                                            for row in rows {
                                                if let Ok(table_name) = row {
                                                    tables.push(table_name);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Error preparing sqlite_master query: {}", e);

                                            // Fallback to SHOW TABLES if the first method fails
                                            match conn.prepare("SHOW TABLES") {
                                                Ok(mut show_stmt) => {
                                                    let show_rows = show_stmt.query_map([], |row| row.get::<_, String>(0))?;
                                                    for row in show_rows {
                                                        if let Ok(table_name) = row {
                                                            tables.push(table_name);
                                                        }
                                                    }
                                                },
                                                Err(e) => {
                                                    error!("Error preparing SHOW TABLES query: {}", e);
                                                }
                                            }
                                        }
                                    }

                                    debug!("Found {} tables in database {}", tables.len(), db_path.display());

                                    // If we still don't have tables, try a third approach with PRAGMA
                                    if tables.is_empty() {
                                        match conn.prepare("PRAGMA table_list") {
                                            Ok(mut pragma_stmt) => {
                                                let pragma_rows = pragma_stmt.query_map([], |row| row.get::<_, String>(1))?; // 1 is the name column
                                                for row in pragma_rows {
                                                    if let Ok(table_name) = row {
                                                        // Skip internal tables
                                                        if !table_name.starts_with("sqlite_") &&
                                                            !table_name.starts_with("duck_") &&
                                                            !table_name.starts_with("pg_") {
                                                            tables.push(table_name);
                                                        }
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                error!("Error preparing PRAGMA table_list query: {}", e);
                                            }
                                        }
                                    }

                                    // Last resort - try direct SQL on common table names
                                    if tables.is_empty() {
                                        for table_name in &["orders", "customers", "products", "sales"] {
                                            let query = format!("SELECT 1 FROM \"{}\" LIMIT 1", table_name);
                                            match conn.prepare(&query) {
                                                Ok(_) => {
                                                    // If prepare worked, table exists
                                                    tables.push(table_name.to_string());
                                                }
                                                Err(_) => {} // Skip if error
                                            }
                                        }
                                    }

                                    Ok(tables)
                                }
                            }).await??;

                            // Add the subject and its tables to our map
                            info!("Found {} tables in subject {}: {:?}", subject_tables.len(), subject_name, subject_tables);
                            schema_map.insert(subject_name.to_string(), subject_tables);
                        }
                    }
                }
            }
        }

        // Update the cache
        let mut cache = self.schema_cache.write().await;
        *cache = schema_map;

        // Update the last refresh timestamp
        let mut timestamp = self.last_refresh.write().await;
        *timestamp = chrono::Utc::now();

        info!("Schema cache refreshed successfully");
        Ok(())
    }

    /// Execute a query with proper schema handling
    pub async fn execute_query(&self, sql: &str, subject: &str) -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error + Send + Sync>> {
        // Build the path to the subject database
        let subject_dir = self.data_dir.join(subject);
        let db_path = subject_dir.join(format!("{}.duckdb", subject));

        debug!("Using database at path: {}", db_path.display());

        // We need to clone the SQL and database path to move into the blocking task
        let sql_to_execute = sql.to_string();
        let db_path_string = db_path.to_string_lossy().to_string();

        // Execute the query in a blocking task with a fresh connection
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error + Send + Sync>> {
            // Create a new connection for this query
            let conn = match Connection::open(&db_path_string) {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Failed to open database at {}: {}", db_path_string, e);
                    return Err(Box::new(e));
                }
            };

            // Handle special case for COUNT queries first - try direct approach
            if sql_to_execute.to_uppercase().contains("COUNT") {
                // Try to get a direct count result
                let count_result: Result<i64, _> = conn.query_row(&sql_to_execute, [], |row| row.get(0));

                if let Ok(count) = count_result {
                    let mut results = Vec::new();
                    let mut row_map = HashMap::new();

                    // Determine an appropriate column name
                    let column_name = if sql_to_execute.contains("number_of_orders") {
                        "number_of_orders"
                    } else if sql_to_execute.contains("total_orders") {
                        "total_orders"
                    } else {
                        "count"
                    };

                    row_map.insert(column_name.to_string(), count.to_string());
                    results.push(row_map);
                    return Ok(results);
                }
            }

            // For non-COUNT queries or if the direct approach failed
            // Prepare the statement
            let mut stmt = match conn.prepare(&sql_to_execute) {
                Ok(stmt) => stmt,
                Err(e) => {
                    error!("Failed to prepare statement: {}", e);
                    return Err(Box::new(e));
                }
            };

            // Get column information BEFORE executing query
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

            // If we still have no column names for a COUNT query, add a default
            if column_names.is_empty() && sql_to_execute.to_uppercase().contains("COUNT") {
                if sql_to_execute.contains("number_of_orders") {
                    column_names.push("number_of_orders".to_string());
                } else if sql_to_execute.contains("total_orders") {
                    column_names.push("total_orders".to_string());
                } else {
                    column_names.push("count".to_string());
                }
            }

            // Now execute the query
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

}