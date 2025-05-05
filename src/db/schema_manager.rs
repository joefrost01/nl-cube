use crate::db::multi_db_pool::MultiDbConnectionManager;
use duckdb::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// A struct to cache and manage database schema information
pub struct SchemaManager {
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
        conn_manager: Arc<MultiDbConnectionManager>,
        data_dir: PathBuf,
    ) -> Self {
        // Create the schema manager
        let manager = Self {
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
                                    db_path.to_string_lossy().to_string().as_str(),
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
                                        }
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
                                                }
                                                Err(e) => {
                                                    error!("Error preparing SHOW TABLES query: {}", e);
                                                }
                                            }
                                        }
                                    }

                                    debug!("Found {} tables in database {}", tables.len(), db_path.display());

                                    // If we still don't have tables, try a third approach with PRAGMA
                                    if tables.is_empty() {
                                        match conn.prepare("PRAGMA table_info(sqlite_master)") {
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
                                            }
                                            Err(e) => {
                                                error!("Error preparing PRAGMA table_info(sqlite_master) query: {}", e);
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
}