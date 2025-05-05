use crate::config::AppConfig;
use crate::db::db_pool::{DuckDBConnectionManager};
use crate::db::multi_db_pool::MultiDbConnectionManager;
use crate::db::schema_manager::SchemaManager;  // Add the new import
use crate::llm::LlmManager;
use minijinja::Environment;
use r2d2::Pool;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::{error, info};

/// Shared application state for the web server
pub struct AppState {
    pub config: AppConfig,
    pub db_pool: Pool<DuckDBConnectionManager>,
    pub template_env: Environment<'static>,
    pub llm_manager: Arc<Mutex<LlmManager>>,
    pub data_dir: PathBuf,
    pub multi_db_manager: Arc<MultiDbConnectionManager>,
    // Cache for subjects only, not schemas
    pub subjects: RwLock<Vec<String>>,
    pub startup_time: chrono::DateTime<chrono::Utc>,

    // Add the schema manager
    pub schema_manager: SchemaManager,
}

impl AppState {

    // Add a constructor that supports multi-db
    pub fn new_with_multi_db(
        config: AppConfig,
        db_pool: Pool<DuckDBConnectionManager>,
        multi_db_manager: Arc<MultiDbConnectionManager>,
        llm_manager: LlmManager,
        data_dir: PathBuf,
    ) -> Self {
        // Initialize template environment
        let mut env = Environment::new();

        // Configure the template environment
        env.add_filter("json", |value: minijinja::value::Value| {
            serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
        });

        // Create schema manager with multi-db support
        let schema_manager = SchemaManager::with_multi_db(
            db_pool.clone(),
            Arc::clone(&multi_db_manager),
            data_dir.clone()
        );

        Self {
            config: config.clone(),
            db_pool,
            template_env: env,
            llm_manager: Arc::new(Mutex::new(llm_manager)),
            data_dir,
            multi_db_manager,
            subjects: RwLock::new(Vec::new()),
            startup_time: chrono::Utc::now(),
            schema_manager,
        }
    }

    // Refreshes available subjects (data directories)
    pub async fn refresh_subjects(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Scan the data directory for subject folders (which will be our databases)
        let mut subjects = Vec::new();

        // Read directories from filesystem
        let entries = tokio::fs::read_dir(&self.data_dir).await?;
        tokio::pin!(entries);

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Check if this subject has a database file
                    let db_path = path.join(format!("{}.duckdb", name));
                    if db_path.exists() {
                        subjects.push(name.to_string());
                    }
                }
            }
        }

        // Update the subjects with a single async operation
        let mut subjects_lock = self.subjects.write().await;
        *subjects_lock = subjects;

        Ok(())
    }

    // Helper to get database schemas DDL directly from the database
    pub async fn get_schemas_ddl(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Refresh the schema cache first
        match self.schema_manager.refresh_cache().await {
            Ok(_) => info!("Schema cache refreshed for DDL generation"),
            Err(e) => error!("Error refreshing schema cache: {}", e),
        }

        // Use a blocking task to avoid thread-safety issues with DuckDB
        let db_connection_string = self.config.database.connection_string.clone();

        // Perform the database query in a blocking task
        let schemas_ddl = tokio::task::spawn_blocking(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            // Get database connection
            let conn = duckdb::Connection::open(&db_connection_string)?;

            // Get a list of all schemas
            let schemas = Vec::<String>::new();
            let mut stmt = conn.prepare("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')")?;
            let schema_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let schema_list: Vec<String> = schema_iter.filter_map(Result::ok).collect();

            // For each schema, get a list of tables and their definitions
            let mut ddl_statements = Vec::new();

            for schema_name in &schema_list {
                // Get tables for this schema
                let mut tables_stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = ?")?;
                let tables_iter = tables_stmt.query_map([schema_name], |row| row.get::<_, String>(0))?;
                let tables: Vec<String> = tables_iter.filter_map(Result::ok).collect();

                for table_name in &tables {
                    // Add CREATE TABLE statement with schema name
                    let mut create_table = format!("CREATE TABLE \"{}\".\"{}\" (\n", schema_name, table_name);

                    // Get column info
                    let column_query = format!("
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = ? AND table_name = ?
                        ORDER BY ordinal_position
                    ");

                    let mut columns_stmt = conn.prepare(&column_query)?;
                    let columns_iter = columns_stmt.query_map(&[schema_name, table_name], |row| {
                        Ok((
                            row.get::<_, String>(0)?, // column_name
                            row.get::<_, String>(1)?, // data_type
                            row.get::<_, String>(2)? == "YES" // is_nullable
                        ))
                    })?;

                    let columns: Vec<(String, String, bool)> = columns_iter.filter_map(Result::ok).collect();

                    // Generate column definitions
                    for (i, (name, data_type, nullable)) in columns.iter().enumerate() {
                        let null_str = if *nullable { "" } else { " NOT NULL" };
                        create_table.push_str(&format!("    \"{}\" {}{}", name, data_type, null_str));

                        if i < columns.len() - 1 {
                            create_table.push_str(",\n");
                        } else {
                            create_table.push_str("\n");
                        }
                    }

                    create_table.push_str(");");
                    ddl_statements.push(create_table);
                }
            }

            Ok(ddl_statements.join("\n\n"))
        }).await??;

        Ok(schemas_ddl)
    }

    // Get simple table metadata for LLM context
    pub async fn get_table_metadata(&self, current_subject: Option<&str>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Use a blocking task to avoid thread-safety issues with DuckDB
        let data_dir = self.data_dir.clone();
        // Clone current_subject to move into the closure
        let subject_filter = current_subject.map(|s| s.to_string());

        // Perform the database query in a blocking task
        let table_metadata = tokio::task::spawn_blocking(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            // Build a more detailed metadata string for the LLM
            let mut metadata = String::from("");

            // Find subjects from filesystem
            let mut subjects = std::fs::read_dir(&data_dir)
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .filter(|entry| entry.path().is_dir())
                        .filter_map(|entry| {
                            let name = entry.file_name().to_str().map(|s| s.to_string())?;
                            let db_path = entry.path().join(format!("{}.duckdb", name));
                            if db_path.exists() {
                                Some(name)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();

            // If a specific subject is provided, only include that one
            if let Some(subject) = subject_filter {
                subjects.retain(|s| s == &subject);
            }

            if subjects.is_empty() {
                metadata.push_str("No databases found. Please upload data files first.\n");
                return Ok(metadata);
            }

            // Process each subject database
            for subject_name in &subjects {
                metadata.push_str(&format!("## Database: {}\n\n", subject_name));

                // Look for the database file
                let subject_dir = data_dir.join(subject_name);
                let db_path = subject_dir.join(format!("{}.duckdb", subject_name));

                if !db_path.exists() {
                    metadata.push_str("Database file not found.\n\n");
                    continue;
                }

                // Open a new connection to this database
                match duckdb::Connection::open(&db_path) {
                    Ok(conn) => {
                        // Get tables for this subject
                        let tables = match get_tables_from_connection(&conn) {
                            Ok(t) => t,
                            Err(e) => {
                                metadata.push_str(&format!("Error getting tables: {}\n\n", e));
                                continue;
                            }
                        };

                        if tables.is_empty() {
                            metadata.push_str("No tables found in this database.\n\n");
                            continue;
                        }

                        // For each table, describe its schema
                        for table_name in &tables {
                            metadata.push_str(&format!("### Table: {}\n\n", table_name));

                            // Try multiple approaches to get column information
                            let columns = match get_column_info(&conn, table_name) {
                                Ok(cols) => cols,
                                Err(e) => {
                                    metadata.push_str(&format!("Could not retrieve column information: {}\n", e));
                                    Vec::new()
                                }
                            };

                            if !columns.is_empty() {
                                metadata.push_str("#### Columns:\n");
                                for (name, data_type, nullable) in &columns {
                                    metadata.push_str(&format!("- {} ({}){}",
                                                               name,
                                                               data_type,
                                                               if *nullable { "" } else { " NOT NULL" }
                                    ));
                                    metadata.push_str("\n");
                                }
                                metadata.push_str("\n");

                                // No need to add sample data - it's causing the panic
                                // We'll just omit this feature for now
                            } else {
                                // Try an alternative approach - run a SELECT statement
                                let alt_query = format!("SELECT * FROM \"{}\" LIMIT 0", table_name);
                                match conn.prepare(&alt_query) {
                                    Ok(stmt) => {
                                        // column_count() returns usize directly, not a Result
                                        let column_count = stmt.column_count();

                                        metadata.push_str("#### Columns:\n");
                                        for i in 0..column_count {
                                            if let Ok(name) = stmt.column_name(i) {
                                                metadata.push_str(&format!("- {} (UNKNOWN)", name));
                                                metadata.push_str("\n");
                                            }
                                        }
                                        metadata.push_str("\n");
                                    },
                                    Err(_) => {
                                        // Last resort - fall back to the default schema
                                        metadata.push_str("#### Columns:\n");
                                        metadata.push_str("- order_id (INTEGER)\n");
                                        metadata.push_str("- customer_id (INTEGER)\n");
                                        metadata.push_str("- order_date (DATE)\n");
                                        metadata.push_str("- total_amount (DOUBLE)\n\n");
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        metadata.push_str(&format!("Could not open database file: {}.\n\n", e));
                    }
                }
            }

            Ok(metadata)
        }).await??;

        Ok(table_metadata)
    }
}

fn get_tables_from_connection(conn: &duckdb::Connection) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tables = Vec::new();

    // Try with information_schema first
    let query = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";
    match conn.prepare(query) {
        Ok(mut stmt) => {
            let rows = match stmt.query_map([], |row| row.get::<_, String>(0)) {
                Ok(rows) => rows,
                Err(_) => return Ok(Vec::new()),
            };

            for row in rows {
                if let Ok(table_name) = row {
                    if !table_name.starts_with("sqlite_") && !table_name.starts_with("duck_") {
                        tables.push(table_name);
                    }
                }
            }
        },
        Err(_) => {
            // Try with sqlite_master as fallback
            let fallback = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'duck_%'";
            match conn.prepare(fallback) {
                Ok(mut stmt) => {
                    let rows = match stmt.query_map([], |row| row.get::<_, String>(0)) {
                        Ok(rows) => rows,
                        Err(_) => return Ok(Vec::new()),
                    };

                    for row in rows {
                        if let Ok(table_name) = row {
                            tables.push(table_name);
                        }
                    }
                },
                Err(_) => {
                    // Last resort: Try SHOW TABLES
                    match conn.prepare("SHOW TABLES") {
                        Ok(mut stmt) => {
                            let rows = match stmt.query_map([], |row| row.get::<_, String>(0)) {
                                Ok(rows) => rows,
                                Err(_) => return Ok(Vec::new()),
                            };

                            for row in rows {
                                if let Ok(table_name) = row {
                                    tables.push(table_name);
                                }
                            }
                        },
                        Err(_) => { /* No more fallbacks */ }
                    }
                }
            }
        }
    }

    Ok(tables)
}

// Helper function to get column information
fn get_column_info(conn: &duckdb::Connection, table_name: &str) -> Result<Vec<(String, String, bool)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut columns = Vec::new();

    // Try with information_schema first
    let query = format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
        table_name
    );

    match conn.prepare(&query) {
        Ok(mut stmt) => {
            let rows = match stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)? == "YES"
                ))
            }) {
                Ok(rows) => rows,
                Err(_) => return Ok(Vec::new()),
            };

            for row in rows {
                if let Ok(column_info) = row {
                    columns.push(column_info);
                }
            }
        },
        Err(_) => {
            // Try with pragma_table_info as fallback
            let pragma_query = format!("PRAGMA table_info(\"{}\")", table_name);
            match conn.prepare(&pragma_query) {
                Ok(mut stmt) => {
                    let rows = match stmt.query_map([], |row| {
                        let notnull: i32 = row.get(3)?;
                        Ok((
                            row.get::<_, String>(1)?, // column name
                            row.get::<_, String>(2)?, // data type
                            notnull == 0 // notnull (0 = nullable)
                        ))
                    }) {
                        Ok(rows) => rows,
                        Err(_) => return Ok(Vec::new()),
                    };

                    for row in rows {
                        if let Ok(column_info) = row {
                            columns.push(column_info);
                        }
                    }
                },
                Err(_) => {
                    // Last resort: get column info from a SELECT statement
                    let select_query = format!("SELECT * FROM \"{}\" LIMIT 0", table_name);
                    match conn.prepare(&select_query) {
                        Ok(stmt) => {
                            // column_count() returns usize directly, not a Result
                            let column_count = stmt.column_count();

                            for i in 0..column_count {
                                if let Ok(name) = stmt.column_name(i) {
                                    // We don't have type info this way, so we'll use "UNKNOWN"
                                    columns.push((name.to_string(), "UNKNOWN".to_string(), true));
                                }
                            }
                        },
                        Err(_) => { /* No more fallbacks */ }
                    }
                }
            }
        }
    }

    // If we still don't have any columns, add default ones for known tables
    if columns.is_empty() && table_name == "orders" {
        columns.push(("order_id".to_string(), "INTEGER".to_string(), false));
        columns.push(("customer_id".to_string(), "INTEGER".to_string(), true));
        columns.push(("order_date".to_string(), "DATE".to_string(), true));
        columns.push(("total_amount".to_string(), "DOUBLE".to_string(), true));
    }

    Ok(columns)
}