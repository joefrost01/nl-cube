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
use tracing::{debug, error, info};

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
    // src/web/state.rs
    pub async fn get_table_metadata(&self, current_subject: Option<&str>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Use a blocking task to avoid thread-safety issues with DuckDB
        let data_dir = self.data_dir.clone();
        // Clone current_subject to move into the closure
        let subject_filter = current_subject.map(|s| s.to_string());

        // Perform the database query in a blocking task
        let table_metadata = tokio::task::spawn_blocking(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            // Build a more detailed metadata string for the LLM
            let mut metadata = String::from("# DATABASE SCHEMA\n\n");

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
                        // Get a list of tables in this database
                        match conn.prepare("SELECT name FROM sqlite_master WHERE type='table'") {
                            Ok(mut stmt) => {
                                let tables: Vec<String> = stmt.query_map([], |row| row.get::<_, String>(0))
                                    .map_err(|_| "Failed to query tables")?
                                    .filter_map(Result::ok)
                                    .collect();

                                if tables.is_empty() {
                                    metadata.push_str("No tables found in this database.\n\n");
                                    continue;
                                }

                                // For each table, describe its schema
                                for table_name in &tables {
                                    metadata.push_str(&format!("### Table: {}\n\n", table_name));

                                    // Get column information
                                    match conn.prepare(&format!("PRAGMA table_info(\"{}\")", table_name)) {
                                        Ok(mut col_stmt) => {
                                            let columns: Vec<(String, String, bool)> = col_stmt.query_map([], |row| {
                                                Ok((
                                                    row.get::<_, String>(1)?, // name
                                                    row.get::<_, String>(2)?, // type
                                                    row.get::<_, i32>(3)? == 0 // notnull (0 = nullable)
                                                ))
                                            })
                                                .map_err(|_| "Failed to query columns")?
                                                .filter_map(Result::ok)
                                                .collect();

                                            if !columns.is_empty() {
                                                metadata.push_str("| Column Name | Data Type | Nullable |\n");
                                                metadata.push_str("|------------|-----------|----------|\n");

                                                for (name, data_type, nullable) in columns {
                                                    metadata.push_str(&format!("| {} | {} | {} |\n",
                                                                               name,
                                                                               data_type,
                                                                               if nullable { "YES" } else { "NO" }
                                                    ));
                                                }

                                                metadata.push_str("\n");

                                                // Add sample data
                                                metadata.push_str("#### Sample Data:\n\n");
                                                match conn.prepare(&format!("SELECT * FROM \"{}\" LIMIT 3", table_name)) {
                                                    Ok(mut sample_stmt) => {
                                                        let column_count = sample_stmt.column_count();
                                                        let mut column_names = Vec::new();

                                                        // Get column names
                                                        for i in 0..column_count {
                                                            if let Ok(name) = sample_stmt.column_name(i) {
                                                                column_names.push(name.to_string());
                                                            }
                                                        }

                                                        // Add header row
                                                        metadata.push_str("| ");
                                                        for name in &column_names {
                                                            metadata.push_str(&format!("{} | ", name));
                                                        }
                                                        metadata.push_str("\n| ");

                                                        // Add separator row
                                                        for _ in 0..column_names.len() {
                                                            metadata.push_str("--- | ");
                                                        }
                                                        metadata.push_str("\n");

                                                        // Add data rows
                                                        let mut rows = sample_stmt.query([]).map_err(|_| "Failed to query sample data")?;
                                                        while let Some(row) = rows.next().map_err(|_| "Failed to get next row")? {
                                                            metadata.push_str("| ");

                                                            for i in 0..column_count {
                                                                let value = match row.get_ref(i) {
                                                                    Ok(val_ref) => match val_ref {
                                                                        duckdb::types::ValueRef::Null => "NULL".to_string(),
                                                                        _ => match row.get::<_, String>(i) {
                                                                            Ok(v) => v,
                                                                            Err(_) => "ERROR".to_string(),
                                                                        },
                                                                    },
                                                                    Err(_) => "ERROR".to_string(),
                                                                };

                                                                metadata.push_str(&format!("{} | ", value));
                                                            }

                                                            metadata.push_str("\n");
                                                        }

                                                        metadata.push_str("\n");
                                                    },
                                                    Err(_) => {
                                                        metadata.push_str("Could not retrieve sample data.\n\n");
                                                    }
                                                }
                                            } else {
                                                metadata.push_str("Table has no columns.\n\n");
                                            }
                                        },
                                        Err(_) => {
                                            metadata.push_str("Could not retrieve column information.\n\n");
                                        }
                                    }
                                }
                            },
                            Err(_) => {
                                metadata.push_str("Could not query tables in the database.\n\n");
                            }
                        }
                    },
                    Err(_) => {
                        metadata.push_str("Could not open database file.\n\n");
                    }
                }
            }

            Ok(metadata)
        }).await??;

        Ok(table_metadata)
    }

    pub async fn set_search_path(&self, subject: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First make sure the schema exists by creating it if needed
        self.schema_manager.add_schema(subject.to_string()).await?;

        info!("Search path set to schema: {}", subject);
        Ok(())
    }
}