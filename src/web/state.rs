use crate::config::AppConfig;
use crate::db::db_pool::DuckDBConnectionManager;
use crate::llm::LlmManager;
use minijinja::Environment;
use r2d2::Pool;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

/// Shared application state for the web server
pub struct AppState {
    pub config: AppConfig,
    pub db_pool: Pool<DuckDBConnectionManager>,
    pub template_env: Environment<'static>,
    pub llm_manager: Arc<Mutex<LlmManager>>,
    pub data_dir: PathBuf,

    // Cache for subjects only, not schemas
    pub subjects: RwLock<Vec<String>>,
    pub startup_time: chrono::DateTime<chrono::Utc>,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db_pool: Pool<DuckDBConnectionManager>,
        llm_manager: LlmManager,
    ) -> Self {
        // Initialize template environment
        let mut env = Environment::new();

        // In a real app, we would load templates from files
        // env.set_loader(minijinja::loaders::FileSystemLoader::new("templates"));

        // Configure the template environment
        env.add_filter("json", |value: minijinja::value::Value| {
            serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
        });

        Self {
            config: config.clone(),
            db_pool,
            template_env: env,
            llm_manager: Arc::new(Mutex::new(llm_manager)),
            data_dir: PathBuf::from(&config.data_dir),
            subjects: RwLock::new(Vec::new()),
            startup_time: chrono::Utc::now(),
        }
    }

    // Helper to get database schemas DDL directly from the database
    pub async fn get_schemas_ddl(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Use a blocking task to avoid thread-safety issues with DuckDB
        let db_connection_string = self.config.database.connection_string.clone();

        // Perform the database query in a blocking task
        let schemas_ddl = tokio::task::spawn_blocking(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            // Get database connection
            let conn = duckdb::Connection::open(&db_connection_string)?;

            // Get a list of all schemas
            let mut schemas = Vec::<String>::new();
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
    pub async fn get_table_metadata(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Use a blocking task to avoid thread-safety issues with DuckDB
        let db_connection_string = self.config.database.connection_string.clone();

        // Perform the database query in a blocking task
        let table_metadata = tokio::task::spawn_blocking(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            // Get database connection
            let conn = duckdb::Connection::open(&db_connection_string)?;

            // Get a list of all schemas (except system schemas)
            let mut stmt = conn.prepare("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')")?;
            let schema_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let schema_list: Vec<String> = schema_iter.filter_map(Result::ok).collect();

            // Build a more detailed metadata string for the LLM
            let mut metadata = String::from("# DATABASE SCHEMA\n\n");

            for schema_name in &schema_list {
                metadata.push_str(&format!("## Schema: {}\n\n", schema_name));

                // Get tables for this schema
                let mut tables_stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = ?")?;
                let tables_iter = tables_stmt.query_map([schema_name], |row| row.get::<_, String>(0))?;
                let tables: Vec<String> = tables_iter.filter_map(Result::ok).collect();

                for table_name in &tables {
                    metadata.push_str(&format!("### Table: {}\n\n", table_name));

                    // Get column info
                    let column_query = format!(
                        "SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = ? AND table_name = ?
                        ORDER BY ordinal_position"
                    );

                    let mut columns_stmt = conn.prepare(&column_query)?;
                    let columns_iter = columns_stmt.query_map(&[schema_name, table_name], |row| {
                        Ok((
                            row.get::<_, String>(0)?, // column_name
                            row.get::<_, String>(1)?, // data_type
                            row.get::<_, String>(2)? == "YES" // is_nullable
                        ))
                    })?;

                    let columns: Vec<(String, String, bool)> = columns_iter.filter_map(Result::ok).collect();

                    metadata.push_str("| Column Name | Data Type | Nullable |\n");
                    metadata.push_str("|------------|-----------|----------|\n");

                    for (name, data_type, nullable) in columns {
                        let nullable_str = if nullable { "YES" } else { "NO" };
                        metadata.push_str(&format!("| {} | {} | {} |\n", name, data_type, nullable_str));
                    }

                    metadata.push_str("\n");

                    // Get some sample data if available
                    metadata.push_str("#### Sample Data:\n\n");
                    let sample_query = format!("SELECT * FROM \"{}\".\"{}\" LIMIT 3", schema_name, table_name);
                    match conn.prepare(&sample_query) {
                        Ok(mut sample_stmt) => {
                            // First get column names from statement metadata
                            let column_count = sample_stmt.column_count();
                            let mut column_names = Vec::new();

                            for i in 0..column_count {
                                if let Ok(name) = sample_stmt.column_name(i) {
                                    column_names.push(name.to_string());
                                } else {
                                    column_names.push(format!("Column{}", i));
                                }
                            }

                            // Add table header
                            if !column_names.is_empty() {
                                metadata.push_str("| ");
                                for name in &column_names {
                                    metadata.push_str(&format!("{} | ", name));
                                }
                                metadata.push_str("\n| ");

                                for _ in 0..column_names.len() {
                                    metadata.push_str("--- | ");
                                }
                                metadata.push_str("\n");
                            }

                            // Now fetch rows and values
                            let rows = match sample_stmt.query_map([], |row| {
                                let mut row_data = Vec::new();
                                for i in 0..column_count {
                                    let value: Result<String, _> = row.get(i);
                                    match value {
                                        Ok(v) => row_data.push(v),
                                        Err(_) => row_data.push("NULL".to_string()),
                                    }
                                }
                                Ok(row_data)
                            }) {
                                Ok(mapped_rows) => {
                                    let mut rows_data = Vec::new();
                                    for row_result in mapped_rows {
                                        if let Ok(row_data) = row_result {
                                            rows_data.push(row_data);
                                        }
                                    }
                                    rows_data
                                },
                                Err(e) => {
                                    metadata.push_str(&format!("Error fetching sample data: {}\n", e));
                                    Vec::new()
                                }
                            };

                            // Render rows
                            for row_data in rows {
                                metadata.push_str("| ");
                                for cell in row_data {
                                    metadata.push_str(&format!("{} | ", cell));
                                }
                                metadata.push_str("\n");
                            }
                        },
                        Err(e) => {
                            metadata.push_str(&format!("Could not get sample data: {}\n", e));
                        }
                    }

                    metadata.push_str("\n");
                }
            }

            Ok(metadata)
        }).await??;

        Ok(table_metadata)
    }

    // Refreshes available subjects (data directories)
    pub async fn refresh_subjects(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First, scan the data directory for subject folders
        let mut subjects = Vec::new();
        let entries = tokio::fs::read_dir(&self.data_dir).await?;

        tokio::pin!(entries);

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    subjects.push(name.to_string());
                }
            }
        }

        // Then update the subjects with a single async operation
        let mut subjects_lock = self.subjects.write().await;
        *subjects_lock = subjects;

        Ok(())
    }

    // Set the search path for a subject before querying
    pub async fn set_search_path(&self, subject: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let subject = subject.to_string();

        // Use a blocking task to avoid thread-safety issues with DuckDB
        tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // Create schemas if they don't exist to avoid errors
            let conn = duckdb::Connection::open("nl-cube.db")?;

            // Create the schema if it doesn't exist
            let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", subject);
            conn.execute(&create_schema_sql, [])?;

            // Set the search path
            let search_path_sql = format!("SET search_path = '{}', 'main'", subject);
            conn.execute(&search_path_sql, [])?;

            Ok(())
        }).await??;

        Ok(())
    }
}