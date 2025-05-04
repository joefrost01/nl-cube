use crate::config::AppConfig;
use crate::db::db_pool::DuckDBConnectionManager;
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

    // Cache for subjects only, not schemas
    pub subjects: RwLock<Vec<String>>,
    pub startup_time: chrono::DateTime<chrono::Utc>,

    // Add the schema manager
    pub schema_manager: SchemaManager,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db_pool: Pool<DuckDBConnectionManager>,
        llm_manager: LlmManager,
    ) -> Self {
        // Initialize template environment
        let mut env = Environment::new();

        // Configure the template environment
        env.add_filter("json", |value: minijinja::value::Value| {
            serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
        });

        // Create standard schema manager since we're not using the MultiDb approach
        // in this part of the code yet
        let schema_manager = SchemaManager::new(db_pool.clone());

        Self {
            config: config.clone(),
            db_pool,
            template_env: env,
            llm_manager: Arc::new(Mutex::new(llm_manager)),
            data_dir: PathBuf::from(&config.data_dir),
            subjects: RwLock::new(Vec::new()),
            startup_time: chrono::Utc::now(),
            schema_manager,
        }
    }

    // Refreshes available subjects (data directories)
    pub async fn refresh_subjects(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First, scan the data directory for subject folders
        let mut subjects = Vec::new();

        // Then try to use the schema manager to get schemas (more reliable)
        match self.schema_manager.refresh_cache().await {
            Ok(_) => {
                // Get schemas from the schema manager
                subjects = self.schema_manager.get_schemas().await;
                debug!("Found {} subjects from schema manager", subjects.len());
            },
            Err(e) => {
                error!("Failed to refresh schema cache: {}", e);

                // Fall back to filesystem check
                info!("Falling back to filesystem subject detection");
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
        let db_connection_string = self.config.database.connection_string.clone();
        let data_dir = self.data_dir.clone();
        // Clone current_subject to move into the closure
        let subject_filter = current_subject.map(|s| s.to_string());

        // Perform the database query in a blocking task
        let table_metadata = tokio::task::spawn_blocking(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            // Get database connection
            let conn = duckdb::Connection::open(&db_connection_string)?;

            // Build a more detailed metadata string for the LLM
            let mut metadata = String::from("# DATABASE SCHEMA\n\n");

            // Find schemas from filesystem since this is most reliable
            let mut schemas = std::fs::read_dir(&data_dir)
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .filter(|entry| entry.path().is_dir())
                        .filter_map(|entry| {
                            entry.file_name().to_str().map(|s| s.to_string())
                        })
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();

            // If a specific subject is provided, only include that one
            if let Some(subject) = subject_filter {
                schemas.retain(|s| s == &subject);
            }

            if schemas.is_empty() {
                metadata.push_str("No schemas found in database. Please upload data files first.\n");
                return Ok(metadata);
            }

            // Process each schema
            for schema_name in &schemas {
                metadata.push_str(&format!("## Schema: {}\n\n", schema_name));

                // Look for CSV files in the data directory to know what tables should exist
                let schema_dir = data_dir.join(schema_name);
                if !schema_dir.exists() || !schema_dir.is_dir() {
                    metadata.push_str("Schema folder not found on filesystem.\n\n");
                    continue;
                }

                let csv_files = match std::fs::read_dir(&schema_dir) {
                    Ok(entries) => {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                if let Some(ext) = entry.path().extension() {
                                    ext.to_string_lossy().to_lowercase() == "csv"
                                } else {
                                    false
                                }
                            })
                            .map(|entry| {
                                entry.path().file_stem()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or("unknown")
                                    .to_string()
                            })
                            .collect::<Vec<String>>()
                    },
                    Err(_) => Vec::new()
                };

                if csv_files.is_empty() {
                    metadata.push_str("No CSV files found in this schema.\n\n");
                    continue;
                }

                // For each CSV file, describe it as a table
                for table_name in &csv_files {
                    metadata.push_str(&format!("### Table: {}\n\n", table_name));

                    // Try to infer schema from CSV file
                    let csv_path = schema_dir.join(format!("{}.csv", table_name));

                    match std::fs::File::open(&csv_path) {
                        Ok(file) => {
                            let mut reader = std::io::BufReader::new(file);
                            let mut header_line = String::new();
                            let mut data_line = String::new();

                            // Read header
                            if std::io::BufRead::read_line(&mut reader, &mut header_line).is_ok() && !header_line.is_empty() {
                                // Read first data row
                                if std::io::BufRead::read_line(&mut reader, &mut data_line).is_ok() && !data_line.is_empty() {
                                    // Parse header
                                    let headers: Vec<String> = header_line.trim()
                                        .split(',')
                                        .map(|s| s.trim().trim_matches('"').to_string())
                                        .collect();

                                    // Parse data row to infer types
                                    let data_items: Vec<String> = data_line.trim()
                                        .split(',')
                                        .map(|s| s.trim().trim_matches('"').to_string())
                                        .collect();

                                    if !headers.is_empty() {
                                        metadata.push_str("| Column Name | Data Type | Nullable |\n");
                                        metadata.push_str("|------------|-----------|----------|\n");

                                        for (i, header) in headers.iter().enumerate() {
                                            let data_type = if i < data_items.len() {
                                                // Very basic type inference
                                                let value = &data_items[i];

                                                if value.parse::<i64>().is_ok() {
                                                    "INTEGER".to_string()
                                                } else if value.parse::<f64>().is_ok() {
                                                    "DOUBLE".to_string()
                                                } else if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                                                    "BOOLEAN".to_string()
                                                } else if value.contains('-') && value.len() == 10 &&
                                                    value.chars().filter(|&c| c == '-').count() == 2 {
                                                    // Simple date check (YYYY-MM-DD)
                                                    "DATE".to_string()
                                                } else {
                                                    "VARCHAR".to_string()
                                                }
                                            } else {
                                                "VARCHAR".to_string()
                                            };

                                            metadata.push_str(&format!("| {} | {} | YES |\n", header, data_type));
                                        }

                                        metadata.push_str("\n");

                                        // Add some sample data
                                        metadata.push_str("#### Sample Data:\n\n");

                                        // Add header row
                                        metadata.push_str("| ");
                                        for header in &headers {
                                            metadata.push_str(&format!("{} | ", header));
                                        }
                                        metadata.push_str("\n| ");

                                        for _ in 0..headers.len() {
                                            metadata.push_str("--- | ");
                                        }
                                        metadata.push_str("\n");

                                        // Add first data row
                                        metadata.push_str("| ");
                                        for (i, value) in data_items.iter().enumerate() {
                                            if i < headers.len() {
                                                metadata.push_str(&format!("{} | ", value));
                                            }
                                        }
                                        metadata.push_str("\n");

                                        // Try to read a couple more rows
                                        for _ in 0..2 {
                                            let mut line = String::new();
                                            if std::io::BufRead::read_line(&mut reader, &mut line).is_ok() && !line.is_empty() {
                                                let items: Vec<String> = line.trim()
                                                    .split(',')
                                                    .map(|s| s.trim().trim_matches('"').to_string())
                                                    .collect();

                                                if !items.is_empty() {
                                                    metadata.push_str("| ");
                                                    for (i, value) in items.iter().enumerate() {
                                                        if i < headers.len() {
                                                            metadata.push_str(&format!("{} | ", value));
                                                        }
                                                    }
                                                    metadata.push_str("\n");
                                                }
                                            }
                                        }

                                        metadata.push_str("\n");
                                    } else {
                                        metadata.push_str("CSV file has no headers.\n\n");
                                    }
                                } else {
                                    metadata.push_str("CSV file has no data rows.\n\n");
                                }
                            } else {
                                metadata.push_str("CSV file is empty.\n\n");
                            }
                        },
                        Err(_) => {
                            metadata.push_str("Could not open CSV file.\n\n");
                        }
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