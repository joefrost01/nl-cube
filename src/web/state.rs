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

    // Cache for schemas and other dynamic data
    pub schemas: RwLock<Vec<String>>,
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
            schemas: RwLock::new(Vec::new()),
            subjects: RwLock::new(Vec::new()),
            startup_time: chrono::Utc::now(),
        }
    }

    // Helper to get database schemas as a string
    pub async fn get_schemas_ddl(&self) -> String {
        let schemas = self.schemas.read().await;
        schemas.join("\n\n")
    }

    // Refreshes available schemas from the database
    pub async fn refresh_schemas(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Use a blocking task to avoid thread-safety issues with DuckDB
        let db_connection_string = self.config.database.connection_string.clone();

        // Perform the database query in a blocking task
        let schemas = tokio::task::spawn_blocking(move || -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
            // Get database connection
            let conn = duckdb::Connection::open(&db_connection_string)?;
            let mut schemas = Vec::new();

            // Try multiple approaches to get tables
            // First try with SHOW TABLES which works well with DuckDB
            let mut tables = Vec::new();

            match conn.prepare("SHOW TABLES") {
                Ok(mut stmt) => {
                    let table_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
                    tables = table_iter.filter_map(Result::ok).collect();
                    tracing::info!("Found {} tables using SHOW TABLES: {:?}", tables.len(), tables);
                },
                Err(e) => {
                    tracing::warn!("SHOW TABLES failed: {}", e);

                    // Fall back to sqlite_master
                    match conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'") {
                        Ok(mut stmt) => {
                            let table_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
                            tables = table_iter.filter_map(Result::ok).collect();
                            tracing::info!("Found {} tables using sqlite_master: {:?}", tables.len(), tables);
                        },
                        Err(e) => {
                            tracing::error!("Failed to get tables from sqlite_master: {}", e);
                            return Ok(Vec::new());
                        }
                    }
                }
            }

            // Process each table to build schema DDL
            for table in tables {
                tracing::info!("Processing table schema for: {}", table);

                // Get column info using a direct query
                let mut create_table = format!("CREATE TABLE \"{}\" (\n", table);
                let columns = match conn.prepare(&format!("PRAGMA table_info(\"{}\")", table)) {
                    Ok(mut stmt) => {
                        let cols = stmt.query_map([], |row| {
                            Ok((
                                row.get::<_, String>(1)?, // name
                                row.get::<_, String>(2)?, // type
                                row.get::<_, i32>(3)? != 0 // notnull
                            ))
                        })?;

                        cols.filter_map(Result::ok).collect::<Vec<_>>()
                    },
                    Err(e) => {
                        tracing::warn!("Failed to get column info for {}: {}", table, e);

                        // Fallback to SELECT * LIMIT 0 to at least get column names
                        match conn.prepare(&format!("SELECT * FROM \"{}\" LIMIT 0", table)) {
                            Ok(stmt) => {
                                let column_count = stmt.column_count();
                                let mut cols = Vec::new();

                                for i in 0..column_count {
                                    // Fix: column_name returns a Result, not an Option
                                    match stmt.column_name(i) {
                                        Ok(name) => {
                                            // Default all types to VARCHAR when we can't determine
                                            cols.push((name.to_string(), "VARCHAR".to_string(), false));
                                        },
                                        Err(e) => {
                                            tracing::warn!("Error getting column name at index {}: {}", i, e);
                                            // Use a placeholder name for this column
                                            cols.push((format!("column_{}", i), "VARCHAR".to_string(), false));
                                        }
                                    }
                                }

                                cols
                            },
                            Err(e) => {
                                tracing::error!("Failed to get any column info for {}: {}", table, e);
                                // Create a placeholder column
                                vec![("id".to_string(), "INTEGER".to_string(), false)]
                            }
                        }
                    }
                };

                // Generate column definitions
                for (i, (name, data_type, not_null)) in columns.iter().enumerate() {
                    let null_str = if *not_null { " NOT NULL" } else { "" };
                    create_table.push_str(&format!("    \"{}\" {}{}", name, data_type, null_str));

                    if i < columns.len() - 1 {
                        create_table.push_str(",\n");
                    } else {
                        create_table.push_str("\n");
                    }
                }

                create_table.push_str(");");
                schemas.push(create_table);

                tracing::info!("Added schema for table {}: {} columns", table, columns.len());
            }

            Ok(schemas)
        }).await??;

        // Now update the schemas with a single async operation
        let mut schemas_lock = self.schemas.write().await;
        *schemas_lock = schemas;

        tracing::info!("Schema refresh complete, found {} schemas", schemas_lock.len());

        Ok(())
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

    pub async fn diagnose_db(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get database connection from the pool
        let conn = self.db_pool.get()?;

        tracing::info!("Running database diagnostic...");

        // Try various ways to list tables
        tracing::info!("Trying sqlite_master:");
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: Result<Vec<String>, _> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect();

        match tables {
            Ok(tables) => {
                tracing::info!("Found {} tables in sqlite_master: {:?}", tables.len(), tables);
            },
            Err(e) => {
                tracing::error!("Error querying sqlite_master: {}", e);
            }
        }

        // Try SHOW TABLES
        tracing::info!("Trying SHOW TABLES:");
        match conn.prepare("SHOW TABLES") {
            Ok(mut stmt) => {
                let tables: Result<Vec<String>, _> = stmt
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect();

                match tables {
                    Ok(tables) => {
                        tracing::info!("Found {} tables with SHOW TABLES: {:?}", tables.len(), tables);
                    },
                    Err(e) => {
                        tracing::error!("Error executing SHOW TABLES: {}", e);
                    }
                }
            },
            Err(e) => {
                tracing::error!("Error preparing SHOW TABLES: {}", e);
            }
        }

        // Try information_schema
        tracing::info!("Trying information_schema:");
        match conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'") {
            Ok(mut stmt) => {
                let tables: Result<Vec<String>, _> = stmt
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect();

                match tables {
                    Ok(tables) => {
                        tracing::info!("Found {} tables in information_schema: {:?}", tables.len(), tables);
                    },
                    Err(e) => {
                        tracing::error!("Error querying information_schema: {}", e);
                    }
                }
            },
            Err(e) => {
                tracing::error!("Error preparing information_schema query: {}", e);
            }
        }

        // Get connection string from config
        tracing::info!("Using database connection string: {}", self.config.database.connection_string);

        Ok(())
    }
}