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
        // First collect all tables and their schemas synchronously
        let schemas = {
            // Get database connection
            let conn = self.db_pool.get()?;
            let mut schemas = Vec::new();

            // Use EXACTLY the same SQL that succeeded in the diagnostics
            tracing::info!("Refreshing schemas using direct sqlite_master query");
            let mut stmt = conn.prepare("
            SELECT name FROM sqlite_master
            WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            AND name NOT LIKE 'pg_%'
            AND name NOT LIKE 'information_schema.%'
        ")?;

            let tables: Result<Vec<String>, _> = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect();

            let tables = tables?;

            if tables.is_empty() {
                tracing::warn!("No tables found in the database during schema refresh");

                // Try SHOW TABLES as a fallback since that worked in diagnostics
                match conn.prepare("SHOW TABLES") {
                    Ok(mut show_stmt) => {
                        let show_tables: Result<Vec<String>, _> = show_stmt
                            .query_map([], |row| row.get::<_, String>(0))
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
                            .collect();

                        match show_tables {
                            Ok(show_tables) => {
                                tracing::info!("SHOW TABLES found {} tables: {:?}", show_tables.len(), show_tables);
                                if !show_tables.is_empty() {
                                    // Use these tables instead
                                    tracing::info!("Using tables from SHOW TABLES command");
                                    for table in &show_tables {
                                        schemas.push(format!("CREATE TABLE \"{}\" (placeholder VARCHAR);", table));
                                    }
                                    // Skip further processing
                                    let mut schemas_lock = self.schemas.write().await;
                                    *schemas_lock = schemas;
                                    tracing::info!("Schema refresh complete using SHOW TABLES, found {} schemas", schemas_lock.len());
                                    return Ok(());
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error collecting SHOW TABLES results: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error preparing SHOW TABLES: {}", e);
                    }
                }
            } else {
                tracing::info!("Found {} tables during schema refresh: {:?}", tables.len(), tables);
            }

            // Process each table to build schema DDL
            for table in tables {
                tracing::info!("Processing table schema for: {}", table);

                // Simplified approach - just create a basic CREATE TABLE statement
                // since we had issues with PRAGMA table_info
                let mut ddl = format!("CREATE TABLE \"{}\" (\n", table);

                // Try to get column info using a SELECT * LIMIT 0
                match conn.prepare(&format!("SELECT * FROM \"{}\" LIMIT 0", table)) {
                    Ok(stmt) => {
                        let column_count = stmt.column_count();
                        tracing::info!("Table {} has {} columns", table, column_count);

                        for i in 0..column_count {
                            let column_name = stmt.column_name(i).map_or("unknown", |s| s);
                            let column_type = "VARCHAR"; // Default to VARCHAR

                            ddl.push_str(&format!("    \"{}\" {}", column_name, column_type));

                            if i < column_count - 1 {
                                ddl.push_str(",\n");
                            } else {
                                ddl.push_str("\n");
                            }
                        }

                        ddl.push_str(");");
                        schemas.push(ddl.clone());
                        tracing::info!("Added schema: {}", ddl);
                    },
                    Err(e) => {
                        tracing::error!("Failed to get column info for table {}: {}", table, e);
                        // Still create a basic schema
                        ddl.push_str("    placeholder VARCHAR\n);");
                        schemas.push(ddl.clone());
                        tracing::info!("Added placeholder schema: {}", ddl);
                    }
                }
            }

            schemas
        };

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