use crate::config::AppConfig;
use crate::db::db_pool::DuckDBConnectionManager;
use crate::llm::LlmManager;
use minijinja::Environment;
use r2d2::Pool;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared application state for the web server
pub struct AppState {
    pub config: AppConfig,
    pub db_pool: Pool<DuckDBConnectionManager>,
    pub template_env: Environment<'static>,
    pub llm_manager: LlmManager,
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
        env.add_filter("json", |value| {
            serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
        });

        Self {
            config: config.clone(),
            db_pool,
            template_env: env,
            llm_manager,
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
        let conn = self.db_pool.get()?;

        // Query DuckDB for all tables
        let mut stmt = conn.prepare("
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ")?;

        let tables: Result<Vec<String>, _> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect();

        let tables = tables?;
        let mut schemas = Vec::new();

        for table in tables {
            // Get the schema for each table
            let mut stmt = conn.prepare(&format!(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns 
                WHERE table_schema = 'main' AND table_name = '{}'",
                table
            ))?;

            let columns: Result<Vec<(String, String, String)>, _> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect();

            let columns = columns?;

            // Build CREATE TABLE DDL 
            let mut ddl = format!("CREATE TABLE {} (\n", table);

            for (i, (column, data_type, is_nullable)) in columns.iter().enumerate() {
                let nullable = if is_nullable == "YES" { "" } else { " NOT NULL" };
                ddl.push_str(&format!("    {} {}{}", column, data_type, nullable));

                if i < columns.len() - 1 {
                    ddl.push_str(",\n");
                } else {
                    ddl.push_str("\n");
                }
            }

            ddl.push_str(");");
            schemas.push(ddl);
        }

        // Update the schemas
        let mut schemas_lock = self.schemas.write().await;
        *schemas_lock = schemas;

        Ok(())
    }

    // Refreshes available subjects (data directories)
    pub async fn refresh_subjects(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut subjects = Vec::new();

        // Scan the data directory for subject folders
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

        // Update the subjects
        let mut subjects_lock = self.subjects.write().await;
        *subjects_lock = subjects;

        Ok(())
    }
}