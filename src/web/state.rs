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

    pub fn find_schemas_from_filesystem(data_dir: &str) -> Vec<String> {
        let mut schemas = Vec::new();
        let path = std::path::Path::new(data_dir);

        if path.exists() && path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.filter_map(Result::ok) {
                    if entry.path().is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            schemas.push(name.to_string());
                        }
                    }
                }
            }
        }

        schemas
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

    pub fn find_tables_for_schema(conn: &duckdb::Connection, schema_name: &str) -> Vec<(String, Vec<(String, String, bool)>)> {
        let mut tables_with_columns = Vec::new();

        // Method 1: Try information_schema.tables
        let query1 = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'",
            schema_name
        );

        let mut table_names = Vec::new();

        match conn.prepare(&query1) {
            Ok(mut stmt) => {
                if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                    for row in rows.filter_map(Result::ok) {
                        table_names.push(row);
                    }
                }
            },
            Err(_) => {}
        }

        // Method 2: Try sqlite_master (DuckDB backs onto SQLite)
        if table_names.is_empty() {
            let query2 = format!(
                "SELECT name FROM main.sqlite_master WHERE type='table' AND name LIKE '{}.%'",
                schema_name
            );

            match conn.prepare(&query2) {
                Ok(mut stmt) => {
                    if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                        for row in rows.filter_map(Result::ok) {
                            // Extract table name from schema.table format
                            if let Some(table_name) = row.split('.').nth(1) {
                                table_names.push(table_name.to_string());
                            }
                        }
                    }
                },
                Err(_) => {}
            }
        }

        // Method 3: Try a direct query to known tables
        if table_names.is_empty() {
            // Try to query a known table like "orders" directly
            let common_tables = ["orders", "customers", "products", "sales", "users"];

            for table in &common_tables {
                let test_query = format!(
                    "SELECT 1 FROM \"{}\".\"{}\" WHERE 1=0",
                    schema_name, table
                );

                match conn.prepare(&test_query) {
                    Ok(_) => {
                        // If no error, the table exists
                        table_names.push(table.to_string());
                    },
                    Err(_) => {}
                }
            }
        }

        // Method 4: Try SHOW TABLES command which DuckDB supports
        if table_names.is_empty() {
            let query4 = format!("SHOW TABLES IN \"{}\"", schema_name);

            match conn.prepare(&query4) {
                Ok(mut stmt) => {
                    if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                        for row in rows.filter_map(Result::ok) {
                            table_names.push(row);
                        }
                    }
                },
                Err(_) => {}
            }
        }

        // For each table found, get column information
        for table_name in table_names {
            let mut columns = Vec::new();

            // Try to get column info from information_schema.columns
            let column_query = format!(
                "SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{}' AND table_name = '{}'
            ORDER BY ordinal_position",
                schema_name, table_name
            );

            match conn.prepare(&column_query) {
                Ok(mut stmt) => {
                    if let Ok(rows) = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?, // column_name
                            row.get::<_, String>(1)?, // data_type
                            row.get::<_, String>(2)? == "YES" // is_nullable
                        ))
                    }) {
                        columns = rows.filter_map(Result::ok).collect();
                    }
                },
                Err(_) => {}
            }

            // If columns are empty, try a PRAGMA statement (DuckDB supports some SQLite pragmas)
            if columns.is_empty() {
                let pragma_query = format!("PRAGMA table_info(\"{}.{}\")", schema_name, table_name);

                match conn.prepare(&pragma_query) {
                    Ok(mut stmt) => {
                        if let Ok(rows) = stmt.query_map([], |row| {
                            Ok((
                                row.get::<_, String>(1)?, // name
                                row.get::<_, String>(2)?, // type
                                row.get::<_, i32>(3)? == 0 // notnull (0 = nullable)
                            ))
                        }) {
                            columns = rows.filter_map(Result::ok).collect();
                        }
                    },
                    Err(_) => {}
                }
            }

            tables_with_columns.push((table_name, columns));
        }

        tables_with_columns
    }

    pub fn get_sample_data(conn: &duckdb::Connection, schema_name: &str, table_name: &str) -> String {
        let mut sample_output = String::new();

        // Try to query the table
        let sample_query = format!(
            "SELECT * FROM \"{}\".\"{}\" LIMIT 3",
            schema_name, table_name
        );

        // Use safe approach to avoid crashes
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut result = String::new();

            match conn.prepare(&sample_query) {
                Ok(mut stmt) => {
                    // Get column count safely
                    let column_count = stmt.column_count();

                    // Get column names
                    let mut column_names = Vec::new();
                    for i in 0..column_count {
                        match stmt.column_name(i) {
                            Ok(name) => column_names.push(name.to_string()),
                            Err(_) => column_names.push(format!("Column{}", i))
                        }
                    }

                    // Create header row
                    if !column_names.is_empty() {
                        result.push_str("| ");
                        for name in &column_names {
                            result.push_str(&format!("{} | ", name));
                        }
                        result.push_str("\n| ");

                        for _ in 0..column_names.len() {
                            result.push_str("--- | ");
                        }
                        result.push_str("\n");

                        // Now fetch data rows
                        if let Ok(mut rows) = stmt.query([]) {
                            // Process each row with safeguards
                            let mut row_data_vec = Vec::new();

                            // For each row
                            while let Ok(Some(row)) = rows.next() {
                                let mut row_data = Vec::new();

                                // For each column
                                for i in 0..column_count {
                                    let value: Result<String, _> = row.get(i);
                                    match value {
                                        Ok(v) => row_data.push(v),
                                        Err(_) => row_data.push("NULL".to_string()),
                                    }
                                }

                                row_data_vec.push(row_data);
                            }

                            // Add data rows
                            for row_data in row_data_vec {
                                result.push_str("| ");
                                for cell in row_data {
                                    result.push_str(&format!("{} | ", cell));
                                }
                                result.push_str("\n");
                            }
                        }
                    }
                },
                Err(_) => {}
            }

            result
        }));

        match result {
            Ok(string_result) => sample_output = string_result,
            Err(_) => sample_output = "Error getting sample data\n".to_string(),
        }

        sample_output
    }

    pub fn infer_csv_schema(path: &std::path::Path) -> Option<Vec<(String, String)>> {
        if !path.exists() {
            return None;
        }

        // Very simple CSV reader to just get the header row and first data row
        match std::fs::File::open(path) {
            Ok(file) => {
                let mut reader = std::io::BufReader::new(file);
                let mut header_line = String::new();
                let mut data_line = String::new();

                // Read header
                if let Ok(len) = std::io::BufRead::read_line(&mut reader, &mut header_line) {
                    if len == 0 {
                        return None;
                    }
                } else {
                    return None;
                }

                // Read first data row
                if let Ok(len) = std::io::BufRead::read_line(&mut reader, &mut data_line) {
                    if len == 0 {
                        return None;
                    }
                } else {
                    return None;
                }

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

                let mut schema = Vec::new();

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

                    schema.push((header.clone(), data_type));
                }

                Some(schema)
            },
            Err(_) => None,
        }
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