// src/db/multi_db_pool.rs
use duckdb::Connection;
use r2d2::ManageConnection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

pub struct MultiDbConnectionManager {
    main_db_path: String,
    attached_dbs: Arc<Mutex<HashMap<String, String>>>,
}

impl MultiDbConnectionManager {
    pub fn new(main_db_path: String) -> Self {
        Self {
            main_db_path,
            attached_dbs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Register a subject database that should be attached to connections
    pub fn register_subject_db(&self, subject: &str, db_path: &str) {
        let mut dbs = self.attached_dbs.lock().unwrap();
        dbs.insert(subject.to_string(), db_path.to_string());
        debug!("Registered subject database: {} at {}", subject, db_path);
    }
}

impl ManageConnection for MultiDbConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // First connect to the main database
        let conn = Connection::open(&self.main_db_path)?;

        // Then attach any registered subject databases
        let dbs = self.attached_dbs.lock().unwrap();
        for (subject, db_path) in dbs.iter() {
            let attach_sql = format!("ATTACH DATABASE '{}' AS {}", db_path, subject);
            match conn.execute(&attach_sql, []) {
                Ok(_) => {
                    debug!("Attached database for subject: {}", subject);
                }
                Err(e) => {
                    // If the error is about the database already being attached, that's fine
                    if e.to_string().contains("already attached") {
                        debug!("Database for subject {} is already attached", subject);
                    } else {
                        warn!("Failed to attach database for subject {}: {}", subject, e);
                    }
                }
            }
        }

        Ok(conn)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute("SELECT 1", [])?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}