use duckdb::Connection;
use r2d2::ManageConnection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, warn};

pub struct MultiDbConnectionManager {
    main_db_path: String,
    data_dir: PathBuf,
    attached_dbs: Arc<Mutex<HashMap<String, String>>>,
}

impl MultiDbConnectionManager {
    pub fn new(main_db_path: String, data_dir: PathBuf) -> Self {
        Self {
            main_db_path,
            data_dir,
            attached_dbs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Register a subject database that should be attached to connections
    pub fn register_subject_db(&self, subject: &str, db_path: &str) {
        let mut dbs = self.attached_dbs.lock().unwrap();
        dbs.insert(subject.to_string(), db_path.to_string());
        debug!("Registered subject database: {} at {}", subject, db_path);
    }

    // Get the path to a subject database, creating parent directories if needed
    pub fn get_subject_db_path(&self, subject: &str) -> PathBuf {
        let subject_dir = self.data_dir.join(subject);
        if !subject_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(&subject_dir) {
                warn!("Failed to create subject directory for {}: {}", subject, e);
            }
        }
        subject_dir.join(format!("{}.duckdb", subject))
    }
}

impl ManageConnection for MultiDbConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // Connect to the main database
        Connection::open(&self.main_db_path)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute("SELECT 1", [])?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
