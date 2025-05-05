use duckdb::Connection;
use r2d2::ManageConnection;

pub struct DuckDBConnectionManager {
    connection_string: String,
}

impl DuckDBConnectionManager {
    pub fn new(connection_string: String) -> Self {
        Self { connection_string }
    }
}

impl ManageConnection for DuckDBConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Connection::open(&self.connection_string)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute("SELECT 1", [])?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
