use serde::{Deserialize, Serialize};

// Input data for SQL generation
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlGenerationInput {
    pub question: String,
    pub schema: String,
}

// Output from SQL generation
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlGenerationOutput {
    pub sql: String,
    pub explanation: Option<String>,
}

// Structure to represent a query to be executed
#[derive(Debug, Serialize, Deserialize)]
pub struct NlQuery {
    pub question: String,
    pub generated_sql: Option<String>,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

// Structure to represent a saved query with results
#[derive(Debug, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: String,
    pub name: String,
    pub question: String,
    pub sql: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// History item for tracking query execution
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryHistoryItem {
    pub question: String,
    pub sql: String,
    pub execution_time_ms: u64,
    pub row_count: usize,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}