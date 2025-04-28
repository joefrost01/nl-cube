use crate::config::LlmConfig;
use crate::llm::{LlmError, SqlGenerator};
use async_trait::async_trait;

pub struct LocalLlmProvider {
    model_path: String,
    // Here we'd add fields for the local model, likely using ezllama or similar
}

impl LocalLlmProvider {
    pub fn new(config: &LlmConfig) -> Result<Self, LlmError> {
        // In a full implementation, this would load a local model using ezllama
        // For now, just validate config requirements

        Ok(Self {
            model_path: config.model.clone(),
        })
    }

    fn prepare_prompt(&self, question: &str, schema: &str) -> String {
        format!(
            r#"
### Instructions:
Your task is to convert a question into a SQL query, given a database schema.
Adhere to these rules:
- **Deliberately go through the question and database schema word by word** to appropriately answer the question
- **Use Table Aliases** to prevent ambiguity. For example, `SELECT table1.col1, table2.col1 FROM table1 JOIN table2 ON table1.id = table2.id`.
- When creating a ratio, always cast the numerator as float

### Input:
Generate a SQL query that answers the question `{}`.
This query will run on a database whose schema is represented in this string:
{}

### Response:
Based on your instructions, here is the SQL query I have generated to answer the question `{}`:
```sql
"#,
            question, schema, question
        )
    }
}

#[async_trait]
impl SqlGenerator for LocalLlmProvider {
    async fn generate_sql(&self, question: &str, schema: &str) -> Result<String, LlmError> {
        // This is a placeholder. In a real implementation, this would:
        // 1. Format the prompt
        // 2. Send it to the local model through ezllama
        // 3. Process the response

        let _prompt = self.prepare_prompt(question, schema);

        // To be implemented when local LLM feature is enabled
        Err(LlmError::ConfigError("Local LLM provider not fully implemented".to_string()))
    }
}