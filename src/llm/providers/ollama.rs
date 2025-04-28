use crate::config::LlmConfig;
use crate::llm::{LlmError, SqlGenerator};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub struct OllamaProvider {
    client: reqwest::Client,
    api_url: String,
    model: String,
}

#[derive(Serialize)]
struct OllamaRequest {
    model: String,
    prompt: String,
    temperature: f32,
}

#[derive(Deserialize)]
struct OllamaResponse {
    response: String,
}

impl OllamaProvider {
    pub fn new(config: &LlmConfig) -> Result<Self, LlmError> {
        let api_url = config
            .api_url
            .clone()
            .unwrap_or_else(|| "http://localhost:11434/api/generate".to_string());

        let client = reqwest::Client::new();

        Ok(Self {
            client,
            api_url,
            model: config.model.clone(),
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
impl SqlGenerator for OllamaProvider {
    async fn generate_sql(&self, question: &str, schema: &str) -> Result<String, LlmError> {
        let prompt = self.prepare_prompt(question, schema);

        let request = OllamaRequest {
            model: self.model.clone(),
            prompt,
            temperature: 0.1,
        };

        let response = self
            .client
            .post(&self.api_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| LlmError::ConnectionError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(LlmError::ResponseError(format!(
                "Ollama API responded with status code: {}",
                response.status()
            )));
        }

        let ollama_response: OllamaResponse = response
            .json()
            .await
            .map_err(|e| LlmError::ResponseError(e.to_string()))?;

        let content = ollama_response.response;

        // Extract SQL from the response
        if let Some(start) = content.find("```sql") {
            if let Some(end) = content.rfind("```") {
                let sql = &content[start + 6..end].trim();
                return Ok(sql.to_string());
            }
        }

        // If we couldn't find explicit SQL code block, assume the entire response is the SQL
        Ok(content)
    }
}