use crate::config::LlmConfig;
use crate::llm::{LlmError, SqlGenerator};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

pub struct OllamaProvider {
    client: reqwest::Client,
    api_url: String,
    model: String,
}

#[derive(Serialize, Debug)]
struct OllamaRequest {
    model: String,
    prompt: String,
    temperature: f32,
    stream: bool,
}

#[derive(Deserialize, Debug)]
struct OllamaResponse {
    response: String,
    // Add other fields that might be in the response
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    done: Option<bool>,
    // Use serde to ignore unknown fields
    #[serde(flatten)]
    extra: std::collections::HashMap<String, serde_json::Value>,
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
        let prompt = format!(
            r#"
### Instructions:
Your task is to convert a question into a SQL query for DuckDB, given a database schema.
Adhere to these rules:
- **Be careful with column names - they are case sensitive**
- **Use the exact spelling of column names as provided in the schema**
- **Deliberately go through the question and database schema word by word** to appropriately answer the question
- **Use Table Aliases** to prevent ambiguity. For example, `SELECT table1.col1, table2.col1 FROM table1 JOIN table2 ON table1.id = table2.id`.
- When creating a ratio, always cast the numerator as float

### Input:
Generate a SQL query that answers the question `{}`.
This query will run on a DuckDB database with the following tables and columns:

{}

### Expected SQL Format:
- Use lowercase for SQL keywords (SELECT, FROM, WHERE, etc.)
- Reference column names exactly as shown in the schema
- Make sure to use double quotes around column names with spaces or special characters
- End your query with a semicolon

### Response:
Based on your instructions, here is the SQL query I have generated to answer the question `{}`:
```sql
"#,
            question, schema, question
        );

        info!("Prepared LLM prompt: {}", prompt);
        prompt
    }

    fn extract_sql(&self, content: &str) -> String {
        // Try to extract SQL from between ```sql and ``` markers
        if let Some(start) = content.find("```sql") {
            if let Some(end) = content.rfind("```") {
                let sql = &content[start + 6..end].trim();
                info!("Successfully extracted SQL from Ollama response using code block markers");
                debug!("Extracted SQL: {}", sql);
                return sql.to_string();
            }
        }

        // Try alternate syntax without a language specifier: ``` and ```
        if let Some(start) = content.find("```") {
            let content_after_first = &content[start + 3..];
            if let Some(end) = content_after_first.find("```") {
                let sql = &content_after_first[..end].trim();
                info!("Successfully extracted SQL using simple code block markers");
                debug!("Extracted SQL: {}", sql);
                return sql.to_string();
            }
        }

        // If we couldn't find explicit code blocks, try to extract SQL statements directly
        // Look for a line starting with SELECT, WITH, or another SQL keyword
        let sql_keywords = ["SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"];
        let lines: Vec<&str> = content.lines().collect();

        for (i, line) in lines.iter().enumerate() {
            let trimmed = line.trim().to_uppercase();
            if sql_keywords.iter().any(|kw| trimmed.starts_with(kw)) {
                // Found a line that looks like SQL - now collect until we find the end
                let mut sql = line.trim().to_string();

                // Collect subsequent lines that appear to be part of the SQL
                for j in (i+1)..lines.len() {
                    let next_line = lines[j].trim();

                    // Stop if we hit a markdown code block end
                    if next_line == "```" {
                        break;
                    }

                    // Stop if we hit another code block start
                    if next_line.starts_with("```") {
                        break;
                    }

                    // Add the line to our SQL
                    sql.push(' ');
                    sql.push_str(next_line);

                    // Stop if we reach the end of the statement (semicolon)
                    if next_line.ends_with(";") {
                        break;
                    }
                }

                info!("Extracted SQL using line scanning");
                debug!("Extracted SQL: {}", sql);
                return sql;
            }
        }

        // If still no SQL found, return the content as-is with a warning
        info!("Could not extract SQL using usual methods, returning full content");
        content.to_string()
    }
}

#[async_trait]
impl SqlGenerator for OllamaProvider {
    async fn generate_sql(&self, question: &str, schema: &str) -> Result<String, LlmError> {
        let prompt = self.prepare_prompt(question, schema);

        info!("Sending request to Ollama with model: {}", self.model);
        debug!("API URL: {}", self.api_url);

        let request = OllamaRequest {
            model: self.model.clone(),
            prompt,
            temperature: 0.1,
            stream: false, // Explicitly disable streaming
        };

        // Log the request for debugging
        debug!("Sending request to Ollama: {:?}", request);

        let response = self
            .client
            .post(&self.api_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| LlmError::ConnectionError(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            // Try to get the error message from the response body
            let error_body = match response.text().await {
                Ok(body) => format!(" - Response body: {}", body),
                Err(_) => String::new(),
            };

            error!("Ollama API responded with status code: {}{}", status, error_body);
            return Err(LlmError::ResponseError(format!(
                "Ollama API responded with status code: {}{}",
                status, error_body
            )));
        }

        // Get the raw text response first for diagnostics
        let response_text = response.text().await
            .map_err(|e| LlmError::ResponseError(format!("Failed to read response body: {}", e)))?;

        debug!("Raw response from Ollama: {}", response_text);

        // Parse the JSON response
        let ollama_response = match serde_json::from_str::<OllamaResponse>(&response_text) {
            Ok(resp) => resp,
            Err(e) => {
                error!("Failed to parse Ollama response: {} - Response was: {}", e, response_text);
                return Err(LlmError::ResponseError(format!(
                    "Failed to parse Ollama response: {} - Response was: {}",
                    e, response_text
                )));
            }
        };

        let content = ollama_response.response;
        debug!("Extracted response from Ollama: {}", content);

        // Use our improved SQL extraction method
        let sql = self.extract_sql(&content);

        // Ensure we don't return empty SQL
        if sql.trim().is_empty() {
            return Err(LlmError::ResponseError("Failed to extract valid SQL from response".to_string()));
        }

        Ok(sql)
    }
}