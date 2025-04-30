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
        );

        info!("Prepared LLM prompt: {}", prompt);
        prompt
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

        // Extract SQL from the response
        if let Some(start) = content.find("```sql") {
            if let Some(end) = content.rfind("```") {
                let sql = &content[start + 6..end].trim();
                info!("Successfully extracted SQL from Ollama response");
                debug!("Extracted SQL: {}", sql);
                return Ok(sql.to_string());
            }
        }

        // If we couldn't find explicit SQL code block, look for SQL statement patterns
        if content.to_lowercase().contains("select") &&
            (content.to_lowercase().contains("from") || content.to_lowercase().contains("where")) {
            // Try to extract SQL from plain text using heuristics
            let lines: Vec<&str> = content.lines()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .collect();

            for (i, line) in lines.iter().enumerate() {
                if line.to_lowercase().starts_with("select") {
                    // Found potential SQL start, collect until end
                    let mut sql = line.to_string();
                    for j in (i+1)..lines.len() {
                        sql.push(' ');
                        sql.push_str(lines[j]);

                        // Stop if we reach a line that looks like the end of SQL
                        if lines[j].ends_with(";") {
                            break;
                        }
                    }

                    info!("Extracted SQL using heuristics");
                    debug!("Extracted SQL: {}", sql);
                    return Ok(sql);
                }
            }
        }

        // If still no SQL found, return the whole content as a last resort
        info!("No SQL code block found, returning entire response");
        Ok(content)
    }
}