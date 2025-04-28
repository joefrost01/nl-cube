use crate::config::LlmConfig;
use crate::llm::{LlmError, SqlGenerator};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub struct RemoteLlmProvider {
    client: reqwest::Client,
    api_url: String,
    api_key: String,
    model: String,
}

#[derive(Serialize)]
struct PromptRequest {
    model: String,
    messages: Vec<Message>,
    temperature: f32,
    max_tokens: usize,
}

#[derive(Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct PromptResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

impl RemoteLlmProvider {
    pub fn new(config: &LlmConfig) -> Result<Self, LlmError> {
        let api_url = config.api_url.clone().ok_or_else(|| {
            LlmError::ConfigError("API URL is required for remote LLM provider".to_string())
        })?;

        let api_key = config.api_key.clone().ok_or_else(|| {
            LlmError::ConfigError("API key is required for remote LLM provider".to_string())
        })?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| LlmError::ConnectionError(e.to_string()))?;

        Ok(Self {
            client,
            api_url,
            api_key,
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
impl SqlGenerator for RemoteLlmProvider {
    async fn generate_sql(&self, question: &str, schema: &str) -> Result<String, LlmError> {
        let prompt = self.prepare_prompt(question, schema);

        let request = PromptRequest {
            model: self.model.clone(),
            messages: vec![Message {
                role: "user".to_string(),
                content: prompt,
            }],
            temperature: 0.1,
            max_tokens: 2000,
        };

        let response = self
            .client
            .post(&self.api_url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await
            .map_err(|e| LlmError::ConnectionError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(LlmError::ResponseError(format!(
                "API responded with status code: {}",
                response.status()
            )));
        }

        let prompt_response: PromptResponse = response
            .json()
            .await
            .map_err(|e| LlmError::ResponseError(e.to_string()))?;

        if prompt_response.choices.is_empty() {
            return Err(LlmError::ResponseError("No choices in response".to_string()));
        }

        let content = &prompt_response.choices[0].message.content;

        // Extract SQL from the response
        if let Some(start) = content.find("```sql") {
            if let Some(end) = content.rfind("```") {
                let sql = &content[start + 6..end].trim();
                return Ok(sql.to_string());
            }
        }

        // If we couldn't find explicit SQL code block, return the whole thing
        Ok(content.clone())
    }
}