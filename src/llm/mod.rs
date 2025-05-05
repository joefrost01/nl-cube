pub mod models;
pub mod providers;

use crate::config::LlmConfig;
use async_trait::async_trait;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum LlmError {
    ConnectionError(String),
    ResponseError(String),
    ConfigError(String),
}

impl fmt::Display for LlmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LlmError::ConnectionError(msg) => write!(f, "LLM connection error: {}", msg),
            LlmError::ResponseError(msg) => write!(f, "LLM response error: {}", msg),
            LlmError::ConfigError(msg) => write!(f, "LLM configuration error: {}", msg),
        }
    }
}

impl Error for LlmError {}

#[async_trait]
pub trait SqlGenerator: Send + Sync {
    async fn generate_sql(&self, question: &str, schema: &str) -> Result<String, LlmError>;
}

pub struct LlmManager {
    generator: Box<dyn SqlGenerator + Send + Sync>,
}

impl LlmManager {
    pub fn new(config: &LlmConfig) -> Result<Self, LlmError> {
        let generator: Box<dyn SqlGenerator + Send + Sync> = match config.backend.as_str() {
            "remote" => Box::new(providers::remote::RemoteLlmProvider::new(config)?),
            "ollama" => Box::new(providers::ollama::OllamaProvider::new(config)?),
            _ => {
                return Err(LlmError::ConfigError(format!(
                    "Unsupported LLM backend: {}",
                    config.backend
                )))
            }
        };

        Ok(Self { generator })
    }

    pub async fn generate_sql(&self, question: &str, schema: &str) -> Result<String, LlmError> {
        self.generator.generate_sql(question, schema).await
    }
}