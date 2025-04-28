
pub mod remote;
pub mod ollama;

#[cfg(feature = "local_llm")]
pub mod local;
mod local;