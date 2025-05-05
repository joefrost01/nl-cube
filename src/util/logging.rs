use tracing_subscriber::{fmt, EnvFilter};

/// Initializes tracing/logging based on environment variables.
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(false);

    subscriber.init();
}
