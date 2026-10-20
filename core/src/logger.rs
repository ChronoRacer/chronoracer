use tracing_appender::non_blocking::WorkerGuard;

/// Env var that turns ANSI colour on in log output.
pub const LOG_COLOR_ENV_VAR_NAME: &str = "CORE_LOG_COLOR";

/// Release the Core was built from, baked in by `build.rs` from `core/version.txt`.
pub const CORE_VERSION: &str = env!("CORE_VERSION");

fn color_enabled() -> bool {
    std::env::var(LOG_COLOR_ENV_VAR_NAME)
        .is_ok_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true"))
}

pub fn init_logger() -> WorkerGuard {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(color_enabled())
        .with_target(true)
        .with_thread_ids(true)
        .init();
    guard
}
