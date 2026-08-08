use tracing_appender::non_blocking::WorkerGuard;

pub fn init_logger() -> WorkerGuard {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_target(true)
        .with_thread_ids(true)
        .init();
    guard
}