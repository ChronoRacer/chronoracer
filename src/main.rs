#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::messaging::messaging_entrypoint::setup_message_sinks;
use crate::persistence::persistence_entrypoint::setup_persistence_pipe;
use crate::processing::processing_entrypoint::setup_processing_pipe;

mod configuration;
pub mod messaging;
pub mod persistence;
pub mod processing;

#[tokio::main]
async fn main() {
    let config_val = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");

    let config: &'static CoreConfig = Box::leak(Box::new(config_val));

    let (mut sink, sink_rx, dlq_handler, dlq_tx) = setup_message_sinks(&config);
    let (mut collector, processed_data_rx) = setup_processing_pipe(sink_rx, dlq_tx);

    let (mut splitter, mut fast_batcher, mut normal_batcher, mut flusher) =
        setup_persistence_pipe(&config, processed_data_rx).await;

    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn(async move { sink.ingest().await });
    tasks.spawn(async move { dlq_handler.ingest().await });

    tasks.spawn(async move { collector.ingest().await });

    tasks.spawn(async move { splitter.ingest().await });
    tasks.spawn(async move { fast_batcher.ingest().await });
    tasks.spawn(async move { normal_batcher.ingest().await });
    tasks.spawn(async move { flusher.ingest().await });

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(_) => {
                print!("A component exited pipeline");
                tasks.abort_all();
                break;
            }
            Err(e) => {
                eprintln!(
                    "A component task panicked or failed: {:?}. Aborting pipeline.",
                    e
                );
                tasks.abort_all();
                break;
            }
        }
    }
}
