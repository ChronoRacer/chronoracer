#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::messaging::messaging_entrypoint::setup_message_sinks;
use crate::persistence::persistence_entrypoint::setup_persistence_pipe;
use crate::processing::processing_entrypoint::setup_processing_pipe;

mod configuration;
pub mod messaging;
pub mod persistence;
pub mod processing;

#[tokio::main] // TODO: configure workers
async fn main() {
    let config = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");
    let (mut sink, sink_rx, dlq_handler, dlq_tx) = setup_message_sinks(&config);
    let (collector, processed_data_rx) = setup_processing_pipe(sink_rx, dlq_tx);
    let (fast_batcher, normal_batcher, flusher) = setup_persistence_pipe(&config, processed_data_rx).await;
}
