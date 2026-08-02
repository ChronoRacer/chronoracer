#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::messaging::messaging_entrypoint::MessagingLayerEntrypoint;
use crate::persistence::persistence_entrypoint::PersistanceLayerEntryPoint;
use crate::processing::processing_entrypoint::ProcessingLayerEntrypoint;
use crate::runtime_configuration::RetainReadTimeValue;

mod configuration;
pub mod messaging;
pub mod persistence;
pub mod processing;
pub mod runtime_configuration;

#[tokio::main]
async fn main() {
    // Config
    let config_val = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");
    let config: &'static CoreConfig = Box::leak(Box::new(config_val));
    // Messaging
    let mut messaging_wrapper = MessagingLayerEntrypoint::init(&config);
    let current_driver_id_handler = RetainReadTimeValue::new(config.race.initial_driver_id.clone());

    // Processing
    let mut processing_layer = ProcessingLayerEntrypoint::init(
        messaging_wrapper.sensor_sink_rx,
        messaging_wrapper.control_sink_rx,
        messaging_wrapper.dlq_tx,
        current_driver_id_handler.clone(),
    );

    // Persistence
    let mut persistance_wrapper = PersistanceLayerEntryPoint::init(
        &config,
        processing_layer.processed_data_rx,
        current_driver_id_handler,
    )
        .await;

    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn(async move { messaging_wrapper.sensor_data_sink.ingest().await });
    tasks.spawn(async move { messaging_wrapper.dlq_handler.ingest().await });

    tasks.spawn(async move { processing_layer.collector.ingest().await });

    tasks.spawn(async move { persistance_wrapper.splitter.ingest().await });
    tasks.spawn(async move { persistance_wrapper.fast_batcher.ingest().await });
    tasks.spawn(async move { persistance_wrapper.normal_batcher.ingest().await });
    tasks.spawn(async move { persistance_wrapper.flusher.ingest().await });

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
