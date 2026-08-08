#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::messaging::messaging_entrypoint::MessagingLayerEntrypoint;
use crate::persistence::persistence_entrypoint::PersistanceLayerEntryPoint;
use crate::processing::processing_entrypoint::ProcessingLayerEntrypoint;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;

mod configuration;
pub mod messaging;
pub mod persistence;
pub mod processing;
pub mod runtime_configuration;

fn main() {
    // Config
    let config_val = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");
    let config: &'static CoreConfig = Box::leak(Box::new(config_val));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.runtime.tokio_workers as usize)
        .max_blocking_threads(config.runtime.tokio_blocking as usize)
        .enable_all()
        .build()
        .expect("Tokio runtime build failed");

    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.runtime.rayon_threads as usize)
            .build()
            .expect("Failed to build Rayon pool")
    );

    rt.block_on(async {
        // Messaging
        let mut messaging_wrapper = MessagingLayerEntrypoint::init(&config);
        let current_driver_id_handler =
            RetainReadTimeValue::new(config.race.initial_driver_id.clone());
        let handler: &'static RetainReadTimeValue<CompactString> = Box::leak(Box::new(current_driver_id_handler));

        // Processing
        let mut processing_layer = ProcessingLayerEntrypoint::init(
            messaging_wrapper.sensor_sink_rx,
            messaging_wrapper.control_sink_rx,
            messaging_wrapper.dlq_tx,
            handler,
            rayon_pool,
        );

        // Persistence
        let mut persistance_wrapper = PersistanceLayerEntryPoint::init(
            &config,
            processing_layer.processed_data_rx,
            handler,
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
    });
}
