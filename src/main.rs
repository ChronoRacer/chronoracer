#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::logger::init_logger;
use crate::messaging::messaging_entrypoint::MessagingLayerEntrypoint;
use crate::persistence::persistence_entrypoint::PersistanceLayerEntryPoint;
use crate::processing::processing_entrypoint::ProcessingLayerEntrypoint;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;
use tracing::{error, info};

mod configuration;
pub mod logger;
pub mod messaging;
pub mod persistence;
pub mod processing;
pub mod runtime_configuration;

fn main() {
    // Logger
    let _logger_guard = init_logger();

    // Config
    let config_val = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");
    let config: &'static CoreConfig = Box::leak(Box::new(config_val));
    info!("Configuration loaded: {:#?}", config);
    info!(
        "Starting Chronoracer core for race_id={}",
        config.race.race_id
    );

    // Runtime
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.runtime.tokio_workers as usize)
        .max_blocking_threads(config.runtime.tokio_blocking as usize)
        .enable_all()
        .build()
        .expect("Tokio runtime build failed");

    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.runtime.rayon_threads as usize)
            .build()
            .expect("Failed to build Rayon pool"),
    );
    info!("Runtime initalized. Starting...");

    tokio_runtime.block_on(async {
        // Runtime configuration
        let current_driver_id_handler =
            RetainReadTimeValue::new(config.race.initial_driver_id.clone());
        let handler: &'static RetainReadTimeValue<CompactString> =
            Box::leak(Box::new(current_driver_id_handler));
        info!(
            "Runtime configuration initialized. Starting with driver_id={}",
            handler.get()
        );

        // Messaging
        let mut messaging_wrapper = MessagingLayerEntrypoint::init(config).await;
        info!("Messaging layer initalized");

        // Processing
        let mut processing_layer = ProcessingLayerEntrypoint::init(
            messaging_wrapper.sensor_sink_rx,
            messaging_wrapper.control_sink_rx,
            messaging_wrapper.dlq_tx,
            handler,
            rayon_pool,
        );
        info!("Processing layer initalized");

        // Persistence
        let mut persistance_wrapper =
            PersistanceLayerEntryPoint::init(config, processing_layer.processed_data_rx, handler)
                .await;
        info!("Persistance layer initalized");

        let mut tasks = tokio::task::JoinSet::new();

        tasks.spawn(async move { messaging_wrapper.sensor_data_sink.ingest().await });
        tasks.spawn(async move { messaging_wrapper.control_sink.ingest().await });
        tasks.spawn(async move { messaging_wrapper.dlq_handler.ingest().await });

        tasks.spawn(async move { processing_layer.collector.ingest().await });
        tasks.spawn(async move { processing_layer.control_filter.ingest().await });

        tasks.spawn(async move { persistance_wrapper.splitter.ingest().await });
        tasks.spawn(async move { persistance_wrapper.fast_batcher.ingest().await });
        tasks.spawn(async move { persistance_wrapper.normal_batcher.ingest().await });
        tasks.spawn(async move { persistance_wrapper.flusher.ingest().await });

        tasks.spawn(async move { messaging_wrapper.heartbeat_handler.keep_alive().await });
        
        info!("Running...");
        if let Some(result) = tasks.join_next().await {
            match result {
                Ok(()) => {
                    error!("A component exited pipeline. Aborting all tasks");
                    tasks.abort_all();
                }
                Err(e) => {
                    error!(
                        "A component task panicked or failed: {:?}. Aborting pipeline.",
                        e
                    );
                    tasks.abort_all();
                }
            }
        }
    });
    info!("Run complete");
}
