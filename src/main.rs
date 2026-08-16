#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::logger::init_logger;
use crate::messaging::messaging_entrypoint::MessagingLayerEntrypoint;
use crate::monitor::Monitor;
use crate::persistence::persistence_entrypoint::PersistanceLayerEntryPoint;
use crate::processing::processing_entrypoint::ProcessingLayerEntrypoint;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

mod configuration;
pub mod logger;
pub mod messaging;
pub mod monitor;
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
        let data_cancel_token = CancellationToken::new();
        let data_tracker = TaskTracker::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

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
            shutdown_tx,
            rayon_pool,
        );
        info!("Processing layer initalized");

        // Persistence
        let mut persistance_wrapper =
            PersistanceLayerEntryPoint::init(config, processing_layer.processed_data_rx, handler)
                .await;
        info!("Persistance layer initalized");

        // Monitor

        let monitor = Monitor::new(
            data_cancel_token.clone(),
            data_tracker,
            config,
            persistance_wrapper.exporter,
        );

        // Data Pipeline
        let data_sink_token = data_cancel_token.clone();
        monitor.track_task(async move {
            messaging_wrapper
                .sensor_data_sink
                .ingest_with_cancellation(data_sink_token)
                .await;
        });
        monitor.track_task(async move { processing_layer.collector.ingest().await });
        monitor.track_task(async move { persistance_wrapper.splitter.ingest().await });
        monitor.track_task(async move { persistance_wrapper.fast_batcher.ingest().await });
        monitor.track_task(async move { persistance_wrapper.normal_batcher.ingest().await });
        monitor.track_task(async move { persistance_wrapper.flusher.ingest().await });
        info!("Data pipeline started");

        // Control tasks

        let mut control_tasks = JoinSet::new();
        control_tasks.spawn(async move { processing_layer.control_filter.ingest().await });
        control_tasks.spawn(async move { messaging_wrapper.control_sink.ingest().await });
        control_tasks.spawn(async move { messaging_wrapper.dlq_handler.ingest().await });
        control_tasks.spawn(async move { messaging_wrapper.heartbeat_handler.keep_alive().await });
        info!("Control plane initalized");

        run(control_tasks, monitor, shutdown_rx).await;
    });
    info!("Run complete");
}

async fn run(
    mut control_tasks: JoinSet<()>,
    monitor: Monitor,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    info!("Core running...");
    loop {
        tokio::select! {
            res = control_tasks.join_next() => {
                match res {
                    Some(Ok(_)) => {
                        if control_tasks.is_empty() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!("Control task failed: {:?}", e);
                        control_tasks.abort_all();
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }
            res = &mut shutdown_rx => {
                match res {
                    Ok(()) => {
                        monitor.shutdown_signal_received();
                    }
                    Err(_) => {
                        error!("Shutdown sender dropped before signaling, aborting...");
                    }
                }
                control_tasks.abort_all();
                break;
            }
        }
    }
}
