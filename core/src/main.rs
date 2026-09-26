#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::gcp::gcp_entrypoint::GcpLayerEntrypoint;
use crate::logger::init_logger;
use crate::messaging::messaging_entrypoint::MessagingLayerEntrypoint;
use crate::monitor::Monitor;
use crate::persistence::persistence_entrypoint::PersistenceLayerEntryPoint;
use crate::processing::processing_entrypoint::ProcessingLayerEntrypoint;
use crate::runtime_configuration::RetainReadTimeValue;
use crate::telemetry::PreviewHub;
use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

mod configuration;
pub mod gcp;
pub mod logger;
pub mod messaging;
pub mod monitor;
pub mod persistence;
pub mod processing;
pub mod runtime_configuration;
pub mod telemetry;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> ExitCode {
    let _logger_guard = init_logger();
    if let Err(error) = run() {
        error!(%error, "Chronoracer core failed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[tracing::instrument(err(Debug))]
fn run() -> Result<()> {
    let config_val = CoreConfig::load_from_env()?;
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
        .context("Failed to build Tokio runtime")?;

    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.runtime.rayon_threads as usize)
            .build()
            .context("Failed to build Rayon pool")?,
    );
    info!("Runtime initialized. Starting...");

    tokio_runtime.block_on(async {
        let data_cancel_token = CancellationToken::new();
        let data_tracker = TaskTracker::new();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Runtime configuration
        let current_driver_id_handler =
            RetainReadTimeValue::new(config.race.initial_driver_id.clone());
        let handler: &'static RetainReadTimeValue<CompactString> =
            Box::leak(Box::new(current_driver_id_handler));
        let preview = PreviewHub::new(
            handler,
            config.telemetry.max_queue_points,
            Duration::from_millis(u64::from(config.telemetry.max_queue_age_ms)),
        );
        info!(
            "Runtime configuration initialized. Starting with driver_id={}",
            handler.get()
        );

        // Messaging
        let mut messaging_wrapper = MessagingLayerEntrypoint::init(config).await?;
        info!("Messaging layer initialized");

        // Processing
        let mut processing_layer = ProcessingLayerEntrypoint::init(
            messaging_wrapper.sensor_sink_rx,
            messaging_wrapper.control_sink_rx,
            messaging_wrapper.dlq_tx,
            handler,
            shutdown_tx,
            rayon_pool,
            preview.clone(),
        )?;
        info!("Processing layer initialized");

        // Persistence
        let mut persistence_wrapper =
            PersistenceLayerEntryPoint::init(config, processing_layer.processed_data_rx, handler)
                .await?;
        info!("Persistence layer initialized");

        // GCP
        let gcp_wrapper = GcpLayerEntrypoint::init(&config.race.race_id, &config.gcp).await?;
        let (storage, pubsub) = match gcp_wrapper {
            Some(w) => (Some(w.gcs_storage_manager), Some(w.gcs_pub_sub_manager)),
            None => (None, None),
        };

        info!("GCP layer initialized");

        // Monitor
        let monitor = Monitor::new(
            data_cancel_token.clone(),
            data_tracker,
            config,
            persistence_wrapper.exporter,
            messaging_wrapper.kafka_exporter,
            storage,
            pubsub,
            config.runtime.notify_shutdown_on_failed_db_export,
            config.runtime.notify_shutdown_on_failed_kafka_export,
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
        monitor.track_task(async move { persistence_wrapper.splitter.ingest().await });
        monitor.track_task(async move { persistence_wrapper.fast_batcher.ingest().await });
        monitor.track_task(async move { persistence_wrapper.normal_batcher.ingest().await });
        monitor.track_task(async move { persistence_wrapper.flusher.ingest().await });
        info!("Data pipeline started");

        let telemetry_cancel = data_cancel_token.clone();
        let telemetry_port = config.telemetry.port;
        let telemetry_race = config.race.race_id.clone();
        tokio::spawn(async move {
            telemetry::serve(preview, telemetry_cancel, telemetry_port, telemetry_race).await;
        });

        // Control tasks
        let mut control_tasks = JoinSet::new();
        if config.gcp.enabled {
            let write_metrics = Arc::clone(&persistence_wrapper.write_metrics);
            let project_id = config.gcp.project_id.clone();
            let max_retries = config.gcp.max_retries;
            control_tasks.spawn(async move {
                persistence::write_metrics_publisher::publish_write_metrics(
                    project_id,
                    max_retries,
                    write_metrics,
                    config.gcp.allow_unknown_gce_metrics,
                )
                .await;
            });
        }
        control_tasks.spawn(async move { processing_layer.control_filter.ingest().await });
        control_tasks.spawn(async move { messaging_wrapper.control_sink.ingest().await });
        control_tasks.spawn(async move { messaging_wrapper.dlq_handler.ingest().await });
        control_tasks.spawn(async move { messaging_wrapper.heartbeat_handler.keep_alive().await });
        info!("Control plane initialized");

        run_tasks(control_tasks, monitor, shutdown_rx).await?;
        Ok::<(), anyhow::Error>(())
    })?;
    info!("Run complete");
    Ok(())
}

async fn run_tasks(
    mut control_tasks: JoinSet<()>,
    mut monitor: Monitor<'_>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let mut data_task_termination_rx = monitor.take_data_task_termination_receiver();
    info!("Core running...");
    loop {
        tokio::select! {
            biased;
            data_task_termination = data_task_termination_rx.recv() => {
                if data_task_termination.is_some() {
                    if shutdown_rx.try_recv().is_ok() {
                        monitor.execute_shutdown_sequence().await;
                        control_tasks.abort_all();
                        break;
                    }
                    error!("Data pipeline task terminated unexpectedly; stopping Core without export");
                    control_tasks.abort_all();
                    monitor.stop_data_pipeline();
                    bail!("Data pipeline task terminated unexpectedly");
                }
            }
            res = control_tasks.join_next() => {
                match res {
                    Some(Ok(())) => {
                        if control_tasks.is_empty() {
                            monitor.execute_shutdown_sequence().await;
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        error!(?error, "Control task failed");
                        control_tasks.abort_all();
                        monitor.execute_shutdown_sequence().await;
                        break;
                    }
                    None => {
                        monitor.execute_shutdown_sequence().await;
                        break;
                    }
                }
            }
            res = &mut shutdown_rx => {
                match res {
                    Ok(()) => {
                        monitor.execute_shutdown_sequence().await;
                    }
                    Err(_) => {
                        error!("Shutdown sender dropped before signaling, aborting...");
                        monitor.execute_shutdown_sequence().await;
                    }
                }
                control_tasks.abort_all();
                break;
            }
        }
    }
    Ok(())
}
