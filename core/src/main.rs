#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;
use crate::gcp::gcp_entrypoint::GcpLayerEntrypoint;
use crate::logger::init_logger;
use crate::messaging::messaging_entrypoint::MessagingLayerEntrypoint;
use crate::messaging::partial_message::SourceOffset;
use crate::monitor::Monitor;
use crate::persistence::persistence_entrypoint::PersistenceLayerEntryPoint;
use crate::processing::processing_entrypoint::ProcessingLayerEntrypoint;
use crate::runtime_configuration::RetainReadTimeValue;
use crate::telemetry::PreviewHub;
use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use std::collections::HashMap;
use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{Instant, timeout};
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

const CONTROL_SINK: &str = "control_sink";
const CONTROL_FILTER: &str = "control_filter";
const DLQ_HANDLER: &str = "dlq_handler";
const HEARTBEAT: &str = "heartbeat";
const WRITE_METRICS: &str = "write_metrics";

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
        .worker_threads(config.runtime.tokio_workers.get() as usize)
        .max_blocking_threads(config.runtime.tokio_blocking.get() as usize)
        .enable_all()
        .build()
        .context("Failed to build Tokio runtime")?;

    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.runtime.rayon_threads.get() as usize)
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
            config.telemetry.max_queue_points.get(),
            Duration::from_millis(u64::from(config.telemetry.max_queue_age_ms.get())),
            config.telemetry.max_subscribers,
        );
        info!(
            "Runtime configuration initialized. Starting with driver_id={}",
            handler.get()
        );

        // Messaging
        let messaging_wrapper = MessagingLayerEntrypoint::init(config).await?;
        info!("Messaging layer initialized");

        // Processing
        let mut processing_layer = ProcessingLayerEntrypoint::init(
            messaging_wrapper.sensor_sink_rx,
            messaging_wrapper.control_sink_rx,
            messaging_wrapper.dlq_tx,
            handler,
            shutdown_tx,
            messaging_wrapper.control_offsets.clone(),
            rayon_pool,
            preview.clone(),
        )?;
        info!("Processing layer initialized");

        // Persistence
        let mut persistence_wrapper =
            PersistenceLayerEntryPoint::init(
                config,
                processing_layer.processed_data_rx,
                handler,
                messaging_wrapper.sensor_offsets.clone(),
            )
            .await?;
        info!("Persistence layer initialized");

        // GCP
        let gcp_wrapper = GcpLayerEntrypoint::init(&config.race.race_id, &config.gcp).await?;
        let (storage, pubsub) = match gcp_wrapper {
            Some(w) => (Some(w.gcs_storage_manager), Some(w.pubsub_manager)),
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
            messaging_wrapper.sensor_offsets.clone(),
            messaging_wrapper.control_offsets.clone(),
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
        let mut control_tasks = NamedControlTasks::new();
        let write_metrics_stop = CancellationToken::new();
        if config.gcp.enabled {
            let write_metrics = Arc::clone(&persistence_wrapper.write_metrics);
            let project_id = config.gcp.project_id.clone();
            let max_retries = config.gcp.max_retries;
            let stop = write_metrics_stop.clone();
            control_tasks.spawn(WRITE_METRICS, async move {
                persistence::write_metrics_publisher::publish_write_metrics(
                    project_id,
                    max_retries,
                    write_metrics,
                    config.gcp.allow_unknown_gce_metrics,
                    stop,
                )
                .await;
            });
        }
        control_tasks.spawn(CONTROL_FILTER, async move {
            processing_layer.control_filter.ingest().await;
        });
        control_tasks.spawn(CONTROL_SINK, async move {
            messaging_wrapper.control_sink.ingest().await;
        });
        control_tasks.spawn(DLQ_HANDLER, async move {
            messaging_wrapper.dlq_handler.ingest().await;
        });
        let heartbeat_write_metrics = Arc::clone(&persistence_wrapper.write_metrics);
        control_tasks.spawn(HEARTBEAT, async move {
            let _ = messaging_wrapper
                .heartbeat_handler
                .keep_alive(heartbeat_write_metrics)
                .await;
        });
        info!("Control plane initialized");

        run_tasks(control_tasks, monitor, shutdown_rx, write_metrics_stop).await?;
        Ok::<(), anyhow::Error>(())
    })?;
    info!("Run complete");
    Ok(())
}

struct NamedControlTasks {
    join_set: JoinSet<&'static str>,
    abort_handles: HashMap<&'static str, AbortHandle>,
}

impl NamedControlTasks {
    fn new() -> Self {
        Self {
            join_set: JoinSet::new(),
            abort_handles: HashMap::new(),
        }
    }

    fn spawn<F>(&mut self, name: &'static str, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.join_set.spawn(async move {
            fut.await;
            name
        });
        self.abort_handles.insert(name, handle);
    }

    fn abort(&mut self, name: &str) {
        if let Some(handle) = self.abort_handles.remove(name) {
            handle.abort();
        }
    }

    fn abort_all(&mut self) {
        self.join_set.abort_all();
        self.abort_handles.clear();
    }

    fn has_task(&self, name: &str) -> bool {
        self.abort_handles.contains_key(name)
    }

    async fn join_next(&mut self) -> Option<Result<&'static str, tokio::task::JoinError>> {
        let result = self.join_set.join_next().await?;
        if let Ok(name) = &result {
            self.abort_handles.remove(name);
        }
        Some(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisorPhase {
    Running,
    /// SHUTDOWN received or OS signal: drain in progress / export may follow.
    Draining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlTaskAction {
    /// Unexpected end of an essential control task before race end / signal drain.
    Fatal,
    /// Non-essential task ended; keep running.
    Ignore,
    /// Expected during drain (e.g. control filter after SHUTDOWN, DLQ after senders drop).
    ExpectedDuringDrain,
}

fn is_essential_control_task(name: &str) -> bool {
    name != WRITE_METRICS
}

fn control_task_action(phase: SupervisorPhase, name: &str) -> ControlTaskAction {
    match phase {
        SupervisorPhase::Running => {
            if is_essential_control_task(name) {
                ControlTaskAction::Fatal
            } else {
                ControlTaskAction::Ignore
            }
        }
        SupervisorPhase::Draining => match name {
            DLQ_HANDLER | CONTROL_FILTER => ControlTaskAction::ExpectedDuringDrain,
            _ => ControlTaskAction::Ignore,
        },
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RunOutcome {
    /// Carries the offset of the SHUTDOWN event, which stays uncommitted until the export is over.
    RaceShutdown(SourceOffset),
    SignalDrain,
    FatalDataPipeline,
    FatalControlTask(&'static str),
    FatalShutdownSenderDropped,
}

async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn ignore_signals_until_cancelled(cancel: CancellationToken) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = wait_for_termination_signal() => {
                warn!("Termination signal received during shutdown export; ignoring");
            }
        }
    }
}

/// After data tasks have dropped their DLQ senders, abort the control filter if it still holds a
/// sender (signal path / late exit), then wait for the DLQ handler to finish its queue.
async fn await_dlq_drained(control_tasks: &mut NamedControlTasks, drain_timeout: Duration) {
    control_tasks.abort(CONTROL_FILTER);

    if !control_tasks.has_task(DLQ_HANDLER) {
        info!("DLQ handler already finished");
        return;
    }

    let deadline = Instant::now() + drain_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            error!("DLQ drain timed out; proceeding without waiting for remaining DLQ records");
            return;
        }

        match timeout(remaining, control_tasks.join_next()).await {
            Ok(Some(Ok(name))) => match control_task_action(SupervisorPhase::Draining, name) {
                ControlTaskAction::ExpectedDuringDrain if name == DLQ_HANDLER => {
                    info!("DLQ handler finished; queue drained");
                    return;
                }
                ControlTaskAction::ExpectedDuringDrain | ControlTaskAction::Ignore => {
                    info!(task = name, "Control task ended during DLQ drain");
                    if !control_tasks.has_task(DLQ_HANDLER) {
                        info!("DLQ handler already finished");
                        return;
                    }
                }
                ControlTaskAction::Fatal => {
                    warn!(task = name, "Unexpected control task end during DLQ drain");
                    if !control_tasks.has_task(DLQ_HANDLER) {
                        return;
                    }
                }
            },
            Ok(Some(Err(error))) => {
                if error.is_cancelled() {
                    continue;
                }
                warn!(?error, "Control task join error during DLQ drain");
                if !control_tasks.has_task(DLQ_HANDLER) {
                    return;
                }
            }
            Ok(None) => {
                info!("No control tasks left during DLQ drain");
                return;
            }
            Err(_) => {
                error!("DLQ drain timed out; proceeding without waiting for remaining DLQ records");
                return;
            }
        }
    }
}

/// After the data pipeline has drained, cancel the write-metrics publisher so it sends one final
/// sample, then wait briefly for that task to finish. Publish failures are non-fatal.
async fn finalize_write_metrics(
    control_tasks: &mut NamedControlTasks,
    write_metrics_stop: &CancellationToken,
    final_timeout: Duration,
) {
    if !control_tasks.has_task(WRITE_METRICS) {
        return;
    }

    write_metrics_stop.cancel();
    let deadline = Instant::now() + final_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            warn!("Final write-metrics publish timed out; continuing");
            control_tasks.abort(WRITE_METRICS);
            return;
        }

        match timeout(remaining, control_tasks.join_next()).await {
            Ok(Some(Ok(name))) if name == WRITE_METRICS => {
                info!("Write-metrics publisher finished after final sample");
                return;
            }
            Ok(Some(Ok(name))) => {
                info!(task = name, "Control task ended while awaiting final metrics");
                if !control_tasks.has_task(WRITE_METRICS) {
                    return;
                }
            }
            Ok(Some(Err(error))) => {
                if error.is_cancelled() {
                    continue;
                }
                warn!(?error, "Control task join error while awaiting final metrics");
                if !control_tasks.has_task(WRITE_METRICS) {
                    return;
                }
            }
            Ok(None) => return,
            Err(_) => {
                warn!("Final write-metrics publish timed out; continuing");
                control_tasks.abort(WRITE_METRICS);
                return;
            }
        }
    }
}

async fn run_tasks(
    mut control_tasks: NamedControlTasks,
    mut monitor: Monitor,
    mut shutdown_rx: oneshot::Receiver<SourceOffset>,
    write_metrics_stop: CancellationToken,
) -> Result<()> {
    let mut data_task_termination_rx = monitor.take_data_task_termination_receiver();
    info!("Core running...");

    // Biased: prefer race SHUTDOWN over OS signal so an in-flight race end still exports.
    let outcome = loop {
        tokio::select! {
            biased;
            res = &mut shutdown_rx => {
                match res {
                    Ok(at) => break RunOutcome::RaceShutdown(at),
                    Err(_) => {
                        error!("Shutdown sender dropped before signaling; stopping Core without export");
                        break RunOutcome::FatalShutdownSenderDropped;
                    }
                }
            }
            () = wait_for_termination_signal() => {
                break RunOutcome::SignalDrain;
            }
            data_task_termination = data_task_termination_rx.recv() => {
                if data_task_termination.is_some() {
                    // SHUTDOWN may have raced with pipeline teardown; prefer export if signaled.
                    if let Ok(at) = shutdown_rx.try_recv() {
                        break RunOutcome::RaceShutdown(at);
                    }
                    error!("Data pipeline task terminated unexpectedly; stopping Core without export");
                    break RunOutcome::FatalDataPipeline;
                }
            }
            res = control_tasks.join_next() => {
                match res {
                    Some(Ok(name)) => {
                        // Filter exits right after signaling SHUTDOWN; oneshot may already be ready.
                        if matches!(name, CONTROL_FILTER | DLQ_HANDLER) {
                            if let Ok(at) = shutdown_rx.try_recv() {
                                break RunOutcome::RaceShutdown(at);
                            }
                        }
                        match control_task_action(SupervisorPhase::Running, name) {
                            ControlTaskAction::Fatal => {
                                error!(
                                    task = name,
                                    "Essential control task ended unexpectedly; stopping Core without export"
                                );
                                break RunOutcome::FatalControlTask(name);
                            }
                            ControlTaskAction::Ignore => {
                                info!(task = name, "Non-essential control task ended; continuing");
                            }
                            ControlTaskAction::ExpectedDuringDrain => {
                                info!(task = name, "Control task ended during drain");
                            }
                        }
                    }
                    Some(Err(error)) => {
                        // Release uses panic=abort; JoinError here is mainly cancellation in tests.
                        error!(
                            ?error,
                            "Control task failed; stopping Core without export"
                        );
                        break RunOutcome::FatalControlTask("control_task");
                    }
                    None => {
                        error!("All control tasks ended unexpectedly; stopping Core without export");
                        break RunOutcome::FatalControlTask("control_tasks");
                    }
                }
            }
        }
    };

    let write_metrics_final_timeout = monitor.write_metrics_final_timeout();

    match outcome {
        RunOutcome::RaceShutdown(shutdown_at) => {
            info!("SHUTDOWN received; draining pipeline before export");
            let signal_cancel = CancellationToken::new();
            let ignore = tokio::spawn(ignore_signals_until_cancelled(signal_cancel.clone()));
            monitor.drain_data_pipeline().await;
            await_dlq_drained(&mut control_tasks, monitor.data_pipeline_drain_timeout()).await;
            monitor.commit_offsets().await;
            finalize_write_metrics(
                &mut control_tasks,
                &write_metrics_stop,
                write_metrics_final_timeout,
            )
            .await;
            // Heartbeat keeps running during export.
            monitor.execute_exports().await;
            // Whether the exports succeeded or not: a restart must not repeat the race end.
            monitor.commit_shutdown(shutdown_at).await;
            signal_cancel.cancel();
            let _ = ignore.await;
            control_tasks.abort_all();
            Ok(())
        }
        RunOutcome::SignalDrain => {
            info!("Termination signal received; draining without export");
            monitor.drain_data_pipeline().await;
            await_dlq_drained(&mut control_tasks, monitor.data_pipeline_drain_timeout()).await;
            monitor.commit_offsets().await;
            finalize_write_metrics(
                &mut control_tasks,
                &write_metrics_stop,
                write_metrics_final_timeout,
            )
            .await;
            control_tasks.abort_all();
            Ok(())
        }
        RunOutcome::FatalDataPipeline
        | RunOutcome::FatalControlTask(_)
        | RunOutcome::FatalShutdownSenderDropped => {
            control_tasks.abort_all();
            monitor.stop_data_pipeline();
            bail!("Core exiting without export due to unexpected failure");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn essential_control_end_is_fatal_while_running() {
        for name in [CONTROL_SINK, CONTROL_FILTER, DLQ_HANDLER, HEARTBEAT] {
            assert_eq!(
                control_task_action(SupervisorPhase::Running, name),
                ControlTaskAction::Fatal
            );
        }
    }

    #[test]
    fn write_metrics_end_is_ignored() {
        assert_eq!(
            control_task_action(SupervisorPhase::Running, WRITE_METRICS),
            ControlTaskAction::Ignore
        );
        assert_eq!(
            control_task_action(SupervisorPhase::Draining, WRITE_METRICS),
            ControlTaskAction::Ignore
        );
    }

    #[test]
    fn dlq_and_filter_end_expected_during_drain() {
        assert_eq!(
            control_task_action(SupervisorPhase::Draining, DLQ_HANDLER),
            ControlTaskAction::ExpectedDuringDrain
        );
        assert_eq!(
            control_task_action(SupervisorPhase::Draining, CONTROL_FILTER),
            ControlTaskAction::ExpectedDuringDrain
        );
    }
}
