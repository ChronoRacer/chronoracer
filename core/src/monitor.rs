use crate::configuration::CoreConfig;
use crate::gcp;
use crate::gcp::pubsub::PubSubManager;
use crate::messaging::kafka_exporter::KafkaExporter;
use crate::messaging::offset_ledger::OffsetLedger;
use crate::messaging::partial_message::SourceOffset;
use crate::persistence::database_exporter::InfluxDbExporter;
use anyhow::Result;
use gcp::storage;
use std::path::PathBuf;
use std::time::Duration;
use storage::GcsStorageManager;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

struct DataTaskTerminationNotifier {
    sender: UnboundedSender<()>,
}

impl Drop for DataTaskTerminationNotifier {
    fn drop(&mut self) {
        let _ = self.sender.send(());
    }
}

pub struct Monitor {
    data_cancel_token: CancellationToken,
    data_tracker: TaskTracker,
    config: &'static CoreConfig,
    database_exporter: InfluxDbExporter,
    kafka_exporter: KafkaExporter,
    gcs_storage_manager: Option<GcsStorageManager>,
    pubsub_manager: Option<PubSubManager>,
    sensor_offsets: OffsetLedger,
    control_offsets: OffsetLedger,
    data_task_termination_sender: UnboundedSender<()>,
    data_task_termination_receiver: Option<UnboundedReceiver<()>>,
}

impl Monitor {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_cancel_token: CancellationToken,
        data_task_tracker: TaskTracker,
        config: &'static CoreConfig,
        database_exporter: InfluxDbExporter,
        kafka_exporter: KafkaExporter,
        gcs_storage_manager: Option<GcsStorageManager>,
        pubsub_manager: Option<PubSubManager>,
        sensor_offsets: OffsetLedger,
        control_offsets: OffsetLedger,
    ) -> Self {
        let (data_task_termination_sender, data_task_termination_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        Self {
            data_cancel_token,
            data_tracker: data_task_tracker,
            config,
            database_exporter,
            kafka_exporter,
            gcs_storage_manager,
            pubsub_manager,
            sensor_offsets,
            control_offsets,
            data_task_termination_sender,
            data_task_termination_receiver: Some(data_task_termination_receiver),
        }
    }
    pub fn track_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        <F as Future>::Output: Send + 'static,
    {
        let termination_notifier = DataTaskTerminationNotifier {
            sender: self.data_task_termination_sender.clone(),
        };
        self.data_tracker.spawn(async move {
            let _termination_notifier = termination_notifier;
            task.await;
        });
    }

    pub fn take_data_task_termination_receiver(&mut self) -> UnboundedReceiver<()> {
        self.data_task_termination_receiver
            .take()
            .expect("data task termination receiver already taken")
    }

    pub fn stop_data_pipeline(&self) {
        self.data_cancel_token.cancel();
        self.data_tracker.close();
    }

    pub fn data_pipeline_drain_timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.config.runtime.data_pipeline_drain_timeout_s))
    }

    pub fn write_metrics_final_timeout(&self) -> Duration {
        Duration::from_secs(u64::from(
            self.config.runtime.write_metrics_final_timeout_s.get(),
        ))
    }

    /// Synchronously commits what both consumers have stored, each bounded by
    /// `runtime.offset_final_commit_timeout_s`. Call after the data pipeline and the DLQ are drained:
    /// only then is everything the pipeline will ever settle stored. Failure is logged; what stays
    /// uncommitted is replayed after a restart.
    pub async fn commit_offsets(&self) {
        let timeout = Duration::from_secs(u64::from(
            self.config.runtime.offset_final_commit_timeout_s.get(),
        ));
        self.sensor_offsets.commit_final(timeout).await;
        self.control_offsets.commit_final(timeout).await;
    }

    /// The race-end sequence is over (exports succeeded or not): settles the SHUTDOWN event, which
    /// the control filter kept back, and commits it.
    pub async fn commit_shutdown(&self, shutdown: SourceOffset) {
        self.control_offsets
            .settle_one(shutdown.partition, shutdown.offset);
        self.control_offsets
            .commit_final(Duration::from_secs(u64::from(
                self.config.runtime.offset_final_commit_timeout_s.get(),
            )))
            .await;
    }

    /// Stops the data sink and waits for tracked data tasks to finish (bounded by drain timeout).
    pub async fn drain_data_pipeline(&self) {
        info!("Stopping data pipeline ingestion");
        self.data_cancel_token.cancel();
        info!("Waiting for data pipeline drain");
        self.data_tracker.close();

        match timeout(self.data_pipeline_drain_timeout(), self.data_tracker.wait()).await {
            Ok(()) => {
                info!("Data pipeline drained");
            }
            Err(_) => {
                error!("Data pipeline drain timed out. Still proceeding.");
            }
        }
    }

    /// Kafka + Influx export, GCS upload, and optional export-done Pub/Sub. Call only after the
    /// data pipeline and DLQ queue have been drained.
    pub async fn execute_exports(&self) {
        let export_kafka = self.kafka_exporter.export_kafka();

        let export_database_future = self.database_exporter.export_database();
        let (db_export, kafka_export) = tokio::join!(export_database_future, export_kafka);

        let kafka_succeeded = kafka_export.map_or_else(
            |error| {
                error!(?error, "Error exporting to Kafka");
                false
            },
            |()| {
                info!("Kafka export complete");
                true
            },
        );

        let db_succeeded = match db_export {
            Ok(paths) => {
                info!("Database export complete with {} files", paths.len());
                match Self::upload_to_gcs(
                    self.gcs_storage_manager.as_ref(),
                    paths,
                    &self.config.persistence.export.nested_path_in_bucket,
                )
                .await
                {
                    Ok(()) => true,
                    Err(error) => {
                        error!(?error, "Failed to upload to GCS");
                        false
                    }
                }
            }
            Err(error) => {
                error!(%error, "Error exporting database; will not upload");
                false
            }
        };

        let kafka_notification = export_notification_allowed(
            kafka_succeeded,
            self.config.runtime.notify_shutdown_on_failed_kafka_export,
        );
        let db_notification = export_notification_allowed(
            db_succeeded,
            self.config.runtime.notify_shutdown_on_failed_db_export,
        );

        if kafka_notification && db_notification {
            info!("Sending shutdown notification...");
            if let Err(error) = Self::notify_shutdown_complete(self.pubsub_manager.as_ref()).await {
                error!(?error, "Failed to send shutdown notification");
            }
        } else {
            error!(
                "Shutdown notification not sent (Kafka: {kafka_notification}, DB: {db_notification})"
            );
        }

        info!("Shutdown sequence complete");
    }

    async fn upload_to_gcs(
        gcs_storage_manager: Option<&GcsStorageManager>,
        file_paths: Vec<PathBuf>,
        nested_path_in_bucket: &str,
    ) -> Result<()> {
        if file_paths.is_empty() {
            warn!("GCS upload skipped - no files to upload");
            return Ok(());
        }
        if let Some(gcs_storage_manager) = gcs_storage_manager {
            info!("Uploading database export to GCS");
            return gcs_storage_manager
                .upload_files(file_paths, nested_path_in_bucket)
                .await;
        }
        warn!("GCS upload skipped - storage manager not provided");
        Ok(())
    }

    async fn notify_shutdown_complete(pubsub_manager: Option<&PubSubManager>) -> Result<()> {
        if let Some(pubsub_manager) = pubsub_manager {
            info!("Publishing shutdown notification");
            return pubsub_manager.publish_export_done_message().await;
        }
        warn!("Shutdown message publish skipped - pubsub manager not provided");
        Ok(())
    }
}

fn export_notification_allowed(export_succeeded: bool, notify_on_failure: bool) -> bool {
    export_succeeded || notify_on_failure
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upload_to_gcs_succeeds_when_storage_is_not_configured() {
        let result =
            Monitor::upload_to_gcs(None, vec![PathBuf::from("export.parquet")], "export/db").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn upload_to_gcs_succeeds_when_no_files() {
        let result = Monitor::upload_to_gcs(None, vec![], "export/db").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_shutdown_succeeds_when_pubsub_is_not_configured() {
        let result = Monitor::notify_shutdown_complete(None).await;

        assert!(result.is_ok());
    }

    #[test]
    fn successful_exports_allow_export_done_notification() {
        let kafka_notification = export_notification_allowed(true, false);
        let db_notification = export_notification_allowed(true, false);

        assert!(kafka_notification && db_notification);
    }
}
