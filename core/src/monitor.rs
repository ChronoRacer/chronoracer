use crate::configuration::CoreConfig;
use crate::gcp;
use crate::gcp::pubsub::GcsPubSubManager;
use crate::gcp::storage::Bucket;
use crate::messaging::kafka_exporter::KafkaExporter;
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

pub struct Monitor<'a> {
    data_cancel_token: CancellationToken,
    data_tracker: TaskTracker,
    config: &'static CoreConfig,
    database_exporter: InfluxDbExporter,
    kafka_exporter: KafkaExporter<'a>,
    gcs_storage_manager: Option<GcsStorageManager<'a>>,
    gcp_pubsub_manager: Option<GcsPubSubManager<'a>>,
    notify_shutdown_on_failed_db_export: bool,
    notify_shutdown_on_failed_kafka_export: bool,
    data_task_termination_sender: UnboundedSender<()>,
    data_task_termination_receiver: Option<UnboundedReceiver<()>>,
}

impl<'a> Monitor<'a> {
    #[must_use]
    pub fn new(
        data_cancel_token: CancellationToken,
        data_task_tracker: TaskTracker,
        config: &'static CoreConfig,
        database_exporter: InfluxDbExporter,
        kafka_exporter: KafkaExporter<'a>,
        gcs_storage_manager: Option<GcsStorageManager<'a>>,
        gcp_pubsub_manager: Option<GcsPubSubManager<'a>>,
        notify_shutdown_on_failed_db_export: bool,
        notify_shutdown_on_failed_kafka_export: bool,
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
            gcp_pubsub_manager,
            notify_shutdown_on_failed_db_export,
            notify_shutdown_on_failed_kafka_export,
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

    pub async fn execute_shutdown_sequence(&self) {
        info!("Stopping data pipeline ingestion");
        self.data_cancel_token.cancel();
        info!("Waiting for data pipeline drain");
        self.data_tracker.close();

        let drain_timeout =
            Duration::from_secs(u64::from(self.config.runtime.data_pipeline_drain_timeout_s));
        match timeout(drain_timeout, self.data_tracker.wait()).await {
            Ok(()) => {
                info!("Data pipeline drained");
            }
            Err(_) => {
                error!("Data pipeline drain timed out. Still proceeding with export.");
            }
        }

        let export_kafka = self.kafka_exporter.export_kafka();

        let export_database_future = self.database_exporter.export_database();
        let (db_export, kafka_export) = tokio::join!(export_database_future, export_kafka);

        let kafka_succeeded = kafka_export.map_or_else(
            |e| {
                error!("Error exporting to Kafka: {e}");
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
                match Self::upload_to_gcs(self.gcs_storage_manager.as_ref(), paths).await {
                    Ok(()) => true,
                    Err(e) => {
                        error!("Failed to upload to GCS: {e}");
                        false
                    }
                }
            }
            Err(e) => {
                error!("Error exporting database: {e}; will not upload");
                false
            }
        };

        let kafka_notification = export_notification_allowed(
            kafka_succeeded,
            self.notify_shutdown_on_failed_kafka_export,
        );
        let db_notification = export_notification_allowed(
            db_succeeded,
            self.notify_shutdown_on_failed_db_export,
        );

        if kafka_notification && db_notification {
            info!("Sending shutdown notification...");
            if let Err(e) = Self::notify_shutdown_complete(self.gcp_pubsub_manager.as_ref()).await {
                error!("Failed to send shutdown notification: {e}");
            }
        } else {
            error!("Shutdown notification not sent (Kafka: {kafka_notification}, DB: {db_notification})");
        }

        info!("Shutdown sequence complete");
    }

    async fn upload_to_gcs(
        gcs_storage_manager: Option<&GcsStorageManager<'a>>,
        file_paths: Vec<PathBuf>,
    ) -> Result<()> {
        if file_paths.is_empty() {
            warn!("GCS upload skipped - no files to upload");
            return Ok(());
        }
        if let Some(gcs_storage_manager) = gcs_storage_manager {
            info!("Uploading database export to GCS");
            return gcs_storage_manager
                .upload_files(file_paths, Bucket::ExportBucket)
                .await;
        }
        warn!("GCS upload skipped - storage manager not provided");
        Ok(())
    }

    async fn notify_shutdown_complete(
        gcp_pubsub_manager: Option<&GcsPubSubManager<'a>>,
    ) -> Result<()> {
        if let Some(gcp_pubsub_manager) = gcp_pubsub_manager {
            info!("Publishing shutdown notification");
            return gcp_pubsub_manager.publish_export_done_message().await;
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
        let result = Monitor::upload_to_gcs(None, vec![PathBuf::from("export.parquet")]).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn upload_to_gcs_succeeds_when_no_files() {
        let result = Monitor::upload_to_gcs(None, vec!()).await;

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
