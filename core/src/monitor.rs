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
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

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
        }
    }
    pub fn track_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        <F as Future>::Output: Send,
    {
        self.data_tracker.spawn(task);
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
        let kafka_notification = if kafka_export.is_err() {
            self.notify_shutdown_on_failed_kafka_export
        } else {
            true
        };
        match kafka_export {
            Ok(()) => {
                info!("Kafka export complete");
            }
            Err(e) => {
                error!("Error exporting to Kafka: {e}");
            }
        }

        let db_notification = if db_export.is_err() {
            self.notify_shutdown_on_failed_db_export
        } else {
            false
        };
        match db_export {
            Ok(paths) => {
                info!("Database export complete with {} files", paths.len());
                if let Err(e) = Self::upload_to_gcs(self.gcs_storage_manager.as_ref(), paths).await
                {
                    error!("Failed to upload to GCS: {e}");
                }
            }
            Err(e) => {
                error!("Error exporting database: {e} ; will not upload");
            }
        }

        if kafka_notification && db_notification {
            info!("Sending shutdown notification...");
            if let Err(e) = Self::notify_shutdown_complete(self.gcp_pubsub_manager.as_ref()).await {
                error!("Failed to send shutdown notification: {e}");
            }
        }

        info!("Shutdown sequence complete");
    }

    async fn upload_to_gcs(
        gcs_storage_manager: Option<&GcsStorageManager<'a>>,
        file_paths: Vec<PathBuf>,
    ) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upload_to_gcs_succeeds_when_storage_is_not_configured() {
        let result = Monitor::upload_to_gcs(None, vec![PathBuf::from("export.parquet")]).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_shutdown_succeeds_when_pubsub_is_not_configured() {
        let result = Monitor::notify_shutdown_complete(None).await;

        assert!(result.is_ok());
    }
}
