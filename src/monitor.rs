use crate::configuration::CoreConfig;
use log::error;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info};

pub struct Monitor {
    data_cancel_token: CancellationToken,
    data_tracker: TaskTracker,
    config: &'static CoreConfig,
}

impl Monitor {
    #[must_use]
    pub fn new(
        data_cancel_token: CancellationToken,
        data_tracker: TaskTracker,
        config: &'static CoreConfig,
    ) -> Self {
        Self {
            data_cancel_token,
            data_tracker,
            config,
        }
    }
    pub fn track_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        <F as Future>::Output: Send,
    {
        self.data_tracker.spawn(task);
    }

    pub fn shutdown_signal_received(&self) {}

    pub async fn execute_shutdown_sequence(&self) {
        info!("Stopping data pipeline ingestion");
        self.data_cancel_token.cancel();
        info!("Waiting for data pipeline drain");
        self.data_tracker.close();

        let drain_timeout =
            Duration::from_secs(self.config.runtime.data_pipeline_drain_timeout_s as u64);
        match timeout(drain_timeout, self.data_tracker.wait()).await {
            Ok(()) => {
                info!("Data pipeline drained");
            }
            Err(_) => {
                error!("Data pipeline drain timed out. Still proceeding with export.");
            }
        }

        let export_kafka = async {

        };

        let export_db = async {

        };


        todo!();

    }

    async fn upload_to_gcs(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        todo!();
    }

    async fn notify_shutdown_complete(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        todo!();
    }
}
