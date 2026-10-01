use crate::configuration::GcpConfig;
use crate::gcp::pubsub::PubSubManager;
use crate::gcp::storage::GcsStorageManager;
use anyhow::{Context, Result};
use compact_str::CompactString;
use tracing::warn;

pub struct GcpLayerEntrypoint {
    pub pubsub_manager: PubSubManager,
    pub gcs_storage_manager: GcsStorageManager,
}

impl GcpLayerEntrypoint {
    pub async fn init(
        race_id: &'static CompactString,
        gcp_config: &'static GcpConfig,
    ) -> Result<Option<Self>> {
        if !gcp_config.enabled {
            warn!("GCP marked as disabled in configuration. Skipping initialization.");
            return Ok(None);
        }
        let pubsub_manager = PubSubManager::new(race_id, gcp_config)
            .await
            .context("Failed to create Pub/Sub manager")?;
        let gcs_storage_manager = GcsStorageManager::new(race_id, gcp_config)
            .await
            .context("Failed to create GCS storage manager")?;
        Ok(Some(Self {
            pubsub_manager,
            gcs_storage_manager,
        }))
    }
}
