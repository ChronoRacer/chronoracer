use crate::configuration::GcpConfig;
use crate::gcp::pubsub::GcsPubSubManager;
use crate::gcp::storage::GcsStorageManager;
use compact_str::CompactString;
use tracing::warn;

pub struct GcpLayerEntrypoint<'a> {
    pub gcs_pub_sub_manager: GcsPubSubManager<'a>,
    pub gcs_storage_manager: GcsStorageManager<'a>,
}

impl<'a> GcpLayerEntrypoint<'a> {
    #[must_use]
    pub async fn init(
        race_id: &'static CompactString,
        gcp_config: &'static GcpConfig,
    ) -> Option<Self> {
        if !gcp_config.enabled {
            warn!("GCP marked as disabled in configuration. Skipping initialization.");
            return None;
        }
        let gcs_pub_sub_manager = GcsPubSubManager::new(&race_id, &gcp_config)
            .await
            .expect("Failed to create GCS PubSub manager");
        let gcs_storage_manager = GcsStorageManager::new(&race_id, &gcp_config)
            .await
            .expect("Failed to create GCS Storage manager");
        Some(Self {
            gcs_pub_sub_manager,
            gcs_storage_manager,
        })
    }
}
