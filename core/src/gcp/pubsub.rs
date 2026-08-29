use crate::configuration::GcpConfig;
use anyhow::Result;
use compact_str::CompactString;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use serde_json::json;
use std::sync::Arc;
use tracing::info;

pub struct GcsPubSubManager<'a> {
    publisher: Arc<Publisher>,
    race_id: &'a CompactString,
}

impl<'a> GcsPubSubManager<'a> {
    pub async fn new(race_id: &'a CompactString, gcp_config: &'a GcpConfig) -> Result<Self> {
        let publisher = Publisher::builder(&gcp_config.pubsub_topic_id)
            .build()
            .await?;
        Ok(Self {
            publisher: Arc::new(publisher),
            race_id,
        })
    }

    pub async fn publish_export_done_message(&self) -> Result<()> {
        let payload = json!({ "race_id":  self.race_id.to_string() });

        let message = Message::new()
            .set_ordering_key("EXPORT_DONE")
            .set_data(payload.to_string());

        let message_id = self.publisher.publish(message).await?;
        info!("Published shutdown message, id={message_id}");
        self.publisher.flush().await;
        Ok(())
    }
}
