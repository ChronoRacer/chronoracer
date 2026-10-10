use crate::configuration::GcpConfig;
use anyhow::{Result, anyhow};
use compact_str::CompactString;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::info;

pub struct PubSubManager {
    publisher: Arc<Publisher>,
    race_id: &'static CompactString,
    publish_timeout: Duration,
}

fn topic_resource_name(project_id: &str, topic_id: &str) -> String {
    format!("projects/{project_id}/topics/{topic_id}")
}

impl PubSubManager {
    pub async fn new(
        race_id: &'static CompactString,
        gcp_config: &'static GcpConfig,
    ) -> Result<Self> {
        let topic_name = topic_resource_name(&gcp_config.project_id, &gcp_config.pubsub_topic_id);
        let publisher = Publisher::builder(&topic_name).build().await?;
        Ok(Self {
            publisher: Arc::new(publisher),
            race_id,
            publish_timeout: Duration::from_secs(u64::from(
                gcp_config.pubsub_publish_timeout_s.get(),
            )),
        })
    }

    pub async fn publish_export_done_message(&self) -> Result<()> {
        let payload = json!({ "race_id":  self.race_id.to_string() });

        let message = Message::new()
            .set_ordering_key("EXPORT_DONE")
            .set_data(payload.to_string());

        let publish = async {
            let message_id = self.publisher.publish(message).await?;
            info!("Published shutdown message, id={message_id}");
            self.publisher.flush().await;
            Ok(())
        };

        match timeout(self.publish_timeout, publish).await {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "Pub/Sub export-done publish timed out after {:?}",
                self.publish_timeout
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn resolves_topic_id_to_pubsub_resource_name() {
        let project_id = "chronoracer";
        let topic_id = "export-done";

        assert_eq!(
            super::topic_resource_name(project_id, topic_id),
            "projects/chronoracer/topics/export-done"
        );
    }
}
