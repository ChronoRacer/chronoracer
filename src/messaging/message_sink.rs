use crate::configuration::KafkaConfig;
use crate::messaging::partial_message::PartialMessage;
use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tracing::{error, warn};

pub type SinkRx = tokio::sync::mpsc::Receiver<PartialMessage>;
pub type SinkTx = tokio::sync::mpsc::Sender<PartialMessage>;


pub struct MessageSink {
    consumer: StreamConsumer,
    flush_tx: SinkTx,
}

impl MessageSink {

    #[must_use]
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: &[&str],
        flush_tx: SinkTx,
    ) -> Self {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "6000")
            .create()
            .expect("Failed to create message sink consumer");
        consumer
            .subscribe(topics)
            .unwrap_or_else(|_| panic!("Failed to subscribe to {} topics", topics.join(",")));
        Self { consumer, flush_tx }
    }

    pub async fn ingest(&mut self) {
        let mut message_stream = self.consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(borrowed_message) => {
                    if let Err(e) = self
                        .flush_tx
                        .send(PartialMessage::from(borrowed_message))
                        .await
                    {
                        error!(
                            "Ingestion pipeline broken downstream: {e}"
                        );
                        if self.flush_tx.is_closed() {
                            warn!("Downstream pipeline closed. Breaking ingestion loop");
                            break;
                        }
                    }
                }
                Err(kafka_error) => {
                    error!("Kafka error occurred while polling: {kafka_error}");
                }
            }
        }
    }
}

impl Drop for MessageSink {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}
