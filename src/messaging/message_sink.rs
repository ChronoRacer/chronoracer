use crate::configuration::KafkaConfig;
use crate::messaging::partial_message::PartialMessage;
use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};

pub struct MessageSink {
    consumer: StreamConsumer,
    flush_tx: tokio::sync::mpsc::Sender<PartialMessage>,
}

impl MessageSink {
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: &[&str],
        flush_tx: tokio::sync::mpsc::Sender<PartialMessage>,
    ) -> Self {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "false") // TODO
            .set("session.timeout.ms", "6000")
            .create()
            .expect("Failed to create message sink consumer");
        consumer
            .subscribe(topics)
            .expect(format!("Failed to subscribe to {} topics", topics.join(",")).as_str());
        Self { consumer, flush_tx }
    }

    pub async fn ingest(self: &mut Self) {
        let mut message_stream = self.consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(borrowed_message) => {
                    if self
                        .flush_tx
                        .send(PartialMessage::from(borrowed_message))
                        .await
                        .is_err()
                    {
                        eprintln!(
                            "Pipeline pipeline pipeline broken downstream. Shutting down consumer."
                        );
                        break;
                    }
                }
                Err(kafka_error) => {
                    eprintln!("Kafka error occurred while polling: {}", kafka_error);
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
