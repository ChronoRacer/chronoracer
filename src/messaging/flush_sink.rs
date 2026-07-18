use crate::configuration::CoreConfig;
use crate::messaging::partial_message::PartialMessage;
use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};

pub struct FlushSink {
    consumer: StreamConsumer,
    flush_tx: tokio::sync::mpsc::Sender<PartialMessage>,
}

impl FlushSink {
    pub fn new(config: &CoreConfig, flush_tx: tokio::sync::mpsc::Sender<PartialMessage>) -> Self {
        let kafka_config = &config.kafka;
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "false") // TODO
            .set("session.timeout.ms", "6000")
            .create()
            .expect("Failed to create Flush sensor data consumer");
        consumer
            .subscribe(&[&kafka_config.sensor_data_flush_topic])
            .expect("Failed to subscribe to Flush sensor data topic");
        Self { consumer, flush_tx }
    }

    pub async fn ingest(self: &Self) {
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

impl Drop for FlushSink {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}
