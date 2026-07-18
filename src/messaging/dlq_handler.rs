use crate::configuration::CoreConfig;
use crate::messaging::partial_message::PartialMessageWithError;
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub struct DLQHandler<'a> {
    producer: FutureProducer,
    unknown_key: &'a [u8],
    dlq_topic: &'a str,
    dlq_timeout: rdkafka::util::Timeout,
    dlq_rx: tokio::sync::mpsc::Receiver<PartialMessageWithError>,
}

impl<'a> DLQHandler<'a> {
    pub fn new(
        config: &'a CoreConfig,
        dlq_rx: tokio::sync::mpsc::Receiver<PartialMessageWithError>,
    ) -> Self {
        let kafka_config = &config.kafka;
        let producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "true") // TODO
            .set("session.timeout.ms", "6000")
            .create()
            .expect("Failed to create DLQ handler producer");
        Self {
            producer,
            unknown_key: &kafka_config.dlq_config.dlq_unknown_key.to_bytes(),
            dlq_topic: &kafka_config.dlq_config.dlq_topic,
            dlq_timeout: rdkafka::util::Timeout::from(Duration::from_secs(
                kafka_config.dlq_config.publish_timeout_sec as u64,
            )),
            dlq_rx,
        }
    }

    pub async fn ingest(mut self) {
        while let Some(message) = self.dlq_rx.recv().await {
            match self.send_to_dlq(message).await {
                Ok(_) => println!("Redirected message to DLQ."),
                Err((kafka_err, _)) => {
                    eprintln!("Failed to write to DLQ: {}", kafka_err);
                }
            }
        }
    }

    pub async fn send_to_dlq(
        self: &Self,
        failed_message: PartialMessageWithError,
    ) -> OwnedDeliveryResult {
        let error_description = failed_message.error_msg;
        let inner_message = failed_message.message;
        let message_key = inner_message.key.as_deref().unwrap_or(self.unknown_key);

        let mut r = FutureRecord::to(&self.dlq_topic).key(message_key).headers(
            OwnedHeaders::new_with_capacity(3)
                .insert(Header {
                    key: "o_partition",
                    value: Some(&inner_message.partition.to_le_bytes()),
                })
                .insert(Header {
                    key: "o_offset",
                    value: Some(&inner_message.offset.to_le_bytes()),
                })
                .insert(Header {
                    key: "o_error",
                    value: Some(error_description.as_bytes()),
                }),
        );
        if let Some(payload) = inner_message.payload.as_deref() {
            r = r.payload(payload);
        }
        self.producer.send(r, self.dlq_timeout).await
    }
}
