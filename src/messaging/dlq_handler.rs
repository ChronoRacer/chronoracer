use crate::configuration::{CoreConfig, DLQConfig};
use rdkafka::message::{Header, OwnedHeaders, OwnedMessage, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::time::Duration;

pub struct DLQHandler {
    producer: FutureProducer,
    dlq_config: &'static DLQConfig,
}

impl DLQHandler {
    pub fn new(config: &'static CoreConfig) -> Self {
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
            dlq_config: &kafka_config.dlq_config,
        }
    }

    pub async fn redirect_to_dlq(
        self: &Self,
        owned_message: OwnedMessage,
        original_error_desc: &str,
    ) {
        if let Some(payload) = owned_message.payload() {
            let message_key = owned_message
                .key()
                .unwrap_or(&self.dlq_config.dlq_unknown_key.to_bytes());
            let record = FutureRecord::to(&self.dlq_config.dlq_topic)
                .payload(payload)
                .key(message_key)
                .headers(
                    OwnedHeaders::new_with_capacity(3)
                        .insert(Header {
                            key: "o_partition",
                            value: Some(&owned_message.partition().to_string()),
                        })
                        .insert(Header {
                            key: "o_offset",
                            value: Some(&owned_message.offset().to_string()),
                        })
                        .insert(Header {
                            key: "o_error",
                            value: Some(original_error_desc),
                        }),
                );
            match self
                .producer
                .send(
                    record,
                    rdkafka::util::Timeout::After(Duration::from_secs(
                        self.dlq_config.publish_timeout_sec as u64,
                    )),
                )
                .await
            {
                Ok(_) => println!("Redirected message to DLQ."),
                Err((kafka_err, _)) => {
                    eprintln!("CRITICAL: Failed to write to DLQ: {}", kafka_err);
                }
            }
        }
    }
}
