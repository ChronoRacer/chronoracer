use crate::configuration::CoreConfig;
use crate::messaging::partial_message::PartialMessageWithError;
use async_trait::async_trait;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub type InvalidMessageRx = tokio::sync::mpsc::Receiver<PartialMessageWithError>;
pub type InvalidMessageTx = tokio::sync::mpsc::Sender<PartialMessageWithError>;

#[async_trait]
pub trait KafkaProducer: Send + Sync {
    async fn send_record<'a>(
        &self,
        record: FutureRecord<'a, [u8], [u8]>,
        timeout: rdkafka::util::Timeout,
    ) -> OwnedDeliveryResult;
}

#[async_trait]
impl KafkaProducer for FutureProducer {
    async fn send_record<'a>(
        &self,
        record: FutureRecord<'a, [u8], [u8]>,
        timeout: rdkafka::util::Timeout,
    ) -> OwnedDeliveryResult {
        self.send(record, timeout).await
    }
}

pub struct DLQHandler<'a, P: KafkaProducer = FutureProducer> {
    producer: P,
    unknown_key: &'a [u8],
    dlq_topic: &'a str,
    dlq_timeout: rdkafka::util::Timeout,
    dlq_rx: InvalidMessageRx,
}

impl<'a> DLQHandler<'a, FutureProducer> {
    pub fn new(config: &'a CoreConfig, dlq_rx: InvalidMessageRx) -> Self {
        let kafka_config = &config.kafka;
        let producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "true")
            .set("session.timeout.ms", "6000")
            .create()
            .expect("Failed to create DLQ handler producer");

        Self::with_producer(config, dlq_rx, producer)
    }
}

impl<'a, P: KafkaProducer> DLQHandler<'a, P> {
    pub fn with_producer(config: &'a CoreConfig, dlq_rx: InvalidMessageRx, producer: P) -> Self {
        let kafka_config = &config.kafka;
        Self {
            producer,
            unknown_key: kafka_config.dlq_config.dlq_unknown_key.as_bytes(),
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
        &self,
        failed_message: PartialMessageWithError,
    ) -> OwnedDeliveryResult {
        let error_description = failed_message.error_msg;
        let inner_message = failed_message.message;
        let message_key = inner_message.key.as_deref().unwrap_or(self.unknown_key);

        let mut r = FutureRecord::to(self.dlq_topic).key(message_key).headers(
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
        self.producer.send_record(r, self.dlq_timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::message::Headers;
    use rdkafka::producer::future_producer::Delivery;
    use rdkafka::Timestamp;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;

    #[derive(Clone, Debug)]
    pub struct CapturedRecord {
        pub topic: String,
        pub key: Option<Vec<u8>>,
        pub payload: Option<Vec<u8>>,
        pub headers: Vec<(String, Option<Vec<u8>>)>,
    }

    #[derive(Clone, Default)]
    pub struct MockKafkaProducer {
        pub sent_records: Arc<Mutex<Vec<CapturedRecord>>>,
        pub should_fail: bool,
    }

    #[async_trait]
    impl KafkaProducer for MockKafkaProducer {
        async fn send_record<'a>(
            &self,
            record: FutureRecord<'a, [u8], [u8]>,
            _timeout: rdkafka::util::Timeout,
        ) -> OwnedDeliveryResult {
            if self.should_fail {
                return Err((
                    rdkafka::error::KafkaError::Canceled,
                    rdkafka::message::OwnedMessage::new(
                        None,
                        None,
                        record.topic.to_string(),
                        rdkafka::Timestamp::NotAvailable,
                        0,
                        0,
                        None,
                    ),
                ));
            }

            let mut extracted_headers = Vec::new();
            if let Some(headers) = record.headers {
                for i in 0..headers.count() {
                    let h = headers.get(i);
                    extracted_headers.push((h.key.to_string(), h.value.map(|v| v.to_vec())));
                }
            }

            let captured = CapturedRecord {
                topic: record.topic.to_string(),
                key: record.key.map(|k| k.to_vec()),
                payload: record.payload.map(|p| p.to_vec()),
                headers: extracted_headers,
            };

            self.sent_records.lock().unwrap().push(captured);

            Ok(Delivery {
                partition: 0,
                offset: 0,
                timestamp: Timestamp::NotAvailable,
            })
        }
    }

    #[tokio::test]
    async fn test_send_to_dlq_constructs_headers_and_fallback_key() {
        let config = CoreConfig::mock_config();
        let (_tx, rx) = mpsc::channel(10);
        let mock_producer = MockKafkaProducer::default();

        let handler = DLQHandler::with_producer(&config, rx, mock_producer.clone());

        let failed_msg = PartialMessageWithError {
            error_msg: "Parse Error".to_string(),
            message: crate::messaging::partial_message::PartialMessage {
                key: None, // No key provided, should fallback to UNKNOWN_KEY
                partition: 2,
                offset: 100,
                payload: Some(Box::from(b"bad bytes".to_vec())),
            },
        };

        let result = handler.send_to_dlq(failed_msg).await;
        assert!(result.is_ok());

        let records = mock_producer.sent_records.lock().unwrap();
        assert_eq!(records.len(), 1);

        let rec = &records[0];
        assert_eq!(rec.topic, "test.dlq.topic");
        assert_eq!(rec.key.as_deref(), Some(b"UNKNOWN_KEY".as_slice()));
        assert_eq!(rec.payload.as_deref(), Some(b"bad bytes".as_slice()));

        let headers_map: std::collections::HashMap<_, _> = rec
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone().unwrap()))
            .collect();

        assert_eq!(
            headers_map.get("o_partition").unwrap(),
            &2i32.to_le_bytes().to_vec()
        );
        assert_eq!(
            headers_map.get("o_offset").unwrap(),
            &100i64.to_le_bytes().to_vec()
        );
        assert_eq!(
            headers_map.get("o_error").unwrap(),
            b"Parse Error".as_slice()
        );
    }

    #[tokio::test]
    async fn test_ingest_loop_processes_multiple_messages() {
        let config = CoreConfig::mock_config();
        let (tx, rx) = mpsc::channel(10);
        let mock_producer = MockKafkaProducer::default();

        let handler = DLQHandler::with_producer(&config, rx, mock_producer.clone());

        let msg1 = PartialMessageWithError {
            error_msg: "Err 1".to_string(),
            message: crate::messaging::partial_message::PartialMessage {
                key: Some(Box::from(b"key_1".to_vec())),
                partition: 0,
                offset: 1,
                payload: None,
            },
        };

        let msg2 = PartialMessageWithError {
            error_msg: "Err 2".to_string(),
            message: crate::messaging::partial_message::PartialMessage {
                key: Some(Box::from(b"key_2".to_vec())),
                partition: 0,
                offset: 2,
                payload: None,
            },
        };

        tx.send(msg1).await.unwrap();
        tx.send(msg2).await.unwrap();
        drop(tx);

        handler.ingest().await;

        let records = mock_producer.sent_records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_deref(), Some(b"key_1".as_slice()));
        assert_eq!(records[1].key.as_deref(), Some(b"key_2".as_slice()));
    }
}
