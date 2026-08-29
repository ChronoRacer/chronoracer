use crate::configuration::KafkaConfig;
use crate::messaging::partial_message::PartialMessage;
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub type MessageBatch = Vec<PartialMessage>;
pub type BatchSinkRx = tokio::sync::mpsc::Receiver<MessageBatch>;
pub type BatchSinkTx = tokio::sync::mpsc::Sender<MessageBatch>;
pub type SinkRx = tokio::sync::mpsc::Receiver<PartialMessage>;
pub type SinkTx = tokio::sync::mpsc::Sender<PartialMessage>;

pub struct BatchMessageSink {
    consumer: StreamConsumer,
    flush_tx: BatchSinkTx,
    topics: Vec<&'static str>,
    batch_size: usize,
    batch_flush_interval_ms: u32,
}

impl BatchMessageSink {
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: Vec<&'static str>,
        flush_tx: BatchSinkTx,
        batch_size: usize,
        batch_flush_interval_ms: u32,
    ) -> Result<Self> {
        if batch_size == 0 {
            bail!("Message sink batch size must be greater than zero");
        }
        if batch_flush_interval_ms == 0 {
            bail!("Message sink batch flush interval must be greater than zero");
        }

        let consumer_config = &kafka_config.sensor_data_consumer;
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", kafka_config.brokers.join(","))
            .set("enable.auto.commit", "true")
            .set(
                "auto.commit.interval.ms",
                consumer_config.auto_commit_interval_ms.to_string(),
            )
            .set(
                "fetch.min.bytes",
                consumer_config.fetch_min_bytes.to_string(),
            )
            .set(
                "fetch.wait.max.ms",
                consumer_config.fetch_wait_max_ms.to_string(),
            )
            .set(
                "fetch.message.max.bytes",
                consumer_config.fetch_message_max_bytes.to_string(),
            )
            .set(
                "queued.min.messages",
                consumer_config.queued_min_messages.to_string(),
            )
            .set(
                "queued.max.messages.kbytes",
                consumer_config.queued_max_messages_kbytes.to_string(),
            )
            .set(
                "socket.receive.buffer.bytes",
                consumer_config.socket_receive_buffer_bytes.to_string(),
            )
            .set("session.timeout.ms", "45000")
            .set("heartbeat.interval.ms", "15000")
            .set("max.poll.interval.ms", "300000")
            .create()
            .context("Failed to create message sink consumer")?;
        consumer
            .subscribe(&topics)
            .context("Failed to subscribe message sink")?;
        Ok(Self {
            consumer,
            flush_tx,
            topics,
            batch_size,
            batch_flush_interval_ms,
        })
    }

    pub async fn ingest_with_cancellation(&mut self, cancel_token: CancellationToken) {
        info!(
            "Batch Message sink opened for topics: {}, batch_size={}, flush_interval_ms={}",
            self.topics.join(","),
            self.batch_size,
            self.batch_flush_interval_ms,
        );
        let mut message_stream = self.consumer.stream();
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(
            u64::from(self.batch_flush_interval_ms),
        ));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut buffer = Vec::with_capacity(self.batch_size);

        loop {
            tokio::select! {
                biased;
                () = cancel_token.cancelled() => {
                     info!("Cancellation received. Stopping ingestion for topics: {}", self.topics.join(","));
                    if !flush_message_batch(&self.flush_tx, &mut buffer, self.batch_size).await {
                        error!("Ingestion pipeline broken while flushing the final message batch");
                    }
                    break;
                }

                message_option = message_stream.next() => {
                    match message_option {
                        Some(Ok(borrowed_message)) => {
                            buffer.push(PartialMessage::from(borrowed_message));
                            if buffer.len() >= self.batch_size
                                && !flush_message_batch(&self.flush_tx, &mut buffer, self.batch_size).await
                            {
                                error!("Ingestion pipeline broken downstream");
                                break;
                            }
                        }
                        Some(Err(kafka_error)) => {
                            error!("Kafka error occurred while polling: {kafka_error}");
                        }
                        None => {
                            warn!("Kafka stream ended unexpectedly.");
                            if !flush_message_batch(&self.flush_tx, &mut buffer, self.batch_size).await {
                                error!("Ingestion pipeline broken while flushing the final message batch");
                            }
                            break;
                        }
                    }
                }

                _ = flush_interval.tick() => {
                    if !flush_message_batch(&self.flush_tx, &mut buffer, self.batch_size).await {
                        error!("Ingestion pipeline broken downstream");
                        break;
                    }
                }
            }
        }
        warn!(
            "Message sink terminated for topics: {}",
            self.topics.join(",")
        );
    }
}

async fn flush_message_batch(
    flush_tx: &BatchSinkTx,
    buffer: &mut Vec<PartialMessage>,
    batch_size: usize,
) -> bool {
    if buffer.is_empty() {
        return true;
    }
    let batch = std::mem::replace(buffer, Vec::with_capacity(batch_size));
    flush_tx.send(batch).await.is_ok()
}

impl Drop for BatchMessageSink {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}

pub struct InstantMessageSink {
    consumer: StreamConsumer,
    message_tx: SinkTx,
    topics: Vec<&'static str>,
}

impl InstantMessageSink {
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: Vec<&'static str>,
        message_tx: SinkTx,
    ) -> Result<Self> {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", kafka_config.brokers.join(","))
            .set("enable.auto.commit", "true")
            .set(
                "auto.commit.interval.ms",
                kafka_config
                    .sensor_data_consumer
                    .auto_commit_interval_ms
                    .to_string(),
            )
            .set("fetch.min.bytes", "1")
            .set("fetch.wait.max.ms", "0")
            .set("session.timeout.ms", "45000")
            .set("heartbeat.interval.ms", "15000")
            .set("max.poll.interval.ms", "300000")
            .create()
            .context("Failed to create control message sink consumer")?;
        consumer
            .subscribe(&topics)
            .context("Failed to subscribe control message sink")?;
        Ok(Self {
            consumer,
            message_tx,
            topics,
        })
    }

    pub async fn ingest(&mut self) {
        info!(
            "Control message sink opened for topics: {}",
            self.topics.join(",")
        );
        let mut message_stream = self.consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(borrowed_message) => {
                    if let Err(e) = self
                        .message_tx
                        .send(PartialMessage::from(borrowed_message))
                        .await
                    {
                        error!("Control ingestion pipeline broken downstream: {e}");
                        break;
                    }
                }
                Err(kafka_error) => {
                    error!("Kafka error occurred while polling control events: {kafka_error}");
                }
            }
        }
        warn!(
            "Control message sink terminated for topics: {}",
            self.topics.join(",")
        );
    }
}

impl Drop for InstantMessageSink {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}
