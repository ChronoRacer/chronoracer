use crate::configuration::KafkaConfig;
use crate::messaging::partial_message::PartialMessage;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use log::info;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

pub type SinkRx = tokio::sync::mpsc::Receiver<PartialMessage>;
pub type SinkTx = tokio::sync::mpsc::Sender<PartialMessage>;

pub struct MessageSink {
    consumer: StreamConsumer,
    flush_tx: SinkTx,
    topics: Vec<&'static str>,
}

impl MessageSink {
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: Vec<&'static str>,
        flush_tx: SinkTx,
    ) -> Result<Self> {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", kafka_config.brokers.join(","))
            .set("enable.auto.commit", "false")
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
        })
    }

    pub async fn ingest(&mut self) {
        info!("Message sink opened for topics: {}", self.topics.join(","));
        let mut message_stream = self.consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(borrowed_message) => {
                    if let Err(e) = self
                        .flush_tx
                        .send(PartialMessage::from(borrowed_message))
                        .await
                    {
                        error!("Ingestion pipeline broken downstream: {e}");
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
        warn!(
            "Message sink terminated for topics: {}",
            self.topics.join(",")
        );
    }

    pub async fn ingest_with_cancellation(&mut self, cancel_token: CancellationToken) {
        info!("Message sink opened for topics: {}", self.topics.join(","));

        let mut message_stream = self.consumer.stream();

        loop {
            tokio::select! {
                biased;
                () = cancel_token.cancelled() => {
                    info!("Cancellation received. Stopping ingestion for topics: {}", self.topics.join(","));
                    break;
                }

                message_option = message_stream.next() => {
                    match message_option {
                        Some(Ok(borrowed_message)) => {
                            if let Err(e) = self
                                .flush_tx
                                .send(PartialMessage::from(borrowed_message))
                                .await
                            {
                                error!("Ingestion pipeline broken downstream: {e}");
                                break;
                            }
                        }
                        Some(Err(kafka_error)) => {
                            error!("Kafka error occurred while polling: {kafka_error}");
                        }
                        None => {
                            warn!("Kafka stream ended unexpectedly.");
                            break;
                        }
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

impl Drop for MessageSink {
    fn drop(&mut self) {
        self.consumer.unsubscribe();
    }
}
