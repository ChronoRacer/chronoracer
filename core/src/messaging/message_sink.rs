use crate::configuration::KafkaConfig;
use crate::messaging::offset_ledger::{
    OffsetLedger, OffsetSink, SharedTracker, lock_tracker,
};
use crate::messaging::partial_message::{MessageOrigin, PartialMessage};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, MessageStream, Rebalance, StreamConsumer,
};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::ClientContext;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::Permit;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub type MessageBatch = Vec<PartialMessage>;
pub type BatchSinkRx = tokio::sync::mpsc::Receiver<MessageBatch>;
pub type BatchSinkTx = tokio::sync::mpsc::Sender<MessageBatch>;
pub type SinkRx = tokio::sync::mpsc::Receiver<PartialMessage>;
pub type SinkTx = tokio::sync::mpsc::Sender<PartialMessage>;

const SESSION_TIMEOUT_MS: &str = "45000";
const HEARTBEAT_INTERVAL_MS: &str = "15000";
/// The consumer is polled at least this often even while the sink waits on a full pipeline, see
/// [`BatchMessageSink`].
const MAX_POLL_INTERVAL_MS: &str = "300000";
/// How long the sink waits on a full pipeline before it pauses its partitions. Pausing discards
/// what librdkafka has prefetched (fetched again on resume), so ordinary backpressure must not
/// pause. Well under `MAX_POLL_INTERVAL_MS`.
const PAUSE_AFTER: Duration = Duration::from_secs(30);

/// Rebalance callbacks. They run inside the stream poll, so on the sink's own task.
struct OffsetContext {
    name: &'static str,
    tracker: SharedTracker,
    /// Set while the sink holds its partitions paused: partitions assigned meanwhile are paused too.
    paused: Arc<AtomicBool>,
}

impl OffsetContext {
    fn partitions(tpl: &TopicPartitionList) -> Vec<i32> {
        tpl.elements().iter().map(|element| element.partition()).collect()
    }
}

impl ClientContext for OffsetContext {}

impl ConsumerContext for OffsetContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                let partitions = Self::partitions(tpl);
                info!(consumer = self.name, ?partitions, "Partitions revoked");
                // Nothing is stored for them from here on; librdkafka commits what is already
                // stored when it unassigns them (`enable.auto.commit`). Messages still in the
                // pipeline are written anyway (a new owner may write them again) and settle into nothing.
                lock_tracker(&self.tracker).forget(partitions);
            }
            Rebalance::Assign(tpl) => {
                let partitions = Self::partitions(tpl);
                info!(consumer = self.name, ?partitions, "Partitions assigned");
                // Consumption restarts at the committed offset: whatever was tracked is stale.
                lock_tracker(&self.tracker).forget(partitions);
            }
            Rebalance::Error(error) => {
                warn!(consumer = self.name, %error, "Rebalance error");
            }
        }
    }

    fn post_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = rebalance {
            if self.paused.load(Ordering::Acquire) {
                if let Err(error) = base_consumer.pause(tpl) {
                    warn!(consumer = self.name, %error, "Failed to pause newly assigned partitions");
                }
            }
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        if let Err(error) = result {
            if !is_no_offset(&error) {
                warn!(consumer = self.name, %error, "Offset commit failed");
            }
        }
    }
}

fn is_no_offset(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)
    )
}

type KafkaConsumer = StreamConsumer<OffsetContext>;

/// The consumer plus what the sinks and the shutdown sequence do with it.
struct ManagedConsumer {
    consumer: Arc<KafkaConsumer>,
    paused: Arc<AtomicBool>,
    ledger: OffsetLedger,
}

struct KafkaOffsetSink {
    consumer: Arc<KafkaConsumer>,
    topic: String,
}

impl OffsetSink for KafkaOffsetSink {
    fn store(&self, partition: i32, next_offset: i64) -> Result<(), String> {
        // `store_offset` stores the offset it is given plus one, like a consumed message's offset.
        self.consumer
            .store_offset(&self.topic, partition, next_offset - 1)
            .map_err(|error| error.to_string())
    }

    fn commit(&self) -> Result<(), String> {
        match self.consumer.commit_consumer_state(CommitMode::Sync) {
            Err(error) if is_no_offset(&error) => Ok(()),
            other => other.map_err(|error| error.to_string()),
        }
    }
}

impl ManagedConsumer {
    /// Offsets are stored only when a message is settled (`enable.auto.offset.store=false`), and
    /// librdkafka commits what is stored every `auto.commit.interval.ms`. Until a group has a
    /// committed offset it starts at the beginning: Core creates the topics before anything is
    /// produced, so nothing older than Core is skipped, and a replay is only a duplicate.
    fn open(
        name: &'static str,
        group_id: &str,
        kafka_config: &KafkaConfig,
        topic: &'static str,
        tune: impl FnOnce(&mut rdkafka::ClientConfig),
    ) -> Result<Self> {
        let tracker = SharedTracker::default();
        let paused = Arc::new(AtomicBool::new(false));
        let context = OffsetContext {
            name,
            tracker: Arc::clone(&tracker),
            paused: Arc::clone(&paused),
        };
        let mut config = rdkafka::ClientConfig::new();
        config
            .set("group.id", group_id)
            .set("bootstrap.servers", kafka_config.brokers.join(","))
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set(
                "auto.commit.interval.ms",
                kafka_config
                    .sensor_data_consumer
                    .auto_commit_interval_ms
                    .to_string(),
            )
            .set("session.timeout.ms", SESSION_TIMEOUT_MS)
            .set("heartbeat.interval.ms", HEARTBEAT_INTERVAL_MS)
            .set("max.poll.interval.ms", MAX_POLL_INTERVAL_MS);
        tune(&mut config);
        let consumer: KafkaConsumer = config
            .create_with_context(context)
            .with_context(|| format!("Failed to create {name} consumer"))?;
        let consumer = Arc::new(consumer);
        consumer
            .subscribe(&[topic])
            .with_context(|| format!("Failed to subscribe {name} consumer"))?;
        let ledger = OffsetLedger::new(
            name,
            tracker,
            Box::new(KafkaOffsetSink {
                consumer: Arc::clone(&consumer),
                topic: topic.to_string(),
            }),
        );
        info!(consumer = name, group_id, topic, "Consumer subscribed");
        Ok(Self {
            consumer,
            paused,
            ledger,
        })
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::Release);
        match self.consumer.assignment() {
            Ok(assignment) if assignment.count() > 0 => {
                if let Err(error) = self.consumer.pause(&assignment) {
                    warn!(%error, "Failed to pause partitions");
                }
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "Failed to read the assignment to pause it"),
        }
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        match self.consumer.assignment() {
            Ok(assignment) if assignment.count() > 0 => {
                if let Err(error) = self.consumer.resume(&assignment) {
                    warn!(%error, "Failed to resume partitions");
                }
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "Failed to read the assignment to resume it"),
        }
    }
}

/// Sensor data consumer. A message is registered in the offset ledger when it is consumed and
/// settled by the pipeline ([`crate::messaging::offset_ledger`]).
///
/// `max.poll.interval.ms` is 5 minutes and a `StreamConsumer` only counts as polled while its stream
/// is awaited. A database outage fills the pipeline and the sink waits for room, for as long as the
/// retries last. So once a batch could not be handed over for [`PAUSE_AFTER`], the sink pauses its
/// partitions and keeps awaiting the stream (which also serves rebalances): the consumer stays in
/// the group and nothing is consumed beyond what the pipeline can hold.
pub struct BatchMessageSink {
    consumer: ManagedConsumer,
    flush_tx: BatchSinkTx,
    topics: Vec<&'static str>,
    batch_size: usize,
    batch_flush_interval_ms: u32,
    lag_log_interval: Duration,
    pause_after: Duration,
}

impl BatchMessageSink {
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: Vec<&'static str>,
        flush_tx: BatchSinkTx,
        batch_size: usize,
        batch_flush_interval_ms: u32,
    ) -> Result<Self> {
        let consumer_config = &kafka_config.sensor_data_consumer;
        let [topic] = topics[..] else {
            anyhow::bail!("The sensor data sink reads exactly one topic");
        };
        let consumer = ManagedConsumer::open(
            "sensor_data",
            &kafka_config.data_group_id(),
            kafka_config,
            topic,
            |config| {
                config
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
                    );
            },
        )?;
        Ok(Self {
            consumer,
            flush_tx,
            topics,
            batch_size,
            batch_flush_interval_ms,
            lag_log_interval: Duration::from_secs(u64::from(
                consumer_config.offset_lag_log_interval_s.get(),
            )),
            pause_after: PAUSE_AFTER,
        })
    }

    /// Settle into this to advance the committed offsets. Outlives the sink task, for the final commit.
    #[must_use]
    pub fn offsets(&self) -> OffsetLedger {
        self.consumer.ledger.clone()
    }

    pub async fn ingest_with_cancellation(&self, cancel_token: CancellationToken) {
        info!(
            "Batch message sink opened for topics: {}, batch_size={}, flush_interval_ms={}",
            self.topics.join(","),
            self.batch_size,
            self.batch_flush_interval_ms,
        );
        let mut message_stream = self.consumer.consumer.stream();
        let mut flush_interval = tokio::time::interval(Duration::from_millis(u64::from(
            self.batch_flush_interval_ms,
        )));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut lag_interval = tokio::time::interval_at(
            Instant::now() + self.lag_log_interval,
            self.lag_log_interval,
        );
        lag_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_lag = Vec::new();

        let mut buffer = Vec::with_capacity(self.batch_size);

        loop {
            tokio::select! {
                biased;
                () = cancel_token.cancelled() => {
                    info!("Cancellation received. Stopping ingestion for topics: {}", self.topics.join(","));
                    if !self.flush(&mut message_stream, &mut buffer, &cancel_token).await {
                      error!("Ingestion pipeline broken while flushing the final message batch");
                    }
                    break;
                }

                message_option = message_stream.next() => {
                    match message_option {
                        Some(Ok(borrowed_message)) => {
                            self.accept(&borrowed_message, &mut buffer);
                            if buffer.len() >= self.batch_size {
                                if !self.flush(&mut message_stream, &mut buffer, &cancel_token).await {
                                    error!("Ingestion pipeline broken downstream");
                                    break;
                                }
                                // The next timer flush is due one full interval after this one.
                                flush_interval.reset();
                            }
                        }
                        Some(Err(error)) => {
                            // This branch is taken when the broker dies. Reconnects automatically
                            error!(%error, "Kafka error occurred while polling");
                        }
                        None => {
                            error!("Kafka stream ended unexpectedly");
                            if !self.flush(&mut message_stream, &mut buffer, &cancel_token).await {
                                error!("Ingestion pipeline broken while flushing the final message batch");
                            }
                            break;
                        }
                    }
                }

                _ = flush_interval.tick() => {
                    if !self.flush(&mut message_stream, &mut buffer, &cancel_token).await {
                        error!("Ingestion pipeline broken downstream");
                        break;
                    }
                }

                _ = lag_interval.tick() => self.consumer.ledger.log_lag(&mut last_lag),
            }
        }
        warn!(
            "Batch message sink terminated for topics: {}",
            self.topics.join(",")
        );
    }

    fn accept(&self, message: &rdkafka::message::BorrowedMessage<'_>, buffer: &mut MessageBatch) {
        let message = PartialMessage::from_borrowed(message, MessageOrigin::SensorData);
        // Registered at consumption, never later: a rebalance that revokes the partition between here
        // and the flush has already dropped it from the tracker.
        self.consumer.ledger.register(message.partition, message.offset);
        buffer.push(message);
    }

    /// Sends the buffered messages as one batch. An empty buffer sends nothing. `false` when the
    /// pipeline is gone.
    async fn flush(
        &self,
        stream: &mut MessageStream<'_, OffsetContext>,
        buffer: &mut MessageBatch,
        cancel: &CancellationToken,
    ) -> bool {
        if buffer.is_empty() {
            return !self.flush_tx.is_closed();
        }
        let permit = match tokio::time::timeout(self.pause_after, self.flush_tx.reserve()).await {
            Ok(permit) => permit.ok(),
            Err(_) if cancel.is_cancelled() => self.flush_tx.reserve().await.ok(),
            Err(_) => self.reserve_while_paused(stream, buffer, cancel).await,
        };
        let Some(permit) = permit else {
            return false;
        };
        permit.send(std::mem::replace(buffer, Vec::with_capacity(self.batch_size)));
        true
    }

    async fn reserve_while_paused(
        &self,
        stream: &mut MessageStream<'_, OffsetContext>,
        buffer: &mut MessageBatch,
        cancel: &CancellationToken,
    ) -> Option<Permit<'_, MessageBatch>> {
        warn!(pause_after = ?self.pause_after, "Pipeline still full; pausing partitions until it has room");
        self.consumer.pause();
        let mut polling = true;
        let permit = loop {
            tokio::select! {
                permit = self.flush_tx.reserve() => break permit.ok(),
                () = cancel.cancelled(), if polling => polling = false,
                event = stream.next(), if polling => match event {
                    // Fetched before the pause took effect: part of this batch.
                    Some(Ok(message)) => self.accept(&message, buffer),
                    Some(Err(error)) => error!(%error, "Kafka error occurred while polling"),
                    None => polling = false,
                },
            }
        };
        if !cancel.is_cancelled() {
            self.consumer.resume();
            info!("Pipeline has room again; partitions resumed");
        }
        permit
    }
}

/// Control event consumer. Messages are forwarded one by one; the control filter settles each
/// after handling it, except SHUTDOWN, see [`crate::processing::control_filter`].
pub struct InstantMessageSink {
    consumer: ManagedConsumer,
    message_tx: SinkTx,
    topics: Vec<&'static str>,
}

impl InstantMessageSink {
    pub fn new(
        kafka_config: &KafkaConfig,
        topics: Vec<&'static str>,
        message_tx: SinkTx,
    ) -> Result<Self> {
        let [topic] = topics[..] else {
            anyhow::bail!("The control sink reads exactly one topic");
        };
        let consumer = ManagedConsumer::open(
            "control",
            &kafka_config.control_group_id(),
            kafka_config,
            topic,
            |config| {
                config
                    .set("fetch.min.bytes", "1")
                    .set("fetch.wait.max.ms", "0");
            },
        )?;
        Ok(Self {
            consumer,
            message_tx,
            topics,
        })
    }

    /// Settle into this to advance the committed offsets. Outlives the sink task, for the final commit.
    #[must_use]
    pub fn offsets(&self) -> OffsetLedger {
        self.consumer.ledger.clone()
    }

    pub async fn ingest(&self) {
        info!(
            "Instant message sink opened for topics: {}",
            self.topics.join(",")
        );
        let mut message_stream = self.consumer.consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(borrowed_message) => {
                    let message =
                        PartialMessage::from_borrowed(&borrowed_message, MessageOrigin::Control);
                    self.consumer
                        .ledger
                        .register(message.partition, message.offset);
                    if let Err(error) = self.message_tx.send(message).await {
                        error!(%error, "Control ingestion pipeline broken downstream");
                        break;
                    }
                }
                Err(error) => {
                    // This branch is taken when the broker dies. Reconnects automatically
                    error!(%error, "Kafka error occurred while polling");
                }
            }
        }
        // The filter stopped (SHUTDOWN was relayed, or it ended). Nothing is consumed from here on,
        // but the consumer stays in its group, so the SHUTDOWN offset can still be committed after
        // the export, which can take longer than `max.poll.interval.ms`.
        warn!(
            "Instant message sink stopped forwarding for topics: {}",
            self.topics.join(",")
        );
        self.consumer.pause();
        while message_stream.next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(offset: i64) -> PartialMessage {
        PartialMessage {
            key: None,
            offset,
            partition: 0,
            payload: None,
            origin: MessageOrigin::SensorData,
        }
    }

    fn unconnected_kafka() -> KafkaConfig {
        let mut kafka = KafkaConfig::mock_config();
        kafka.brokers = vec!["127.0.0.1:1".to_string()];
        kafka
    }

    /// No broker needed: a consumer that never joins its group has no assignment.
    fn unconnected_consumer(name: &'static str, group: &str) -> ManagedConsumer {
        ManagedConsumer::open(name, group, &unconnected_kafka(), "data", |_| {}).unwrap()
    }

    fn unconnected_sink(capacity: usize, pause_after: Duration) -> (BatchMessageSink, BatchSinkRx) {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        let mut sink = BatchMessageSink::new(&unconnected_kafka(), vec!["data"], tx, 8, 15).unwrap();
        sink.pause_after = pause_after;
        (sink, rx)
    }

    fn offsets(batch: &MessageBatch) -> Vec<i64> {
        batch.iter().map(|m| m.offset).collect()
    }

    #[tokio::test]
    async fn flush_skips_empty_buffer_and_keeps_its_capacity() {
        let (sink, mut rx) = unconnected_sink(4, PAUSE_AFTER);
        let mut stream = sink.consumer.consumer.stream();
        let mut buffer = Vec::with_capacity(8);

        assert!(sink.flush(&mut stream, &mut buffer, &CancellationToken::new()).await);

        assert!(rx.try_recv().is_err());
        assert_eq!(buffer.capacity(), 8);
    }

    #[tokio::test]
    async fn flush_sends_buffered_messages_and_leaves_an_empty_buffer() {
        let (sink, mut rx) = unconnected_sink(4, PAUSE_AFTER);
        let mut stream = sink.consumer.consumer.stream();
        let mut buffer = vec![message(1), message(2)];

        assert!(sink.flush(&mut stream, &mut buffer, &CancellationToken::new()).await);

        assert_eq!(offsets(&rx.try_recv().unwrap()), [1, 2]);
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn flush_reports_a_closed_receiver_even_when_empty() {
        let (sink, rx) = unconnected_sink(4, PAUSE_AFTER);
        drop(rx);
        let mut stream = sink.consumer.consumer.stream();
        let cancel = CancellationToken::new();

        assert!(!sink.flush(&mut stream, &mut Vec::new(), &cancel).await);
        assert!(!sink.flush(&mut stream, &mut vec![message(1)], &cancel).await);
    }

    /// Frees one slot after `delay`, reporting whether the sink had paused by then.
    fn drain_one_after(
        sink: &BatchMessageSink,
        mut rx: BatchSinkRx,
        delay: Duration,
    ) -> tokio::task::JoinHandle<(bool, BatchSinkRx)> {
        let paused = Arc::clone(&sink.consumer.paused);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let paused_while_full = paused.load(Ordering::Acquire);
            assert_eq!(offsets(&rx.recv().await.unwrap()), [0]);
            (paused_while_full, rx)
        })
    }

    #[tokio::test]
    async fn ordinary_backpressure_waits_without_pausing() {
        let (sink, rx) = unconnected_sink(1, Duration::from_secs(30));
        sink.flush_tx.try_send(vec![message(0)]).unwrap();
        let drain = drain_one_after(&sink, rx, Duration::from_millis(50));
        let mut stream = sink.consumer.consumer.stream();
        let mut buffer = vec![message(1), message(2)];

        assert!(sink.flush(&mut stream, &mut buffer, &CancellationToken::new()).await);

        let (paused_while_full, mut rx) = drain.await.unwrap();
        assert!(!paused_while_full);
        assert_eq!(offsets(&rx.recv().await.unwrap()), [1, 2]);
    }

    #[tokio::test]
    async fn a_long_stall_pauses_and_resumes_once_there_is_room() {
        let (sink, rx) = unconnected_sink(1, Duration::from_millis(10));
        sink.flush_tx.try_send(vec![message(0)]).unwrap();
        let drain = drain_one_after(&sink, rx, Duration::from_millis(200));
        let mut stream = sink.consumer.consumer.stream();
        let mut buffer = vec![message(1), message(2)];

        assert!(sink.flush(&mut stream, &mut buffer, &CancellationToken::new()).await);

        let (paused_while_full, mut rx) = drain.await.unwrap();
        assert!(paused_while_full);
        assert!(!sink.consumer.paused.load(Ordering::Acquire));
        assert_eq!(offsets(&rx.recv().await.unwrap()), [1, 2]);
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_sink_waits_for_room_without_pausing() {
        let (sink, rx) = unconnected_sink(1, Duration::from_millis(10));
        sink.flush_tx.try_send(vec![message(0)]).unwrap();
        let drain = drain_one_after(&sink, rx, Duration::from_millis(200));
        let mut stream = sink.consumer.consumer.stream();
        let cancel = CancellationToken::new();
        cancel.cancel();

        assert!(sink.flush(&mut stream, &mut vec![message(1)], &cancel).await);

        let (paused_while_full, mut rx) = drain.await.unwrap();
        assert!(!paused_while_full);
        assert_eq!(offsets(&rx.recv().await.unwrap()), [1]);
    }

    #[tokio::test]
    async fn settling_without_an_assignment_fails_softly() {
        let consumer = unconnected_consumer("test", "test-group-data");
        consumer.ledger.register(0, 5);

        // The store is refused (the partition is not assigned): logged, never a panic.
        consumer.ledger.settle_one(0, 5);
        let sink = KafkaOffsetSink {
            consumer: Arc::clone(&consumer.consumer),
            topic: "data".to_string(),
        };
        assert!(sink.store(0, 6).is_err());

        consumer.pause();
        consumer.resume();
        // Nothing is stored, so the commit has nothing to do.
        assert!(consumer.ledger.commit_final(Duration::from_secs(5)).await);
    }

    #[test]
    fn no_offset_commit_errors_are_not_failures() {
        assert!(is_no_offset(&KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)));
        assert!(!is_no_offset(&KafkaError::ConsumerCommit(RDKafkaErrorCode::UnknownMemberId)));
    }}
