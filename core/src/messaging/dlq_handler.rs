use crate::configuration::CoreConfig;
use crate::messaging::offset_ledger::OffsetLedger;
use crate::messaging::offset_tracker::OffsetSet;
use crate::messaging::partial_message::{MessageOrigin, PartialMessageWithError, SourceOffset};
use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{error, info, trace, warn};

/// Cadence of the aggregated DLQ log line. Failures are also logged immediately, once per window.
const DLQ_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// How an invalid message's source offset left the pipeline. Every invalid message gets exactly
/// one of these, reported through [`InvalidSettler`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidOutcome {
    /// The DLQ record was delivered to Kafka.
    DeadLettered,
    /// The DLQ record could not be delivered. The message is gone for good, so it is settled anyway.
    DeliveryFailed,
    /// The DLQ queue was full (or closed) and the record was discarded; counted and reported.
    Dropped,
}

/// The one place where "this invalid message's offset is settled", for either consumer. Call sites:
/// [`DlqWindow::record`] for `DeadLettered` / `DeliveryFailed`, and the `Dropped` paths of the
/// sensor data collector and the control filter.
#[derive(Clone)]
pub struct InvalidSettler {
    sensor_data: OffsetLedger,
    control: OffsetLedger,
}

impl InvalidSettler {
    #[must_use]
    pub fn new(sensor_data: OffsetLedger, control: OffsetLedger) -> Self {
        Self { sensor_data, control }
    }

    fn ledger(&self, origin: MessageOrigin) -> &OffsetLedger {
        match origin {
            MessageOrigin::SensorData => &self.sensor_data,
            MessageOrigin::Control => &self.control,
        }
    }

    pub fn settle(&self, at: SourceOffset, outcome: InvalidOutcome) {
        trace!(partition = at.partition, offset = at.offset, ?outcome, "Invalid message settled");
        self.ledger(at.origin).settle_one(at.partition, at.offset);
    }

    /// Settles many at once, for the parse path: one lock per batch.
    pub fn settle_set(&self, origin: MessageOrigin, set: &OffsetSet, outcome: InvalidOutcome) {
        if !set.is_empty() {
            trace!(?outcome, ranges = set.ranges().len(), "Invalid messages settled");
        }
        self.ledger(origin).settle(set);
    }

    #[cfg(test)]
    pub fn detached() -> Self {
        use crate::messaging::offset_ledger::testing::detached_ledger;
        Self::new(detached_ledger(), detached_ledger())
    }
}

/// Result of [`InvalidMessageTx::try_offer`].
pub enum DlqOffer {
    /// The DLQ handler owns the record and will settle its offset after delivery.
    Queued,
    /// Not queued; the record is handed back so the caller can settle its offset.
    Dropped(PartialMessageWithError),
}

#[derive(Clone)]
pub struct InvalidMessageTx {
    tx: mpsc::Sender<PartialMessageWithError>,
    dropped: Arc<AtomicU64>,
    settler: InvalidSettler,
}

impl InvalidMessageTx {
    /// For senders that discard a record themselves and must settle its offset.
    #[must_use]
    pub fn settler(&self) -> &InvalidSettler {
        &self.settler
    }

    /// Never waits. For the data parse path, which must not stall on the DLQ.
    #[must_use]
    pub fn try_offer(&self, message: PartialMessageWithError) -> DlqOffer {
        match self.tx.try_send(message) {
            Ok(()) => DlqOffer::Queued,
            Err(TrySendError::Full(message) | TrySendError::Closed(message)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                DlqOffer::Dropped(message)
            }
        }
    }

    /// Waits for room. For the low-volume control path.
    pub async fn send(
        &self,
        message: PartialMessageWithError,
    ) -> Result<(), SendError<PartialMessageWithError>> {
        self.tx.send(message).await
    }
}

pub struct InvalidMessageRx {
    rx: mpsc::Receiver<PartialMessageWithError>,
    dropped: Arc<AtomicU64>,
    settler: InvalidSettler,
}

impl InvalidMessageRx {
    pub async fn recv(&mut self) -> Option<PartialMessageWithError> {
        self.rx.recv().await
    }

    #[cfg(test)]
    pub fn try_recv(&mut self) -> Option<PartialMessageWithError> {
        self.rx.try_recv().ok()
    }
}

#[must_use]
pub fn invalid_message_channel(
    capacity: usize,
    settler: InvalidSettler,
) -> (InvalidMessageTx, InvalidMessageRx) {
    let (tx, rx) = mpsc::channel(capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    (
        InvalidMessageTx {
            tx,
            dropped: Arc::clone(&dropped),
            settler: settler.clone(),
        },
        InvalidMessageRx { rx, dropped, settler },
    )
}

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

struct DlqPublisher<P: KafkaProducer> {
    producer: Arc<P>,
    unknown_key: &'static [u8],
    dlq_topic: &'static str,
    dlq_timeout: rdkafka::util::Timeout,
}

impl<P: KafkaProducer> DlqPublisher<P> {
    async fn deliver(
        &self,
        message: PartialMessageWithError,
    ) -> (SourceOffset, Result<(), KafkaError>) {
        let at = message.message.source_offset();
        let result = self
            .send_to_dlq(message)
            .await
            .map(|_| ())
            .map_err(|(error, _)| error);
        (at, result)
    }

    async fn send_to_dlq(&self, failed_message: PartialMessageWithError) -> OwnedDeliveryResult {
        let error_description = failed_message.error_msg;
        let inner_message = failed_message.message;
        let message_key = inner_message.key.as_deref().unwrap_or(self.unknown_key);

        let mut record = FutureRecord::to(self.dlq_topic).key(message_key).headers(
            OwnedHeaders::new_with_capacity(3)
                .insert(Header {
                    key: "o_partition",
                    value: Some(inner_message.partition.to_string().as_bytes()),
                })
                .insert(Header {
                    key: "o_offset",
                    value: Some(inner_message.offset.to_string().as_bytes()),
                })
                .insert(Header {
                    key: "o_error",
                    value: Some(error_description.as_bytes()),
                }),
        );
        if let Some(payload) = inner_message.payload.as_deref() {
            record = record.payload(payload);
        }
        self.producer.send_record(record, self.dlq_timeout).await
    }
}

/// Counts for one reporting window.
#[derive(Default)]
struct DlqWindow {
    dead_lettered: u64,
    failed: u64,
    last_error: Option<String>,
    failure_logged: bool,
}

impl DlqWindow {
    fn record(&mut self, settler: &InvalidSettler, at: SourceOffset, result: Result<(), KafkaError>) {
        match result {
            Ok(()) => {
                self.dead_lettered += 1;
                settler.settle(at, InvalidOutcome::DeadLettered);
            }
            Err(error) => {
                self.failed += 1;
                if self.failure_logged {
                    self.last_error = Some(error.to_string());
                } else {
                    self.failure_logged = true;
                    error!(%error, partition = at.partition, offset = at.offset, "Failed to write to DLQ");
                }
                settler.settle(at, InvalidOutcome::DeliveryFailed);
            }
        }
    }

    fn report(&mut self, dropped: u64) {
        if self.dead_lettered > 0 {
            info!(redirected = self.dead_lettered, "Redirected messages to DLQ");
        }
        if let Some(last_error) = self.last_error.take() {
            error!(
                failed = self.failed,
                %last_error, "Failed to write to DLQ (first failure of the window logged above)"
            );
        }
        if dropped > 0 {
            error!(dropped, "DLQ queue full: discarded invalid messages without a DLQ record");
        }
        *self = Self::default();
    }
}

pub struct DLQHandler<P: KafkaProducer = FutureProducer> {
    publisher: DlqPublisher<P>,
    max_inflight: usize,
    dlq_rx: InvalidMessageRx,
}

impl<P: KafkaProducer> DLQHandler<P> {
    pub fn with_producer(
        config: &'static CoreConfig,
        dlq_rx: InvalidMessageRx,
        producer: Arc<P>,
    ) -> Self {
        let kafka_config = &config.kafka;
        Self {
            publisher: DlqPublisher {
                producer,
                unknown_key: kafka_config.dlq_config.dlq_unknown_key.as_bytes(),
                dlq_topic: &kafka_config.dlq_config.topic.name,
                dlq_timeout: rdkafka::util::Timeout::from(Duration::from_secs(u64::from(
                    kafka_config.dlq_config.publish_timeout_sec,
                ))),
            },
            max_inflight: usize::from(kafka_config.dlq_config.max_inflight_sends.get()),
            dlq_rx,
        }
    }

    /// Produces with up to `max_inflight_sends` deliveries outstanding. Returns once the channel
    /// is closed and every outstanding delivery has finished (the shutdown drain relies on this).
    pub async fn ingest(self) {
        let Self {
            publisher,
            max_inflight,
            mut dlq_rx,
        } = self;
        info!(max_inflight, "Starting DLQ ingestion loop");

        let dropped = Arc::clone(&dlq_rx.dropped);
        let settler = dlq_rx.settler.clone();
        let mut in_flight = FuturesUnordered::new();
        let mut window = DlqWindow::default();
        let mut report = tokio::time::interval_at(
            Instant::now() + DLQ_REPORT_INTERVAL,
            DLQ_REPORT_INTERVAL,
        );
        report.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut receiving = true;

        while receiving || !in_flight.is_empty() {
            tokio::select! {
                message = dlq_rx.recv(), if receiving && in_flight.len() < max_inflight => {
                    match message {
                        Some(message) => in_flight.push(publisher.deliver(message)),
                        None => receiving = false,
                    }
                }
                Some((at, result)) = in_flight.next() => window.record(&settler, at, result),
                _ = report.tick() => window.report(dropped.swap(0, Ordering::Relaxed)),
            }
        }
        window.report(dropped.swap(0, Ordering::Relaxed));
        warn!("DLQ ingestion loop terminated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::partial_message::PartialMessage;
    use rdkafka::Timestamp;
    use rdkafka::message::Headers;
    use rdkafka::producer::future_producer::Delivery;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

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
        pub delay: Duration,
        pub active: Arc<AtomicUsize>,
        pub max_active: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl KafkaProducer for MockKafkaProducer {
        async fn send_record<'a>(
            &self,
            record: FutureRecord<'a, [u8], [u8]>,
            _timeout: rdkafka::util::Timeout,
        ) -> OwnedDeliveryResult {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.active.fetch_sub(1, Ordering::SeqCst);

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
                    extracted_headers.push((h.key.to_string(), h.value.map(<[u8]>::to_vec)));
                }
            }

            let captured = CapturedRecord {
                topic: record.topic.to_string(),
                key: record.key.map(<[u8]>::to_vec),
                payload: record.payload.map(<[u8]>::to_vec),
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

    fn failed_message(offset: i64, key: Option<&[u8]>) -> PartialMessageWithError {
        PartialMessageWithError {
            error_msg: format!("Err {offset}"),
            message: PartialMessage {
                key: key.map(Box::from),
                partition: 0,
                offset,
                payload: None,
                origin: MessageOrigin::SensorData,
            },
        }
    }

    fn mock_config(max_inflight_sends: u16) -> &'static CoreConfig {
        let mut config = CoreConfig::mock_config();
        config.kafka.dlq_config.max_inflight_sends =
            std::num::NonZeroU16::new(max_inflight_sends).unwrap();
        Box::leak(Box::new(config))
    }

    #[tokio::test]
    async fn test_send_to_dlq_constructs_headers_and_fallback_key() {
        let config: &'static CoreConfig = Box::leak(Box::new(CoreConfig::mock_config()));
        let (_tx, rx) = invalid_message_channel(10, InvalidSettler::detached());
        let mock_producer = MockKafkaProducer::default();

        let handler = DLQHandler::with_producer(config, rx, Arc::new(mock_producer.clone()));

        let failed_msg = PartialMessageWithError {
            error_msg: "Parse Error".to_string(),
            message: PartialMessage {
                key: None, // No key provided, should fallback to UNKNOWN_KEY
                partition: 2,
                offset: 100,
                payload: Some(Box::from(b"bad bytes".to_vec())),
                origin: MessageOrigin::SensorData,
            },
        };

        let result = handler.publisher.send_to_dlq(failed_msg).await;
        assert!(result.is_ok());

        let records = mock_producer.sent_records.lock().unwrap();
        assert_eq!(records.len(), 1);

        let rec = &records[0];
        assert_eq!(rec.topic, "dlq");
        assert_eq!(rec.key.as_deref(), Some(b"UNKNOWN_KEY".as_slice()));
        assert_eq!(rec.payload.as_deref(), Some(b"bad bytes".as_slice()));

        let headers_map: std::collections::HashMap<_, _> = rec
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone().unwrap()))
            .collect();

        assert_eq!(headers_map.get("o_partition").unwrap(), b"2");
        assert_eq!(headers_map.get("o_offset").unwrap(), b"100");
        assert_eq!(
            headers_map.get("o_error").unwrap(),
            b"Parse Error".as_slice()
        );
    }

    #[tokio::test]
    async fn test_ingest_loop_processes_multiple_messages() {
        let config: &'static CoreConfig = Box::leak(Box::new(CoreConfig::mock_config()));
        let (tx, rx) = invalid_message_channel(10, InvalidSettler::detached());
        let mock_producer = MockKafkaProducer::default();

        let handler = DLQHandler::with_producer(config, rx, Arc::new(mock_producer.clone()));

        tx.send(failed_message(1, Some(b"key_1"))).await.unwrap();
        tx.send(failed_message(2, Some(b"key_2"))).await.unwrap();
        drop(tx);

        handler.ingest().await;

        let records = mock_producer.sent_records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_deref(), Some(b"key_1".as_slice()));
        assert_eq!(records[1].key.as_deref(), Some(b"key_2".as_slice()));
    }

    #[tokio::test]
    async fn ingest_overlaps_deliveries_up_to_the_limit_and_drains_on_close() {
        let config = mock_config(8);
        let (tx, rx) = invalid_message_channel(100, InvalidSettler::detached());
        let mock_producer = MockKafkaProducer {
            delay: Duration::from_millis(20),
            ..MockKafkaProducer::default()
        };
        let handler = DLQHandler::with_producer(config, rx, Arc::new(mock_producer.clone()));

        for offset in 0..40 {
            tx.send(failed_message(offset, None)).await.unwrap();
        }
        drop(tx);

        let started = std::time::Instant::now();
        handler.ingest().await;

        assert_eq!(mock_producer.sent_records.lock().unwrap().len(), 40);
        assert_eq!(mock_producer.max_active.load(Ordering::SeqCst), 8);
        assert_eq!(mock_producer.active.load(Ordering::SeqCst), 0);
        // Serial delivery would take 40 * 20 ms.
        assert!(started.elapsed() < Duration::from_millis(600));
    }

    #[tokio::test]
    async fn failed_deliveries_do_not_stop_the_loop() {
        let config = mock_config(4);
        let (tx, rx) = invalid_message_channel(10, InvalidSettler::detached());
        let mock_producer = MockKafkaProducer {
            should_fail: true,
            ..MockKafkaProducer::default()
        };
        let handler = DLQHandler::with_producer(config, rx, Arc::new(mock_producer.clone()));

        for offset in 0..5 {
            tx.send(failed_message(offset, None)).await.unwrap();
        }
        drop(tx);

        handler.ingest().await;

        assert_eq!(mock_producer.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn try_offer_hands_the_record_back_when_the_queue_is_full() {
        let (tx, mut rx) = invalid_message_channel(1, InvalidSettler::detached());

        assert!(matches!(tx.try_offer(failed_message(1, None)), DlqOffer::Queued));
        let DlqOffer::Dropped(rejected) = tx.try_offer(failed_message(2, None)) else {
            panic!("second record should not fit");
        };
        assert_eq!(rejected.message.source_offset().offset, 2);
        assert_eq!(rx.dropped.load(Ordering::Relaxed), 1);

        // Closed channel behaves the same: dropped, counted, offset recoverable.
        assert_eq!(rx.rx.try_recv().unwrap().message.offset, 1);
        drop(rx.rx);
        assert!(matches!(
            tx.try_offer(failed_message(3, None)),
            DlqOffer::Dropped(_)
        ));
        assert_eq!(rx.dropped.load(Ordering::Relaxed), 2);
    }

    fn registered_ledgers() -> (
        InvalidSettler,
        Arc<crate::messaging::offset_ledger::testing::RecordingSink>,
        Arc<crate::messaging::offset_ledger::testing::RecordingSink>,
    ) {
        use crate::messaging::offset_ledger::testing::recording_ledger;
        let (sensor_ledger, sensor_sink) = recording_ledger();
        let (control_ledger, control_sink) = recording_ledger();
        for offset in 0..4 {
            sensor_ledger.register(0, offset);
        }
        for offset in 10..12 {
            control_ledger.register(0, offset);
        }
        (
            InvalidSettler::new(sensor_ledger, control_ledger),
            sensor_sink,
            control_sink,
        )
    }

    fn control_failed_message(offset: i64) -> PartialMessageWithError {
        let mut message = failed_message(offset, None);
        message.message.origin = MessageOrigin::Control;
        message
    }

    #[tokio::test]
    async fn delivered_records_settle_into_the_ledger_of_their_consumer() {
        let (settler, sensor_sink, control_sink) = registered_ledgers();
        let (tx, rx) = invalid_message_channel(10, settler);
        let handler = DLQHandler::with_producer(
            mock_config(1),
            rx,
            Arc::new(MockKafkaProducer::default()),
        );

        tx.send(failed_message(0, None)).await.unwrap();
        tx.send(failed_message(1, None)).await.unwrap();
        tx.send(control_failed_message(10)).await.unwrap();
        drop(tx);
        handler.ingest().await;

        assert_eq!(sensor_sink.positions(), [(0, 2)]);
        assert_eq!(control_sink.positions(), [(0, 11)]);
    }

    #[tokio::test]
    async fn failed_deliveries_are_settled_too() {
        let (settler, sensor_sink, _control_sink) = registered_ledgers();
        let (tx, rx) = invalid_message_channel(10, settler);
        let producer = MockKafkaProducer {
            should_fail: true,
            ..MockKafkaProducer::default()
        };
        let handler = DLQHandler::with_producer(mock_config(1), rx, Arc::new(producer));

        for offset in 0..4 {
            tx.send(failed_message(offset, None)).await.unwrap();
        }
        drop(tx);
        handler.ingest().await;

        assert_eq!(sensor_sink.positions(), [(0, 4)]);
    }

    #[test]
    fn dropped_records_settle_through_the_channels_settler() {
        let (settler, sensor_sink, control_sink) = registered_ledgers();
        let (tx, _rx) = invalid_message_channel(1, settler);

        let mut dropped = OffsetSet::default();
        for offset in 0..4 {
            dropped.push(0, offset);
        }
        tx.settler()
            .settle_set(MessageOrigin::SensorData, &dropped, InvalidOutcome::Dropped);
        tx.settler().settle(
            control_failed_message(10).message.source_offset(),
            InvalidOutcome::Dropped,
        );

        assert_eq!(sensor_sink.positions(), [(0, 4)]);
        assert_eq!(control_sink.positions(), [(0, 11)]);
    }

    #[test]
    fn a_queued_record_is_not_settled_until_it_is_delivered() {
        let (settler, sensor_sink, _control_sink) = registered_ledgers();
        let (tx, _rx) = invalid_message_channel(1, settler);

        assert!(matches!(tx.try_offer(failed_message(0, None)), DlqOffer::Queued));

        assert!(sensor_sink.stored().is_empty());
    }
}
