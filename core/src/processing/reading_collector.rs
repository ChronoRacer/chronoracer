use crate::messaging::dlq_handler::{DlqOffer, InvalidMessageTx, InvalidOutcome};
use crate::messaging::message_sink::BatchSinkRx;
use crate::messaging::offset_tracker::OffsetSet;
use crate::messaging::partial_message::{MessageOrigin, PartialMessage, PartialMessageWithError};
use crate::persistence::persistence_entrypoint::SensorDataBatch;
use crate::pipeline_stats::{PipelineStats, age_ms, unix_now_ms};
use crate::processing::sensor_data_message::{SensorIdInterner, UnwoundSensorDataMessage};
use crate::telemetry::PreviewHub;
use std::cell::RefCell;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

thread_local! {
    /// One interner per rayon thread, so decoding never contends on it.
    static SENSOR_IDS: RefCell<SensorIdInterner> = RefCell::new(SensorIdInterner::default());
}

pub struct ReadingCollector {
    flush_rx: BatchSinkRx,
    dlq_tx: InvalidMessageTx,
    processed_data_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
    rayon_pool: Arc<rayon::ThreadPool>,
    processing_slots: Arc<Semaphore>,
    preview: PreviewHub,
    stats: Arc<PipelineStats>,
}

impl ReadingCollector {
    #[must_use]
    pub fn new(
        flush_rx: BatchSinkRx,
        dlq_tx: InvalidMessageTx,
        processed_data_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
        rayon_pool: Arc<rayon::ThreadPool>,
        preview: PreviewHub,
        stats: Arc<PipelineStats>,
    ) -> Self {
        Self {
            flush_rx,
            dlq_tx,
            processed_data_tx,
            processing_slots: Arc::new(Semaphore::new(rayon_pool.current_num_threads())),
            rayon_pool,
            preview,
            stats,
        }
    }

    pub async fn ingest(&mut self) {
        info!("Starting Reading collector ingestion loop");
        while let Some(partial_messages) = self.flush_rx.recv().await {
            if partial_messages.is_empty() {
                continue;
            }
            // Rayon's queue is unbounded: one slot per pool thread keeps the batches queued or
            // running bounded, so a slow consumer backs up into the sink channel, not into memory.
            let Ok(processing_slot) = Arc::clone(&self.processing_slots).acquire_owned().await
            else {
                error!("Reading collector processing slots closed");
                break;
            };
            let success_tx = self.processed_data_tx.clone();
            let dlq_tx = self.dlq_tx.clone();
            let preview = self.preview.clone();
            let stats = Arc::clone(&self.stats);
            self.rayon_pool.spawn(move || {
                let _processing_slot = processing_slot;
                let processed = decode_batch(partial_messages, &dlq_tx);

                if !processed.is_empty() {
                    record_decoded(&stats, &processed);
                    preview.offer(&processed);
                    if let Err(error) = success_tx.blocking_send(processed) {
                        error!(%error, "Failed to send processed batch downstream");
                    }
                }
            });
        }
        warn!("Reading collector ingestion loop terminated");
    }
}

fn decode_batch(batch: Vec<PartialMessage>, dlq_tx: &InvalidMessageTx) -> SensorDataBatch {
    let mut processed = Vec::with_capacity(batch.len());
    let mut dropped = OffsetSet::default();
    SENSOR_IDS.with_borrow_mut(|sensor_ids| {
        for partial_message in batch {
            let source = partial_message.source_offset();
            match UnwoundSensorDataMessage::decode(
                partial_message.payload.as_deref(),
                source,
                partial_message.created_ms,
                sensor_ids,
            ) {
                Ok(message) => processed.push(Arc::new(message)),
                Err(error) => dead_letter(
                    dlq_tx,
                    PartialMessageWithError::new(partial_message, &error),
                    &mut dropped,
                ),
            }
        }
    });
    // One settlement for the whole batch, so a flood of invalid messages does not contend per message.
    dlq_tx
        .settler()
        .settle_set(MessageOrigin::SensorData, &dropped, InvalidOutcome::Dropped);
    processed
}

/// Points are available to the preview from this moment on, so this is where their latency ends.
fn record_decoded(stats: &PipelineStats, processed: &SensorDataBatch) {
    let now_ms = unix_now_ms();
    let mut points = 0;
    for message in processed {
        points += message.readings.len() as u64;
        if let Some(created_ms) = message.created_ms {
            stats.record_preview_latency(age_ms(created_ms, now_ms));
        }
    }
    stats.add_decoded_points(points);
}

/// Parsing must not wait on the DLQ: a bad-schema deploy makes every message invalid, and a
/// blocked parse thread would stall the whole ingest. A record that does not fit is discarded,
/// counted by the channel, and reported by the DLQ handler. Its offset is collected into `dropped`
/// to be settled; a queued record is settled by the DLQ handler once delivered.
fn dead_letter(
    dlq_tx: &InvalidMessageTx,
    message: PartialMessageWithError,
    dropped: &mut OffsetSet,
) {
    match dlq_tx.try_offer(message) {
        DlqOffer::Queued => {}
        DlqOffer::Dropped(rejected) => {
            dropped.push(rejected.message.partition, rejected.message.offset);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::dlq_handler::{
        InvalidMessageRx, InvalidSettler, invalid_message_channel,
    };
    use crate::messaging::offset_ledger::testing::recording_ledger;
    use crate::messaging::partial_message::PartialMessage;
    use crate::persistence::persistence_entrypoint::SensorDataBatch;
    use crate::runtime_configuration::RetainReadTimeValue;
    use crate::telemetry::PreviewHub;
    use apache_avro::types::Value;
    use apache_avro::writer::datum::GenericDatumWriter;
    use compact_str::CompactString;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn build_pool() -> rayon::ThreadPool {
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("Failed to build Rayon pool")
    }

    fn idle_preview() -> PreviewHub {
        let driver = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
            "driver",
        ))));
        PreviewHub::new(driver, 16, Duration::from_millis(250), 4)
    }

    fn load_avro_schema() -> apache_avro::Schema {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("avro")
            .join("sensor_data.avsc");
        apache_avro::Schema::parse_str(&std::fs::read_to_string(path).expect("schema file"))
            .expect("sensor data schema should load")
    }

    fn write_valid_avro(
        schema: &apache_avro::Schema,
        sensor_id: String,
        first_reading_value: f64,
    ) -> Vec<u8> {
        let record = Value::Record(vec![
            ("sensor_id".to_string(), Value::String(sensor_id)),
            (
                "timestamp".to_string(),
                Value::TimestampNanos(1_000_000_000),
            ),
            (
                "readings".to_string(),
                Value::Array(vec![Value::Record(vec![
                    ("delta".to_string(), Value::Int(234)),
                    ("value".to_string(), Value::Double(first_reading_value)),
                ])]),
            ),
        ]);
        let writer = GenericDatumWriter::builder(schema)
            .build()
            .expect("sensor data reader should build");
        writer
            .write_value_to_vec(record)
            .expect("Failed to serialize to raw datum")
    }

    fn valid_message(schema: &apache_avro::Schema, offset: i64) -> PartialMessage {
        PartialMessage {
            key: None,
            offset,
            partition: 0,
            payload: Some(write_valid_avro(schema, "test123".to_string(), 1.0).into()),
            origin: MessageOrigin::SensorData,
            created_ms: None,
        }
    }

    fn corrupt_message(offset: i64) -> PartialMessage {
        PartialMessage {
            key: None,
            offset,
            partition: 1,
            payload: Some(vec![0xFF, 0xFF, 0xFF].into()),
            origin: MessageOrigin::SensorData,
            created_ms: None,
        }
    }

    struct Harness {
        flush_tx: mpsc::Sender<Vec<PartialMessage>>,
        dlq_rx: InvalidMessageRx,
        processed_rx: mpsc::Receiver<SensorDataBatch>,
        ingest_handle: tokio::task::JoinHandle<()>,
        stats: Arc<PipelineStats>,
    }

    fn start(dlq_capacity: usize, preview: PreviewHub) -> Harness {
        start_with(dlq_capacity, preview, InvalidSettler::detached())
    }

    fn start_with(dlq_capacity: usize, preview: PreviewHub, settler: InvalidSettler) -> Harness {
        let (flush_tx, flush_rx) = mpsc::channel::<Vec<PartialMessage>>(10);
        let (dlq_tx, dlq_rx) = invalid_message_channel(dlq_capacity, settler);
        let (processed_tx, processed_rx) = mpsc::channel::<SensorDataBatch>(10);
        let stats = Arc::new(PipelineStats::default());
        let mut collector = ReadingCollector::new(
            flush_rx,
            dlq_tx,
            processed_tx,
            build_pool().into(),
            preview,
            Arc::clone(&stats),
        );
        let ingest_handle = tokio::spawn(async move {
            collector.ingest().await;
        });
        Harness {
            flush_tx,
            dlq_rx,
            processed_rx,
            ingest_handle,
            stats,
        }
    }

    #[tokio::test]
    async fn decoded_points_and_preview_latency_are_recorded_before_the_offer() {
        let schema = load_avro_schema();
        let mut harness = start(10, idle_preview());
        let mut stamped = valid_message(&schema, 1);
        stamped.created_ms = Some(unix_now_ms() - 50);

        harness
            .flush_tx
            .send(vec![stamped, valid_message(&schema, 2), corrupt_message(3)])
            .await
            .unwrap();
        timeout(Duration::from_secs(2), harness.processed_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let report = harness.stats.take_report();
        assert_eq!(report.decoded_points, 2, "invalid messages have no points");
        assert_eq!(
            report.preview.count(),
            1,
            "only messages with a Kafka timestamp have a latency"
        );
        assert!(report.preview.quantile_bound_ms(0.5) >= Some(50));

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
    }

    #[tokio::test]
    async fn test_ingest_successful_processing() {
        let schema = load_avro_schema();
        let preview = idle_preview();
        let _subscriber = preview
            .subscribe("test", crate::telemetry::Filter::All)
            .expect("subscriber slot");
        let mut harness = start(10, preview.clone());

        let sensor_id = "test123";
        let first_reading_value = 1.0;
        let message = PartialMessage {
            key: None,
            offset: 0,
            partition: 0,
            payload: Some(Box::from(write_valid_avro(
                &schema,
                sensor_id.to_string(),
                first_reading_value,
            ))),
            origin: MessageOrigin::SensorData,
            created_ms: None,
        };
        harness
            .flush_tx
            .send(vec![message])
            .await
            .expect("Failed to send message");

        let result = timeout(Duration::from_secs(2), harness.processed_rx.recv()).await;
        assert!(harness.dlq_rx.try_recv().is_none());

        assert!(
            result.is_ok(),
            "Timed out waiting for Rayon to process payload"
        );
        let result = result.unwrap();
        assert!(result.is_some(), "Expected processed message, got None");
        let processed_batch = result.unwrap();
        assert_eq!(processed_batch.len(), 1);
        let content = &processed_batch[0];
        assert_eq!(content.sensor_id.as_ref(), sensor_id);
        assert!(!content.readings.is_empty());
        assert_eq!(content.readings[0].value, first_reading_value);
        assert_eq!(content.readings[0].timestamp, 1_000_000_234);
        preview.pull_ingress();
        assert_eq!(preview.queued_points(), content.readings.len());

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
    }

    #[tokio::test]
    async fn test_ingest_routes_to_dlq_on_deserialization_error() {
        let mut harness = start(10, idle_preview());

        harness
            .flush_tx
            .send(vec![corrupt_message(10)])
            .await
            .expect("Failed to send bad message");

        let result = timeout(Duration::from_secs(2), harness.dlq_rx.recv()).await;
        assert!(harness.processed_rx.try_recv().is_err());

        assert!(result.is_ok(), "Timed out waiting for DLQ failure output");
        let dlq_msg = result
            .unwrap()
            .expect("Expected PartialMessageWithError on DLQ");
        assert_eq!(dlq_msg.message.partition, 1);
        assert_eq!(dlq_msg.message.offset, 10);

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
    }

    #[tokio::test]
    async fn mixed_batch_keeps_valid_messages_and_dead_letters_the_rest() {
        let schema = load_avro_schema();
        let mut harness = start(10, idle_preview());

        harness
            .flush_tx
            .send(vec![
                valid_message(&schema, 1),
                corrupt_message(2),
                valid_message(&schema, 3),
            ])
            .await
            .unwrap();

        let processed = timeout(Duration::from_secs(2), harness.processed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(processed.len(), 2);
        assert!(Arc::ptr_eq(
            &processed[0].sensor_id,
            &processed[1].sensor_id
        ));
        let dlq_msg = harness.dlq_rx.try_recv().expect("one invalid message");
        assert_eq!(dlq_msg.message.offset, 2);
        assert!(harness.dlq_rx.try_recv().is_none());

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
    }

    #[tokio::test]
    async fn full_dlq_never_blocks_parsing() {
        let schema = load_avro_schema();
        let mut harness = start(1, idle_preview());

        // Nothing drains the DLQ: only one of the invalid messages fits.
        let mut batch: Vec<_> = (0..50).map(corrupt_message).collect();
        batch.push(valid_message(&schema, 50));
        harness.flush_tx.send(batch).await.unwrap();
        harness
            .flush_tx
            .send(vec![valid_message(&schema, 51)])
            .await
            .unwrap();

        for expected_batch_len in [1, 1] {
            let processed = timeout(Duration::from_secs(2), harness.processed_rx.recv())
                .await
                .expect("parse thread stalled on a full DLQ")
                .unwrap();
            assert_eq!(processed.len(), expected_batch_len);
        }
        assert_eq!(harness.dlq_rx.try_recv().unwrap().message.offset, 0);
        assert!(harness.dlq_rx.try_recv().is_none());

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
    }

    #[tokio::test]
    async fn empty_batches_are_skipped() {
        let schema = load_avro_schema();
        let mut harness = start(10, idle_preview());

        harness.flush_tx.send(Vec::new()).await.unwrap();
        harness.flush_tx.send(Vec::new()).await.unwrap();
        harness
            .flush_tx
            .send(vec![valid_message(&schema, 1)])
            .await
            .unwrap();

        let processed = timeout(Duration::from_secs(2), harness.processed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(processed.len(), 1);

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
        assert!(harness.processed_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn processed_messages_carry_their_source_offset() {
        let schema = load_avro_schema();
        let mut harness = start(10, idle_preview());

        harness
            .flush_tx
            .send(vec![
                valid_message(&schema, 4),
                corrupt_message(5),
                valid_message(&schema, 6),
            ])
            .await
            .unwrap();

        let processed = timeout(Duration::from_secs(2), harness.processed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let sources: Vec<_> = processed
            .iter()
            .map(|message| {
                (
                    message.source.origin,
                    message.source.partition,
                    message.source.offset,
                )
            })
            .collect();
        assert_eq!(
            sources,
            [
                (MessageOrigin::SensorData, 0, 4),
                (MessageOrigin::SensorData, 0, 6)
            ]
        );

        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
    }

    #[tokio::test]
    async fn discarded_invalid_messages_are_settled_and_queued_ones_are_left_to_the_dlq() {
        let (ledger, sink) = recording_ledger();
        for offset in 0..50 {
            ledger.register(1, offset);
        }
        let mut harness = start_with(
            1,
            idle_preview(),
            InvalidSettler::new(ledger.clone(), recording_ledger().0),
        );

        // Nothing drains the DLQ: only offset 0 fits, the other 49 are discarded.
        harness
            .flush_tx
            .send((0..50).map(corrupt_message).collect())
            .await
            .unwrap();
        drop(harness.flush_tx);
        let _ = harness.ingest_handle.await;
        // Decoding runs on the rayon pool, independently of the ingest loop.
        timeout(Duration::from_secs(2), async {
            while ledger.lag()[0].pending > 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("discarded offsets were not settled");

        let lag = ledger.lag();
        assert_eq!(
            (lag[0].partition, lag[0].pending, lag[0].position),
            (1, 1, 0)
        );
        assert!(sink.stored().is_empty(), "offset 0 is still with the DLQ");
        assert_eq!(harness.dlq_rx.try_recv().unwrap().message.offset, 0);

        ledger.settle_one(1, 0);
        assert_eq!(sink.positions(), [(1, 50)]);
    }
}
