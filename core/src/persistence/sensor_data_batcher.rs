use crate::messaging::offset_tracker::OffsetSet;
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::persistence::sensor_data_flusher::BatcherWiringType;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// The `influxdb3_client::Point` implementation is overengineered for this use case and has in-built deduplication and sorting.
/// To avoid this overhead and with very slim points, the batcher writes line protocol directly,
/// see [`LineWriter`].
///
/// Typical size of one line including its newline: sensor id around 10 bytes, race and driver ids
/// around 12, a value of around 10 characters and a 19-digit timestamp. Only a `String`
/// pre-size; longer lines make the buffer grow.
pub const LINE_PROTOCOL_ESTIMATED_LENGTH: usize = 96;

/// Appends `measurement,race=..,driver=.. val=.. timestamp\n` lines. The part up to `val=` is the
/// same for every reading of a message, so it is built once per message.
pub struct LineWriter {
    race_id: Arc<str>,
    prefix: String,
}

impl LineWriter {
    #[must_use]
    pub fn new(race_id: Arc<str>) -> Self {
        Self {
            race_id,
            prefix: String::new(),
        }
    }

    /// Appends one line per reading and returns how many it wrote.
    pub fn write_message(
        &mut self,
        out: &mut String,
        data: &UnwoundSensorDataMessage,
        driver: &str,
    ) -> usize {
        self.prefix.clear();
        self.prefix.push_str(&data.sensor_id);
        self.prefix.push_str(",race=");
        self.prefix.push_str(&self.race_id);
        self.prefix.push_str(",driver=");
        self.prefix.push_str(driver);
        self.prefix.push_str(" val=");

        out.reserve(data.readings.len() * (self.prefix.len() + 32));
        let mut float = ryu::Buffer::new();
        let mut int = itoa::Buffer::new();
        for reading in &data.readings {
            out.push_str(&self.prefix);
            out.push_str(float.format(reading.value));
            out.push(' ');
            out.push_str(int.format(reading.timestamp));
            out.push('\n');
        }
        data.readings.len()
    }
}

/// What the flusher writes in one request: the line protocol of the points, and the Kafka messages
/// they came from. All readings of a message are in the same batch, so writing the batch settles
/// every message listed.
pub struct PointBatch {
    /// One line per point, each ending in a newline.
    pub lines: String,
    pub point_count: usize,
    pub offsets: OffsetSet,
    /// Earliest Kafka record timestamp of the batch's messages, for the persist latency metric.
    /// `NO_TIMESTAMP` when none has one.
    pub oldest_created_ms: i64,
}

/// `oldest_created_ms` of a batch with no timestamped message.
pub const NO_TIMESTAMP: i64 = i64::MAX;

impl PointBatch {
    #[must_use]
    pub fn oldest_created(&self) -> Option<i64> {
        (self.oldest_created_ms != NO_TIMESTAMP).then_some(self.oldest_created_ms)
    }

    /// A message without readings has no points but still has an offset to settle.
    fn is_empty(&self) -> bool {
        self.point_count == 0 && self.offsets.is_empty()
    }
}

pub type PointBatchTx = tokio::sync::mpsc::Sender<PointBatch>;
pub type PointBatchRx = tokio::sync::mpsc::Receiver<PointBatch>;

pub struct SensorDataBatcher {
    data_rx: SensorDataRx,
    pool_rx: tokio::sync::mpsc::Receiver<String>,
    batch_tx: PointBatchTx,
    writer: LineWriter,
    driver_id: &'static RetainReadTimeValue<CompactString>,
    flush_max_combined_batch_size: usize,
    flush_interval_ms: u32,
    batcher_wiring_type: BatcherWiringType,
}

impl SensorDataBatcher {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        flush_max_combined_batch_size: usize,
        flush_interval_ms: u32,
        data_rx: SensorDataRx,
        batch_tx: PointBatchTx,
        pool_rx: tokio::sync::mpsc::Receiver<String>,
        race_id: Arc<str>,
        driver_id: &'static RetainReadTimeValue<CompactString>,
        batcher_wiring_type: BatcherWiringType,
    ) -> Self {
        Self {
            data_rx,
            pool_rx,
            batch_tx,
            writer: LineWriter::new(race_id),
            driver_id,
            flush_max_combined_batch_size,
            flush_interval_ms,
            batcher_wiring_type,
        }
    }

    pub async fn ingest(&mut self) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(u64::from(self.flush_interval_ms)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let wiring_type = self.batcher_wiring_type;
        info!("Starting batcher ingestion loop, batch_type={wiring_type:?}");

        let mut fresh_batch = || PointBatch {
            lines: self.pool_rx.try_recv().unwrap_or_else(|_| {
                String::with_capacity(
                    self.flush_max_combined_batch_size * LINE_PROTOCOL_ESTIMATED_LENGTH,
                )
            }),
            point_count: 0,
            offsets: OffsetSet::default(),
            oldest_created_ms: NO_TIMESTAMP,
        };

        let mut buffer = fresh_batch();

        loop {
            tokio::select! {
                data_batch = self.data_rx.recv() => {
                    let Some(data_batch) = data_batch else {
                        if !buffer.is_empty() {
                            let _ = self.batch_tx.send(buffer).await;
                        }
                        warn!("Closing batcher ingestion loop, batch_type={wiring_type:?}");
                        break;
                    };

                    let driver = self.driver_id.get();
                    let mut downstream_closed = false;
                    for data in data_batch {
                        buffer.point_count +=
                            self.writer
                                .write_message(&mut buffer.lines, &data, &driver);
                        buffer
                            .offsets
                            .push(data.source.partition, data.source.offset);
                        buffer.oldest_created_ms = buffer
                            .oldest_created_ms
                            .min(data.created_ms.unwrap_or(NO_TIMESTAMP));

                        if buffer.point_count >= self.flush_max_combined_batch_size {
                            let to_flush = std::mem::replace(&mut buffer, fresh_batch());
                            if !send_batch(&self.batch_tx, to_flush, wiring_type).await {
                                downstream_closed = true;
                                break;
                            }

                            interval.reset();
                        }
                    }
                    if downstream_closed {
                        break;
                    }
                }

                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        let to_flush = std::mem::replace(&mut buffer, fresh_batch());
                        if !send_batch(&self.batch_tx, to_flush, wiring_type).await {
                            break;
                        }
                    }
                }

            }
        }
    }
}

/// Send only fails once the flusher is gone, so `false` means the ingestion loop must end.
async fn send_batch(
    batch_tx: &PointBatchTx,
    batch: PointBatch,
    wiring_type: BatcherWiringType,
) -> bool {
    if batch_tx.send(batch).await.is_err() {
        error!("Batch channel closed. Closing ingestion loop, batch_type={wiring_type:?}");
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::partial_message::{MessageOrigin, SourceOffset};
    use crate::persistence::persistence_entrypoint::SensorDataBatch;
    use crate::processing::sensor_data_message::{AbsoluteSensorReading, UnwoundSensorDataMessage};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    fn message(sensor_id: &str, value: f64) -> Arc<UnwoundSensorDataMessage> {
        Arc::new(UnwoundSensorDataMessage {
            sensor_id: Arc::from(sensor_id),
            readings: Box::new([AbsoluteSensorReading {
                value,
                timestamp: 1_000_000,
            }]),
            source: SourceOffset::default(),
            created_ms: None,
        })
    }

    /// Created at `1_000 + offset` ms, so later offsets are younger.
    fn message_at(
        sensor_id: &str,
        readings: usize,
        partition: i32,
        offset: i64,
    ) -> Arc<UnwoundSensorDataMessage> {
        Arc::new(UnwoundSensorDataMessage {
            sensor_id: Arc::from(sensor_id),
            readings: (0..readings)
                .map(|index| AbsoluteSensorReading {
                    value: 1.0,
                    timestamp: 1_000_000 + index as u64,
                })
                .collect(),
            source: SourceOffset {
                origin: MessageOrigin::SensorData,
                partition,
                offset,
            },
            created_ms: Some(1_000 + offset),
        })
    }

    fn batcher(
        max_points: usize,
        flush_interval_ms: u32,
    ) -> (
        SensorDataBatcher,
        mpsc::Sender<SensorDataBatch>,
        PointBatchRx,
    ) {
        let (data_tx, data_rx) = mpsc::channel(8);
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (_pool_tx, pool_rx) = mpsc::channel(1);
        let driver_id = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
            "driver",
        ))));
        let batcher = SensorDataBatcher::new(
            max_points,
            flush_interval_ms,
            data_rx,
            batch_tx,
            pool_rx,
            Arc::from("race"),
            driver_id,
            BatcherWiringType::Normal,
        );
        (batcher, data_tx, batch_rx)
    }

    fn ranges(batch: &PointBatch) -> Vec<(i32, i64, i64)> {
        batch
            .offsets
            .ranges()
            .iter()
            .map(|range| (range.partition, range.first, range.last))
            .collect()
    }

    #[tokio::test]
    async fn a_point_batch_lists_the_offsets_of_exactly_its_messages() {
        let (mut batcher, data_tx, mut batch_rx) = batcher(1_000, 1_000);
        let ingest = tokio::spawn(async move { batcher.ingest().await });

        data_tx
            .send(vec![
                message_at("a", 2, 0, 10),
                message_at("b", 1, 1, 70),
                message_at("a", 2, 0, 11),
                message_at("c", 3, 0, 12),
                message_at("d", 1, 0, 20),
            ])
            .await
            .unwrap();
        drop(data_tx);

        let batch = timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.point_count, 9);
        assert_eq!(ranges(&batch), [(0, 10, 12), (1, 70, 70), (0, 20, 20)]);
        ingest.await.unwrap();
    }

    #[tokio::test]
    async fn a_size_flush_never_splits_a_message_and_each_batch_owns_its_offsets() {
        let (mut batcher, data_tx, mut batch_rx) = batcher(5, 60_000);
        let ingest = tokio::spawn(async move { batcher.ingest().await });

        // 3 + 3 crosses the limit inside the second message: it stays whole in the first batch.
        data_tx
            .send(vec![
                message_at("a", 3, 0, 0),
                message_at("a", 3, 0, 1),
                message_at("a", 1, 0, 2),
            ])
            .await
            .unwrap();
        drop(data_tx);

        let first = timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((first.point_count, ranges(&first)), (6, vec![(0, 0, 1)]));
        assert_eq!((second.point_count, ranges(&second)), (1, vec![(0, 2, 2)]));
        assert_eq!(
            (first.oldest_created(), second.oldest_created()),
            (Some(1_000), Some(1_002)),
            "each batch reports the oldest of its own messages"
        );
        ingest.await.unwrap();
    }

    #[tokio::test]
    async fn a_message_without_readings_still_reaches_the_flusher_for_settlement() {
        let (mut batcher, data_tx, mut batch_rx) = batcher(100, 20);
        let ingest = tokio::spawn(async move { batcher.ingest().await });

        data_tx.send(vec![message_at("a", 0, 3, 41)]).await.unwrap();

        let batch = timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(batch.point_count == 0 && batch.lines.is_empty());
        assert_eq!(ranges(&batch), [(3, 41, 41)]);
        drop(data_tx);
        ingest.await.unwrap();
    }
    #[tokio::test]
    async fn ingest_converts_a_processed_batch_to_one_point_batch() {
        let (data_tx, data_rx) = mpsc::channel(1);
        let (batch_tx, mut batch_rx) = mpsc::channel(1);
        let (_pool_tx, pool_rx) = mpsc::channel(1);
        let driver_id = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
            "driver",
        ))));
        let mut batcher = SensorDataBatcher::new(
            100,
            1_000,
            data_rx,
            batch_tx,
            pool_rx,
            Arc::from("race"),
            driver_id,
            BatcherWiringType::Normal,
        );

        let ingest_handle = tokio::spawn(async move { batcher.ingest().await });
        data_tx
            .send(vec![message("sensor-1", 1.0), message("sensor-2", 2.0)])
            .await
            .unwrap();
        drop(data_tx);

        let point_batch = timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(point_batch.point_count, 2);
        assert_eq!(
            point_batch.lines,
            "sensor-1,race=race,driver=driver val=1.0 1000000\n\
             sensor-2,race=race,driver=driver val=2.0 1000000\n"
        );

        ingest_handle.await.unwrap();
    }

    fn reading(value: f64, timestamp: u64) -> AbsoluteSensorReading {
        AbsoluteSensorReading { value, timestamp }
    }

    #[test]
    fn line_protocol_format_is_exact() {
        let message = UnwoundSensorDataMessage {
            sensor_id: Arc::from("sensor-1"),
            readings: Box::new([
                reading(12.5, 1_700_000_000_123_456_789),
                reading(3.0, 1_700_000_000_123_456_790),
                reading(-0.25, 0),
                reading(f64::NAN, 1),
                reading(f64::INFINITY, 2),
            ]),
            source: SourceOffset::default(),
            created_ms: None,
        };
        let mut writer = LineWriter::new(Arc::from("race42"));
        let mut out = String::from("prev\n");
        assert_eq!(writer.write_message(&mut out, &message, "driver7"), 5);
        assert_eq!(
            out,
            "prev\n\
             sensor-1,race=race42,driver=driver7 val=12.5 1700000000123456789\n\
             sensor-1,race=race42,driver=driver7 val=3.0 1700000000123456790\n\
             sensor-1,race=race42,driver=driver7 val=-0.25 0\n\
             sensor-1,race=race42,driver=driver7 val=NaN 1\n\
             sensor-1,race=race42,driver=driver7 val=inf 2\n"
        );

        // The prefix of the previous message does not leak into the next one.
        let mut out = String::new();
        let other = UnwoundSensorDataMessage {
            sensor_id: Arc::from("s"),
            readings: Box::new([reading(1.0, 5)]),
            source: SourceOffset::default(),
            created_ms: None,
        };
        writer.write_message(&mut out, &other, "d");
        assert_eq!(out, "s,race=race42,driver=d val=1.0 5\n");
    }

    #[tokio::test]
    async fn a_driver_change_applies_from_the_next_incoming_batch() {
        let (mut batcher, data_tx, mut batch_rx) = batcher(1_000, 1_000);
        let driver = batcher.driver_id;
        let ingest = tokio::spawn(async move { batcher.ingest().await });

        data_tx.send(vec![message("a", 1.0)]).await.unwrap();
        // Let the batcher take the first batch before the driver changes.
        tokio::time::sleep(Duration::from_millis(50)).await;
        driver.update(CompactString::from("next"));
        data_tx.send(vec![message("a", 2.0)]).await.unwrap();
        drop(data_tx);

        // The interval may flush the first message on its own, so join whatever arrives.
        let mut lines = String::new();
        while let Some(batch) = timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .unwrap()
        {
            lines.push_str(&batch.lines);
        }
        assert_eq!(
            lines,
            "a,race=race,driver=driver val=1.0 1000000\n\
             a,race=race,driver=next val=2.0 1000000\n"
        );
        ingest.await.unwrap();
    }

    #[tokio::test]
    async fn ingest_ends_when_the_flusher_side_is_closed() {
        let (data_tx, data_rx) = mpsc::channel(1);
        let (batch_tx, batch_rx) = mpsc::channel(1);
        let (_pool_tx, pool_rx) = mpsc::channel(1);
        let driver_id = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
            "driver",
        ))));
        let mut batcher = SensorDataBatcher::new(
            1,
            1_000,
            data_rx,
            batch_tx,
            pool_rx,
            Arc::from("race"),
            driver_id,
            BatcherWiringType::Fast,
        );
        drop(batch_rx);

        data_tx.send(vec![message("sensor-1", 1.0)]).await.unwrap();
        timeout(Duration::from_secs(2), batcher.ingest())
            .await
            .expect("batcher must end once the batch channel is closed");
    }
}
