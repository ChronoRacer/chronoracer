use crate::messaging::offset_tracker::OffsetSet;
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::persistence::sensor_data_flusher::BatcherWiringType;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// The `influxdb3_client::Point` implementation is overengineered for this use case and has in-built deduplication and sorting.
/// To avoid this overhead and with very slim points, it's just better to build line protocol manually
pub struct SimplePoint {
    pub measurement: Arc<str>,
    pub driver: Arc<CompactString>,
    pub value: f64,
    pub timestamp_nanos: u64,
}

/// Typical size of one line including its newline: sensor id around 10 bytes, race and driver ids
/// around 12, a value of around 10 characters and a 19-digit timestamp. Only a `String`
/// pre-size; longer lines make the buffer grow.
pub const SIMPLE_POINT_PROTOCOL_ESTIMATED_LENGTH: usize = 96;

impl SimplePoint {
    /// Appends `measurement,race=..,driver=.. val=.. timestamp` without a trailing newline.
    pub fn write_line_protocol(&self, out: &mut String, race_id: &str) {
        // Writing into a `String` cannot fail
        let _ = write!(
            out,
            "{},race={},driver={} val={} {}",
            self.measurement, race_id, self.driver, self.value, self.timestamp_nanos
        );
    }
}

/// What the flusher writes in one request: the points, and the Kafka messages they came from. All
/// readings of a message are in the same batch, so writing the batch settles every message listed.
pub struct PointBatch {
    pub points: Vec<SimplePoint>,
    pub offsets: OffsetSet,
}

impl PointBatch {
    /// A message without readings has no points but still has an offset to settle.
    fn is_empty(&self) -> bool {
        self.points.is_empty() && self.offsets.is_empty()
    }
}

pub type PointBatchTx = tokio::sync::mpsc::Sender<PointBatch>;
pub type PointBatchRx = tokio::sync::mpsc::Receiver<PointBatch>;

pub struct SensorDataBatcher {
    data_rx: SensorDataRx,
    pool_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
    batch_tx: PointBatchTx,
    driver_id: &'static RetainReadTimeValue<CompactString>,
    flush_max_combined_batch_size: usize,
    flush_interval_ms: u32,
    batcher_wiring_type: BatcherWiringType,
}

impl SensorDataBatcher {
    #[must_use]
    pub fn new(
        flush_max_combined_batch_size: usize,
        flush_interval_ms: u32,
        data_rx: SensorDataRx,
        batch_tx: PointBatchTx,
        pool_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
        driver_id: &'static RetainReadTimeValue<CompactString>,
        batcher_wiring_type: BatcherWiringType,
    ) -> Self {
        Self {
            data_rx,
            pool_rx,
            batch_tx,
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
            points: self
                .pool_rx
                .try_recv()
                .unwrap_or_else(|_| Vec::with_capacity(self.flush_max_combined_batch_size)),
            offsets: OffsetSet::default(),
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
                        sensor_data_into_points(&data, &mut buffer.points, &driver);
                        buffer
                            .offsets
                            .push(data.source.partition, data.source.offset);

                        // Checked per message, never inside one: a message stays in one batch.
                        if buffer.points.len() >= self.flush_max_combined_batch_size {
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
fn sensor_data_into_points(
    data: &UnwoundSensorDataMessage,
    buffer: &mut Vec<SimplePoint>,
    current_driver_id: &Arc<CompactString>,
) {
    let shared_measurement = Arc::clone(&data.sensor_id);
    buffer.extend(data.readings.iter().map(|reading| SimplePoint {
        measurement: Arc::clone(&shared_measurement),
        driver: Arc::clone(current_driver_id),
        value: reading.value,
        timestamp_nanos: reading.timestamp,
    }));
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
        })
    }

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
        assert_eq!(batch.points.len(), 9);
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

        let first = timeout(Duration::from_secs(2), batch_rx.recv()).await.unwrap().unwrap();
        let second = timeout(Duration::from_secs(2), batch_rx.recv()).await.unwrap().unwrap();
        assert_eq!((first.points.len(), ranges(&first)), (6, vec![(0, 0, 1)]));
        assert_eq!((second.points.len(), ranges(&second)), (1, vec![(0, 2, 2)]));
        ingest.await.unwrap();
    }

    #[tokio::test]
    async fn a_message_without_readings_still_reaches_the_flusher_for_settlement() {
        let (mut batcher, data_tx, mut batch_rx) = batcher(100, 20);
        let ingest = tokio::spawn(async move { batcher.ingest().await });

        data_tx.send(vec![message_at("a", 0, 3, 41)]).await.unwrap();

        let batch = timeout(Duration::from_secs(2), batch_rx.recv()).await.unwrap().unwrap();
        assert!(batch.points.is_empty());
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
        assert_eq!(point_batch.points.len(), 2);
        assert_eq!(point_batch.points[0].measurement.as_ref(), "sensor-1");
        assert_eq!(point_batch.points[1].measurement.as_ref(), "sensor-2");
        assert_eq!(point_batch.points[0].driver.as_str(), "driver");

        ingest_handle.await.unwrap();
    }

    #[test]
    fn line_protocol_format_is_exact() {
        let point = SimplePoint {
            measurement: Arc::from("sensor-1"),
            driver: Arc::new(CompactString::from("driver7")),
            value: 12.5,
            timestamp_nanos: 1_700_000_000_123_456_789,
        };
        let mut out = String::from("prev\n");
        point.write_line_protocol(&mut out, "race42");
        assert_eq!(
            out,
            "prev\nsensor-1,race=race42,driver=driver7 val=12.5 1700000000123456789"
        );

        let whole = SimplePoint {
            value: 3.0,
            ..point
        };
        let mut out = String::new();
        whole.write_line_protocol(&mut out, "r");
        assert_eq!(
            out,
            "sensor-1,race=r,driver=driver7 val=3 1700000000123456789"
        );
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
