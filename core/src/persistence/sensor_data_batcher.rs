use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::persistence::sensor_data_flusher::BatcherWiringType;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
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

pub const SIMPLE_POINT_PROTOCOL_MAX_LENGTH: usize =
    10 + 1 + 4 + 1 + 16 + 1 + 6 + 1 + 10 + 1 + 3 + 1 + 12 + 1 + 10;

impl SimplePoint {
    #[must_use]
    pub fn to_line_protocol(&self, race_id: &str) -> String {
        format!(
            "{},race={},driver={} val={} {}",
            self.measurement, race_id, self.driver, self.value, self.timestamp_nanos
        )
    }
}

pub struct SensorDataBatcher {
    data_rx: SensorDataRx,
    pool_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
    batch_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
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
        batch_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
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

        let mut get_buffer = || -> Vec<SimplePoint> {
            self.pool_rx
                .try_recv()
                .unwrap_or_else(|_| Vec::with_capacity(self.flush_max_combined_batch_size))
        };

        let mut buffer = get_buffer();

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

                    let mut downstream_closed = false;
                    for data in data_batch {
                        sensor_data_into_points(&data, &mut buffer, &self.driver_id.get());

                        if buffer.len() >= self.flush_max_combined_batch_size {
                            let to_flush = std::mem::replace(&mut buffer, get_buffer());
                            if let Err(error) = self.batch_tx.send(to_flush).await {
                                error!(%error, "Failed to flush data buffer, batch_type={wiring_type:?}");
                                if self.batch_tx.is_closed() {
                                    warn!("Batch data buffer is closed. Closing ingestion loop, batch_type={wiring_type:?}");
                                    downstream_closed = true;
                                    break;
                                }
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
                        let to_flush = std::mem::replace(&mut buffer, get_buffer());
                        if let Err(error) = self.batch_tx.send(to_flush).await {
                            error!(%error, "Failed to flush data buffer, batch_type={wiring_type:?}");
                            if self.batch_tx.is_closed() {
                                warn!("Batch data buffer is closed. Closing ingestion loop, batch_type={wiring_type:?}");
                                break;
                            }
                        }
                    }
                }

            }
        }
    }
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
        })
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
        assert_eq!(point_batch.len(), 2);
        assert_eq!(point_batch[0].measurement.as_ref(), "sensor-1");
        assert_eq!(point_batch[1].measurement.as_ref(), "sensor-2");
        assert_eq!(point_batch[0].driver.as_str(), "driver");

        ingest_handle.await.unwrap();
    }
}
