use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::persistence::sensor_data_flusher::BatcherWiringType;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

/// The `influxdb3_client::Point` implementation is overengineered for this use case and has in-built deduplication and sorting.
/// To avoid this overhead and with very slim points, it's just better to build line protocol manually
pub struct SimplePoint {
    pub measurement: Arc<str>,
    pub driver: Arc<CompactString>,
    pub value: f64,
    pub timestamp_nanos: u64,
}

pub const SIMPLE_POINT_PROTOCOL_MAX_LENGTH: usize = 10 + 1 + 4 + 1 + 16 + 1 + 6 + 1 + 10 + 1 + 3 + 1 + 12 + 1 + 10;

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
                Some(data) = self.data_rx.recv() => {
                    sensor_data_into_points(data, &mut buffer, &self.driver_id.get());

                    if buffer.len() >= self.flush_max_combined_batch_size {
                        let to_flush = std::mem::replace(&mut buffer, get_buffer());
                        if let Err(e) = self.batch_tx.send(to_flush).await {
                            error!("Failed to flush data buffer, batch_type={wiring_type:?}: {e}");
                            if self.batch_tx.is_closed() {
                                warn!("Batch data buffer is closed. Closing ingestion loop, batch_type={wiring_type:?}");
                                break;
                            }
                        }

                        interval.reset();
                    }
                }

                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        let to_flush = std::mem::replace(&mut buffer, get_buffer());
                        if let Err(e) = self.batch_tx.send(to_flush).await {
                            error!("Failed to flush data buffer, batch_type={wiring_type:?}: {e}");
                            if self.batch_tx.is_closed() {
                                warn!("Batch data buffer is closed. Closing ingestion loop, batch_type={wiring_type:?}");
                                break;
                            }
                        }
                    }
                }

                else => {
                    if !buffer.is_empty() {
                        let _ = self.batch_tx.send(buffer).await;
                    }
                    warn!("Closing batcher ingestion loop, batch_type={wiring_type:?}");
                    break;
                }
            }
        }
    }
}

fn sensor_data_into_points(
    data: UnwoundSensorDataMessage,
    buffer: &mut Vec<SimplePoint>,
    current_driver_id: &Arc<CompactString>,
) {
    let shared_measurement: Arc<str> = Arc::from(data.sensor_id);
    buffer.extend(data.readings.into_iter().map(|reading| SimplePoint {
        measurement: Arc::clone(&shared_measurement),
        driver: Arc::clone(current_driver_id),
        value: reading.value,
        timestamp_nanos: reading.timestamp,
    }));
}
