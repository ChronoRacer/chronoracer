use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use std::sync::Arc;
use std::time::Duration;

/// The influxdb3_client::Point implementation is overengineered for this use case and has in-built deduplication and sorting.
/// To avoid this overhead and with very slim points, it's just better to build line protocol manually
pub struct SimplePoint {
    pub measurement: Arc<str>,
    pub driver: Arc<str>,
    pub value: f64,
    pub timestamp_nanos: u64,
}

impl SimplePoint {
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
    current_driver_id: Arc<str>,
    flush_max_combined_batch_size: usize,
    flush_interval_ms: u32,
}

impl SensorDataBatcher {
    pub fn new(
        flush_max_combined_batch_size: usize,
        flush_interval_ms: u32,
        data_rx: SensorDataRx,
        batch_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
        pool_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
        starting_driver_id: String,
    ) -> Self {
        Self {
            data_rx,
            pool_rx,
            batch_tx,
            current_driver_id: Arc::from(starting_driver_id),
            flush_max_combined_batch_size,
            flush_interval_ms,
        }
    }

    pub async fn ingest(&mut self) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(self.flush_interval_ms as u64));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut get_buffer = || -> Vec<SimplePoint> {
            self.pool_rx
                .try_recv()
                .unwrap_or_else(|_| Vec::with_capacity(self.flush_max_combined_batch_size))
        };

        let mut buffer = get_buffer();

        loop {
            tokio::select! {
                Some(data) = self.data_rx.recv() => {
                    sensor_data_into_points(data, &mut buffer, &self.current_driver_id);

                    if buffer.len() >= self.flush_max_combined_batch_size {
                        let to_flush = std::mem::replace(&mut buffer, get_buffer());
                        if self.batch_tx.send(to_flush).await.is_err() {
                            eprintln!("Failed to flush data buffer");
                            if self.batch_tx.is_closed() {
                                eprintln!("Batch data buffer is closed");
                                break;
                            }
                        }

                        interval.reset();
                    }
                }

                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        let to_flush = std::mem::replace(&mut buffer, get_buffer());
                        if self.batch_tx.send(to_flush).await.is_err() {
                            eprintln!("Failed to flush data buffer");
                            if self.batch_tx.is_closed() {
                                eprintln!("Batch data buffer is closed");
                                break;
                            }
                        }
                    }
                }

                else => {
                    if !buffer.is_empty() {
                        let _ = self.batch_tx.send(buffer).await;
                    }
                    break;
                }
            }
        }
    }
}

fn sensor_data_into_points(
    data: UnwoundSensorDataMessage,
    buffer: &mut Vec<SimplePoint>,
    current_driver_id: &Arc<str>,
) {
    let shared_measurement: Arc<str> = Arc::from(data.sensor_id);
    buffer.extend(data.readings.into_iter().map(|reading| SimplePoint {
        measurement: Arc::clone(&shared_measurement),
        driver: Arc::clone(current_driver_id),
        value: reading.value,
        timestamp_nanos: reading.timestamp,
    }));
}
