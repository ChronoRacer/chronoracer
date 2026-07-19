use crate::configuration::PersistenceConfig;
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use std::collections::HashSet;

pub struct SensorDataSplitter {
    fast_flush_sensor_ids: HashSet<String>,
    data_rx: tokio::sync::mpsc::Receiver<UnwoundSensorDataMessage>,
}
impl SensorDataSplitter {
    pub fn new(data_rx: SensorDataRx, persistence_config: &PersistenceConfig) -> Self {
        Self {
            data_rx,
            fast_flush_sensor_ids: HashSet::from_iter(
                persistence_config.fast_flush_sensor_ids.clone(),
            ),
        }
    }

    /// Fast flush , normal flush
    pub fn into_channels(mut self) -> (SensorDataRx, SensorDataRx) {
        let buffer_capacity = 100;
        let (tx_true, rx_true) = tokio::sync::mpsc::channel(buffer_capacity);
        let (tx_false, rx_false) = tokio::sync::mpsc::channel(buffer_capacity);

        tokio::spawn(async move {
            while let Some(data) = self.data_rx.recv().await {
                if self.fast_flush_sensor_ids.contains(&data.sensor_id) {
                    if tx_true.send(data).await.is_err() && tx_false.is_closed() {
                        break;
                    }
                } else {
                    if tx_false.send(data).await.is_err() && tx_true.is_closed() {
                        break;
                    }
                }
            }
        });

        (rx_true, rx_false)
    }
}
