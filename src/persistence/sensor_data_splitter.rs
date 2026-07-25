use crate::configuration::PersistenceConfig;
use std::collections::HashSet;
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;

pub struct SensorDataSplitter {
    fast_flush_sensor_ids: HashSet<String>,
    data_rx: tokio::sync::mpsc::Receiver<UnwoundSensorDataMessage>,
    fast_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
    normal_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
}

impl SensorDataSplitter {
    pub fn new(
        data_rx: SensorDataRx,
        fast_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
        normal_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
        persistence_config: &PersistenceConfig,
    ) -> Self {
        Self {
            data_rx,
            fast_tx,
            normal_tx,
            fast_flush_sensor_ids: HashSet::from_iter(
                persistence_config.fast_flush_sensor_ids.clone(),
            )
        }
    }
    pub async fn ingest(&mut self) {
        while let Some(data) = self.data_rx.recv().await {
            if self.fast_flush_sensor_ids.contains(&data.sensor_id) {
                if self.fast_tx.send(data).await.is_err() && self.normal_tx.is_closed() {
                    break;
                }
            } else {
                if self.normal_tx.send(data).await.is_err() && self.fast_tx.is_closed() {
                    break;
                }
            }
        }
    }
}