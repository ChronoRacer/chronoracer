use crate::configuration::PersistenceConfig;
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use std::collections::HashSet;
use tracing::{error, info, warn};

pub struct SensorDataSplitter {
    fast_flush_sensor_ids: HashSet<String>,
    data_rx: tokio::sync::mpsc::Receiver<UnwoundSensorDataMessage>,
    fast_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
    normal_tx: tokio::sync::mpsc::Sender<UnwoundSensorDataMessage>,
}

impl SensorDataSplitter {
    #[must_use]
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
            ),
        }
    }
    pub async fn ingest(&mut self) {
        info!("Starting Splitter ingestion loop");
        while let Some(data) = self.data_rx.recv().await {
            if self.fast_flush_sensor_ids.contains(&data.sensor_id) {
                if self.fast_tx.send(data).await.is_err() && self.normal_tx.is_closed() {
                    error!("Downstream fast flush channel closed. Breaking ingestion loop");
                    break;
                }
            } else if self.normal_tx.send(data).await.is_err() && self.fast_tx.is_closed() {
                error!("Downstream normal flush channel closed. Breaking ingestion loop");
                break;
            }
        }
        warn!("Splitter ingestion loop terminated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::DatabaseExportConfig;
    use crate::processing::sensor_data_message::AbsoluteSensorReading;
    use tokio::sync::mpsc;

    fn create_message(sensor_id: &str) -> UnwoundSensorDataMessage {
        UnwoundSensorDataMessage {
            sensor_id: sensor_id.to_string(),
            readings: Box::new([AbsoluteSensorReading {
                value: 10.0,
                timestamp: 1_000_000_000,
            }]),
        }
    }

    #[tokio::test]
    async fn test_splitter_routes_to_fast_and_normal_channels() {
        let config = PersistenceConfig {
            fast_flush_sensor_ids: vec!["sensor_fast_1".to_string(), "sensor_fast_2".to_string()],
            db_host: "localhost".to_string(),
            db_port: 5432,
            db_password: "root".to_string(),
            db_name: "chrono_db".to_string(),
            fast_flush_max_combined_batch_size: 1000,
            fast_flush_interval_ms: 1500,
            flush_max_combined_batch_size: 5000,
            flush_interval_ms: 3000,
            max_inflight_db_writes: 7,
            export: DatabaseExportConfig::mock_config(),
        };

        let (data_tx, data_rx) = mpsc::channel(10);
        let (fast_tx, mut fast_rx) = mpsc::channel(10);
        let (normal_tx, mut normal_rx) = mpsc::channel(10);

        let mut splitter = SensorDataSplitter::new(data_rx, fast_tx, normal_tx, &config);

        data_tx.send(create_message("sensor_fast_1")).await.unwrap();
        data_tx
            .send(create_message("sensor_normal_1"))
            .await
            .unwrap();
        data_tx.send(create_message("sensor_fast_2")).await.unwrap();
        data_tx
            .send(create_message("sensor_normal_2"))
            .await
            .unwrap();

        drop(data_tx);

        splitter.ingest().await;

        // Verify fast channel received correct messages
        let fast_msg1 = fast_rx.recv().await.unwrap();
        let fast_msg2 = fast_rx.recv().await.unwrap();
        assert_eq!(fast_msg1.sensor_id, "sensor_fast_1");
        assert_eq!(fast_msg2.sensor_id, "sensor_fast_2");
        assert!(fast_rx.try_recv().is_err());

        // Verify normal channel received correct messages
        let normal_msg1 = normal_rx.recv().await.unwrap();
        let normal_msg2 = normal_rx.recv().await.unwrap();
        assert_eq!(normal_msg1.sensor_id, "sensor_normal_1");
        assert_eq!(normal_msg2.sensor_id, "sensor_normal_2");
        assert!(normal_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_splitter_routes_all_to_normal_when_fast_ids_empty() {
        let config = PersistenceConfig::mock_config();

        let (data_tx, data_rx) = mpsc::channel(10);
        let (fast_tx, mut fast_rx) = mpsc::channel(10);
        let (normal_tx, mut normal_rx) = mpsc::channel(10);

        let mut splitter = SensorDataSplitter::new(data_rx, fast_tx, normal_tx, &config);

        data_tx.send(create_message("sensor_1")).await.unwrap();
        data_tx.send(create_message("sensor_2")).await.unwrap();
        drop(data_tx);

        splitter.ingest().await;

        assert!(fast_rx.try_recv().is_err());

        assert_eq!(normal_rx.recv().await.unwrap().sensor_id, "sensor_1");
        assert_eq!(normal_rx.recv().await.unwrap().sensor_id, "sensor_2");
    }

    #[tokio::test]
    async fn test_ingest_exits_early_when_both_destinations_closed() {
        let config = PersistenceConfig::mock_config();

        let (data_tx, data_rx) = mpsc::channel(10);
        let (fast_tx, fast_rx) = mpsc::channel(10);
        let (normal_tx, normal_rx) = mpsc::channel(10);

        let mut splitter = SensorDataSplitter::new(data_rx, fast_tx, normal_tx, &config);

        drop(fast_rx);
        drop(normal_rx);

        data_tx.send(create_message("fast_sensor")).await.unwrap();

        splitter.ingest().await;
    }
}
