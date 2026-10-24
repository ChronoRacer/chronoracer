use crate::configuration::PersistenceConfig;
use crate::persistence::persistence_entrypoint::{SensorDataBatch, SensorDataRx};
use compact_str::CompactString;
use std::collections::HashSet;
use tracing::{error, info, warn};

pub struct SensorDataSplitter {
    /// This inlines `CompactString` for compare
    fast_flush_sensor_ids: HashSet<CompactString>,
    data_rx: tokio::sync::mpsc::Receiver<SensorDataBatch>,
    fast_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
    normal_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
}

impl SensorDataSplitter {
    /// Connects processed data directly to the normal path unless fast sensors are configured.
    #[must_use]
    pub fn wire(
        data_rx: SensorDataRx,
        channel_capacity: usize,
        persistence_config: &PersistenceConfig,
    ) -> (Option<Self>, Option<SensorDataRx>, SensorDataRx) {
        if persistence_config.fast_flush_sensor_ids.is_empty() {
            return (None, None, data_rx);
        }

        let (fast_tx, fast_rx) = tokio::sync::mpsc::channel(channel_capacity);
        let (normal_tx, normal_rx) = tokio::sync::mpsc::channel(channel_capacity);
        let splitter = Self::new(data_rx, fast_tx, normal_tx, persistence_config);
        (Some(splitter), Some(fast_rx), normal_rx)
    }

    #[must_use]
    pub fn new(
        data_rx: SensorDataRx,
        fast_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
        normal_tx: tokio::sync::mpsc::Sender<SensorDataBatch>,
        persistence_config: &PersistenceConfig,
    ) -> Self {
        let fast_flush_ids = persistence_config
            .fast_flush_sensor_ids
            .iter()
            .map(|v| CompactString::from(v.as_str()))
            .collect();
        Self {
            data_rx,
            fast_tx,
            normal_tx,
            fast_flush_sensor_ids: fast_flush_ids,
        }
    }

    pub async fn ingest(&mut self) {
        info!("Starting Splitter ingestion loop");
        while let Some(data_batch) = self.data_rx.recv().await {
            // Either side closing means data would be dropped silently. End the loop so the
            // supervisor sees a data task exit.
            if self.fast_tx.is_closed() || self.normal_tx.is_closed() {
                error!("Downstream flush channel closed. Breaking ingestion loop");
                break;
            }

            let (fast_batch, normal_batch): (Vec<_>, Vec<_>) = data_batch
                .into_iter()
                .partition(|data| self.fast_flush_sensor_ids.contains(data.sensor_id.as_ref()));

            if !fast_batch.is_empty() && self.fast_tx.send(fast_batch).await.is_err() {
                error!("Downstream fast flush channel closed. Breaking ingestion loop");
                break;
            }
            if !normal_batch.is_empty() && self.normal_tx.send(normal_batch).await.is_err() {
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
    use crate::messaging::partial_message::SourceOffset;
    use crate::processing::sensor_data_message::{AbsoluteSensorReading, UnwoundSensorDataMessage};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn create_message(sensor_id: &str) -> Arc<UnwoundSensorDataMessage> {
        Arc::new(UnwoundSensorDataMessage {
            sensor_id: Arc::from(sensor_id),
            readings: Box::new([AbsoluteSensorReading {
                value: 10.0,
                timestamp: 1_000_000_000,
            }]),
            source: SourceOffset::default(),
            created_ms: None,
        })
    }

    fn create_message_at(sensor_id: &str, offset: i64) -> Arc<UnwoundSensorDataMessage> {
        let mut message = UnwoundSensorDataMessage {
            sensor_id: Arc::from(sensor_id),
            readings: Box::new([]),
            source: SourceOffset::default(),
            created_ms: None,
        };
        message.source.partition = 2;
        message.source.offset = offset;
        Arc::new(message)
    }

    #[tokio::test]
    async fn test_splitter_keeps_each_messages_source_offset_on_its_path() {
        let config = PersistenceConfig {
            fast_flush_sensor_ids: vec!["fast".to_string()],
            ..PersistenceConfig::mock_config()
        };
        let (data_tx, data_rx) = mpsc::channel(10);
        let (fast_tx, mut fast_rx) = mpsc::channel(10);
        let (normal_tx, mut normal_rx) = mpsc::channel(10);
        let mut splitter = SensorDataSplitter::new(data_rx, fast_tx, normal_tx, &config);

        data_tx
            .send(vec![
                create_message_at("fast", 10),
                create_message_at("slow", 11),
                create_message_at("fast", 12),
                create_message_at("slow", 13),
            ])
            .await
            .unwrap();
        drop(data_tx);
        splitter.ingest().await;

        let offsets = |batch: SensorDataBatch| -> Vec<(i32, i64)> {
            batch
                .iter()
                .map(|m| (m.source.partition, m.source.offset))
                .collect()
        };
        assert_eq!(offsets(fast_rx.recv().await.unwrap()), [(2, 10), (2, 12)]);
        assert_eq!(offsets(normal_rx.recv().await.unwrap()), [(2, 11), (2, 13)]);
    }
    #[tokio::test]
    async fn test_splitter_routes_to_fast_and_normal_channels() {
        let config = PersistenceConfig {
            fast_flush_sensor_ids: vec!["s_fast_1".to_string(), "s_fast_2".to_string()],
            ..PersistenceConfig::mock_config()
        };

        let (data_tx, data_rx) = mpsc::channel(10);
        let (fast_tx, mut fast_rx) = mpsc::channel(10);
        let (normal_tx, mut normal_rx) = mpsc::channel(10);

        let mut splitter = SensorDataSplitter::new(data_rx, fast_tx, normal_tx, &config);

        data_tx
            .send(vec![
                create_message("s_fast_1"),
                create_message("s_normal_1"),
                create_message("s_fast_2"),
                create_message("s_normal_2"),
            ])
            .await
            .unwrap();

        drop(data_tx);

        splitter.ingest().await;

        // Verify fast channel received correct messages
        let fast_batch = fast_rx.recv().await.unwrap();
        assert_eq!(fast_batch.len(), 2);
        assert_eq!(fast_batch[0].sensor_id.as_ref(), "s_fast_1");
        assert_eq!(fast_batch[1].sensor_id.as_ref(), "s_fast_2");
        assert!(fast_rx.try_recv().is_err());

        // Verify normal channel received correct messages
        let normal_batch = normal_rx.recv().await.unwrap();
        assert_eq!(normal_batch.len(), 2);
        assert_eq!(normal_batch[0].sensor_id.as_ref(), "s_normal_1");
        assert_eq!(normal_batch[1].sensor_id.as_ref(), "s_normal_2");
        assert!(normal_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_wiring_bypasses_splitter_when_fast_ids_empty() {
        let config = PersistenceConfig::mock_config();

        let (data_tx, data_rx) = mpsc::channel(10);
        // Zero capacity would panic if wiring tried to create splitter channels.
        let (splitter, fast_rx, mut normal_rx) = SensorDataSplitter::wire(data_rx, 0, &config);
        assert!(splitter.is_none());
        assert!(fast_rx.is_none());

        data_tx
            .send(vec![create_message("sensor_1"), create_message("sensor_2")])
            .await
            .unwrap();
        drop(data_tx);

        // Data is available without running a splitter task.
        let normal_batch = normal_rx.try_recv().unwrap();
        assert_eq!(normal_batch.len(), 2);
        assert_eq!(normal_batch[0].sensor_id.as_ref(), "sensor_1");
        assert_eq!(normal_batch[1].sensor_id.as_ref(), "sensor_2");
        assert!(normal_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_wiring_preserves_fast_and_normal_routes_when_fast_ids_present() {
        let config = PersistenceConfig {
            fast_flush_sensor_ids: vec!["fast".to_string()],
            ..PersistenceConfig::mock_config()
        };
        let (data_tx, data_rx) = mpsc::channel(10);
        let (splitter, fast_rx, mut normal_rx) = SensorDataSplitter::wire(data_rx, 10, &config);
        let mut splitter = splitter.expect("fast sensors require a splitter");
        let mut fast_rx = fast_rx.expect("fast sensors require a fast channel");
        data_tx
            .send(vec![
                create_message_at("fast", 10),
                create_message_at("normal", 11),
            ])
            .await
            .unwrap();
        drop(data_tx);
        splitter.ingest().await;

        assert_eq!(fast_rx.recv().await.unwrap()[0].source.offset, 10);
        assert_eq!(normal_rx.recv().await.unwrap()[0].source.offset, 11);
        drop(splitter);
        assert!(fast_rx.recv().await.is_none());
        assert!(normal_rx.recv().await.is_none());
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

        data_tx
            .send(vec![create_message("fast_sensor")])
            .await
            .unwrap();

        splitter.ingest().await;
    }

    #[tokio::test]
    async fn test_ingest_exits_when_only_one_destination_is_closed() {
        for close_fast in [true, false] {
            let config = PersistenceConfig {
                fast_flush_sensor_ids: vec!["fast_sensor".to_string()],
                ..PersistenceConfig::mock_config()
            };

            let (data_tx, data_rx) = mpsc::channel(10);
            let (fast_tx, fast_rx) = mpsc::channel(10);
            let (normal_tx, normal_rx) = mpsc::channel(10);

            let mut splitter = SensorDataSplitter::new(data_rx, fast_tx, normal_tx, &config);

            let _open_rx = if close_fast {
                drop(fast_rx);
                normal_rx
            } else {
                drop(normal_rx);
                fast_rx
            };

            // The message targets the open side, and `data_tx` stays open: only the closed
            // side can end the loop.
            let open_sensor = if close_fast {
                "normal_sensor"
            } else {
                "fast_sensor"
            };
            data_tx
                .send(vec![create_message(open_sensor)])
                .await
                .unwrap();

            timeout(Duration::from_secs(2), splitter.ingest())
                .await
                .expect("splitter must stop when one downstream is closed");
        }
    }
}
