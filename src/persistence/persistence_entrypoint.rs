use crate::configuration::{CoreConfig, PersistenceConfig};
use crate::persistence::sensor_data_batcher::{SensorDataBatcher, SimplePoint};
use crate::persistence::sensor_data_flusher::{BatcherWiring, InfluxDbFlusher};
use crate::persistence::sensor_data_splitter::SensorDataSplitter;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;

pub type SensorDataRx = tokio::sync::mpsc::Receiver<UnwoundSensorDataMessage>;

pub struct PersistanceLayerEntryPoint {
    pub(crate) splitter: SensorDataSplitter,
    pub(crate) fast_batcher: SensorDataBatcher,
    pub(crate) normal_batcher: SensorDataBatcher,
    pub(crate) flusher: InfluxDbFlusher,
}

impl PersistanceLayerEntryPoint {
    pub async fn init(config: &CoreConfig, processed_data_rx: SensorDataRx, driver_id_holder: RetainReadTimeValue<CompactString>) -> Self {
        let persistence_config = &config.persistence;

        let (fast_tx, fast_rx) = tokio::sync::mpsc::channel(10); // TODO size
        let (normal_tx, normal_rx) = tokio::sync::mpsc::channel(10); // TODO size

        let splitter =
            SensorDataSplitter::new(processed_data_rx, fast_tx, normal_tx, persistence_config);

        let (fast_batcher, fast_batcher_wiring) =
            PersistanceLayerEntryPoint::create_batcher(fast_rx, driver_id_holder.clone(), persistence_config, true);
        let (normal_batcher, normal_batcher_wiring) =
            PersistanceLayerEntryPoint::create_batcher(normal_rx, driver_id_holder, persistence_config, false);

        let race_id = config.race.race_id.clone();
        let flusher = InfluxDbFlusher::from_config(
            persistence_config,
            race_id,
            fast_batcher_wiring,
            normal_batcher_wiring,
        ).await;

        Self {
            splitter,
            fast_batcher,
            normal_batcher,
            flusher,
        }
    }

    fn create_batcher(
        batcher_rx: SensorDataRx,
        driver_id_holder: RetainReadTimeValue<CompactString>,
        persistence_config: &PersistenceConfig,
        is_fast: bool,
    ) -> (SensorDataBatcher, BatcherWiring) {
        let (pool_tx, pool_rx) = tokio::sync::mpsc::channel::<Vec<SimplePoint>>(10);
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Vec<SimplePoint>>(10);
        let batch_size = if is_fast {
            persistence_config.fast_flush_max_combined_batch_size
        } else {
            persistence_config.flush_max_combined_batch_size
        };
        let flush_interval = if is_fast {
            persistence_config.fast_flush_interval_ms
        } else {
            persistence_config.flush_interval_ms
        };

        let batcher = SensorDataBatcher::new(
            batch_size,
            flush_interval,
            batcher_rx,
            batch_tx,
            pool_rx,
            driver_id_holder,
        );
        (batcher, (batch_rx, pool_tx))
    }
}
