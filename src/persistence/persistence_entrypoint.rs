use crate::configuration::{CoreConfig, PersistenceConfig};
use crate::persistence::sensor_data_batcher::{SensorDataBatcher, SimplePoint};
use crate::persistence::sensor_data_flusher::{BatcherWiring, InfluxDbFlusher};
use crate::persistence::sensor_data_splitter::SensorDataSplitter;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;

pub type SensorDataRx = tokio::sync::mpsc::Receiver<UnwoundSensorDataMessage>;

pub async fn setup_persistence_pipe(
    config: &CoreConfig,
    processed_data_rx: SensorDataRx,
) -> (SensorDataSplitter, SensorDataBatcher, SensorDataBatcher, InfluxDbFlusher) { //[cite: 2]
    let persistence_config = &config.persistence; //[cite: 2]

    // Wire intra-layer channels here.
    // This allows the splitter to retain ownership of itself for the ingest() loop.
    let (fast_tx, fast_rx) = tokio::sync::mpsc::channel(10); // TODO size
    let (normal_tx, normal_rx) = tokio::sync::mpsc::channel(10); // TODO size

    // Splitter now takes Tx channels instead of creating them internally
    let splitter = SensorDataSplitter::new(
        processed_data_rx,
        fast_tx,
        normal_tx,
        persistence_config
    );

    let (fast_batcher, fast_batcher_wiring) =
        create_batcher(fast_rx, persistence_config, true);
    let (normal_batcher, normal_batcher_wiring) =
        create_batcher(normal_rx, persistence_config, false);

    let flusher = InfluxDbFlusher::from_config(
        persistence_config,
        config.race_id.clone(),
        fast_batcher_wiring,
        normal_batcher_wiring,
    ).await;
    (splitter, fast_batcher, normal_batcher, flusher)
}

fn create_batcher(
    batcher_rx: SensorDataRx,
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
        "todo".to_owned(),
    );
    (batcher, (batch_rx, pool_tx))
}