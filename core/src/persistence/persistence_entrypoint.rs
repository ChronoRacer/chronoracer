use crate::configuration::{CoreConfig, PersistenceConfig};
use crate::persistence::database_exporter::InfluxDbExporter;
use crate::persistence::sensor_data_batcher::{SensorDataBatcher, SimplePoint};
use crate::persistence::sensor_data_flusher::{BatcherWiring, BatcherWiringType, InfluxDbFlusher};
use crate::persistence::sensor_data_splitter::SensorDataSplitter;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use anyhow::{anyhow, Context, Result};
use compact_str::CompactString;
use influxdb3_client::Client;
use std::sync::Arc;
use tracing::info;

pub type SensorDataRx = tokio::sync::mpsc::Receiver<UnwoundSensorDataMessage>;

pub struct PersistanceLayerEntryPoint {
    pub(crate) splitter: SensorDataSplitter,
    pub(crate) fast_batcher: SensorDataBatcher,
    pub(crate) normal_batcher: SensorDataBatcher,
    pub(crate) flusher: InfluxDbFlusher,
    pub(crate) exporter: InfluxDbExporter,
}

impl PersistanceLayerEntryPoint {
    pub async fn init(
        config: &'static CoreConfig,
        processed_data_rx: SensorDataRx,
        driver_id_holder: &'static RetainReadTimeValue<CompactString>,
    ) -> Result<Self> {
        let persistence_config = &config.persistence;
        let db_client = Arc::new(Self::create_influxdb_client(persistence_config).await?);

        let (fast_tx, fast_rx) =
            tokio::sync::mpsc::channel((config.runtime.rayon_threads * 2) as usize);
        let (normal_tx, normal_rx) =
            tokio::sync::mpsc::channel((config.runtime.rayon_threads * 2) as usize);

        let splitter =
            SensorDataSplitter::new(processed_data_rx, fast_tx, normal_tx, persistence_config);

        let (fast_batcher, fast_batcher_wiring) = PersistanceLayerEntryPoint::create_batcher(
            fast_rx,
            driver_id_holder,
            persistence_config,
            BatcherWiringType::Fast,
        );
        let (normal_batcher, normal_batcher_wiring) = PersistanceLayerEntryPoint::create_batcher(
            normal_rx,
            driver_id_holder,
            persistence_config,
            BatcherWiringType::Normal,
        );

        let race_id = config.race.race_id.clone();
        let flusher = InfluxDbFlusher::from_config(
            persistence_config,
            race_id,
            fast_batcher_wiring,
            normal_batcher_wiring,
            Arc::clone(&db_client),
        );

        let exporter = InfluxDbExporter::new(db_client, &persistence_config.export);

        Ok(Self {
            splitter,
            fast_batcher,
            normal_batcher,
            flusher,
            exporter,
        })
    }

    async fn create_influxdb_client(persistence_config: &PersistenceConfig) -> Result<Client> {
        let client = Client::new(
            influxdb3_client::ClientConfig::builder()
                .host(format!(
                    "{}:{}",
                    &persistence_config.db_host, &persistence_config.db_port
                ))
                .token(&persistence_config.db_password)
                .database(&persistence_config.db_name)
                .write_accept_partial(true)
                .write_use_v2_api(false)
                .write_no_sync(false)
                .max_idle_connections((persistence_config.max_inflight_db_writes * 2) as usize)
                .build()
                .map_err(|e| anyhow!("Invalid InfluxDB config: {e}"))?,
        )
            .await
            .context("Failed to create InfluxDb client")?;

        client
            .ping()
            .await
            .map(|version| {
                if version == "unknown" {
                    return Err(anyhow!(
                        "InfluxDB reachable, but version ping failed. Probably invalid access token"
                    ));
                }
                info!("Connected to InfluxDb. Using version {version}");
                Ok(client)
            })
            .context("Failed to connect to InfluxDb")?
    }

    fn create_batcher(
        batcher_rx: SensorDataRx,
        driver_id_holder: &'static RetainReadTimeValue<CompactString>,
        persistence_config: &PersistenceConfig,
        batcher_wiring_type: BatcherWiringType,
    ) -> (SensorDataBatcher, BatcherWiring) {
        let (pool_tx, pool_rx) = tokio::sync::mpsc::channel::<Vec<SimplePoint>>(10);
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Vec<SimplePoint>>(10);
        let (batch_size, flush_interval) = match batcher_wiring_type {
            BatcherWiringType::Normal => (
                persistence_config.flush_max_combined_batch_size,
                persistence_config.flush_interval_ms,
            ),
            BatcherWiringType::Fast => (
                persistence_config.fast_flush_max_combined_batch_size,
                persistence_config.fast_flush_interval_ms,
            ),
        };

        let batcher = SensorDataBatcher::new(
            batch_size,
            flush_interval,
            batcher_rx,
            batch_tx,
            pool_rx,
            driver_id_holder,
            batcher_wiring_type,
        );
        (batcher, (batch_rx, pool_tx, batcher_wiring_type))
    }
}
