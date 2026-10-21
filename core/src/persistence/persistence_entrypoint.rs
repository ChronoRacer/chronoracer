use crate::configuration::{CoreConfig, PersistenceConfig};
use crate::messaging::offset_ledger::OffsetLedger;
use crate::persistence::database_exporter::InfluxDbExporter;
use crate::persistence::sensor_data_batcher::{PointBatch, SensorDataBatcher, SimplePoint};
use crate::persistence::sensor_data_flusher::{BatcherWiring, BatcherWiringType, InfluxDbFlusher};
use crate::persistence::sensor_data_splitter::SensorDataSplitter;
use crate::persistence::write_metrics::WriteMetrics;
use crate::pipeline_stats::PipelineStats;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use anyhow::{Context, Result, anyhow};
use compact_str::CompactString;
use influxdb3_client::{Client, ClientConfig, RetryConfig, WriteOptions};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

pub type SensorDataBatch = Vec<Arc<UnwoundSensorDataMessage>>;
pub type SensorDataRx = tokio::sync::mpsc::Receiver<SensorDataBatch>;

pub struct PersistenceLayerEntryPoint {
    pub(crate) splitter: Option<SensorDataSplitter>,
    pub(crate) fast_batcher: Option<SensorDataBatcher>,
    pub(crate) normal_batcher: SensorDataBatcher,
    pub(crate) flusher: InfluxDbFlusher,
    pub(crate) exporter: InfluxDbExporter,
    pub(crate) write_metrics: Arc<WriteMetrics>,
}

impl PersistenceLayerEntryPoint {
    pub async fn init(
        config: &'static CoreConfig,
        processed_data_rx: SensorDataRx,
        driver_id_holder: &'static RetainReadTimeValue<CompactString>,
        offsets: OffsetLedger,
        stats: Arc<PipelineStats>,
    ) -> Result<Self> {
        let persistence_config = &config.persistence;
        let db_client = Arc::new(Self::create_influxdb_client(persistence_config).await?);

        let splitter_channel_capacity =
            usize::from(config.runtime.rayon_threads.get()).saturating_mul(2);
        let (splitter, fast_rx, normal_rx) = SensorDataSplitter::wire(
            processed_data_rx,
            splitter_channel_capacity,
            persistence_config,
        );
        let (fast_batcher, fast_batcher_wiring) = fast_rx
            .map(|rx| {
                Self::create_batcher(
                    rx,
                    driver_id_holder,
                    persistence_config,
                    BatcherWiringType::Fast,
                )
            })
            .unzip();
        let (normal_batcher, normal_batcher_wiring) = PersistenceLayerEntryPoint::create_batcher(
            normal_rx,
            driver_id_holder,
            persistence_config,
            BatcherWiringType::Normal,
        );

        let race_id = config.race.race_id.clone();
        let write_metrics = Arc::new(WriteMetrics::new());
        let flusher = InfluxDbFlusher::from_config(
            persistence_config,
            race_id,
            fast_batcher_wiring,
            normal_batcher_wiring,
            Arc::clone(&db_client),
            Arc::clone(&write_metrics),
            stats,
            offsets,
        );

        let exporter = InfluxDbExporter::new(db_client, &persistence_config.export);

        Ok(Self {
            splitter,
            fast_batcher,
            normal_batcher,
            flusher,
            exporter,
            write_metrics,
        })
    }

    async fn create_influxdb_client(persistence_config: &PersistenceConfig) -> Result<Client> {
        let client = Client::new(Self::influx_client_config(persistence_config)?)
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

    /// The flusher owns write concurrency, batching and retries. The client is configured so it
    /// never adds its own: each `write` is one request, sent once.
    pub(crate) fn influx_client_config(
        persistence_config: &PersistenceConfig,
    ) -> Result<ClientConfig> {
        ClientConfig::builder()
            .host(format!(
                "{}:{}",
                persistence_config.db_host, persistence_config.db_port
            ))
            .token(&persistence_config.db_password)
            .database(&persistence_config.db_name)
            .max_idle_connections(
                usize::from(persistence_config.max_inflight_db_writes.get()).saturating_mul(2),
            )
            .retry(RetryConfig::disabled())
            .write_options(WriteOptions {
                precision: influxdb3_client::Precision::Nanosecond,
                default_tags: HashMap::new(),
                gzip_threshold: None, // `None` so it never compresses, no point when hosting on the same machine
                no_sync: false,
                accept_partial: true,
                use_v2_api: false,
                tag_order: vec![],
                // Both only apply to `Vec<Point>` input (splitting, concurrent sub-requests).
                // Raw line protocol is always one request; these keep it that way.
                batch_size: usize::MAX / 2,
                max_inflight: 1,
            })
            .build()
            .map_err(|e| anyhow!("Invalid InfluxDB config: {e}"))
    }

    fn create_batcher(
        batcher_rx: SensorDataRx,
        driver_id_holder: &'static RetainReadTimeValue<CompactString>,
        persistence_config: &PersistenceConfig,
        batcher_wiring_type: BatcherWiringType,
    ) -> (SensorDataBatcher, BatcherWiring) {
        let (pool_tx, pool_rx) = tokio::sync::mpsc::channel::<Vec<SimplePoint>>(10);
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<PointBatch>(10);
        let (batch_size, flush_interval) = match batcher_wiring_type {
            BatcherWiringType::Normal => (
                persistence_config.flush_max_combined_batch_size.get(),
                persistence_config.flush_interval_ms.get(),
            ),
            BatcherWiringType::Fast => (
                persistence_config.fast_flush_max_combined_batch_size.get(),
                persistence_config.fast_flush_interval_ms.get(),
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
        (
            batcher,
            BatcherWiring {
                batch_rx,
                pool_tx,
                wiring_type: batcher_wiring_type,
            },
        )
    }
}
