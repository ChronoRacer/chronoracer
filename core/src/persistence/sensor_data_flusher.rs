use crate::configuration::PersistenceConfig;
use crate::persistence::sensor_data_batcher::{SIMPLE_POINT_PROTOCOL_MAX_LENGTH, SimplePoint};
use crate::persistence::write_metrics::WriteMetrics;
use compact_str::CompactString;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

pub struct InfluxDbFlusher {
    client: Arc<influxdb3_client::Client>,
    race_id: Arc<str>,
    fast_batcher_wiring: Option<BatcherWiring>,
    normal_batcher_wiring: Option<BatcherWiring>,
    semaphore: Arc<Semaphore>,
    max_inflight_requests: usize,
    write_metrics: Arc<WriteMetrics>,
}

#[derive(Debug, Copy, Clone)]
pub enum BatcherWiringType {
    Normal,
    Fast,
}
pub type BatcherWiring = (
    tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
    tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
    BatcherWiringType,
);

impl InfluxDbFlusher {
    #[must_use]
    pub fn from_config(
        persistence: &PersistenceConfig,
        race_id: CompactString,
        fast_batcher_wiring: BatcherWiring,
        batcher_wiring: BatcherWiring,
        client: Arc<influxdb3_client::Client>,
        write_metrics: Arc<WriteMetrics>,
    ) -> Self {
        Self {
            client,
            race_id: Arc::from(race_id),
            fast_batcher_wiring: Some(fast_batcher_wiring),
            normal_batcher_wiring: Some(batcher_wiring),
            semaphore: Arc::new(Semaphore::new(persistence.max_inflight_db_writes as usize)),
            max_inflight_requests: persistence.max_inflight_db_writes as usize,
            write_metrics,
        }
    }

    pub async fn ingest(&mut self) {
        let max_inflight = self.max_inflight_requests;
        let Some(fast_wiring) = self.fast_batcher_wiring.take() else {
            error!("Fast batcher wiring is unavailable; stopping flusher");
            return;
        };
        let client_fast = Arc::clone(&self.client);
        let race_id_fast = Arc::clone(&self.race_id);
        let semaphore_fast = Arc::clone(&self.semaphore);
        let metrics_fast = Arc::clone(&self.write_metrics);
        let fast_handle = tokio::spawn(Self::ingest_internal(
            fast_wiring,
            client_fast,
            race_id_fast,
            semaphore_fast,
            max_inflight,
            metrics_fast,
        ));

        let Some(normal_wiring) = self.normal_batcher_wiring.take() else {
            error!("Normal batcher wiring is unavailable; stopping flusher");
            return;
        };
        let client_normal = Arc::clone(&self.client);
        let race_id_normal = Arc::clone(&self.race_id);
        let semaphore_normal = Arc::clone(&self.semaphore);
        let metrics_normal = Arc::clone(&self.write_metrics);
        let normal_handle = tokio::spawn(Self::ingest_internal(
            normal_wiring,
            client_normal,
            race_id_normal,
            semaphore_normal,
            max_inflight,
            metrics_normal,
        ));
        let (_, _) = tokio::join!(fast_handle, normal_handle);
    }

    async fn ingest_internal(
        wiring: BatcherWiring,
        client: Arc<influxdb3_client::Client>,
        race_id: Arc<str>,
        semaphore: Arc<Semaphore>,
        max_inflight_requests: usize,
        write_metrics: Arc<WriteMetrics>,
    ) {
        let (mut batch_rx, pool_tx, wiring_type) = wiring;
        info!("Starting data flush ingestion loop, flush_type={wiring_type:?}");
        let mut write_tasks: VecDeque<tokio::task::JoinHandle<()>> =
            VecDeque::with_capacity(max_inflight_requests);
        while let Some(mut batch) = batch_rx.recv().await {
            write_tasks.retain(|task| !task.is_finished());

            let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
                warn!("Database flush semaphore closed; stopping flush loop");
                break;
            };
            let db = Arc::clone(&client);
            let pool_tx = pool_tx.clone();
            let race_id = Arc::clone(&race_id);
            let write_metrics = Arc::clone(&write_metrics);

            write_tasks.push_back(tokio::spawn(async move {
                let _permit = permit;

                let mut line_protocol =
                    String::with_capacity(batch.len() * (SIMPLE_POINT_PROTOCOL_MAX_LENGTH + 2));
                for point in &batch {
                    line_protocol.push_str(&point.to_line_protocol(&race_id));
                    line_protocol.push('\n');
                }
                let lines = u64::try_from(batch.len()).unwrap_or(u64::MAX);
                let bytes = u64::try_from(line_protocol.len()).unwrap_or(u64::MAX);
                match db
                    .write(line_protocol)
                    .max_inflight(max_inflight_requests)
                    .await
                {
                    Ok(()) => {
                        write_metrics.record_success(lines, bytes);
                        info!(
                            "Flushed data points: batch_size={}, flush_type={:?}",
                            batch.len(),
                            wiring_type
                        );
                    }
                    Err(influxdb3_client::error::Error::PartialWrite(error)) => {
                        let failed_lines = u64::try_from(error.line_errors.len()).unwrap_or(u64::MAX);
                        write_metrics.record_partial(lines, failed_lines);
                        error!(%error,
                            "Database write partially failed. failed_lines={}, flush_type={wiring_type:?}",
                            failed_lines
                        );
                        for line_error in error.line_errors.iter().take(5) {
                            warn!("Line {}: {}", line_error.line, line_error.message);
                        }
                    }
                    Err(error) => {
                        write_metrics.record_error();
                        error!(%error, "Database write failed, flush_type={wiring_type:?}");
                    }
                }
                batch.clear();

                let _ = pool_tx.try_send(batch);
            }));
        }

        while let Some(task) = write_tasks.pop_front() {
            let _ = task.await;
        }

        warn!("Data flush ingestion loop terminated, flush_type={wiring_type:?}");
    }
}
