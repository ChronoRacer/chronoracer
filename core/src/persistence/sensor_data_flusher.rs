use crate::configuration::PersistenceConfig;
use crate::persistence::sensor_data_batcher::{SimplePoint, SIMPLE_POINT_PROTOCOL_MAX_LENGTH};
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
    ) -> Self {
        Self {
            client,
            race_id: Arc::from(race_id),
            fast_batcher_wiring: Some(fast_batcher_wiring),
            normal_batcher_wiring: Some(batcher_wiring),
            semaphore: Arc::new(Semaphore::new(persistence.max_inflight_db_writes as usize)),
            max_inflight_requests: persistence.max_inflight_db_writes as usize,
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
        let fast_handle = tokio::spawn(Self::ingest_internal(
            fast_wiring,
            client_fast,
            race_id_fast,
            semaphore_fast,
            max_inflight,
        ));

        let Some(normal_wiring) = self.normal_batcher_wiring.take() else {
            error!("Normal batcher wiring is unavailable; stopping flusher");
            return;
        };
        let client_normal = Arc::clone(&self.client);
        let race_id_normal = Arc::clone(&self.race_id);
        let semaphore_normal = Arc::clone(&self.semaphore);
        let normal_handle = tokio::spawn(Self::ingest_internal(
            normal_wiring,
            client_normal,
            race_id_normal,
            semaphore_normal,
            max_inflight,
        ));
        let (_, _) = tokio::join!(fast_handle, normal_handle);
    }

    async fn ingest_internal(
        wiring: BatcherWiring,
        client: Arc<influxdb3_client::Client>,
        race_id: Arc<str>,
        semaphore: Arc<Semaphore>,
        max_inflight_requests: usize,
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

            info!(
                "Received new batch to flush: batch_size={}, flush_type={:?}",
                batch.len(),
                wiring_type
            );
            write_tasks.push_back(tokio::spawn(async move {
                let _permit = permit;

                let mut line_protocol =
                    String::with_capacity(batch.len() * (SIMPLE_POINT_PROTOCOL_MAX_LENGTH + 2));
                for point in &batch {
                    line_protocol.push_str(&point.to_line_protocol(&race_id));
                    line_protocol.push('\n');
                }
                match db
                    .write(line_protocol)
                    .max_inflight(max_inflight_requests)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "Flushed data points: batch_size={}, flush_type={:?}",
                            batch.len(),
                            wiring_type
                        );
                    }
                    Err(influxdb3_client::error::Error::PartialWrite(e)) => {
                        error!(
                            "Database write partially failed. failed_lines={}, Error: {e}, flush_type={wiring_type:?}",
                            e.line_errors.len()
                        );
                        // TODO: is logging this needed?
                        for line_error in &e.line_errors {
                            error!("Line {}: {}", line_error.line, line_error.message);
                        }
                    }
                    Err(e) => {
                        error!("Database write failed: {e}, flush_type={wiring_type:?}");
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
