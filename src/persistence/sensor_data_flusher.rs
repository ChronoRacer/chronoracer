use crate::configuration::PersistenceConfig;
use crate::persistence::sensor_data_batcher::SimplePoint;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct InfluxDbFlusher {
    client: Arc<influxdb3_client::Client>,
    race_id: Arc<str>,
    fast_batcher_wiring: Option<BatcherWiring>,
    normal_batcher_wiring: Option<BatcherWiring>,
    semaphore: Arc<Semaphore>,
    max_inflight_requests: usize,
}

pub type BatcherWiring = (
    tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
    tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
);

impl InfluxDbFlusher {
    pub async fn from_config(
        persistence: &PersistenceConfig,
        race_id: String,
        fast_batcher_wiring: BatcherWiring,
        batcher_wiring: BatcherWiring,
    ) -> Self {
        let client = influxdb3_client::Client::new(
            influxdb3_client::ClientConfig::builder()
                .host(format!("{}:{}", &persistence.db_host, &persistence.db_port))
                .token(&persistence.db_password)
                .database(&persistence.db_name)
                .write_accept_partial(true)
                .write_use_v2_api(false)
                .write_no_sync(true)
                .max_idle_connections((persistence.max_inflight_db_writes * 2) as usize)
                .build()
                .expect("Invalid InfluxDB config"),
        )
            .await
            .expect("Failed to create InfluxDb client");

        Self {
            client: Arc::from(client),
            race_id: Arc::from(race_id),
            fast_batcher_wiring: Some(fast_batcher_wiring),
            normal_batcher_wiring: Some(batcher_wiring),
            semaphore: Arc::new(Semaphore::new(persistence.max_inflight_db_writes as usize)),
            max_inflight_requests: persistence.max_inflight_db_writes as usize,
        }
    }

    pub async fn ingest(&mut self) {
        let max_inflight = self.max_inflight_requests;

        let fast_wiring = self
            .fast_batcher_wiring
            .take()
            .expect("Fast batcher wiring already taken");
        let client_fast = Arc::clone(&self.client);
        let race_id_fast = Arc::clone(&self.race_id);
        let semaphore_fast = Arc::clone(&self.semaphore);
        tokio::spawn(Self::ingest_internal(
            fast_wiring,
            client_fast,
            race_id_fast,
            semaphore_fast,
            max_inflight,
        ));

        let normal_wiring = self
            .normal_batcher_wiring
            .take()
            .expect("Normal batcher wiring already taken");
        let client_normal = Arc::clone(&self.client);
        let race_id_normal = Arc::clone(&self.race_id);
        let semaphore_normal = Arc::clone(&self.semaphore);
        tokio::spawn(Self::ingest_internal(
            normal_wiring,
            client_normal,
            race_id_normal,
            semaphore_normal,
            max_inflight,
        ));
    }

    async fn ingest_internal(
        wiring: BatcherWiring,
        client: Arc<influxdb3_client::Client>,
        race_id: Arc<str>,
        semaphore: Arc<Semaphore>,
        max_inflight_requests: usize,
    ) {
        let (mut batch_rx, pool_tx) = wiring;

        while let Some(mut batch) = batch_rx.recv().await {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let db = Arc::clone(&client);
            let pool_tx = pool_tx.clone();
            let race_id = Arc::clone(&race_id);

            tokio::spawn(async move {
                let _permit = permit;

                let mut line_protocol = String::new();
                for point in &batch {
                    line_protocol.push_str(&point.to_line_protocol(&race_id));
                    line_protocol.push('\n');
                }

                if let Err(e) = db
                    .write(line_protocol)
                    .max_inflight(max_inflight_requests)
                    .await
                {
                    // TODO: Error handling + partial writes handling
                    eprintln!("database write failed: {}", e);
                }

                batch.clear();

                let _ = pool_tx.try_send(batch);
            });
        }
    }
}
