use crate::configuration::PersistenceConfig;
use crate::persistence::sensor_data_batcher::{SimplePoint, SimplePointsBatch};
use std::sync::Arc;
use tokio::sync::Semaphore;

trait SensorDataFlusher {
    async fn from_config(
        persistence: &PersistenceConfig,
        race_id: String,
        batch_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
        pool_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
    ) -> Self;

    async fn ingest(&mut self);
}

struct InfluxDbFlusher {
    client: Arc<influxdb3_client::Client>,
    race_id: Arc<str>,
    batch_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
    pool_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
    semaphore: Arc<Semaphore>,
}

impl SensorDataFlusher for InfluxDbFlusher {
    async fn from_config(
        persistence: &PersistenceConfig,
        race_id: String,
        batch_rx: tokio::sync::mpsc::Receiver<Vec<SimplePoint>>,
        pool_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
    ) -> Self {
        let client = influxdb3_client::Client::new(
            influxdb3_client::ClientConfig::builder()
                .host(format!("{}:{}", &persistence.db_host, &persistence.db_port))
                .token(&persistence.db_password)
                .database(&persistence.db_name)
                .write_accept_partial(true)
                .write_use_v2_api(false)
                .write_no_sync(true)
                .build()
                .expect("Invalid InfluxDB config"),
        )
        .await
        .expect("Failed to create InfluxDb client");
        Self {
            client: Arc::from(client),
            race_id: Arc::from(race_id),
            batch_rx,
            pool_tx,
            semaphore: Arc::new(Semaphore::new(
                persistence.max_inflight_db_writes_per_mode as usize,
            )),
        }
    }

    async fn ingest(&mut self) {
        while let Some(mut batch) = self.batch_rx.recv().await {
            let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
            let db = Arc::clone(&self.client);
            let pool_tx = self.pool_tx.clone();
            let race_id = self.race_id.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let w = if let Err(_) = db
                    .write(SimplePointsBatch {
                        points: &batch,
                        race_id: &race_id,
                    })
                    .await
                {
                    eprintln!("database write failed");
                };
                batch.clear();
                let _ = pool_tx.send(batch).await;
            });
        }
    }
}
