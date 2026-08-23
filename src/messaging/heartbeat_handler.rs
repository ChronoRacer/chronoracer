use crate::configuration::CoreConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

pub struct HeartbeatHandler {
    producer: Arc<FutureProducer>,
    heartbeat_topic: &'static str,
    heartbeat_interval_s: Duration,
    race_id: &'static str,
}

impl HeartbeatHandler {
    #[must_use]
    pub fn new(producer: Arc<FutureProducer>, config: &'static CoreConfig) -> Self {
        Self {
            producer,
            heartbeat_topic: &config.kafka.heartbeat_topic.name,
            race_id: &config.race.race_id,
            heartbeat_interval_s: Duration::from_secs(u64::from(
                config.runtime.heartbeat_interval_s,
            )),
        }
    }

    #[must_use]
    pub async fn keep_alive(&self) {
        let mut ticker = tokio::time::interval(self.heartbeat_interval_s);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            ticker.tick().await;

            let record = FutureRecord::to(self.heartbeat_topic)
                .payload("OK") // TODO: heartbeat status
                .key(self.race_id);

            match self
                .producer
                .send(record, Timeout::After(Duration::from_secs(5)))
                .await
            {
                Ok(_) => {
                    info!("Alive");
                }
                Err((err, _)) => {
                    error!("Failed to send heartbeat: {:?}", err);
                }
            }
        }
    }
}
