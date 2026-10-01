use crate::configuration::CoreConfig;
use crate::persistence::write_metrics::WriteMetrics;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

pub struct HeartbeatHandler {
    producer: Arc<FutureProducer>,
    heartbeat_topic: &'static str,
    heartbeat_interval: Duration,
    write_failure_threshold: Duration,
    race_id: &'static str,
}

impl HeartbeatHandler {
    #[must_use]
    pub fn new(producer: Arc<FutureProducer>, config: &'static CoreConfig) -> Self {
        Self {
            producer,
            heartbeat_topic: &config.kafka.heartbeat_topic.name,
            race_id: &config.race.race_id,
            heartbeat_interval: Duration::from_secs(u64::from(
                config.runtime.heartbeat_interval_s.get(),
            )),
            write_failure_threshold: Duration::from_secs(u64::from(
                config.runtime.heartbeat_write_failure_threshold_s.get(),
            )),
        }
    }

    #[must_use]
    pub async fn keep_alive(&self, write_metrics: Arc<WriteMetrics>) {
        let mut ticker = tokio::time::interval(self.heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            ticker.tick().await;

            if write_metrics.writes_failing_longer_than(self.write_failure_threshold) {
                warn!(
                    "Withholding heartbeat: database writes have failed for more than {:?}",
                    self.write_failure_threshold
                );
                continue;
            }

            // Consumers read only the record's timestamp; the body is informational
            let record = FutureRecord::to(self.heartbeat_topic)
                .key("CORE")
                .payload(self.race_id);

            match self
                .producer
                .send(record, Timeout::After(Duration::from_secs(5)))
                .await
            {
                Ok(_) => {
                    info!("Alive");
                }
                Err((error, _)) => {
                    error!(%error, "Failed to send heartbeat");
                }
            }
        }
    }
}
