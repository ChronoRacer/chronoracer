use crate::configuration::{KafkaConfig, TopicConfig};
use anyhow::{Context, Result, anyhow, bail};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::time::{Duration, Instant};
use tracing::{error, info};

pub async fn ensure_all_topics_exists(kafka_config: &KafkaConfig) -> Result<()> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", kafka_config.brokers.join(","))
        .create()?;

    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let results = admin_client
        .create_topics(
            &[
                NewTopic::from(&kafka_config.sensor_data_topic),
                NewTopic::from(&kafka_config.dlq_config.topic),
                NewTopic::from(&kafka_config.control_event_topic),
                NewTopic::from(&kafka_config.heartbeat_topic),
            ],
            &options,
        )
        .await?;

    for result in results {
        match result {
            Ok(topic) => info!("Topic '{topic}' creation request acknowledged."),
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                info!("Topic '{topic}' already exists. Skipping creation.");
            }
            Err((topic, error_code)) => {
                error!("Failed to create topic '{}': {:?}", topic, error_code);
                return Err(anyhow!(KafkaError::AdminOp(error_code)));
            }
        }
    }

    let topics: Vec<(String, usize)> = [
        &kafka_config.sensor_data_topic,
        &kafka_config.dlq_config.topic,
        &kafka_config.control_event_topic,
        &kafka_config.heartbeat_topic,
    ]
    .iter()
    .map(|topic| {
        let partitions = usize::try_from(topic.num_partitions).unwrap_or(0);
        (topic.name.clone(), partitions)
    })
    .collect();
    tokio::task::spawn_blocking(move || wait_for_topics(&admin_client, &topics))
        .await
        .context("Topic visibility check panicked")?
}

/// How long created topics may take to become visible before Core gives up.
const TOPIC_VISIBLE_TIMEOUT: Duration = Duration::from_secs(60);
const TOPIC_VISIBLE_POLL: Duration = Duration::from_millis(500);

fn wait_for_topics(
    admin_client: &AdminClient<DefaultClientContext>,
    topics: &[(String, usize)],
) -> Result<()> {
    let deadline = Instant::now() + TOPIC_VISIBLE_TIMEOUT;
    for (name, min_partitions) in topics {
        loop {
            let visible = admin_client
                .inner()
                .fetch_metadata(Some(name), Duration::from_secs(5))
                .is_ok_and(|metadata| {
                    metadata.topics().iter().any(|topic| {
                        topic.name() == name
                            && topic.error().is_none()
                            && topic.partitions().len() >= *min_partitions
                    })
                });
            if visible {
                info!("Topic '{name}' is visible to clients.");
                break;
            }
            if Instant::now() >= deadline {
                bail!("Topic '{name}' not visible after {TOPIC_VISIBLE_TIMEOUT:?}");
            }
            std::thread::sleep(TOPIC_VISIBLE_POLL);
        }
    }
    Ok(())
}

impl<'a> From<&'a TopicConfig> for NewTopic<'a> {
    fn from(config: &'a TopicConfig) -> Self {
        NewTopic::new(
            &config.name,
            config.num_partitions,
            TopicReplication::Fixed(config.replication_factor),
        )
    }
}
