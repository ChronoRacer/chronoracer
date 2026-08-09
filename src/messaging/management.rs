use crate::configuration::{KafkaConfig, TopicConfig};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::ClientConfig;
use std::time::Duration;
use tracing::{error, info};

pub async fn ensure_all_topics_exists(kafka_config: &KafkaConfig) -> Result<(), KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", kafka_config.brokers.join(","))
        .create()?;

    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let results = admin_client
        .create_topics(
            &[
                NewTopic::from(&kafka_config.sensor_data_flush_topic),
                NewTopic::from(&kafka_config.sensor_data_order_topic),
                NewTopic::from(&kafka_config.dlq_config.topic),
                NewTopic::from(&kafka_config.control_event_topic),
            ],
            &options,
        )
        .await?;

    for result in results {
        match result {
            Ok(topic) => info!("Topic '{topic}' created successfully."),
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                info!("Topic '{topic}' already exists. Skipping creation.");
            }
            Err((topic, error_code)) => {
                error!("Failed to create topic '{}': {:?}", topic, error_code);
                return Err(KafkaError::AdminOp(error_code));
            }
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
