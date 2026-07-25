use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DLQConfig {
    pub dlq_topic: String,
    pub dlq_unknown_key: String,
    pub publish_timeout_sec: u16,
}

impl DLQConfig {
    pub fn mock_config() -> DLQConfig {
        Self {
            dlq_unknown_key: "UNKNOWN_KEY".to_string(),
            dlq_topic: "test.dlq.topic".to_string(),
            publish_timeout_sec: 2,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group_id: String,
    pub sensor_data_flush_topic: String,
    pub sensor_data_order_topic: String,
    pub control_event_topic: String,
    pub dlq_config: DLQConfig,
}

impl KafkaConfig {
    pub fn mock_config() -> KafkaConfig {
        Self {
            group_id: "test_group".to_string(),
            sensor_data_flush_topic: "flush".to_string(),
            sensor_data_order_topic: "order".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            dlq_config: DLQConfig::mock_config(),
            control_event_topic: "event".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ProcessingConfig {
    pub message_channel_buffer_size: usize,
    pub dlq_channel_buffer_size: usize,
}

impl ProcessingConfig {
    pub fn mock_config() -> ProcessingConfig {
        Self {
            message_channel_buffer_size: 0,
            dlq_channel_buffer_size: 0,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PersistenceConfig {
    pub db_host: String,
    pub db_port: u16,
    pub db_password: String,
    pub db_name: String,
    pub fast_flush_max_combined_batch_size: usize,
    pub fast_flush_interval_ms: u32,
    pub fast_flush_sensor_ids: Vec<String>,
    pub flush_max_combined_batch_size: usize,
    pub flush_interval_ms: u32,
    pub max_inflight_db_writes: u16,
}

impl PersistenceConfig {
    pub fn mock_config() -> PersistenceConfig {
        Self {
            db_host: "localhost".to_string(),
            db_port: 5432,
            db_password: "root".to_string(),
            db_name: "chrono_db".to_string(),
            fast_flush_max_combined_batch_size: 1000,
            fast_flush_interval_ms: 1500,
            fast_flush_sensor_ids: vec!["test".to_string(), "test2".to_string()],
            flush_max_combined_batch_size: 5000,
            flush_interval_ms: 3000,
            max_inflight_db_writes: 7,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CoreConfig {
    pub kafka: KafkaConfig,
    pub processing: ProcessingConfig,
    pub persistence: PersistenceConfig,
    pub race_id: String,
}

impl CoreConfig {
    pub const CONFIG_PATH_ENV_VAR_NAME: &'static str = "CORE_CONFIG_PATH";

    /// Loads the configuration from the YAML file path specified by the env variable.
    pub fn load_from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let config_path = std::env::var(Self::CONFIG_PATH_ENV_VAR_NAME).map_err(|_| {
            format!(
                "Environment variable '{}' not set",
                Self::CONFIG_PATH_ENV_VAR_NAME
            )
        })?;
        let settings = Config::builder()
            .add_source(File::with_name(&config_path))
            .build()?;
        let config: Self = settings.try_deserialize()?;
        Ok(config)
    }

    pub fn mock_config() -> CoreConfig {
        Self {
            kafka: KafkaConfig::mock_config(),
            processing: ProcessingConfig::mock_config(),
            persistence: PersistenceConfig::mock_config(),
            race_id: "test-race".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::CoreConfig;
    use std::env;

    #[test]
    fn load_from_env__fails_on_missing_env_var() {
        unsafe {
            env::remove_var(CoreConfig::CONFIG_PATH_ENV_VAR_NAME);
        }
        let loaded = CoreConfig::load_from_env();
        assert!(loaded.is_err());
        assert!(
            loaded
                .unwrap_err()
                .to_string()
                .contains("Environment variable")
        );
    }

    #[test]
    fn load_from_env__sample_config_loads_correctly() {
        unsafe {
            env::set_var(CoreConfig::CONFIG_PATH_ENV_VAR_NAME, "sampe-config.yaml");
        }
        let loaded = CoreConfig::load_from_env();
        assert!(loaded.is_ok());
        let unwrapped = loaded.unwrap();
        assert_eq!(unwrapped.kafka.brokers.len(), 1);
        assert_eq!(unwrapped.kafka.brokers[0], "localhost:9092".to_string());
        assert_eq!(unwrapped.kafka.dlq_config.publish_timeout_sec, 5);
    }
}
