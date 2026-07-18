use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DLQConfig {
    pub dlq_topic: String,
    pub dlq_unknown_key: String,
    pub publish_timeout_sec: u16
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

#[derive(Debug, Deserialize)]
pub struct ProcessingConfig {
    pub message_channel_buffer_size: usize,
    pub dlq_channel_buffer_size: usize,
}

#[derive(Debug, Deserialize)]
pub struct CoreConfig {
    pub kafka: KafkaConfig,
    pub processing: ProcessingConfig,
}

impl CoreConfig {
    pub const CONFIG_PATH_ENV_VAR_NAME: &'static str = "CORE_CONFIG_PATH";

    /// Loads the configuration from the YAML file path specified by the env variable.
    pub fn load_from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let config_path = std::env::var(Self::CONFIG_PATH_ENV_VAR_NAME)
            .map_err(|_| format!("Environment variable '{}' not set", Self::CONFIG_PATH_ENV_VAR_NAME))?;
        let settings = Config::builder()
            .add_source(File::with_name(&config_path))
            .build()?;
        let config: Self = settings.try_deserialize()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use crate::configuration::CoreConfig;

    #[test]
    fn load_from_env__fails_on_missing_env_var() {
        unsafe { env::remove_var(CoreConfig::CONFIG_PATH_ENV_VAR_NAME); }
        let loaded = CoreConfig::load_from_env();
        assert!(loaded.is_err());
        assert!(loaded.unwrap_err().to_string().contains("Environment variable"));
    }

    #[test]
    fn load_from_env__sample_config_loads_correctly() {
        unsafe { env::set_var(CoreConfig::CONFIG_PATH_ENV_VAR_NAME, "sampe-config.yaml"); }
        let loaded = CoreConfig::load_from_env();
        assert!(loaded.is_ok());
        let unwrapped = loaded.unwrap();
        assert_eq!(unwrapped.kafka.brokers.len(), 1);
        assert_eq!(unwrapped.kafka.brokers[0], "localhost:9092".to_string());
        assert_eq!(unwrapped.kafka.dlq_config.publish_timeout_sec, 5);
    }
}