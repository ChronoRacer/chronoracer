use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CoreConfig {
    pub kafka: KafkaConfig,
}

impl CoreConfig {
    pub const CONFIG_ENV_VAR_NAME: &'static str = "CORE_CONFIG_PATH";

    /// Loads the configuration from the YAML file path specified in the given env variable.
    pub fn load_from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let config_path = std::env::var(Self::CONFIG_ENV_VAR_NAME)
            .map_err(|_| format!("Environment variable '{}' not set", Self::CONFIG_ENV_VAR_NAME))?;
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
        unsafe { env::remove_var(CoreConfig::CONFIG_ENV_VAR_NAME); }
        let loaded = CoreConfig::load_from_env();
        assert!(loaded.is_err());
        assert!(loaded.unwrap_err().to_string().contains("Environment variable"));
    }

    #[test]
    fn load_from_env__sample_config_loads_correctly() {
        unsafe { env::set_var(CoreConfig::CONFIG_ENV_VAR_NAME, "sampe-config.yaml"); }
        let loaded = CoreConfig::load_from_env();
        assert!(loaded.is_ok());
        let unwrapped = loaded.unwrap();
        assert_eq!(unwrapped.kafka.brokers.len(), 1);
        assert_eq!(unwrapped.kafka.brokers[0], "localhost:9092".to_string());
    }
}