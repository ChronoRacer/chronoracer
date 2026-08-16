use compact_str::CompactString;
use config::{Config, File};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct DLQConfig {
    pub topic: TopicConfig,
    pub dlq_unknown_key: String,
    pub publish_timeout_sec: u16,
}

impl DLQConfig {
    pub fn mock_config() -> DLQConfig {
        Self {
            dlq_unknown_key: "UNKNOWN_KEY".to_string(),
            topic: TopicConfig::mock_config("dlq"),
            publish_timeout_sec: 2,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i32,
}

impl TopicConfig {
    pub fn mock_config(name: &str) -> TopicConfig {
        Self {
            name: name.to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct KafkaExportConfig {
    pub redpanda_yaml_dir_path: PathBuf,
    pub export_topics: Vec<String>,
    pub nested_path_in_bucket: String,
    pub local_path: PathBuf,
    pub max_retries: u16
}

impl KafkaExportConfig {
    pub fn mock_config() -> KafkaExportConfig {
        Self {
            redpanda_yaml_dir_path: PathBuf::from("/redpanda"),
            export_topics: vec!["dlq".to_string()],
            nested_path_in_bucket: "export/kafka".to_string(),
            local_path: PathBuf::from("/export/kafka"),
            max_retries: 2
        }
    }

}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group_id: String,
    pub sensor_data_topic: TopicConfig,
    pub control_event_topic: TopicConfig,
    pub dlq_config: DLQConfig,
    pub heartbeat_topic: TopicConfig,
    pub export: KafkaExportConfig
}

impl KafkaConfig {
    pub fn mock_config() -> KafkaConfig {
        Self {
            group_id: "test_group".to_string(),
            sensor_data_topic: TopicConfig::mock_config("data"),
            brokers: vec!["localhost:9092".to_string()],
            dlq_config: DLQConfig::mock_config(),
            control_event_topic: TopicConfig::mock_config("event"),
            heartbeat_topic: TopicConfig::mock_config("core"),
            export: KafkaExportConfig::mock_config()
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
pub struct DatabaseExportConfig {
    pub compression_level: u16,
    pub compression_type: CompactString,
    pub vm_export_dir: PathBuf,
    pub max_retries: u16
}

impl DatabaseExportConfig {
    pub fn mock_config() -> DatabaseExportConfig {
        Self {
            compression_type: CompactString::const_new("zstd"),
            compression_level: 7,
            vm_export_dir: PathBuf::from("/export/db"),
            max_retries: 2,
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
    pub export: DatabaseExportConfig,
}

impl PersistenceConfig {
    pub fn mock_config() -> PersistenceConfig {
        Self {
            db_host: "http://localhost".to_string(),
            db_port: 5432,
            db_password: "root".to_string(),
            db_name: "chrono_db".to_string(),
            fast_flush_max_combined_batch_size: 1000,
            fast_flush_interval_ms: 1500,
            fast_flush_sensor_ids: vec![],
            flush_max_combined_batch_size: 5000,
            flush_interval_ms: 3000,
            max_inflight_db_writes: 7,
            export: DatabaseExportConfig::mock_config(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RaceConfig {
    /// Id of current race. Prefered under 24 bytes
    pub race_id: CompactString,
    /// Id of starting driver. Prefered under 24 bytes
    pub initial_driver_id: CompactString,
}

impl RaceConfig {
    pub fn mock_config() -> RaceConfig {
        Self {
            race_id: CompactString::const_new("race123"),
            initial_driver_id: CompactString::const_new("driver123"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RuntimeConfig {
    pub rayon_threads: u16,
    pub tokio_workers: u16,
    pub tokio_blocking: u16,
    pub heartbeat_interval_s: u16,
    pub data_pipeline_drain_timeout_s: u16
}

impl RuntimeConfig {
    pub fn mock_config() -> RuntimeConfig {
        Self {
            rayon_threads: 4,
            tokio_workers: 8,
            tokio_blocking: 1,
            heartbeat_interval_s: 30,
            data_pipeline_drain_timeout_s: 120,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct GcpConfig {
    pub enabled: bool,
    pub export_bucket: String
}

impl GcpConfig {
    pub fn mock_config() -> Self {
        Self {
            enabled: false,
            export_bucket: "exports".to_string()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CoreConfig {
    pub kafka: KafkaConfig,
    pub processing: ProcessingConfig,
    pub persistence: PersistenceConfig,
    pub race: RaceConfig,
    pub runtime: RuntimeConfig,
    pub gcp: GcpConfig
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
            race: RaceConfig::mock_config(),
            runtime: RuntimeConfig::mock_config(),
            gcp: GcpConfig::mock_config()
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
        match loaded {
            Ok(loaded) => {
                assert_eq!(loaded.kafka.brokers.len(), 1);
                assert_eq!(loaded.kafka.brokers[0], "localhost:9092".to_string());
                assert_eq!(loaded.kafka.dlq_config.publish_timeout_sec, 5);
            },
            Err(e) => {
                panic!("{}", e);
            }
        }
    }
}
