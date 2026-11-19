use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use config::{Config, File};
use parquet::basic::{Compression, ZstdLevel};
use serde::Deserialize;
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::path::PathBuf;

use crate::messaging::control_message::{MAX_DRIVER_ID_BYTES, validate_driver_id};
use crate::processing::sensor_data_message::MAX_SENSOR_ID_BYTES;
use std::fmt;

#[derive(Debug, Deserialize)]
pub struct DLQConfig {
    pub topic: TopicConfig,
    pub dlq_unknown_key: String,
    pub publish_timeout_sec: u16,
    /// DLQ records produced concurrently. Bounds producer memory during a burst of invalid messages.
    pub max_inflight_sends: NonZeroU16,
}

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i32,
}

#[derive(Debug, Deserialize)]
pub struct KafkaExportConfig {
    pub redpanda_yaml_dir_path: PathBuf,
    pub export_topics: Vec<String>,
    pub nested_path_in_bucket: String,
    pub local_path: PathBuf,
    pub max_retries: u16,
    /// Per-attempt wall-clock limit for `rpk connect run` (child is killed on expiry).
    pub attempt_timeout_s: NonZeroU16,
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group_id: String,
    pub sensor_data_consumer: KafkaConsumerConfig,
    pub sensor_data_topic: TopicConfig,
    pub control_event_topic: TopicConfig,
    pub dlq_config: DLQConfig,
    pub heartbeat_topic: TopicConfig,
    pub export: KafkaExportConfig,
}

impl KafkaConfig {
    /// `group_id` is a prefix: the two consumers must not share a group, or a rebalance of one
    /// (a data consumer that stalled under backpressure) would rebalance the other.
    #[must_use]
    pub fn data_group_id(&self) -> String {
        format!("{}-data", self.group_id)
    }

    #[must_use]
    pub fn control_group_id(&self) -> String {
        format!("{}-control", self.group_id)
    }
}

#[derive(Debug, Deserialize)]
pub struct KafkaConsumerConfig {
    pub fetch_min_bytes: usize,
    pub fetch_wait_max_ms: u32,
    pub fetch_message_max_bytes: usize,
    pub queued_min_messages: usize,
    pub queued_max_messages_kbytes: usize,
    pub socket_receive_buffer_bytes: usize,
    pub auto_commit_interval_ms: u32,
    /// How often consumed-but-unsettled offsets are logged (only while some are pending).
    pub offset_lag_log_interval_s: NonZeroU16,
}

#[derive(Debug, Deserialize)]
pub struct ProcessingConfig {
    pub message_channel_buffer_size: NonZeroUsize,
    pub message_batch_size: NonZeroUsize,
    pub message_batch_flush_interval_ms: NonZeroU32,
    pub dlq_channel_buffer_size: NonZeroUsize,
    /// Sensor data DLQ sampling: one invalid message in this many is dead-lettered, the rest are
    /// discarded and counted. `1` keeps every message.
    pub dlq_sample_every: NonZeroU32,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseExportConfig {
    pub compression_level: u16,
    pub compression_type: CompactString,
    pub vm_export_dir: PathBuf,
    /// Object-key prefix under `{raceId}/` in the race bucket (file name only after this).
    pub nested_path_in_bucket: String,
    pub max_retries: u16,
    /// Per-attempt wall-clock limit for exporting one table.
    pub attempt_timeout_s: NonZeroU16,
    /// Longest time span of one export query.
    pub query_window_s: NonZeroU32,
    /// Rows one export query may cover, estimated from persisted Parquet file metadata. Bounds
    /// what `InfluxDB` deduplicates and sorts per query.
    pub max_rows_per_window: NonZeroU32,
}

impl DatabaseExportConfig {
    /// Only ZSTD is compiled into the Parquet writer.
    pub fn parquet_compression(&self) -> Result<Compression> {
        if !self.compression_type.eq_ignore_ascii_case("zstd") {
            bail!(
                "persistence.export.compression_type must be ZSTD, got '{}'",
                self.compression_type
            );
        }
        let level = ZstdLevel::try_new(i32::from(self.compression_level)).with_context(|| {
            format!(
                "persistence.export.compression_level must be in 1..=22, got {}",
                self.compression_level
            )
        })?;
        Ok(Compression::ZSTD(level))
    }
}

#[derive(Deserialize)]
pub struct PersistenceConfig {
    pub db_host: String,
    pub db_port: u16,
    pub db_password: String,
    pub db_name: String,
    pub fast_flush_max_combined_batch_size: NonZeroUsize,
    pub fast_flush_interval_ms: NonZeroU32,
    pub fast_flush_sensor_ids: Vec<String>,
    pub flush_max_combined_batch_size: NonZeroUsize,
    pub flush_interval_ms: NonZeroU32,
    pub max_inflight_db_writes: NonZeroU16,
    /// First wait before re-sending a write that failed transiently; doubles per retry.
    pub write_retry_initial_backoff_ms: NonZeroU32,
    /// Cap on the wait between retries of one write. Must be at least the initial backoff.
    pub write_retry_max_backoff_ms: NonZeroU32,
    pub export: DatabaseExportConfig,
}

impl fmt::Debug for PersistenceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistenceConfig")
            .field("db_host", &self.db_host)
            .field("db_port", &self.db_port)
            .field("db_password", &"<redacted>")
            .field("db_name", &self.db_name)
            .field(
                "fast_flush_max_combined_batch_size",
                &self.fast_flush_max_combined_batch_size,
            )
            .field("fast_flush_interval_ms", &self.fast_flush_interval_ms)
            .field("fast_flush_sensor_ids", &self.fast_flush_sensor_ids)
            .field(
                "flush_max_combined_batch_size",
                &self.flush_max_combined_batch_size,
            )
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("max_inflight_db_writes", &self.max_inflight_db_writes)
            .field(
                "write_retry_initial_backoff_ms",
                &self.write_retry_initial_backoff_ms,
            )
            .field(
                "write_retry_max_backoff_ms",
                &self.write_retry_max_backoff_ms,
            )
            .field("export", &self.export)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
pub struct RaceConfig {
    /// Id of current race. Prefered under 24 bytes
    pub race_id: CompactString,
    /// Id of starting driver. Prefered under 24 bytes
    pub initial_driver_id: CompactString,
}

#[derive(Debug, Deserialize)]
pub struct RuntimeConfig {
    pub rayon_threads: NonZeroU16,
    pub tokio_workers: NonZeroU16,
    pub tokio_blocking: NonZeroU16,
    pub heartbeat_interval_s: NonZeroU16,
    /// Heartbeats are withheld once Influx writes have failed continuously for longer than this.
    pub heartbeat_write_failure_threshold_s: NonZeroU16,
    pub data_pipeline_drain_timeout_s: u16,
    /// Bound for the final Cloud Monitoring sample after the data pipeline drains.
    pub write_metrics_final_timeout_s: NonZeroU16,
    /// Bound for each synchronous offset commit of the shutdown sequence.
    pub offset_final_commit_timeout_s: NonZeroU16,
    pub notify_shutdown_on_failed_db_export: bool,
    pub notify_shutdown_on_failed_kafka_export: bool,
}

#[derive(Debug, Deserialize)]
pub struct GcpConfig {
    pub enabled: bool,
    pub project_id: String,
    pub pubsub_topic_id: String,
    pub export_bucket: String,
    pub max_retries: u16,
    pub max_gcs_concurrent_uploads: NonZeroU16,
    /// Per-file wall-clock limit for a GCS export upload.
    pub upload_timeout_s: NonZeroU16,
    /// Wall-clock limit for export-done Pub/Sub publish + flush.
    pub pubsub_publish_timeout_s: NonZeroU16,
    pub allow_unknown_gce_metrics: bool,
}

#[derive(Debug, Deserialize)]
pub struct TelemetryConfig {
    pub port: u16,
    pub max_queue_points: NonZeroUsize,
    pub max_queue_age_ms: NonZeroU32,
    /// Concurrent `Watch` subscribers. Valid range: `1..=MAX_SUBSCRIBERS_LIMIT`.
    pub max_subscribers: usize,
}

impl TelemetryConfig {
    pub const MAX_SUBSCRIBERS_LIMIT: usize = 16;

    fn validate(&self) -> Result<()> {
        if !(1..=Self::MAX_SUBSCRIBERS_LIMIT).contains(&self.max_subscribers) {
            bail!(
                "telemetry.max_subscribers must be in 1..={}, got {}",
                Self::MAX_SUBSCRIBERS_LIMIT,
                self.max_subscribers
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct CoreConfig {
    pub kafka: KafkaConfig,
    pub processing: ProcessingConfig,
    pub persistence: PersistenceConfig,
    pub race: RaceConfig,
    pub runtime: RuntimeConfig,
    pub gcp: GcpConfig,
    pub telemetry: TelemetryConfig,
}

impl CoreConfig {
    pub const CONFIG_PATH_ENV_VAR_NAME: &'static str = "CORE_CONFIG_PATH";

    /// Loads the configuration from the YAML file path specified by the env variable.
    pub fn load_from_env() -> Result<Self> {
        let config_path = std::env::var(Self::CONFIG_PATH_ENV_VAR_NAME).with_context(|| {
            format!(
                "Environment variable '{}' not set",
                Self::CONFIG_PATH_ENV_VAR_NAME
            )
        })?;
        let settings = Config::builder()
            .add_source(File::with_name(&config_path))
            .build()
            .context("Failed to build configuration")?;
        let config: Self = settings
            .try_deserialize()
            .context("Failed to deserialize configuration")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.telemetry.validate()?;

        if self.kafka.group_id.trim().is_empty() {
            bail!("kafka.group_id must not be empty");
        }

        let persistence = &self.persistence;
        if persistence.write_retry_max_backoff_ms < persistence.write_retry_initial_backoff_ms {
            bail!(
                "persistence.write_retry_max_backoff_ms ({}) must be at least write_retry_initial_backoff_ms ({})",
                persistence.write_retry_max_backoff_ms,
                persistence.write_retry_initial_backoff_ms
            );
        }

        persistence.export.parquet_compression()?;

        for sensor_id in &self.persistence.fast_flush_sensor_ids {
            if sensor_id.len() > MAX_SENSOR_ID_BYTES {
                bail!("Sensor ID '{sensor_id}' too long. At most {MAX_SENSOR_ID_BYTES} bytes");
            }
        }

        if let Err(reason) = validate_driver_id(self.race.initial_driver_id.as_str()) {
            bail!(
                "race.initial_driver_id invalid ({reason}); at most {MAX_DRIVER_ID_BYTES} bytes, no ASCII control or line-protocol special characters"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

#[cfg(test)]
mod mock {
    use super::*;

    impl DLQConfig {
        pub fn mock_config() -> DLQConfig {
            Self {
                dlq_unknown_key: "UNKNOWN_KEY".to_string(),
                topic: TopicConfig::mock_config("dlq"),
                publish_timeout_sec: 2,
                max_inflight_sends: NonZeroU16::new(256).unwrap(),
            }
        }
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

    impl KafkaExportConfig {
        pub fn mock_config() -> KafkaExportConfig {
            Self {
                redpanda_yaml_dir_path: PathBuf::from("/redpanda"),
                export_topics: vec!["dlq".to_string()],
                nested_path_in_bucket: "export/kafka".to_string(),
                local_path: PathBuf::from("/export/kafka"),
                max_retries: 2,
                attempt_timeout_s: NonZeroU16::new(300).unwrap(),
            }
        }
    }

    impl KafkaConfig {
        pub fn mock_config() -> KafkaConfig {
            Self {
                group_id: "test_group".to_string(),
                sensor_data_consumer: KafkaConsumerConfig::mock_config(),
                sensor_data_topic: TopicConfig::mock_config("data"),
                brokers: vec!["localhost:9092".to_string()],
                dlq_config: DLQConfig::mock_config(),
                control_event_topic: TopicConfig::mock_config("event"),
                heartbeat_topic: TopicConfig::mock_config("core"),
                export: KafkaExportConfig::mock_config(),
            }
        }
    }

    impl KafkaConsumerConfig {
        pub fn mock_config() -> KafkaConsumerConfig {
            Self {
                fetch_min_bytes: 1,
                fetch_wait_max_ms: 100,
                fetch_message_max_bytes: 5 * 1024 * 1024,
                queued_min_messages: 200_000,
                queued_max_messages_kbytes: 256 * 1024,
                socket_receive_buffer_bytes: 1024 * 1024,
                auto_commit_interval_ms: 5_000,
                offset_lag_log_interval_s: NonZeroU16::new(60).unwrap(),
            }
        }
    }

    impl ProcessingConfig {
        pub fn mock_config() -> ProcessingConfig {
            Self {
                message_channel_buffer_size: NonZeroUsize::new(64).unwrap(),
                message_batch_size: NonZeroUsize::new(1_000).unwrap(),
                message_batch_flush_interval_ms: NonZeroU32::new(5).unwrap(),
                dlq_channel_buffer_size: NonZeroUsize::new(10_000).unwrap(),
                dlq_sample_every: NonZeroU32::new(1).unwrap(),
            }
        }
    }

    impl DatabaseExportConfig {
        pub fn mock_config() -> DatabaseExportConfig {
            Self {
                compression_type: CompactString::const_new("zstd"),
                compression_level: 7,
                vm_export_dir: PathBuf::from("/export/db"),
                nested_path_in_bucket: "export/db".to_string(),
                max_retries: 2,
                attempt_timeout_s: NonZeroU16::new(600).unwrap(),
                query_window_s: NonZeroU32::new(600).unwrap(),
                max_rows_per_window: NonZeroU32::new(1_000_000).unwrap(),
            }
        }
    }

    impl PersistenceConfig {
        pub fn mock_config() -> PersistenceConfig {
            Self {
                db_host: "http://localhost".to_string(),
                db_port: 5432,
                db_password: "root".to_string(),
                db_name: "chrono_db".to_string(),
                fast_flush_max_combined_batch_size: NonZeroUsize::new(1000).unwrap(),
                fast_flush_interval_ms: NonZeroU32::new(1500).unwrap(),
                fast_flush_sensor_ids: vec![],
                flush_max_combined_batch_size: NonZeroUsize::new(5000).unwrap(),
                flush_interval_ms: NonZeroU32::new(3000).unwrap(),
                max_inflight_db_writes: NonZeroU16::new(7).unwrap(),
                write_retry_initial_backoff_ms: NonZeroU32::new(200).unwrap(),
                write_retry_max_backoff_ms: NonZeroU32::new(10_000).unwrap(),
                export: DatabaseExportConfig::mock_config(),
            }
        }
    }

    impl RaceConfig {
        pub fn mock_config() -> RaceConfig {
            Self {
                race_id: CompactString::const_new("race123"),
                initial_driver_id: CompactString::const_new("driver123"),
            }
        }
    }

    impl RuntimeConfig {
        pub fn mock_config() -> RuntimeConfig {
            Self {
                rayon_threads: NonZeroU16::new(4).unwrap(),
                tokio_workers: NonZeroU16::new(2).unwrap(),
                tokio_blocking: NonZeroU16::new(1).unwrap(),
                heartbeat_interval_s: NonZeroU16::new(30).unwrap(),
                heartbeat_write_failure_threshold_s: NonZeroU16::new(60).unwrap(),
                data_pipeline_drain_timeout_s: 180,
                write_metrics_final_timeout_s: NonZeroU16::new(15).unwrap(),
                offset_final_commit_timeout_s: NonZeroU16::new(15).unwrap(),
                notify_shutdown_on_failed_db_export: false,
                notify_shutdown_on_failed_kafka_export: true,
            }
        }
    }

    impl GcpConfig {
        pub fn mock_config() -> Self {
            Self {
                enabled: false,
                export_bucket: "chronoracer-race".to_string(),
                max_retries: 2,
                max_gcs_concurrent_uploads: NonZeroU16::new(3).unwrap(),
                upload_timeout_s: NonZeroU16::new(180).unwrap(),
                pubsub_publish_timeout_s: NonZeroU16::new(30).unwrap(),
                project_id: "1234".to_string(),
                pubsub_topic_id: "todo".to_string(),
                allow_unknown_gce_metrics: false,
            }
        }
    }

    impl TelemetryConfig {
        pub fn mock_config() -> Self {
            Self {
                port: 9470,
                max_queue_points: NonZeroUsize::new(16_384).unwrap(),
                max_queue_age_ms: NonZeroU32::new(250).unwrap(),
                max_subscribers: 4,
            }
        }
    }

    impl CoreConfig {
        pub fn mock_config() -> CoreConfig {
            Self {
                kafka: KafkaConfig::mock_config(),
                processing: ProcessingConfig::mock_config(),
                persistence: PersistenceConfig::mock_config(),
                race: RaceConfig::mock_config(),
                runtime: RuntimeConfig::mock_config(),
                gcp: GcpConfig::mock_config(),
                telemetry: TelemetryConfig::mock_config(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::{CoreConfig, TelemetryConfig, env_lock};
    use crate::processing::sensor_data_message::MAX_SENSOR_ID_BYTES;
    use compact_str::CompactString;
    use std::env;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

    #[test]
    fn load_from_env_fails_on_missing_env_var() {
        let _lock = env_lock();
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
    fn load_from_env_default_config_loads_correctly() {
        let _lock = env_lock();
        unsafe {
            env::set_var(CoreConfig::CONFIG_PATH_ENV_VAR_NAME, "default-config.yaml");
        }
        let loaded = CoreConfig::load_from_env();
        match loaded {
            Ok(loaded) => {
                assert_eq!(loaded.kafka.brokers.len(), 1);
                assert_eq!(loaded.kafka.brokers[0], "localhost:9092".to_string());
                assert_eq!(loaded.kafka.dlq_config.publish_timeout_sec, 5);
                assert_eq!(loaded.kafka.dlq_config.max_inflight_sends.get(), 256);
                assert_eq!(loaded.telemetry.port, 9470);
                assert_eq!(loaded.telemetry.max_queue_points.get(), 16_384);
                assert_eq!(loaded.telemetry.max_queue_age_ms.get(), 250);
                assert_eq!(loaded.telemetry.max_subscribers, 4);
                assert_eq!(loaded.persistence.write_retry_initial_backoff_ms.get(), 200);
                assert_eq!(loaded.persistence.write_retry_max_backoff_ms.get(), 10_000);
                assert_eq!(loaded.runtime.heartbeat_write_failure_threshold_s.get(), 60);
                assert_eq!(loaded.runtime.offset_final_commit_timeout_s.get(), 15);
                assert_eq!(
                    loaded
                        .kafka
                        .sensor_data_consumer
                        .offset_lag_log_interval_s
                        .get(),
                    60
                );
                assert_eq!(loaded.kafka.group_id, "chrono-core");
                assert!(loaded.validate().is_ok());
            }
            Err(e) => {
                panic!("{}", e);
            }
        }
    }

    #[test]
    fn telemetry_max_subscribers_must_be_in_range() {
        let mut config = CoreConfig::mock_config();
        for valid in [1, 4, TelemetryConfig::MAX_SUBSCRIBERS_LIMIT] {
            config.telemetry.max_subscribers = valid;
            assert!(config.validate().is_ok());
        }
        for invalid in [0, TelemetryConfig::MAX_SUBSCRIBERS_LIMIT + 1] {
            config.telemetry.max_subscribers = invalid;
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn validate_rejects_oversized_fast_flush_sensor_ids() {
        let mut config = CoreConfig::mock_config();
        config.persistence.fast_flush_sensor_ids = vec!["a".repeat(MAX_SENSOR_ID_BYTES + 1)];
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("too long"));
    }

    #[test]
    fn validate_rejects_retry_max_backoff_below_initial() {
        let mut config = CoreConfig::mock_config();
        config.persistence.write_retry_initial_backoff_ms = NonZeroU32::new(500).unwrap();
        config.persistence.write_retry_max_backoff_ms = NonZeroU32::new(499).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("write_retry_max_backoff_ms"));

        config.persistence.write_retry_max_backoff_ms = NonZeroU32::new(500).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn data_and_control_consumers_get_separate_groups_derived_from_the_group_id() {
        let config = CoreConfig::mock_config();

        assert_eq!(config.kafka.data_group_id(), "test_group-data");
        assert_eq!(config.kafka.control_group_id(), "test_group-control");
        assert_ne!(
            config.kafka.data_group_id(),
            config.kafka.control_group_id()
        );
        assert_ne!(config.kafka.data_group_id(), config.kafka.group_id);
    }

    #[test]
    fn validate_rejects_an_empty_group_id() {
        let mut config = CoreConfig::mock_config();
        config.kafka.group_id = "  ".to_string();

        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("group_id")
        );
    }

    #[test]
    fn deserialize_rejects_zero_offset_intervals() {
        let consumer = |lag: u16| {
            format!(
                "fetch_min_bytes: 1\nfetch_wait_max_ms: 1\nfetch_message_max_bytes: 1\nqueued_min_messages: 1\nqueued_max_messages_kbytes: 1\nsocket_receive_buffer_bytes: 1\nauto_commit_interval_ms: 5000\noffset_lag_log_interval_s: {lag}\n"
            )
        };
        let parse = |yaml: String| {
            config::Config::builder()
                .add_source(config::File::from_str(&yaml, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<crate::configuration::KafkaConsumerConfig>()
        };

        assert!(parse(consumer(1)).is_ok());
        assert!(parse(consumer(0)).is_err());
    }

    #[test]
    fn deserialize_rejects_a_zero_final_commit_timeout() {
        let runtime = |timeout: u16| {
            format!(
                "rayon_threads: 4\ntokio_workers: 2\ntokio_blocking: 1\nheartbeat_interval_s: 30\nheartbeat_write_failure_threshold_s: 60\ndata_pipeline_drain_timeout_s: 180\nwrite_metrics_final_timeout_s: 15\noffset_final_commit_timeout_s: {timeout}\nnotify_shutdown_on_failed_db_export: false\nnotify_shutdown_on_failed_kafka_export: true\n"
            )
        };
        let parse = |yaml: String| {
            config::Config::builder()
                .add_source(config::File::from_str(&yaml, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<crate::configuration::RuntimeConfig>()
        };

        assert!(parse(runtime(15)).is_ok());
        assert!(parse(runtime(0)).is_err());
    }

    #[test]
    fn validate_rejects_invalid_initial_driver_id() {
        let mut config = CoreConfig::mock_config();
        config.race.initial_driver_id = CompactString::from("bad,tag");
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("initial_driver_id"));
    }

    #[test]
    fn persistence_debug_redacts_db_password() {
        let config = CoreConfig::mock_config();
        let rendered = format!("{:?}", config.persistence);
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains(&config.persistence.db_password));
        let pretty = format!("{config:#?}");
        assert!(!pretty.contains(&config.persistence.db_password));
    }

    #[test]
    fn deserialize_rejects_zero_runtime_and_channel_values() {
        let yaml = r"
runtime:
  rayon_threads: 0
  tokio_workers: 2
  tokio_blocking: 1
  heartbeat_interval_s: 30
  heartbeat_write_failure_threshold_s: 60
  data_pipeline_drain_timeout_s: 180
  write_metrics_final_timeout_s: 15
  offset_final_commit_timeout_s: 15
  notify_shutdown_on_failed_db_export: false
  notify_shutdown_on_failed_kafka_export: true
";
        let result = config::Config::builder()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize::<crate::configuration::RuntimeConfig>();
        assert!(
            result.is_err(),
            "zero rayon_threads must be rejected, got: {result:?}"
        );
    }

    #[test]
    fn deserialize_rejects_zero_processing_buffers() {
        let yaml = r"
message_channel_buffer_size: 0
message_batch_size: 1000
message_batch_flush_interval_ms: 15
dlq_channel_buffer_size: 10
dlq_sample_every: 1
";
        assert!(
            config::Config::builder()
                .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<crate::configuration::ProcessingConfig>()
                .is_err()
        );
    }

    #[test]
    fn deserialize_rejects_zero_dlq_inflight_sends() {
        let yaml = r#"
topic:
  name: "dlq"
  num_partitions: 3
  replication_factor: 1
dlq_unknown_key: "unknown"
publish_timeout_sec: 5
max_inflight_sends: 0
"#;
        assert!(
            config::Config::builder()
                .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<crate::configuration::DLQConfig>()
                .is_err()
        );
    }

    #[test]
    fn deserialize_rejects_zero_persistence_intervals() {
        let yaml = r#"
db_host: "http://localhost"
db_port: 8181
db_password: "token"
db_name: "chrono_db"
fast_flush_max_combined_batch_size: 3000
fast_flush_interval_ms: 0
fast_flush_sensor_ids: []
flush_max_combined_batch_size: 7000
flush_interval_ms: 4000
max_inflight_db_writes: 7
write_retry_initial_backoff_ms: 200
write_retry_max_backoff_ms: 10000
export:
  compression_level: 6
  compression_type: ZSTD
  vm_export_dir: "/export/db"
  nested_path_in_bucket: "export/db"
  max_retries: 3
  attempt_timeout_s: 600
  query_window_s: 600
  max_rows_per_window: 1000000
"#;
        assert!(
            config::Config::builder()
                .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<crate::configuration::PersistenceConfig>()
                .is_err()
        );
    }

    #[test]
    fn validate_accepts_zstd_case_insensitively() {
        let mut config = CoreConfig::mock_config();
        for name in ["zstd", "ZSTD", "Zstd"] {
            config.persistence.export.compression_type = CompactString::from(name);
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn validate_rejects_non_zstd_compression() {
        let mut config = CoreConfig::mock_config();
        config.persistence.export.compression_type = CompactString::from("SNAPPY");
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("compression_type"));
    }

    #[test]
    fn validate_rejects_out_of_range_zstd_level() {
        let mut config = CoreConfig::mock_config();
        for invalid in [0, 23] {
            config.persistence.export.compression_level = invalid;
            let err = config.validate().unwrap_err().to_string();
            assert!(err.contains("compression_level"));
        }
        for valid in [1, 22] {
            config.persistence.export.compression_level = valid;
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn mock_config_passes_validation() {
        let config = CoreConfig::mock_config();
        assert!(config.validate().is_ok());
        assert_ne!(
            config.processing.message_channel_buffer_size,
            NonZeroUsize::MIN
        );
        assert_ne!(config.runtime.tokio_workers, NonZeroU16::MIN);
        assert_ne!(config.persistence.flush_interval_ms, NonZeroU32::MIN);
    }
}
