use crate::configuration::{GcpConfig, KafkaConfig};
use crate::gcp::gcp_utils::to_bucket_path;
use compact_str::CompactString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::Command;
use tracing::{error, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum KafkaExporterError {
    #[error("Redpanda CLI not available: {0}")]
    CliUnavailable(#[from] which::Error),
    #[error("Template file not found at: {0}")]
    TemplateNotFound(PathBuf),
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Process failed after {attempts} attempts. Reason: {reason}")]
    ProcessFailed { attempts: u16, reason: String },
}

pub struct KafkaExporter<'a> {
    race_id: &'a CompactString,
    gcp_config: &'a GcpConfig,
    kafka_config: &'a KafkaConfig,
    rendered_template_path: PathBuf,
}

impl<'a> KafkaExporter<'a> {
    const RENDERED_TEMPLATE_FILE: &'static str = "export.yaml";
    const REDPANDA_CLI: &'static str = "rpk";

    pub fn new(
        race_id: &'a CompactString,
        gcp_config: &'a GcpConfig,
        kafka_config: &'a KafkaConfig,
    ) -> Result<Self, KafkaExporterError> {
        Self::ensure_redpanda_available()?;

        let template_dir = &kafka_config.export.redpanda_yaml_dir_path;
        let template_file_name = format!(
            "export.{}.yaml",
            if gcp_config.enabled { "gcs" } else { "local" }
        );

        let raw_template_path = template_dir.join(template_file_name);
        if !raw_template_path.exists() {
            return Err(KafkaExporterError::TemplateNotFound(raw_template_path));
        }

        let rendered_template_path = raw_template_path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(Self::RENDERED_TEMPLATE_FILE);

        let instance = Self {
            race_id,
            gcp_config,
            kafka_config,
            rendered_template_path,
        };

        instance.render_template(&raw_template_path)?;

        Ok(instance)
    }

    fn ensure_redpanda_available() -> Result<(), KafkaExporterError> {
        let path = which::which(Self::REDPANDA_CLI)?;
        info!("Redpanda CLI available at: {}", path.display());
        Ok(())
    }

    fn render_template(&self, raw_template_path: &Path) -> Result<(), KafkaExporterError> {
        let mut content = std::fs::read_to_string(raw_template_path)?;

        let brokers = self
            .kafka_config
            .brokers
            .iter()
            .map(|b| format!("\"{b}\""))
            .collect::<Vec<_>>()
            .join(",");

        let topics = self
            .kafka_config
            .export
            .export_topics
            .iter()
            .map(|t| format!("\"{t}\""))
            .collect::<Vec<_>>()
            .join(",");

        let mut local_path = self.kafka_config.export.local_path.display().to_string();
        if !local_path.ends_with(std::path::MAIN_SEPARATOR) {
            local_path.push(std::path::MAIN_SEPARATOR);
        }

        let bucket_dir = to_bucket_path(
            self.race_id,
            &self.kafka_config.export.nested_path_in_bucket,
        );

        content = content
            .replace("__KAFKA_BROKERS_ARRAY__", &brokers)
            .replace("__KAFKA_TOPICS_ARRAY__", &topics)
            .replace(
                "__KAFKA_CONSUMER_GROUP__",
                &format!("\"{}\"", self.kafka_config.group_id),
            )
            .replace(
                "__EXPORT_BUCKET__",
                &format!("\"{}\"", self.gcp_config.export_bucket),
            )
            .replace("__BUCKET_DIR_PATH__", &format!("\"{bucket_dir}\""))
            .replace("__LOCAL_PATH__", &format!("\"{local_path}\""));

        std::fs::write(&self.rendered_template_path, content)?;

        info!(
            "Kafka exporter template rendered to {}",
            self.rendered_template_path.display()
        );
        Ok(())
    }

    pub async fn export_kafka(&self) -> Result<(), KafkaExporterError> {
        let started_at = Instant::now();
        info!("Starting Kafka export");

        let outcome = self.execute_redpanda_with_retry().await;

        info!(
            "Export process complete. Time taken: {:.2?}",
            started_at.elapsed()
        );
        outcome
    }

    async fn execute_redpanda_with_retry(&self) -> Result<(), KafkaExporterError> {
        let max_retries = self.kafka_config.export.max_retries;
        let mut backoff = Duration::from_secs(1);

        for attempt in 1..=(max_retries + 1) {
            let output = Command::new(Self::REDPANDA_CLI)
                .args(["connect", "run"])
                .arg(&self.rendered_template_path)
                .output()
                .await?;

            info!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
            if !output.stderr.is_empty() {
                warn!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));
            }

            if output.status.success() {
                info!("Redpanda export process successful");
                return Ok(());
            }

            let status_msg = output
                .status
                .code()
                .map(|c| format!("Exit code {c}"))
                .unwrap_or_else(|| "Process killed by signal".to_string());

            if attempt > max_retries {
                error!("Process failed permanently. Last status: {}", status_msg);
                return Err(KafkaExporterError::ProcessFailed {
                    attempts: attempt,
                    reason: status_msg,
                });
            }

            warn!(
                "Process failed (attempt {}/{}). Retrying in {:?}... {}",
                attempt,
                max_retries + 1,
                backoff,
                status_msg
            );

            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }

        unreachable!()
    }
}
