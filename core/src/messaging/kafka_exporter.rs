use crate::configuration::{GcpConfig, KafkaConfig};
use crate::gcp::gcp_utils::to_bucket_path;
use anyhow::{Result, anyhow};
use compact_str::CompactString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::Command;
use tracing::{error, info, warn};

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
    ) -> Result<Self> {
        Self::ensure_redpanda_available()?;

        let template_dir = &kafka_config.export.redpanda_yaml_dir_path;
        let template_file_name = format!(
            "export.{}.yaml",
            if gcp_config.enabled { "gcs" } else { "local" }
        );

        let raw_template_path = template_dir.join(template_file_name);
        if !raw_template_path.exists() {
            return Err(anyhow!(
                "Template file not found at {}",
                raw_template_path.display()
            ));
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

    fn ensure_redpanda_available() -> Result<()> {
        let path = which::which(Self::REDPANDA_CLI)?;
        info!("Redpanda CLI available at: {}", path.display());
        Ok(())
    }

    fn render_template(&self, raw_template_path: &Path) -> Result<()> {
        let to_quoted = |s: &String| format!("\"{s}\"");
        let to_array = |s: &String| format!("[{s}]");
        let mut content = std::fs::read_to_string(raw_template_path)?;

        let brokers = to_array(
            &self
                .kafka_config
                .brokers
                .iter()
                .map(to_quoted)
                .collect::<Vec<_>>()
                .join(","),
        );

        let topics = to_array(
            &self
                .kafka_config
                .export
                .export_topics
                .iter()
                .map(to_quoted)
                .collect::<Vec<_>>()
                .join(","),
        );

        let mut local_path = self.kafka_config.export.local_path.display().to_string();
        if !local_path.ends_with(std::path::MAIN_SEPARATOR) {
            local_path.push(std::path::MAIN_SEPARATOR);
        }

        let bucket_dir = to_bucket_path(
            self.race_id,
            &self.kafka_config.export.nested_path_in_bucket,
        );

        content = content
            .replace("__RACE_ID__", self.race_id)
            .replace("__KAFKA_BROKERS_ARRAY__", &brokers)
            .replace("__KAFKA_TOPICS_ARRAY__", &topics)
            .replace(
                "__EXPORT_BUCKET__",
                &to_quoted(&self.gcp_config.export_bucket),
            )
            .replace("__BUCKET_DIR_PATH__", &bucket_dir)
            .replace("__LOCAL_PATH__", &local_path);

        std::fs::write(&self.rendered_template_path, content)?;

        info!(
            "Kafka exporter template rendered to {}",
            self.rendered_template_path.display()
        );
        Ok(())
    }

    pub async fn export_kafka(&self) -> Result<()> {
        let started_at = Instant::now();
        info!("Starting Kafka export");

        let outcome = self.execute_redpanda_with_retry().await;

        info!(
            "Export process complete. Time taken: {:.2?}",
            started_at.elapsed()
        );
        outcome
    }

    async fn execute_redpanda_with_retry(&self) -> Result<()> {
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

            let status_msg = output.status.code().map_or_else(
                || "Process killed by signal".to_string(),
                |c| format!("Exit code {c}"),
            );

            if attempt > max_retries {
                error!("Process failed permanently. Last status: {}", status_msg);
                return Err(anyhow!(
                    "Process failed after {attempt} attempts. Reason: {status_msg}"
                ));
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

        Err(anyhow!("Retry count overflowed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{GcpConfig, KafkaConfig};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn render_template_replaces_export_placeholders() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("chronoracer-export-{unique}"));
        std::fs::create_dir_all(&directory).expect("temporary directory should be created");
        let raw_path = directory.join("template.yaml");
        let rendered_path = directory.join(KafkaExporter::RENDERED_TEMPLATE_FILE);
        std::fs::write(
            &raw_path,
            "brokers: __KAFKA_BROKERS_ARRAY__\ntopics: __KAFKA_TOPICS_ARRAY__\ngroup: __RACE_ID__\nbucket: __EXPORT_BUCKET__\ndir: __BUCKET_DIR_PATH__\nlocal: __LOCAL_PATH__",
        )
            .expect("template should be written");

        let race_id = CompactString::from("race-123");
        let gcp_config = GcpConfig {
            enabled: true,
            project_id: "project".to_string(),
            pubsub_topic_id: "topic".to_string(),
            export_bucket: "bucket".to_string(),
            max_retries: 1,
            max_gcs_concurrent_uploads: 1,
            allow_unknown_gce_metrics: false,
        };
        let mut kafka_config = KafkaConfig::mock_config();
        kafka_config.brokers = vec!["broker-a:9092".to_string(), "broker-b:9092".to_string()];
        kafka_config.export.export_topics = vec!["data".to_string(), "dlq".to_string()];
        kafka_config.export.nested_path_in_bucket = "/exports/".to_string();
        kafka_config.export.local_path = PathBuf::from("exports");
        let exporter = KafkaExporter {
            race_id: &race_id,
            gcp_config: &gcp_config,
            kafka_config: &kafka_config,
            rendered_template_path: rendered_path.clone(),
        };

        exporter
            .render_template(&raw_path)
            .expect("template should render");
        let rendered = std::fs::read_to_string(&rendered_path).expect("rendered file should exist");

        assert!(rendered.contains(r#"brokers: ["broker-a:9092","broker-b:9092"]"#));
        assert!(rendered.contains(r#"topics: ["data","dlq"]"#));
        assert!(rendered.contains("group: race-123"));
        assert!(rendered.contains(r#"bucket: "bucket""#));
        assert!(rendered.contains("dir: race-123/exports"));

        let mut local_export_path = "exports".to_string();
        local_export_path.push(std::path::MAIN_SEPARATOR);
        assert!(rendered.contains(&format!("local: {local_export_path}")));

        std::fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }
}
