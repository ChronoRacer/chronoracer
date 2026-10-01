use crate::configuration::DatabaseExportConfig;
use anyhow::{Context, Result, anyhow};
use influxdb3_client::Client;
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

const EXPORT_TRIGGER_PATH: &str = "/api/v3/engine/export_database";

#[derive(Debug, Deserialize)]
struct ExportPluginResponse {
    tables_found: usize,
    tables_exported: usize,
    files: Vec<String>,
    failures: Vec<ExportFailure>,
}

#[derive(Debug, Deserialize)]
struct ExportFailure {
    table: String,
    error: String,
}

pub struct InfluxDbExporter {
    client: Arc<Client>,
    http_client: reqwest::Client,
    database_export_config: &'static DatabaseExportConfig,
}

impl InfluxDbExporter {
    pub fn new(client: Arc<Client>, export_config: &'static DatabaseExportConfig) -> Self {
        let timeout = Duration::from_secs(u64::from(export_config.attempt_timeout_s.get()));
        let http_client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("reqwest client with timeout should build");
        Self {
            client,
            http_client,
            database_export_config: export_config,
        }
    }

    async fn invoke_export_trigger(&self) -> Result<ExportPluginResponse> {
        let url = format!("{}{}", self.client.config().host_url(), EXPORT_TRIGGER_PATH);
        let authorization = self
            .client
            .config()
            .authorization_header()
            .context("Failed to create InfluxDB authorization header")?;

        let mut attempts = 0;
        let mut backoff = Duration::from_secs(1);
        loop {
            let mut request = self.http_client.post(&url);
            if let Some(header) = authorization.clone() {
                request = request.header(AUTHORIZATION, header);
            }
            request = request.json(&serde_json::json!({
                "export_dir": self.database_export_config.vm_export_dir,
                "compression": self.database_export_config.compression_type,
                "compression_level": self.database_export_config.compression_level,
            }));

            match request.send().await {
                Ok(response) if response.status().is_success() => {
                    let body = response
                        .text()
                        .await
                        .context("Failed to read database export plugin response")?;
                    return serde_json::from_str(&body)
                        .context("Invalid database export plugin response");
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    let error = format!("HTTP {status}: {body}");
                    if status.is_client_error() {
                        return Err(anyhow!(
                            "Database export trigger rejected with non-retryable status: {error}"
                        ));
                    }
                    self.retry_or_fail(&mut attempts, &mut backoff, error)
                        .await?;
                }
                Err(error) => {
                    self.retry_or_fail(&mut attempts, &mut backoff, error.to_string())
                        .await?;
                }
            }
        }
    }

    async fn retry_or_fail(
        &self,
        attempts: &mut u16,
        backoff: &mut Duration,
        error: String,
    ) -> Result<()> {
        *attempts += 1;
        if *attempts > self.database_export_config.max_retries {
            return Err(anyhow!(
                "Database export trigger failed after {} attempts: {}",
                *attempts,
                error
            ));
        }

        warn!(
            "Database export trigger failed (attempt {}/{}). Retrying in {:?}: {}",
            *attempts,
            self.database_export_config.max_retries + 1,
            *backoff,
            error
        );
        tokio::time::sleep(*backoff).await;
        *backoff *= 2;
        Ok(())
    }

    pub async fn export_database(&self) -> Result<Vec<PathBuf>> {
        let started_at = Instant::now();
        info!("Starting database export");
        let response = self.invoke_export_trigger().await?;

        for failure in &response.failures {
            error!(
                "Database export plugin failed for table '{}': {}",
                failure.table, failure.error
            );
        }

        evaluate_export_plugin_response(
            response.tables_found,
            response.tables_exported,
            &response.failures,
        )?;

        let exported_files = response
            .files
            .iter()
            .map(|file| {
                Path::new(file)
                    .file_name()
                    .map(|name| self.database_export_config.vm_export_dir.join(name))
                    .ok_or_else(|| anyhow!("Invalid export path returned by plugin: '{file}'"))
            })
            .collect::<Result<Vec<_>>>()?;

        info!(
            "Database export finished. Exported {} out of {} tables. Time taken: {:.2?}",
            response.tables_exported,
            response.tables_found,
            started_at.elapsed()
        );
        Ok(exported_files)
    }
}

/// Returns an error when the plugin reported table failures or an incomplete export count.
fn evaluate_export_plugin_response(
    tables_found: usize,
    tables_exported: usize,
    failures: &[ExportFailure],
) -> Result<()> {
    if !failures.is_empty() {
        return Err(anyhow!(
            "Database export plugin reported {} table failure(s)",
            failures.len()
        ));
    }
    if tables_exported < tables_found {
        return Err(anyhow!(
            "Database export incomplete: exported {tables_exported} of {tables_found} tables"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_accepts_complete_export_without_failures() {
        assert!(evaluate_export_plugin_response(3, 3, &[]).is_ok());
        assert!(evaluate_export_plugin_response(0, 0, &[]).is_ok());
    }

    #[test]
    fn evaluate_rejects_non_empty_failures() {
        let failures = [ExportFailure {
            table: "sensor_a".to_string(),
            error: "disk full".to_string(),
        }];
        let err = evaluate_export_plugin_response(2, 2, &failures)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 table failure"));
    }

    #[test]
    fn evaluate_rejects_incomplete_table_count() {
        let err = evaluate_export_plugin_response(5, 4, &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("exported 4 of 5"));
    }

    #[test]
    fn evaluate_prefers_failure_list_over_count_mismatch() {
        let failures = [ExportFailure {
            table: "sensor_b".to_string(),
            error: "timeout".to_string(),
        }];
        let err = evaluate_export_plugin_response(3, 1, &failures)
            .unwrap_err()
            .to_string();
        assert!(err.contains("table failure"));
    }
}
