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
        Self {
            client,
            http_client: reqwest::Client::new(),
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
                    self.retry_or_fail(
                        &mut attempts,
                        &mut backoff,
                        format!("HTTP {status}: {body}"),
                    )
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
        // TODO: this might timeout
        let response = self.invoke_export_trigger().await?;

        for failure in &response.failures {
            error!(
                "Database export plugin failed for table '{}': {}",
                failure.table, failure.error
            );
        }

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
            response.tables_exported, response.tables_found, started_at.elapsed()
        );
        Ok(exported_files)
    }
}
