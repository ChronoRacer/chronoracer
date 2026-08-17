use crate::configuration::DatabaseExportConfig;
use anyhow::{Context, Result, anyhow};
use influxdb3_client::{Client, QueryResult, Row};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

pub struct InfluxDbExporter {
    client: Arc<Client>,
    database_export_config: &'static DatabaseExportConfig,
}

impl InfluxDbExporter {
    pub fn new(client: Arc<Client>, export_config: &'static DatabaseExportConfig) -> Self {
        Self {
            client,
            database_export_config: export_config,
        }
    }

    async fn execute_query_with_retry(&self, query: &str) -> Result<QueryResult> {
        let mut attempts = 0;
        let mut backoff = Duration::from_secs(1);
        let max_retries = self.database_export_config.max_retries;
        loop {
            match self.client.sql(query).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    attempts += 1;
                    if attempts > max_retries {
                        return Err(anyhow!(
                            "Failed after {attempts} attempts. Last error: {err:?}"
                        ));
                    }
                    warn!(
                        "Query failed (attempt {}/{}). Retrying in {:?}... Error: {:?}",
                        attempts,
                        max_retries + 1,
                        backoff,
                        err
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
    }

    pub async fn export_database(&self) -> Result<Vec<PathBuf>> {
        tokio::fs::create_dir_all(&self.database_export_config.vm_export_dir)
            .await
            .with_context(|| {
                format!(
                    "Cannot create root export directory '{}'",
                    self.database_export_config.vm_export_dir.display()
                )
            })?;
        let table_names = self.fetch_tables_to_export().await?;

        let total_count = table_names.len();
        info!("Will export {} tables", total_count);

        let export_start = Instant::now();
        let mut successful_exports: Vec<PathBuf> = Vec::with_capacity(total_count);

        for table_name in table_names {
            let export_destination = self.table_name_to_export_path(&table_name);

            if let Err(err) = self
                .ensure_export_destination_valid(&export_destination)
                .await
            {
                error!(
                    "Skipping table '{}': Failed to prepare export destination: {}",
                    table_name, err
                );
                continue;
            }

            let export_table_start = Instant::now();
            info!("Exporting table {}...", table_name);

            match self.export_table(&table_name, &export_destination).await {
                Ok(()) => {
                    info!(
                        "Exported table {} to '{}'. Time taken: {:.2?}",
                        table_name,
                        export_destination.display(),
                        export_table_start.elapsed()
                    );
                    successful_exports.push(export_destination);
                }
                Err(error) => {
                    error!(
                        "Failed to export table {} to '{}': {}",
                        table_name,
                        export_destination.display(),
                        error
                    );
                }
            };
        }

        info!(
            "Exporting finished. Exported {} out of {} tables. Total time taken: {:.2?}",
            successful_exports.len(),
            total_count,
            export_start.elapsed()
        );

        Ok(successful_exports)
    }

    async fn fetch_tables_to_export(&self) -> Result<Vec<String>> {
        const TABLE_NAMES_QUERY: &str = "SELECT table_name \
        FROM information_schema.tables \
        WHERE table_schema = 'public';";

        let result_set = self.execute_query_with_retry(TABLE_NAMES_QUERY).await?;

        let rows: Vec<Row> = result_set.rows().context("Failed to extract rows")?;

        rows.iter()
            .map(|row| {
                row.get("table_name")
                    .map(ToString::to_string)
                    .ok_or_else(|| anyhow!("table name missing"))
            })
            .collect::<Result<Vec<String>, _>>()
    }

    async fn export_table(&self, table_name: &str, export_destination: &Path) -> Result<()> {
        let path_str = export_destination.display().to_string().replace('\\', "/");

        let export_query: String = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET, COMPRESSION {}, COMPRESSION_LEVEL {});",
            table_name,
            path_str,
            self.database_export_config.compression_type.to_uppercase(),
            self.database_export_config.compression_level
        );

        self.execute_query_with_retry(&export_query).await.map(drop)
    }

    fn table_name_to_export_path(&self, table_name: &str) -> PathBuf {
        let mut export_path = self.database_export_config.vm_export_dir.join(table_name);
        export_path.set_extension("parquet");
        export_path
    }

    async fn ensure_export_destination_valid(&self, export_destination: &Path) -> Result<()> {
        if let Some(parent) = export_destination.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::remove_file(export_destination).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
