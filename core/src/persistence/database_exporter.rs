use crate::configuration::DatabaseExportConfig;
use anyhow::{Context, Result, anyhow};
use futures_util::TryStreamExt;
use influxdb3_client::{Client, QueryRequest, QueryResult, Row, Value};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tracing::{error, info, warn};

const TABLES_QUERY: &str = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY table_name";
const PARTIAL_EXTENSION: &str = "partial";
const NANOS_PER_SECOND: i64 = 1_000_000_000;

#[derive(Debug)]
struct ExportFailure {
    table: String,
    error: String,
}

pub struct InfluxDbExporter {
    client: Arc<Client>,
    database_export_config: &'static DatabaseExportConfig,
    compression: Compression,
}

impl InfluxDbExporter {
    /// Fails on an export compression setting that config validation would also reject.
    pub fn new(client: Arc<Client>, export_config: &'static DatabaseExportConfig) -> Result<Self> {
        Ok(Self {
            client,
            database_export_config: export_config,
            compression: export_config.parquet_compression()?,
        })
    }

    /// Exports every table to `{vm_export_dir}/{table}.parquet` and returns the written files.
    pub async fn export_database(&self) -> Result<Vec<PathBuf>> {
        let started_at = Instant::now();
        let export_dir = &self.database_export_config.vm_export_dir;
        info!("Starting database export into {}", export_dir.display());

        tokio::fs::create_dir_all(export_dir)
            .await
            .with_context(|| format!("Failed to create export dir {}", export_dir.display()))?;
        remove_partial_files(export_dir).await?;

        let tables = self.list_tables().await?;
        let mut exported_files = Vec::with_capacity(tables.len());
        let mut tables_exported = 0;
        let mut failures = Vec::new();

        for table in &tables {
            match self.export_table_with_retries(table).await {
                Ok(file) => {
                    tables_exported += 1;
                    exported_files.extend(file);
                }
                Err(error) => {
                    error!("Database export failed for table '{table}': {error:#}");
                    failures.push(ExportFailure {
                        table: table.clone(),
                        error: error.root_cause().to_string(),
                    });
                }
            }
        }

        evaluate_export_report(tables.len(), tables_exported, &failures)?;

        info!(
            "Database export finished. Exported {} out of {} tables. Time taken: {:.2?}",
            tables_exported,
            tables.len(),
            started_at.elapsed()
        );
        Ok(exported_files)
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        collect_rows(self.client.sql(TABLES_QUERY))
            .await
            .context("Failed to list InfluxDB tables")?
            .iter()
            .map(|row| {
                row.get("table_name")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("Table listing returned a row without table_name"))
            })
            .collect()
    }

    async fn export_table_with_retries(&self, table: &str) -> Result<Option<PathBuf>> {
        let config = self.database_export_config;
        let attempt_timeout = Duration::from_secs(u64::from(config.attempt_timeout_s.get()));
        let partial = partial_path(&config.vm_export_dir, table);
        let mut attempts: u16 = 0;
        let mut backoff = Duration::from_secs(1);
        loop {
            let error = match tokio::time::timeout(attempt_timeout, self.export_table(table)).await
            {
                Ok(Ok(file)) => return Ok(file),
                Ok(Err(error)) => error,
                Err(_) => anyhow!("Table export timed out after {attempt_timeout:?}"),
            };
            remove_if_exists(&partial).await?;

            attempts += 1;
            if attempts > config.max_retries {
                return Err(error.context(format!("Failed after {attempts} attempts")));
            }
            warn!(
                "Export of table '{table}' failed (attempt {attempts}/{}). Retrying in {backoff:?}: {error:#}",
                config.max_retries + 1
            );
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }

    /// Streams the table window by window into `{table}.parquet.partial` and renames it on
    /// success. Returns `None` for a table without rows.
    async fn export_table(&self, table: &str) -> Result<Option<PathBuf>> {
        let started_at = Instant::now();
        let export_dir = &self.database_export_config.vm_export_dir;
        let quoted_table = quote_identifier(table);

        let Some((min_time, max_time)) = self.time_bounds(&quoted_table).await? else {
            info!("Table '{table}' has no rows; no file written");
            return Ok(None);
        };

        let partial = partial_path(export_dir, table);
        let destination = export_dir.join(format!("{table}.parquet"));
        // Positive JSON params arrive as UInt64, which to_timestamp_nanos rejects.
        let window_query = format!(
            "SELECT * FROM {quoted_table} \
             WHERE time >= to_timestamp_nanos(CAST($start AS BIGINT)) \
             AND time < to_timestamp_nanos(CAST($end AS BIGINT)) \
             ORDER BY time"
        );
        let window_ns = i64::from(self.database_export_config.query_window_s.get())
            .saturating_mul(NANOS_PER_SECOND);

        let mut writer: Option<AsyncArrowWriter<File>> = None;
        let mut rows: usize = 0;
        let mut windows: usize = 0;
        for (start, end) in time_windows(min_time, max_time, window_ns) {
            windows += 1;
            let mut stream = self
                .client
                .sql(window_query.as_str())
                .param("start", start)
                .param("end", end)
                .stream()
                .await
                .with_context(|| format!("Failed to open window [{start}, {end})"))?;
            while let Some(batch) = stream.try_next().await? {
                if batch.num_rows() == 0 {
                    continue;
                }
                let active = if let Some(active) = writer.as_mut() {
                    active
                } else {
                    let file = File::create(&partial)
                        .await
                        .with_context(|| format!("Failed to create {}", partial.display()))?;
                    let properties = WriterProperties::builder()
                        .set_compression(self.compression)
                        .build();
                    writer.insert(AsyncArrowWriter::try_new(
                        file,
                        batch.schema(),
                        Some(properties),
                    )?)
                };
                active
                    .write(&batch)
                    .await
                    .with_context(|| format!("Failed to write window [{start}, {end})"))?;
                rows += batch.num_rows();
            }
        }

        let Some(writer) = writer else {
            info!("Table '{table}' returned no rows; no file written");
            return Ok(None);
        };
        writer
            .close()
            .await
            .context("Failed to finish Parquet file")?;
        tokio::fs::rename(&partial, &destination)
            .await
            .with_context(|| format!("Failed to rename to {}", destination.display()))?;

        info!(
            "Exported table '{table}': {rows} rows in {windows} windows. Time taken: {:.2?}",
            started_at.elapsed()
        );
        Ok(Some(destination))
    }

    async fn time_bounds(&self, quoted_table: &str) -> Result<Option<(i64, i64)>> {
        let rows = collect_rows(self.client.sql(format!(
            "SELECT min(time) AS min_time, max(time) AS max_time FROM {quoted_table}"
        )))
        .await
        .context("Failed to read table time bounds")?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let min_time = row.get("min_time").and_then(Value::as_i64);
        let max_time = row.get("max_time").and_then(Value::as_i64);
        Ok(min_time.zip(max_time))
    }
}

async fn collect_rows(query: QueryRequest<'_>) -> Result<Vec<Row>> {
    let batches: Vec<_> = query.stream().await?.try_collect().await?;
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    Ok(QueryResult::new(first.schema(), batches).rows()?)
}

/// Half-open `[start, end)` windows covering `[min_time, max_time]` without gaps or overlap.
fn time_windows(min_time: i64, max_time: i64, window_ns: i64) -> impl Iterator<Item = (i64, i64)> {
    let last_end = max_time.saturating_add(1);
    let window_ns = window_ns.max(1);
    let mut next_start = (min_time <= max_time).then_some(min_time);
    std::iter::from_fn(move || {
        let start = next_start?;
        let end = start.saturating_add(window_ns).min(last_end);
        next_start = (end < last_end).then_some(end);
        Some((start, end))
    })
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn partial_path(export_dir: &Path, table: &str) -> PathBuf {
    export_dir.join(format!("{table}.parquet.{PARTIAL_EXTENSION}"))
}

async fn remove_if_exists(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("Failed to remove {}", path.display())),
    }
}

/// Leftovers of an export that crashed or timed out before its rename.
async fn remove_partial_files(export_dir: &Path) -> Result<()> {
    let mut entries = tokio::fs::read_dir(export_dir)
        .await
        .with_context(|| format!("Failed to read export dir {}", export_dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == PARTIAL_EXTENSION) {
            warn!("Removing stale partial export {}", path.display());
            remove_if_exists(&path).await?;
        }
    }
    Ok(())
}

/// Returns an error when any table failed or the exported count is incomplete.
fn evaluate_export_report(
    tables_found: usize,
    tables_exported: usize,
    failures: &[ExportFailure],
) -> Result<()> {
    if !failures.is_empty() {
        let details = failures
            .iter()
            .map(|failure| format!("{} ({})", failure.table, failure.error))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(anyhow!(
            "Database export reported {} table failure(s): {details}",
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

    fn windows(min: i64, max: i64, step: i64) -> Vec<(i64, i64)> {
        time_windows(min, max, step).collect()
    }

    #[test]
    fn windows_single_point_table_covers_that_point() {
        assert_eq!(windows(5, 5, 10), vec![(5, 6)]);
    }

    #[test]
    fn windows_include_max_time_in_last_window() {
        assert_eq!(windows(0, 20, 10), vec![(0, 10), (10, 20), (20, 21)]);
        assert_eq!(windows(0, 19, 10), vec![(0, 10), (10, 20)]);
    }

    #[test]
    fn windows_are_contiguous_without_overlap() {
        let all = windows(-1_000, 123_456, 997);
        assert_eq!(all.first().unwrap().0, -1_000);
        assert_eq!(all.last().unwrap().1, 123_457);
        for pair in all.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
            assert!(pair[0].0 < pair[0].1);
        }
    }

    #[test]
    fn windows_empty_when_bounds_are_inverted() {
        assert_eq!(windows(10, 9, 5), Vec::new());
    }

    #[test]
    fn windows_saturate_at_i64_max() {
        let all = windows(i64::MAX - 5, i64::MAX, 4);
        assert_eq!(
            all,
            vec![(i64::MAX - 5, i64::MAX - 1), (i64::MAX - 1, i64::MAX)]
        );
    }

    #[test]
    fn windows_treat_non_positive_step_as_one_nanosecond() {
        assert_eq!(windows(0, 1, 0), vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn quote_identifier_escapes_double_quotes() {
        assert_eq!(quote_identifier("speed"), "\"speed\"");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[tokio::test]
    async fn remove_partial_files_keeps_finished_exports() {
        let dir = std::env::temp_dir().join(format!(
            "chronoracer-export-test-{}-{}",
            std::process::id(),
            line!()
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let finished = dir.join("speed.parquet");
        let partial = partial_path(&dir, "rpm");
        tokio::fs::write(&finished, b"done").await.unwrap();
        tokio::fs::write(&partial, b"half").await.unwrap();

        remove_partial_files(&dir).await.unwrap();

        assert!(finished.exists());
        assert!(!partial.exists());
        remove_if_exists(&partial).await.unwrap();
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[test]
    fn evaluate_accepts_complete_export_without_failures() {
        assert!(evaluate_export_report(3, 3, &[]).is_ok());
        assert!(evaluate_export_report(0, 0, &[]).is_ok());
    }

    #[test]
    fn evaluate_rejects_non_empty_failures() {
        let failures = [ExportFailure {
            table: "sensor_a".to_string(),
            error: "disk full".to_string(),
        }];
        let err = evaluate_export_report(2, 2, &failures)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 table failure"));
        assert!(err.contains("sensor_a (disk full)"));
    }

    #[test]
    fn evaluate_rejects_incomplete_table_count() {
        let err = evaluate_export_report(5, 4, &[]).unwrap_err().to_string();
        assert!(err.contains("exported 4 of 5"));
    }

    #[test]
    fn evaluate_prefers_failure_list_over_count_mismatch() {
        let failures = [ExportFailure {
            table: "sensor_b".to_string(),
            error: "timeout".to_string(),
        }];
        let err = evaluate_export_report(3, 1, &failures)
            .unwrap_err()
            .to_string();
        assert!(err.contains("table failure"));
    }
}
