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
        let config = self.database_export_config;
        let export_dir = &config.vm_export_dir;
        let quoted_table = quote_identifier(table);
        let max_window_ns = i64::from(config.query_window_s.get()).saturating_mul(NANOS_PER_SECOND);

        let files = self.persisted_files(table).await?;
        let windows = if files.is_empty() {
            let Some((min_time, max_time)) = self.scanned_time_bounds(&quoted_table).await? else {
                info!("Table '{table}' has no rows; no file written");
                return Ok(None);
            };
            time_windows(min_time, max_time, max_window_ns)
                .map(|(start, end)| Window::between(start, end))
                .collect()
        } else {
            planned_windows(
                &files,
                max_window_ns,
                u64::from(config.max_rows_per_window.get()),
            )
        };

        let partial = partial_path(export_dir, table);
        let destination = export_dir.join(format!("{table}.parquet"));

        let mut writer: Option<AsyncArrowWriter<File>> = None;
        let mut rows: usize = 0;
        for window in &windows {
            let mut request = self.client.sql(window.query(&quoted_table));
            if let Some(start) = window.start {
                request = request.param("start", start);
            }
            if let Some(end) = window.end {
                request = request.param("end", end);
            }
            let mut stream = request
                .stream()
                .await
                .with_context(|| format!("Failed to open window {window}"))?;
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
                    .with_context(|| format!("Failed to write window {window}"))?;
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
            "Exported table '{table}': {rows} rows in {} windows over {} Parquet files. Time taken: {:.2?}",
            windows.len(),
            files.len(),
            started_at.elapsed()
        );
        Ok(Some(destination))
    }

    /// Metadata of the table's persisted Parquet files; reading it scans no rows.
    async fn persisted_files(&self, table: &str) -> Result<Vec<FileStats>> {
        let query = format!(
            "SELECT CAST(min_time AS BIGINT) AS min_time, CAST(max_time AS BIGINT) AS max_time, \
             CAST(row_count AS BIGINT) AS row_count \
             FROM system.parquet_files WHERE table_name = {}",
            quote_literal(table)
        );
        collect_rows(self.client.sql(query))
            .await
            .context("Failed to read persisted file metadata")?
            .iter()
            .map(|row| {
                let column = |name: &str| {
                    row.get(name)
                        .and_then(Value::as_i64)
                        .ok_or_else(|| anyhow!("Parquet file metadata without {name}"))
                };
                Ok(FileStats {
                    min_time: column("min_time")?,
                    max_time: column("max_time")?,
                    row_count: u64::try_from(column("row_count")?).unwrap_or(0),
                })
            })
            .collect()
    }

    /// Without persisted files every row is in the write buffer, so this scan stays small.
    async fn scanned_time_bounds(&self, quoted_table: &str) -> Result<Option<(i64, i64)>> {
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

/// One persisted Parquet file: inclusive time range and its rows before deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStats {
    min_time: i64,
    max_time: i64,
    row_count: u64,
}

/// Half-open `[start, end)` export window; `None` leaves that side unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    start: Option<i64>,
    end: Option<i64>,
}

impl Window {
    fn between(start: i64, end: i64) -> Self {
        Self {
            start: Some(start),
            end: Some(end),
        }
    }

    fn query(&self, quoted_table: &str) -> String {
        // Positive JSON params arrive as UInt64, which to_timestamp_nanos rejects.
        let predicate = match (self.start, self.end) {
            (Some(_), Some(_)) => {
                "WHERE time >= to_timestamp_nanos(CAST($start AS BIGINT)) \
                 AND time < to_timestamp_nanos(CAST($end AS BIGINT))"
            }
            (Some(_), None) => "WHERE time >= to_timestamp_nanos(CAST($start AS BIGINT))",
            (None, Some(_)) => "WHERE time < to_timestamp_nanos(CAST($end AS BIGINT))",
            (None, None) => "",
        };
        format!("SELECT * FROM {quoted_table} {predicate} ORDER BY time")
    }
}

impl std::fmt::Display for Window {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.start {
            Some(start) => write!(f, "[{start}, ")?,
            None => f.write_str("(-inf, ")?,
        }
        match self.end {
            Some(end) => write!(f, "{end})"),
            None => f.write_str("+inf)"),
        }
    }
}

/// Shortest window [`planned_windows`] splits down to, whatever its estimated rows.
const MIN_WINDOW_NS: i64 = NANOS_PER_SECOND;

/// Windows covering every timestamp, each at most `max_window_ns` long and, down to
/// [`MIN_WINDOW_NS`], estimated at most `max_rows` rows. The windows before the first and after the
/// last file overlap no file, so they read only rows still in the write buffer.
fn planned_windows(files: &[FileStats], max_window_ns: i64, max_rows: u64) -> Vec<Window> {
    let (Some(first), Some(last)) = (
        files.iter().map(|file| file.min_time).min(),
        files.iter().map(|file| file.max_time).max(),
    ) else {
        return vec![Window {
            start: None,
            end: None,
        }];
    };
    let mut windows = vec![Window {
        start: None,
        end: Some(first),
    }];
    for (start, end) in time_windows(first, last, max_window_ns) {
        split_window(files, start, end, max_rows, &mut windows);
    }
    if let Some(after_last) = last.checked_add(1) {
        windows.push(Window {
            start: Some(after_last),
            end: None,
        });
    }
    windows
}

#[allow(clippy::cast_precision_loss)]
fn split_window(files: &[FileStats], start: i64, end: i64, max_rows: u64, out: &mut Vec<Window>) {
    let width = end - start;
    if width <= MIN_WINDOW_NS || estimated_rows(files, start, end) <= max_rows as f64 {
        out.push(Window::between(start, end));
        return;
    }
    let middle = start + width / 2;
    split_window(files, start, middle, max_rows, out);
    split_window(files, middle, end, max_rows, out);
}

/// Rows of `files` inside `[start, end)`, spreading each file's rows evenly over its time range.
/// Duplicates across overlapping files count once per file, so this errs high.
#[allow(clippy::cast_precision_loss)]
fn estimated_rows(files: &[FileStats], start: i64, end: i64) -> f64 {
    files
        .iter()
        .map(|file| {
            let file_end = i128::from(file.max_time) + 1;
            let file_start = i128::from(file.min_time);
            let overlap = i128::from(end).min(file_end) - i128::from(start).max(file_start);
            if overlap <= 0 {
                return 0.0;
            }
            file.row_count as f64 * overlap as f64 / (file_end - file_start) as f64
        })
        .sum()
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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

    const S: i64 = NANOS_PER_SECOND;

    fn file(min_time: i64, max_time: i64, row_count: u64) -> FileStats {
        FileStats {
            min_time,
            max_time,
            row_count,
        }
    }

    fn bounded(windows: &[Window]) -> Vec<(i64, i64)> {
        windows
            .iter()
            .filter_map(|window| window.start.zip(window.end))
            .collect()
    }

    /// Every timestamp falls into exactly one window.
    fn assert_covers_everything(windows: &[Window]) {
        assert_eq!(windows.first().unwrap().start, None);
        assert_eq!(windows.last().unwrap().end, None);
        for pair in windows.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
    }

    #[test]
    fn planned_windows_add_buffer_windows_around_the_files() {
        let windows = planned_windows(&[file(100 * S, 200 * S - 1, 10)], 600 * S, 1_000);

        assert_covers_everything(&windows);
        assert_eq!(windows[0].end, Some(100 * S));
        assert_eq!(bounded(&windows), vec![(100 * S, 200 * S)]);
        assert_eq!(windows.last().unwrap().start, Some(200 * S));
    }

    #[test]
    fn planned_windows_split_dense_overlapping_files_to_the_row_budget() {
        // 30 overlapping files of 100k rows each over the same 600 s: 3M rows.
        let files: Vec<_> = (0..30).map(|_| file(0, 600 * S - 1, 100_000)).collect();

        let windows = planned_windows(&files, 600 * S, 1_000_000);

        assert_covers_everything(&windows);
        let bounded = bounded(&windows);
        assert_eq!(bounded.len(), 4);
        for (start, end) in bounded {
            assert!(estimated_rows(&files, start, end) <= 1_000_000.0);
        }
    }

    #[test]
    fn planned_windows_keep_sparse_gaps_as_whole_windows() {
        let files = [file(0, 10 * S, 5_000_000), file(3_000 * S, 3_010 * S, 10)];

        let windows = planned_windows(&files, 600 * S, 1_000_000);

        assert_covers_everything(&windows);
        let bounded = bounded(&windows);
        assert!(bounded.contains(&(600 * S, 1_200 * S)));
        assert!(
            bounded
                .iter()
                .filter(|(_, end)| *end <= 10 * S)
                .all(|(start, end)| end - start >= MIN_WINDOW_NS / 2)
        );
    }

    #[test]
    fn planned_windows_stop_splitting_at_the_minimum_width() {
        let windows = planned_windows(&[file(0, S - 1, 10_000_000)], 600 * S, 1);

        assert_eq!(bounded(&windows), vec![(0, S)]);
    }

    #[test]
    fn planned_windows_cover_a_single_point_file() {
        let windows = planned_windows(&[file(5, 5, 1)], 600 * S, 1_000);

        assert_covers_everything(&windows);
        assert_eq!(bounded(&windows), vec![(5, 6)]);
    }

    #[test]
    fn planned_windows_omit_the_tail_at_i64_max() {
        let windows = planned_windows(&[file(i64::MAX - 5, i64::MAX, 6)], 600 * S, 1_000);

        assert_eq!(windows.last().unwrap().end, Some(i64::MAX));
        assert_eq!(windows[0].end, Some(i64::MAX - 5));
    }

    #[test]
    fn estimated_rows_spread_each_file_evenly() {
        let files = [file(0, 99, 100), file(50, 149, 100)];

        assert!((estimated_rows(&files, 0, 50) - 50.0).abs() < 1e-9);
        assert!((estimated_rows(&files, 50, 100) - 100.0).abs() < 1e-9);
        assert!(estimated_rows(&files, 150, 200).abs() < 1e-9);
    }

    #[test]
    fn window_query_binds_only_present_bounds() {
        let lead = Window {
            start: None,
            end: Some(1),
        };
        let tail = Window {
            start: Some(1),
            end: None,
        };
        assert!(!lead.query("\"t\"").contains("$start"));
        assert!(lead.query("\"t\"").contains("$end"));
        assert!(!tail.query("\"t\"").contains("$end"));
        assert!(Window::between(0, 1).query("\"t\"").contains("$start"));
        assert_eq!(lead.to_string(), "(-inf, 1)");
        assert_eq!(Window::between(0, 1).to_string(), "[0, 1)");
    }

    #[test]
    fn quote_literal_escapes_single_quotes() {
        assert_eq!(quote_literal("speed"), "'speed'");
        assert_eq!(quote_literal("a'b"), "'a''b'");
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

    /// Exports every table of a running `InfluxDB` and prints each file's row count. Env:
    /// `CHRONORACER_TEST_INFLUX_PORT`, `CHRONORACER_TEST_INFLUX_TOKEN`, optional
    /// `CHRONORACER_TEST_INFLUX_DB` (default `chrono_db`) and `CHRONORACER_TEST_MAX_ROWS_PER_WINDOW`.
    #[tokio::test]
    #[ignore = "needs a local InfluxDB with data"]
    async fn export_against_local_influx() {
        use crate::configuration::PersistenceConfig;
        use crate::persistence::persistence_entrypoint::PersistenceLayerEntryPoint;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let var = |name: &str| std::env::var(name).ok();
        let required = |name: &str| var(name).unwrap_or_else(|| panic!("{name} not set"));
        let dir =
            std::env::temp_dir().join(format!("chronoracer-export-it-{}", std::process::id()));
        let mut export = DatabaseExportConfig {
            vm_export_dir: dir.clone(),
            ..DatabaseExportConfig::mock_config()
        };
        if let Some(rows) = var("CHRONORACER_TEST_MAX_ROWS_PER_WINDOW") {
            export.max_rows_per_window = rows.parse().unwrap();
        }
        let export: &'static DatabaseExportConfig = Box::leak(Box::new(export));
        let persistence = PersistenceConfig {
            db_host: "http://localhost".to_string(),
            db_port: required("CHRONORACER_TEST_INFLUX_PORT").parse().unwrap(),
            db_password: required("CHRONORACER_TEST_INFLUX_TOKEN"),
            db_name: var("CHRONORACER_TEST_INFLUX_DB").unwrap_or_else(|| "chrono_db".to_string()),
            ..PersistenceConfig::mock_config()
        };
        let client =
            Client::new(PersistenceLayerEntryPoint::influx_client_config(&persistence).unwrap())
                .await
                .unwrap();

        let files = InfluxDbExporter::new(Arc::new(client), export)
            .unwrap()
            .export_database()
            .await
            .unwrap();

        assert!(!files.is_empty());
        for path in &files {
            let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
            println!(
                "{} rows={}",
                path.display(),
                reader.metadata().file_metadata().num_rows()
            );
        }
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
