use crate::configuration::PersistenceConfig;
use crate::messaging::offset_ledger::OffsetLedger;
use crate::persistence::sensor_data_batcher::{
    PointBatch, PointBatchRx, SIMPLE_POINT_PROTOCOL_ESTIMATED_LENGTH, SimplePoint,
};
use crate::persistence::write_metrics::WriteMetrics;
use crate::pipeline_stats::PipelineStats;
use compact_str::CompactString;
use influxdb3_client::error::Error as InfluxError;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

/// A write that keeps failing transiently logs on its first retry and then on every Nth retry.
const RETRY_LOG_EVERY: u32 = 10;

#[derive(Debug, Copy, Clone)]
pub enum BatcherWiringType {
    Normal,
    Fast,
}

pub struct BatcherWiring {
    pub batch_rx: PointBatchRx,
    pub pool_tx: tokio::sync::mpsc::Sender<Vec<SimplePoint>>,
    pub wiring_type: BatcherWiringType,
}

/// How a batch ended. A batch is settled for every outcome: nothing will send it again.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Influx accepted every line.
    Written,
    /// Influx accepted the valid lines and permanently rejected the rest.
    PartiallyWritten,
    /// Non-retryable failure; the batch was given up on.
    Dropped,
}

/// The single limit on concurrent database writes. `max_inflight_db_writes` permits in total:
/// a pool only the enabled fast path may use, plus a shared pool. Reserved is a quarter of the total, at
/// least one when more than one permit exists, and never the last permit. One permit is shared.
/// With two or more, normal batches cannot occupy every permit and starve fast flushes.
/// Without a fast path, all permits are shared.
struct WritePermits {
    fast_reserved: Arc<Semaphore>,
    shared: Arc<Semaphore>,
}

/// A quarter of the permits, at least one when more than one exists, never the last shared permit.
fn fast_reserved_permits(max_inflight: usize) -> usize {
    (max_inflight / 4)
        .max(1)
        .min(max_inflight.saturating_sub(1))
}

impl WritePermits {
    fn new(max_inflight: usize, fast_path_enabled: bool) -> Self {
        let reserved = if fast_path_enabled {
            fast_reserved_permits(max_inflight)
        } else {
            0
        };
        Self {
            fast_reserved: Arc::new(Semaphore::new(reserved)),
            shared: Arc::new(Semaphore::new(max_inflight - reserved)),
        }
    }

    /// `None` only if a semaphore was closed, which never happens while the flusher runs.
    async fn acquire(&self, path: BatcherWiringType) -> Option<OwnedSemaphorePermit> {
        match path {
            BatcherWiringType::Normal => Arc::clone(&self.shared).acquire_owned().await.ok(),
            BatcherWiringType::Fast => {
                // Reserved first so the shared pool stays free for normal batches
                tokio::select! {
                    biased;
                    permit = Arc::clone(&self.fast_reserved).acquire_owned() => permit.ok(),
                    permit = Arc::clone(&self.shared).acquire_owned() => permit.ok(),
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct RetryBackoff {
    initial: Duration,
    max: Duration,
}

impl RetryBackoff {
    /// `initial` doubled per retry, capped at `max`.
    fn ceiling(self, retry: u32) -> Duration {
        let factor = 1_u32.checked_shl(retry).unwrap_or(u32::MAX);
        self.initial.saturating_mul(factor).min(self.max)
    }

    /// Uniform in `[ceiling / 2, ceiling]`, so writes failing together do not retry in lock step.
    fn delay(self, retry: u32) -> Duration {
        self.delay_with_jitter(retry, RandomState::new().build_hasher().finish())
    }

    fn delay_with_jitter(self, retry: u32, random: u64) -> Duration {
        let half_ms = u64::try_from(self.ceiling(retry).as_millis()).unwrap_or(u64::MAX) / 2;
        Duration::from_millis(half_ms + random % (half_ms + 1))
    }
}

/// 401/403: token missing, revoked, or rotated. 404: database name is wrong or not created yet.
/// The line protocol itself is fine, so the same batch can succeed once the server is fixed.
fn is_auth_or_config_status(code: u16) -> bool {
    matches!(code, 401 | 403 | 404)
}

/// Whether the same write can succeed later: transport failures (connection, timeout), 429,
/// any 5xx, and auth/config statuses ([`is_auth_or_config_status`]). Payload 4xx and partial
/// writes are deterministic and would be rejected again. Re-sending a batch is safe: line
/// protocol writes are idempotent per series and timestamp.
fn is_retryable(error: &InfluxError) -> bool {
    match error {
        InfluxError::Http(error) => error.is_timeout() || error.is_connect() || error.is_request(),
        InfluxError::Server { code, .. } => {
            is_auth_or_config_status(*code) || *code == 429 || (500..600).contains(code)
        }
        InfluxError::Timeout(_) => true,
        _ => false,
    }
}

fn is_auth_or_config(error: &InfluxError) -> bool {
    matches!(error, InfluxError::Server { code, .. } if is_auth_or_config_status(*code))
}

struct FlushContext {
    client: Arc<influxdb3_client::Client>,
    race_id: Arc<str>,
    permits: WritePermits,
    backoff: RetryBackoff,
    write_metrics: Arc<WriteMetrics>,
    stats: Arc<PipelineStats>,
    offsets: OffsetLedger,
}

impl FlushContext {
    /// Sends one batch, re-sending it after a backoff for as long as the failure is transient.
    /// The caller holds the write permit throughout, so a struggling database slows the whole
    /// pipeline down and the backpressure reaches Kafka instead of dropping data.
    async fn flush_batch(
        &self,
        batch: &[SimplePoint],
        wiring_type: BatcherWiringType,
    ) -> FlushOutcome {
        let mut line_protocol =
            String::with_capacity(batch.len() * SIMPLE_POINT_PROTOCOL_ESTIMATED_LENGTH);
        for point in batch {
            point.write_line_protocol(&mut line_protocol, &self.race_id);
            line_protocol.push('\n');
        }
        let lines = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let bytes = u64::try_from(line_protocol.len()).unwrap_or(u64::MAX);

        let mut retries: u32 = 0;
        loop {
            match self.client.write(line_protocol.as_str()).await {
                Ok(()) => {
                    self.write_metrics.record_success(lines, bytes);
                    if retries > 0 {
                        info!(
                            "Database write recovered after {retries} retries, flush_type={wiring_type:?}"
                        );
                    }
                    return FlushOutcome::Written;
                }
                Err(InfluxError::PartialWrite(error)) => {
                    let failed_lines = u64::try_from(error.line_errors.len()).unwrap_or(u64::MAX);
                    self.write_metrics.record_partial(lines, failed_lines);
                    error!(%error,
                        "Database write partially failed. failed_lines={}, flush_type={wiring_type:?}",
                        failed_lines
                    );
                    for line_error in error.line_errors.iter().take(5) {
                        warn!("Line {}: {}", line_error.line, line_error.message);
                    }
                    return FlushOutcome::PartiallyWritten;
                }
                Err(error) if is_retryable(&error) => {
                    self.write_metrics.record_retry();
                    if retries.is_multiple_of(RETRY_LOG_EVERY) {
                        if is_auth_or_config(&error) {
                            warn!(%error, "Database write failed: auth/config problem, retrying. retries_so_far={retries}, flush_type={wiring_type:?}");
                        } else {
                            warn!(%error, "Database write failed, retrying. retries_so_far={retries}, flush_type={wiring_type:?}");
                        }
                    }
                    tokio::time::sleep(self.backoff.delay(retries)).await;
                    retries = retries.saturating_add(1);
                }
                Err(error) => {
                    self.write_metrics.record_error();
                    error!(%error, "Database write failed, batch dropped. lines={lines}, flush_type={wiring_type:?}");
                    return FlushOutcome::Dropped;
                }
            }
        }
    }
}

impl FlushContext {
    /// Writes the batch, then settles the Kafka messages it was made of. Returns the emptied point
    /// buffer for reuse.
    async fn flush_and_settle(
        &self,
        batch: PointBatch,
        wiring_type: BatcherWiringType,
    ) -> Vec<SimplePoint> {
        let oldest_created = batch.oldest_created();
        let PointBatch {
            mut points,
            offsets,
            ..
        } = batch;
        // Messages without readings have nothing to write.
        if !points.is_empty() {
            // The outcome is counted by `WriteMetrics`; failures are logged inside.
            let _ = self.flush_batch(&points, wiring_type).await;
        }
        // Durable point. Every line of the batch is settled, and so is every message it came
        // from (one message never spans batches); the outcome only says how.
        self.offsets.settle(&offsets);
        self.stats.record_settled(oldest_created);

        points.clear();
        points
    }
}

pub struct InfluxDbFlusher {
    fast_wiring: Option<BatcherWiring>,
    normal_wiring: BatcherWiring,
    context: Arc<FlushContext>,
}

impl InfluxDbFlusher {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        persistence: &PersistenceConfig,
        race_id: CompactString,
        fast_wiring: Option<BatcherWiring>,
        normal_wiring: BatcherWiring,
        client: Arc<influxdb3_client::Client>,
        write_metrics: Arc<WriteMetrics>,
        stats: Arc<PipelineStats>,
        offsets: OffsetLedger,
    ) -> Self {
        let permits = WritePermits::new(
            usize::from(persistence.max_inflight_db_writes.get()),
            fast_wiring.is_some(),
        );
        Self {
            fast_wiring,
            normal_wiring,
            context: Arc::new(FlushContext {
                client,
                race_id: Arc::from(race_id),
                permits,
                backoff: RetryBackoff {
                    initial: Duration::from_millis(u64::from(
                        persistence.write_retry_initial_backoff_ms.get(),
                    )),
                    max: Duration::from_millis(u64::from(
                        persistence.write_retry_max_backoff_ms.get(),
                    )),
                },
                write_metrics,
                stats,
                offsets,
            }),
        }
    }

    pub async fn ingest(self) {
        let Self {
            fast_wiring,
            normal_wiring,
            context,
        } = self;
        if let Some(fast_wiring) = fast_wiring {
            tokio::join!(
                Self::ingest_internal(fast_wiring, Arc::clone(&context)),
                Self::ingest_internal(normal_wiring, context),
            );
        } else {
            Self::ingest_internal(normal_wiring, context).await;
        }
    }

    async fn ingest_internal(wiring: BatcherWiring, context: Arc<FlushContext>) {
        let BatcherWiring {
            mut batch_rx,
            pool_tx,
            wiring_type,
        } = wiring;
        info!("Starting data flush ingestion loop, flush_type={wiring_type:?}");
        let mut write_tasks = JoinSet::new();
        while let Some(batch) = batch_rx.recv().await {
            // Drop finished tasks so the set does not grow with every batch
            while write_tasks.try_join_next().is_some() {}

            let Some(permit) = context.permits.acquire(wiring_type).await else {
                warn!("Database flush semaphore closed; stopping flush loop");
                break;
            };
            let context = Arc::clone(&context);
            let pool_tx = pool_tx.clone();

            write_tasks.spawn(async move {
                let _permit = permit;
                let points = context.flush_and_settle(batch, wiring_type).await;
                let _ = pool_tx.try_send(points);
            });
        }
        while let Some(result) = write_tasks.join_next().await {
            if let Err(error) = result {
                error!(%error, "Database write task failed, flush_type={wiring_type:?}");
            }
        }

        warn!("Data flush ingestion loop terminated, flush_type={wiring_type:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::offset_ledger::testing::{
        RecordingSink, detached_ledger, recording_ledger,
    };
    use crate::messaging::offset_tracker::OffsetSet;
    use crate::persistence::persistence_entrypoint::PersistenceLayerEntryPoint;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;
    use tokio::time::timeout;

    const SHORT: Duration = Duration::from_millis(50);

    #[test]
    fn backoff_doubles_and_is_capped() {
        let backoff = RetryBackoff {
            initial: Duration::from_millis(200),
            max: Duration::from_millis(1_500),
        };
        let ceilings: Vec<u128> = (0..5).map(|n| backoff.ceiling(n).as_millis()).collect();
        assert_eq!(ceilings, [200, 400, 800, 1_500, 1_500]);
        for huge in [31, 32, 33, 1_000, u32::MAX] {
            assert_eq!(backoff.ceiling(huge), backoff.max);
        }
    }

    #[test]
    fn backoff_delay_stays_between_half_the_ceiling_and_the_ceiling() {
        let backoff = RetryBackoff {
            initial: Duration::from_millis(200),
            max: Duration::from_millis(1_500),
        };
        for retry in [0, 1, 2, 3, 50] {
            let ceiling = backoff.ceiling(retry);
            for random in [0, 1, 12_345, u64::MAX] {
                let delay = backoff.delay_with_jitter(retry, random);
                assert!(
                    delay >= ceiling / 2 && delay <= ceiling,
                    "{delay:?} for {ceiling:?}"
                );
            }
            assert_eq!(backoff.delay_with_jitter(retry, 0), ceiling / 2);
            assert!(backoff.delay(retry) <= ceiling);
        }
    }

    #[test]
    fn infrastructure_and_server_errors_are_retryable() {
        for code in [401, 403, 404, 429, 500, 502, 503, 504, 507, 599] {
            let error = InfluxError::Server {
                code,
                message: String::new(),
            };
            assert!(is_retryable(&error), "{code} should retry");
            assert_eq!(
                is_auth_or_config(&error),
                is_auth_or_config_status(code),
                "{code}"
            );
        }
        assert!(is_retryable(&InfluxError::Timeout(Duration::from_secs(1))));
    }

    #[test]
    fn payload_errors_and_non_transport_errors_are_not_retryable() {
        for code in [400, 413, 422, 499, 600] {
            let error = InfluxError::Server {
                code,
                message: String::new(),
            };
            assert!(!is_retryable(&error), "{code} should not retry");
            assert!(!is_auth_or_config(&error), "{code}");
        }
        let partial = InfluxError::PartialWrite(influxdb3_client::error::PartialWriteError {
            line_errors: vec![],
        });
        assert!(!is_retryable(&partial));
        assert!(!is_retryable(&InfluxError::Config("bad".into())));
    }

    #[tokio::test]
    async fn refused_connection_is_retryable() {
        let closed_port = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let context = context_for(closed_port, Arc::new(WriteMetrics::new())).await;

        let error = context.client.write("m val=1 1").await.unwrap_err();

        assert!(matches!(error, InfluxError::Http(_)), "{error:?}");
        assert!(is_retryable(&error));
    }

    #[test]
    fn fast_reserve_is_a_quarter_of_the_permits_and_leaves_one_shared() {
        let reserved: Vec<usize> = [1, 2, 3, 4, 7, 8, 100]
            .into_iter()
            .map(fast_reserved_permits)
            .collect();
        assert_eq!(reserved, [0, 1, 1, 1, 1, 2, 25]);
    }

    #[tokio::test]
    async fn normal_path_uses_all_permits_when_fast_path_is_disabled() {
        let permits = WritePermits::new(3, false);
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(
                timeout(SHORT, permits.acquire(BatcherWiringType::Normal))
                    .await
                    .expect("normal path must have access to every permit")
                    .unwrap(),
            );
        }
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Normal))
                .await
                .is_err(),
            "normal path must still respect the total write limit"
        );
    }

    #[tokio::test]
    async fn normal_path_cannot_use_the_fast_reserve() {
        let permits = WritePermits::new(3, true);
        let _first = permits.acquire(BatcherWiringType::Normal).await.unwrap();
        let _second = permits.acquire(BatcherWiringType::Normal).await.unwrap();

        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Normal))
                .await
                .is_err(),
            "normal must wait even though the fast reserve is free"
        );
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Fast))
                .await
                .is_ok(),
            "fast must get its reserve while normal holds every shared permit"
        );
    }

    #[tokio::test]
    async fn fast_path_takes_the_reserve_first_and_total_stays_at_the_limit() {
        let permits = WritePermits::new(3, true);
        let _fast = permits.acquire(BatcherWiringType::Fast).await.unwrap();

        // The reserve was used, so both shared permits are still free for normal.
        let _normal_a = permits.acquire(BatcherWiringType::Normal).await.unwrap();
        let _normal_b = permits.acquire(BatcherWiringType::Normal).await.unwrap();

        // 3 permits are out: nothing else can start on either path.
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Fast))
                .await
                .is_err()
        );
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Normal))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn fast_path_overflows_into_the_shared_pool() {
        let permits = WritePermits::new(3, true);
        let _reserved = permits.acquire(BatcherWiringType::Fast).await.unwrap();
        let _shared_a = permits.acquire(BatcherWiringType::Fast).await.unwrap();
        let _shared_b = permits.acquire(BatcherWiringType::Fast).await.unwrap();
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Fast))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn single_permit_is_shared_by_both_paths() {
        let permits = WritePermits::new(1, true);
        let held = permits.acquire(BatcherWiringType::Normal).await.unwrap();
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Fast))
                .await
                .is_err()
        );
        drop(held);
        assert!(
            timeout(SHORT, permits.acquire(BatcherWiringType::Fast))
                .await
                .is_ok()
        );
    }

    /// Serves one scripted `(status, body)` per connection, then returns every request body.
    fn spawn_mock_influx(responses: Vec<(u16, &'static str)>) -> (u16, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            let mut bodies = Vec::new();
            for (status, response_body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut received = Vec::new();
                let mut chunk = [0_u8; 4096];
                let (header_end, content_length) = loop {
                    let read = stream.read(&mut chunk).unwrap();
                    assert!(read > 0, "client closed before sending a full request");
                    received.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&received).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text[..end]
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (end + 4, length);
                    }
                };
                while received.len() < header_end + content_length {
                    let read = stream.read(&mut chunk).unwrap();
                    assert!(read > 0, "client closed mid-body");
                    received.extend_from_slice(&chunk[..read]);
                }
                bodies.push(
                    String::from_utf8(received[header_end..header_end + content_length].to_vec())
                        .unwrap(),
                );
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len()
                )
                .unwrap();
            }
            bodies
        });
        (port, handle)
    }

    async fn context_for(port: u16, write_metrics: Arc<WriteMetrics>) -> FlushContext {
        let config = PersistenceConfig {
            db_host: "http://127.0.0.1".to_string(),
            db_port: port,
            ..PersistenceConfig::mock_config()
        };
        let client = influxdb3_client::Client::new(
            PersistenceLayerEntryPoint::influx_client_config(&config).unwrap(),
        )
        .await
        .unwrap();
        FlushContext {
            client: Arc::new(client),
            race_id: Arc::from("race"),
            permits: WritePermits::new(2, true),
            backoff: RetryBackoff {
                initial: Duration::from_millis(2),
                max: Duration::from_millis(4),
            },
            write_metrics,
            stats: Arc::default(),
            offsets: detached_ledger(),
        }
    }

    fn batch() -> Vec<SimplePoint> {
        ["a", "b"]
            .into_iter()
            .zip([1.5, 2.0])
            .map(|(measurement, value)| SimplePoint {
                measurement: Arc::from(measurement),
                driver: Arc::new(CompactString::from("drv")),
                value,
                timestamp_nanos: 42,
            })
            .collect()
    }

    const EXPECTED_BODY: &str =
        "a,race=race,driver=drv val=1.5 42\nb,race=race,driver=drv val=2 42\n";

    #[tokio::test]
    async fn transient_failures_are_resent_until_the_write_succeeds() {
        let (port, server) = spawn_mock_influx(vec![(503, ""), (429, ""), (500, "{}"), (204, "")]);
        let metrics = Arc::new(WriteMetrics::new());
        let context = context_for(port, Arc::clone(&metrics)).await;

        let outcome = context
            .flush_batch(&batch(), BatcherWiringType::Normal)
            .await;

        assert_eq!(outcome, FlushOutcome::Written);
        let snapshot = metrics.snapshot();
        assert_eq!(
            (
                snapshot.requests_retry,
                snapshot.requests_ok,
                snapshot.requests_error
            ),
            (3, 1, 0)
        );
        assert_eq!(snapshot.lines, 2);
        assert_eq!(snapshot.bytes, EXPECTED_BODY.len() as u64);
        assert!(
            !metrics.writes_failing_longer_than(Duration::ZERO),
            "success ends the streak"
        );
        assert_eq!(server.join().unwrap(), vec![EXPECTED_BODY; 4]);
    }

    #[tokio::test]
    async fn auth_and_missing_database_are_retried_until_the_write_succeeds() {
        for code in [401, 403, 404] {
            let (port, server) =
                spawn_mock_influx(vec![(code, r#"{"error":"rejected"}"#), (204, "")]);
            let metrics = Arc::new(WriteMetrics::new());
            let context = context_for(port, Arc::clone(&metrics)).await;

            let outcome = context
                .flush_batch(&batch(), BatcherWiringType::Normal)
                .await;

            assert_eq!(outcome, FlushOutcome::Written, "{code}");
            let snapshot = metrics.snapshot();
            assert_eq!(
                (
                    snapshot.requests_retry,
                    snapshot.requests_ok,
                    snapshot.requests_error
                ),
                (1, 1, 0),
                "{code}"
            );
            assert_eq!(server.join().unwrap().len(), 2, "{code}");
        }
    }

    #[tokio::test]
    async fn non_retryable_failure_drops_the_batch_after_one_attempt() {
        let (port, server) = spawn_mock_influx(vec![(422, r#"{"error":"unprocessable"}"#)]);
        let metrics = Arc::new(WriteMetrics::new());
        let context = context_for(port, Arc::clone(&metrics)).await;

        let outcome = context.flush_batch(&batch(), BatcherWiringType::Fast).await;

        assert_eq!(outcome, FlushOutcome::Dropped);
        let snapshot = metrics.snapshot();
        assert_eq!(
            (
                snapshot.requests_retry,
                snapshot.requests_ok,
                snapshot.requests_error
            ),
            (0, 0, 1)
        );
        assert_eq!(snapshot.lines, 0);
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn partial_write_is_not_retried_and_counts_accepted_lines() {
        let body = r#"{"error":"partial write of line protocol occurred","data":[{"line_number":2,"error_message":"bad field type"}]}"#;
        let (port, server) = spawn_mock_influx(vec![(400, body)]);
        let metrics = Arc::new(WriteMetrics::new());
        let context = context_for(port, Arc::clone(&metrics)).await;

        let outcome = context
            .flush_batch(&batch(), BatcherWiringType::Normal)
            .await;

        assert_eq!(outcome, FlushOutcome::PartiallyWritten);
        let snapshot = metrics.snapshot();
        assert_eq!(
            (
                snapshot.requests_partial,
                snapshot.requests_retry,
                snapshot.requests_error
            ),
            (1, 0, 0)
        );
        assert_eq!(snapshot.lines, 1);
        assert_eq!(server.join().unwrap().len(), 1);
    }

    /// Registers the offsets with the context's ledger as the sink would, and pairs them with points.
    fn point_batch(
        context: &FlushContext,
        offsets: &[(i32, i64)],
        points: Vec<SimplePoint>,
    ) -> PointBatch {
        let mut set = OffsetSet::default();
        for &(partition, offset) in offsets {
            context.offsets.register(partition, offset);
            set.push(partition, offset);
        }
        PointBatch {
            points,
            offsets: set,
            oldest_created_ms: crate::persistence::sensor_data_batcher::NO_TIMESTAMP,
        }
    }

    fn recording_context(context: &mut FlushContext) -> Arc<RecordingSink> {
        let (ledger, sink) = recording_ledger();
        context.offsets = ledger;
        sink
    }

    #[tokio::test]
    async fn every_flush_outcome_settles_the_messages_of_the_batch() {
        let partial = r#"{"error":"partial write of line protocol occurred","data":[{"line_number":2,"error_message":"bad field type"}]}"#;
        for (status, body, expected) in [
            (204, "", FlushOutcome::Written),
            (400, partial, FlushOutcome::PartiallyWritten),
            (422, r#"{"error":"unprocessable"}"#, FlushOutcome::Dropped),
        ] {
            let (port, server) = spawn_mock_influx(vec![(status, body)]);
            let mut context = context_for(port, Arc::new(WriteMetrics::new())).await;
            let sink = recording_context(&mut context);
            let batch = point_batch(&context, &[(0, 5), (0, 6), (1, 9)], batch());

            let points = context
                .flush_and_settle(batch, BatcherWiringType::Normal)
                .await;

            assert!(
                points.is_empty(),
                "{expected:?}: the buffer comes back cleared"
            );
            assert_eq!(sink.positions(), [(0, 7), (1, 10)], "{expected:?}");
            server.join().unwrap();
        }
    }

    #[tokio::test]
    async fn a_batch_is_settled_only_after_the_database_answered() {
        // Accepts connections but never answers: the write hangs like an unreachable database.
        let hung = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut context = context_for(
            hung.local_addr().unwrap().port(),
            Arc::new(WriteMetrics::new()),
        )
        .await;
        let sink = recording_context(&mut context);
        let batch = point_batch(&context, &[(0, 0)], batch());

        let pending = timeout(
            SHORT,
            context.flush_and_settle(batch, BatcherWiringType::Fast),
        )
        .await;

        assert!(pending.is_err());
        assert!(sink.stored().is_empty());
        assert_eq!(context.offsets.lag()[0].pending, 1);
    }

    #[tokio::test]
    async fn a_transiently_failing_batch_settles_once_it_is_finally_written() {
        let (port, server) = spawn_mock_influx(vec![(503, ""), (500, ""), (204, "")]);
        let mut context = context_for(port, Arc::new(WriteMetrics::new())).await;
        let sink = recording_context(&mut context);
        let batch = point_batch(&context, &[(2, 100)], batch());

        context
            .flush_and_settle(batch, BatcherWiringType::Normal)
            .await;

        assert_eq!(sink.stored(), [(2, 101)]);
        assert_eq!(server.join().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn a_batch_without_points_settles_without_writing() {
        // Nothing listens on this port: a write attempt would retry forever.
        let closed_port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut context = context_for(closed_port, Arc::new(WriteMetrics::new())).await;
        let sink = recording_context(&mut context);
        let mut batch = point_batch(&context, &[(0, 3)], Vec::new());
        batch.oldest_created_ms = crate::pipeline_stats::unix_now_ms();

        timeout(
            Duration::from_secs(2),
            context.flush_and_settle(batch, BatcherWiringType::Normal),
        )
        .await
        .expect("an empty batch must not write");

        assert_eq!(sink.stored(), [(0, 4)]);
        assert_eq!(
            context.stats.take_persist().count(),
            1,
            "settled without a write, still counted as persisted"
        );
    }

    #[tokio::test]
    async fn settlement_advances_only_over_the_contiguous_settled_prefix() {
        let (port, server) = spawn_mock_influx(vec![(204, ""), (204, "")]);
        let mut context = context_for(port, Arc::new(WriteMetrics::new())).await;
        let sink = recording_context(&mut context);
        // Batches of one partition finishing out of order: 5..=6 is written before 0..=4.
        let first = point_batch(&context, &[(0, 0), (0, 1), (0, 2), (0, 3), (0, 4)], batch());
        let second = point_batch(&context, &[(0, 5), (0, 6)], batch());

        context
            .flush_and_settle(second, BatcherWiringType::Normal)
            .await;
        assert!(
            sink.stored().is_empty(),
            "offsets 0..=4 are still in flight"
        );
        context
            .flush_and_settle(first, BatcherWiringType::Normal)
            .await;

        assert_eq!(sink.stored(), [(0, 7)]);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn flusher_without_fast_wiring_writes_and_settles_normal_batches() {
        let (port, server) = spawn_mock_influx(vec![(204, "")]);
        let mut context = context_for(port, Arc::new(WriteMetrics::new())).await;
        let sink = recording_context(&mut context);
        let batch = point_batch(&context, &[(0, 0), (0, 1)], batch());
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(4);
        let (pool_tx, mut pool_rx) = tokio::sync::mpsc::channel(4);
        batch_tx.send(batch).await.unwrap();
        drop(batch_tx);

        let config = PersistenceConfig::mock_config();
        let flusher = InfluxDbFlusher::from_config(
            &config,
            CompactString::from("race"),
            None,
            BatcherWiring {
                batch_rx,
                pool_tx,
                wiring_type: BatcherWiringType::Normal,
            },
            context.client,
            context.write_metrics,
            context.stats,
            context.offsets,
        );
        assert_eq!(
            flusher.context.permits.shared.available_permits(),
            usize::from(config.max_inflight_db_writes.get()),
        );
        timeout(Duration::from_secs(2), flusher.ingest())
            .await
            .expect("normal-only flusher must drain and terminate");

        assert_eq!(sink.positions(), [(0, 2)]);
        assert!(pool_rx.try_recv().unwrap().is_empty());
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_flush_loop_settles_each_batch_and_recycles_its_buffer() {
        let (port, server) = spawn_mock_influx(vec![(204, ""), (204, "")]);
        let mut context = context_for(port, Arc::new(WriteMetrics::new())).await;
        let sink = recording_context(&mut context);
        let first = point_batch(&context, &[(0, 0), (0, 1)], batch());
        let second = point_batch(&context, &[(0, 2)], batch());
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(4);
        let (pool_tx, mut pool_rx) = tokio::sync::mpsc::channel(4);
        batch_tx.send(first).await.unwrap();
        batch_tx.send(second).await.unwrap();
        drop(batch_tx);

        InfluxDbFlusher::ingest_internal(
            BatcherWiring {
                batch_rx,
                pool_tx,
                wiring_type: BatcherWiringType::Normal,
            },
            Arc::new(context),
        )
        .await;

        assert_eq!(sink.positions(), [(0, 3)]);
        let recycled = pool_rx.try_recv().unwrap();
        assert!(recycled.is_empty() && recycled.capacity() > 0);
        server.join().unwrap();
    }
}
