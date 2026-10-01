use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cumulative Influx write counters plus the current write-failure streak.
/// Counter increments happen once per write attempt, off the per-point path.
/// Cloud Monitoring reads a snapshot on its own task; the heartbeat reads the streak.
///
/// Every HTTP write attempt is counted once under exactly one outcome:
/// `ok` and `partial` are terminal and count lines, `error` is a terminal drop of a
/// batch, and `retry` is a transient failure whose batch is sent again.
#[derive(Debug)]
pub struct WriteMetrics {
    lines: AtomicU64,
    bytes: AtomicU64,
    requests_ok: AtomicU64,
    requests_partial: AtomicU64,
    requests_error: AtomicU64,
    requests_retry: AtomicU64,
    epoch: Instant,
    /// Milliseconds after `epoch` (plus one, so `0` can mean "not failing") at which the
    /// current streak of failed attempts began. A streak ends on any `ok` or `partial`.
    failing_since_ms: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteMetricSnapshot {
    pub lines: u64,
    pub bytes: u64,
    pub requests_ok: u64,
    pub requests_partial: u64,
    pub requests_error: u64,
    pub requests_retry: u64,
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self {
            lines: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            requests_ok: AtomicU64::new(0),
            requests_partial: AtomicU64::new(0),
            requests_error: AtomicU64::new(0),
            requests_retry: AtomicU64::new(0),
            epoch: Instant::now(),
            failing_since_ms: AtomicU64::new(0),
        }
    }

    pub fn record_success(&self, lines: u64, bytes: u64) {
        self.lines.fetch_add(lines, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.requests_ok.fetch_add(1, Ordering::Relaxed);
        self.clear_failure();
    }

    /// `failed_lines` is the number of line errors Influx reported. Bytes are
    /// not counted: a partial write does not say how many bytes were accepted.
    /// Influx answered and stored the valid lines, so the failure streak ends.
    pub fn record_partial(&self, attempted_lines: u64, failed_lines: u64) {
        self.lines.fetch_add(
            attempted_lines.saturating_sub(failed_lines),
            Ordering::Relaxed,
        );
        self.requests_partial.fetch_add(1, Ordering::Relaxed);
        self.clear_failure();
    }

    /// A batch was dropped after a non-retryable failure.
    pub fn record_error(&self) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
        self.mark_failure(self.now_ms());
    }

    /// A transient failure; the same batch is sent again after a backoff.
    pub fn record_retry(&self) {
        self.requests_retry.fetch_add(1, Ordering::Relaxed);
        self.mark_failure(self.now_ms());
    }

    /// True when attempts have failed continuously, with no `ok` or `partial` since the first
    /// failure, for longer than `threshold`. A pipeline with no writes never fails.
    #[must_use]
    pub fn writes_failing_longer_than(&self, threshold: Duration) -> bool {
        self.failing_longer_than_at(self.now_ms(), threshold)
    }

    #[must_use]
    pub fn snapshot(&self) -> WriteMetricSnapshot {
        WriteMetricSnapshot {
            lines: self.lines.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            requests_ok: self.requests_ok.load(Ordering::Relaxed),
            requests_partial: self.requests_partial.load(Ordering::Relaxed),
            requests_error: self.requests_error.load(Ordering::Relaxed),
            requests_retry: self.requests_retry.load(Ordering::Relaxed),
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1)
    }

    fn mark_failure(&self, now_ms: u64) {
        let _ =
            self.failing_since_ms
                .compare_exchange(0, now_ms, Ordering::Relaxed, Ordering::Relaxed);
    }

    fn clear_failure(&self) {
        if self.failing_since_ms.load(Ordering::Relaxed) != 0 {
            self.failing_since_ms.store(0, Ordering::Relaxed);
        }
    }

    fn failing_longer_than_at(&self, now_ms: u64, threshold: Duration) -> bool {
        let since = self.failing_since_ms.load(Ordering::Relaxed);
        since != 0 && u128::from(now_ms.saturating_sub(since)) > threshold.as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_counts_lines_bytes_and_one_request() {
        let metrics = WriteMetrics::new();
        metrics.record_success(4, 80);
        metrics.record_success(1, 10);

        assert_eq!(
            metrics.snapshot(),
            WriteMetricSnapshot {
                lines: 5,
                bytes: 90,
                requests_ok: 2,
                requests_partial: 0,
                requests_error: 0,
                requests_retry: 0,
            }
        );
    }

    #[test]
    fn partial_counts_accepted_lines_and_skips_bytes() {
        let metrics = WriteMetrics::new();
        metrics.record_partial(10, 3);
        metrics.record_partial(2, 9);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.lines, 7);
        assert_eq!(snapshot.bytes, 0);
        assert_eq!(snapshot.requests_partial, 2);
    }

    #[test]
    fn error_and_retry_count_only_the_request() {
        let metrics = WriteMetrics::new();
        metrics.record_error();
        metrics.record_retry();
        metrics.record_retry();

        assert_eq!(
            metrics.snapshot(),
            WriteMetricSnapshot {
                lines: 0,
                bytes: 0,
                requests_ok: 0,
                requests_partial: 0,
                requests_error: 1,
                requests_retry: 2,
            }
        );
    }

    #[test]
    fn idle_pipeline_is_never_failing() {
        let metrics = WriteMetrics::new();
        assert!(!metrics.failing_longer_than_at(u64::MAX, Duration::ZERO));
    }

    #[test]
    fn failure_streak_starts_at_first_failure_and_needs_the_threshold() {
        let metrics = WriteMetrics::new();
        let threshold = Duration::from_secs(60);
        metrics.mark_failure(1_000);
        metrics.mark_failure(50_000);

        assert!(!metrics.failing_longer_than_at(61_000, threshold));
        assert!(metrics.failing_longer_than_at(61_001, threshold));
    }

    #[test]
    fn success_and_partial_end_the_failure_streak() {
        let threshold = Duration::from_secs(1);
        for end_streak in [
            (|m: &WriteMetrics| m.record_success(1, 1)) as fn(&WriteMetrics),
            |m| m.record_partial(2, 1),
        ] {
            let metrics = WriteMetrics::new();
            metrics.record_retry();
            assert_ne!(metrics.failing_since_ms.load(Ordering::Relaxed), 0);
            end_streak(&metrics);
            assert!(!metrics.failing_longer_than_at(u64::MAX, threshold));
        }
    }

    #[test]
    fn retry_and_error_both_start_a_failure_streak() {
        for fail in [
            (WriteMetrics::record_retry) as fn(&WriteMetrics),
            WriteMetrics::record_error,
        ] {
            let metrics = WriteMetrics::new();
            fail(&metrics);
            assert!(metrics.failing_longer_than_at(u64::MAX, Duration::from_secs(1)));
        }
    }
}
