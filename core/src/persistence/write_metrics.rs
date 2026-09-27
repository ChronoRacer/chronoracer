use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative Influx write counters. Increments happen once per flush, off the
/// per-point path. Cloud Monitoring reads a snapshot on its own task.
#[derive(Debug, Default)]
pub struct WriteMetrics {
    lines: AtomicU64,
    bytes: AtomicU64,
    requests_ok: AtomicU64,
    requests_partial: AtomicU64,
    requests_error: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteMetricSnapshot {
    pub lines: u64,
    pub bytes: u64,
    pub requests_ok: u64,
    pub requests_partial: u64,
    pub requests_error: u64,
}

impl WriteMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self, lines: u64, bytes: u64) {
        self.lines.fetch_add(lines, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.requests_ok.fetch_add(1, Ordering::Relaxed);
    }

    /// `failed_lines` is the number of line errors Influx reported. Bytes are
    /// not counted: a partial write does not say how many bytes were accepted.
    pub fn record_partial(&self, attempted_lines: u64, failed_lines: u64) {
        self.lines.fetch_add(
            attempted_lines.saturating_sub(failed_lines),
            Ordering::Relaxed,
        );
        self.requests_partial.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> WriteMetricSnapshot {
        WriteMetricSnapshot {
            lines: self.lines.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            requests_ok: self.requests_ok.load(Ordering::Relaxed),
            requests_partial: self.requests_partial.load(Ordering::Relaxed),
            requests_error: self.requests_error.load(Ordering::Relaxed),
        }
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
    fn error_counts_only_the_request() {
        let metrics = WriteMetrics::new();
        metrics.record_error();

        assert_eq!(
            metrics.snapshot(),
            WriteMetricSnapshot {
                lines: 0,
                bytes: 0,
                requests_ok: 0,
                requests_partial: 0,
                requests_error: 1,
            }
        );
    }
}
