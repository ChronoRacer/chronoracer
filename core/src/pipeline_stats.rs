//! Windowed counters behind the periodic pipeline report, see `docs/component-core.md`.
//!
//! Every report takes and resets the counters, so each line covers exactly its own window.
//! Latencies are measured against the Kafka record timestamp (producer `CreateTime`), so they
//! include any clock difference between the producer and this VM.

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Inclusive upper bounds of the latency buckets. One more bucket counts everything above.
const BUCKET_BOUNDS_MS: [u64; 15] = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000,
];
const BUCKETS: usize = BUCKET_BOUNDS_MS.len() + 1;

/// `unconsumed` before the first librdkafka statistics arrived.
const UNKNOWN: i64 = -1;

/// Milliseconds since the Unix epoch on this machine's clock.
#[must_use]
pub fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Age of a record created at `created_ms`. A record from the future (producer clock ahead)
/// counts as zero.
#[must_use]
pub fn age_ms(created_ms: i64, now_ms: i64) -> u64 {
    u64::try_from(now_ms.saturating_sub(created_ms)).unwrap_or(0)
}

/// Fixed-bucket latency histogram, written concurrently with relaxed atomics.
pub struct LatencyHistogram {
    buckets: [AtomicU64; BUCKETS],
    sum_ms: AtomicU64,
    max_ms: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ms: AtomicU64::new(0),
            max_ms: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    pub fn record(&self, ms: u64) {
        let bucket = BUCKET_BOUNDS_MS.partition_point(|&bound| bound < ms);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(ms, Ordering::Relaxed);
        self.max_ms.fetch_max(ms, Ordering::Relaxed);
    }

    pub fn record_duration(&self, duration: Duration) {
        self.record(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    }

    /// Everything recorded since the previous `take`. Records racing with it land in either window.
    pub fn take(&self) -> HistogramWindow {
        HistogramWindow {
            buckets: std::array::from_fn(|index| self.buckets[index].swap(0, Ordering::Relaxed)),
            sum_ms: self.sum_ms.swap(0, Ordering::Relaxed),
            max_ms: self.max_ms.swap(0, Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramWindow {
    buckets: [u64; BUCKETS],
    sum_ms: u64,
    max_ms: u64,
}

impl HistogramWindow {
    #[must_use]
    pub fn count(&self) -> u64 {
        self.buckets.iter().sum()
    }

    /// Upper bound of the bucket holding quantile `q`; `None` when it is in the overflow bucket
    /// or the window is empty.
    #[must_use]
    pub fn quantile_bound_ms(&self, q: f64) -> Option<u64> {
        let count = self.count();
        if count == 0 {
            return None;
        }
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rank = ((q * count as f64).ceil() as u64).clamp(1, count);
        let mut seen = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            seen += bucket;
            if seen >= rank {
                return BUCKET_BOUNDS_MS.get(index).copied();
            }
        }
        None
    }
}

impl fmt::Display for HistogramWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.count();
        if count == 0 {
            return write!(f, "n=0");
        }
        let overflow = BUCKET_BOUNDS_MS[BUCKET_BOUNDS_MS.len() - 1];
        for (label, q) in [("p50", 0.5), ("p99", 0.99)] {
            match self.quantile_bound_ms(q) {
                Some(bound) => write!(f, "{label}<={bound},")?,
                None => write!(f, "{label}>{overflow},")?,
            }
        }
        write!(
            f,
            "max={},mean={},n={count}",
            self.max_ms,
            self.sum_ms / count
        )
    }
}

/// Shared by the sensor data sink, the collector, and the flusher. The sink logs the report.
pub struct PipelineStats {
    consumed_messages: AtomicU64,
    sink_blocked_us: AtomicU64,
    decoded_points: AtomicU64,
    written_lines: AtomicU64,
    unconsumed: AtomicI64,
    preview: LatencyHistogram,
    write: LatencyHistogram,
    persist: LatencyHistogram,
    window_start: Mutex<Instant>,
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self {
            consumed_messages: AtomicU64::new(0),
            sink_blocked_us: AtomicU64::new(0),
            decoded_points: AtomicU64::new(0),
            written_lines: AtomicU64::new(0),
            unconsumed: AtomicI64::new(UNKNOWN),
            preview: LatencyHistogram::default(),
            write: LatencyHistogram::default(),
            persist: LatencyHistogram::default(),
            window_start: Mutex::new(Instant::now()),
        }
    }
}

impl PipelineStats {
    pub fn add_consumed(&self, messages: u64) {
        self.consumed_messages
            .fetch_add(messages, Ordering::Relaxed);
    }

    /// Time the sink waited for room in the processing channel.
    pub fn add_sink_blocked(&self, waited: Duration) {
        let micros = u64::try_from(waited.as_micros()).unwrap_or(u64::MAX);
        self.sink_blocked_us.fetch_add(micros, Ordering::Relaxed);
    }

    pub fn add_decoded_points(&self, points: u64) {
        self.decoded_points.fetch_add(points, Ordering::Relaxed);
    }

    /// From the Kafka record timestamp to the moment the points are offered to the preview.
    pub fn record_preview_latency(&self, ms: u64) {
        self.preview.record(ms);
    }

    /// One database write attempt.
    pub fn record_write(&self, duration: Duration) {
        self.write.record_duration(duration);
    }

    /// Lines the database accepted.
    pub fn add_written_lines(&self, lines: u64) {
        self.written_lines.fetch_add(lines, Ordering::Relaxed);
    }

    /// A point batch settled; its oldest message was created at `oldest_created_ms`.
    pub fn record_settled(&self, oldest_created_ms: Option<i64>) {
        if let Some(created_ms) = oldest_created_ms {
            self.persist.record(age_ms(created_ms, unix_now_ms()));
        }
    }

    /// Messages on the broker not yet handed to Core, from librdkafka statistics.
    pub fn set_unconsumed(&self, messages: i64) {
        self.unconsumed.store(messages, Ordering::Relaxed);
    }

    /// Takes the counters of the window that ends now.
    pub fn take_report(&self) -> PipelineReport {
        let window = {
            let mut start = self
                .window_start
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let now = Instant::now();
            let window = now.duration_since(*start);
            *start = now;
            window
        };
        PipelineReport {
            window,
            consumed_messages: self.consumed_messages.swap(0, Ordering::Relaxed),
            sink_blocked: Duration::from_micros(self.sink_blocked_us.swap(0, Ordering::Relaxed)),
            decoded_points: self.decoded_points.swap(0, Ordering::Relaxed),
            written_lines: self.written_lines.swap(0, Ordering::Relaxed),
            unconsumed: Some(self.unconsumed.load(Ordering::Relaxed)).filter(|lag| *lag >= 0),
            preview: self.preview.take(),
            write: self.write.take(),
            persist: self.persist.take(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineReport {
    pub(crate) window: Duration,
    pub(crate) consumed_messages: u64,
    pub(crate) sink_blocked: Duration,
    pub(crate) decoded_points: u64,
    pub(crate) written_lines: u64,
    pub(crate) unconsumed: Option<i64>,
    pub(crate) preview: HistogramWindow,
    pub(crate) write: HistogramWindow,
    pub(crate) persist: HistogramWindow,
}

impl fmt::Display for PipelineReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.window.as_secs_f64().max(0.001);
        #[allow(clippy::cast_precision_loss)]
        let per_second = |count: u64| (count as f64 / seconds).round();
        write!(
            f,
            "window_s={:.1} consumed_msg_s={} decoded_pts_s={} written_lines_s={} sink_blocked_pct={:.1} unconsumed=",
            seconds,
            per_second(self.consumed_messages),
            per_second(self.decoded_points),
            per_second(self.written_lines),
            self.sink_blocked.as_secs_f64() / seconds * 100.0,
        )?;
        match self.unconsumed {
            Some(messages) => write!(f, "{messages}")?,
            None => write!(f, "unknown")?,
        }
        write!(
            f,
            " preview_ms={} write_ms={} persist_ms={}",
            self.preview, self.write, self.persist
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_land_in_the_first_bucket_whose_bound_is_not_below_them() {
        let histogram = LatencyHistogram::default();
        for ms in [0, 1, 2, 3, 60_000, 60_001] {
            histogram.record(ms);
        }
        let window = histogram.take();
        assert_eq!(window.buckets[0], 2, "0 and 1 are <= 1");
        assert_eq!(window.buckets[1], 1, "2 is <= 2");
        assert_eq!(window.buckets[2], 1, "3 is <= 5");
        assert_eq!(window.buckets[BUCKETS - 2], 1, "60 000 is the last bound");
        assert_eq!(window.buckets[BUCKETS - 1], 1, "above every bound");
        assert_eq!((window.max_ms, window.count()), (60_001, 6));
    }

    #[test]
    fn quantiles_report_the_bucket_bound() {
        let histogram = LatencyHistogram::default();
        for _ in 0..98 {
            histogram.record(7);
        }
        histogram.record(150);
        histogram.record(90_000);
        let window = histogram.take();
        assert_eq!(window.quantile_bound_ms(0.5), Some(10));
        assert_eq!(window.quantile_bound_ms(0.99), Some(200));
        assert_eq!(window.quantile_bound_ms(1.0), None, "overflow bucket");
        assert_eq!(
            window.to_string(),
            "p50<=10,p99<=200,max=90000,mean=908,n=100"
        );
    }

    #[test]
    fn take_resets_the_window() {
        let histogram = LatencyHistogram::default();
        histogram.record(4);
        assert_eq!(histogram.take().count(), 1);
        let empty = histogram.take();
        assert_eq!((empty.count(), empty.max_ms), (0, 0));
        assert_eq!(empty.quantile_bound_ms(0.5), None);
        assert_eq!(empty.to_string(), "n=0");
    }

    #[test]
    fn a_record_from_the_future_has_age_zero() {
        assert_eq!(age_ms(1_000, 900), 0);
        assert_eq!(age_ms(1_000, 1_250), 250);
        assert_eq!(age_ms(i64::MIN, i64::MAX), u64::try_from(i64::MAX).unwrap());
    }

    #[test]
    fn report_takes_and_resets_every_counter() {
        let stats = PipelineStats::default();
        stats.add_consumed(10);
        stats.add_decoded_points(80);
        stats.add_sink_blocked(Duration::from_millis(5));
        stats.record_preview_latency(12);
        stats.record_write(Duration::from_millis(700));
        stats.add_written_lines(80);
        stats.record_settled(None);

        let report = stats.take_report();
        assert_eq!(
            (
                report.consumed_messages,
                report.decoded_points,
                report.written_lines,
                report.sink_blocked,
                report.unconsumed,
            ),
            (10, 80, 80, Duration::from_millis(5), None)
        );
        assert_eq!(
            (
                report.preview.count(),
                report.write.count(),
                report.persist.count()
            ),
            (1, 1, 0),
            "a batch without a timestamp has no persist latency"
        );

        stats.set_unconsumed(37);
        let next = stats.take_report();
        assert_eq!((next.consumed_messages, next.preview.count()), (0, 0));
        assert_eq!(next.unconsumed, Some(37), "a gauge, not a window counter");
    }

    #[test]
    fn report_line_names_every_field() {
        let stats = PipelineStats::default();
        stats.record_settled(Some(unix_now_ms()));
        let line = stats.take_report().to_string();
        for field in [
            "window_s=",
            "consumed_msg_s=0",
            "decoded_pts_s=0",
            "written_lines_s=",
            "sink_blocked_pct=0.0",
            "unconsumed=unknown",
            "preview_ms=n=0",
            "write_ms=n=0",
            "persist_ms=p50<=",
        ] {
            assert!(line.contains(field), "{field} missing in {line}");
        }
    }
}
