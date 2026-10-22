//! Pipeline counters published to Cloud Monitoring and logged with each sample, see
//! `docs/component-core.md#cloud-monitoring-metrics`.
//!
//! Counters are cumulative; the persist latency histogram is taken (and reset) per sample.
//! Latency is measured against the Kafka record timestamp (producer `CreateTime`), so it
//! includes any clock difference between the producer and this VM.

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Inclusive upper bounds of the latency buckets. One more bucket counts everything above.
pub const BUCKET_BOUNDS_MS: [u64; 15] = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000,
];
pub const BUCKETS: usize = BUCKET_BOUNDS_MS.len() + 1;

/// Cadence of Cloud Monitoring samples, and of the librdkafka statistics behind `unconsumed`.
pub const SAMPLE_INTERVAL: Duration = Duration::from_mins(1);

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

    /// Per-bucket counts, one per `BUCKET_BOUNDS_MS` bound plus the overflow bucket.
    #[must_use]
    pub fn bucket_counts(&self) -> &[u64; BUCKETS] {
        &self.buckets
    }

    #[must_use]
    pub fn sum_ms(&self) -> u64 {
        self.sum_ms
    }

    #[must_use]
    pub fn max_ms(&self) -> u64 {
        self.max_ms
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

/// Shared by the sensor data sink and the flusher; read by the Cloud Monitoring publisher.
pub struct PipelineStats {
    consumed_messages: AtomicU64,
    sink_blocked_us: AtomicU64,
    unconsumed: AtomicI64,
    persist: LatencyHistogram,
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self {
            consumed_messages: AtomicU64::new(0),
            sink_blocked_us: AtomicU64::new(0),
            unconsumed: AtomicI64::new(UNKNOWN),
            persist: LatencyHistogram::default(),
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

    /// Cumulative counters and the gauge, as of now.
    #[must_use]
    pub fn snapshot(&self) -> PipelineSnapshot {
        PipelineSnapshot {
            consumed_messages: self.consumed_messages.load(Ordering::Relaxed),
            sink_blocked: Duration::from_micros(self.sink_blocked_us.load(Ordering::Relaxed)),
            unconsumed: Some(self.unconsumed.load(Ordering::Relaxed)).filter(|lag| *lag >= 0),
        }
    }

    /// Persist latencies recorded since the previous call.
    pub fn take_persist(&self) -> HistogramWindow {
        self.persist.take()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSnapshot {
    pub consumed_messages: u64,
    pub sink_blocked: Duration,
    pub unconsumed: Option<i64>,
}

/// One sample window as logged: rates and shares computed from two snapshots.
pub struct PipelineReport<'a> {
    pub window: Duration,
    pub previous: &'a PipelineSnapshot,
    pub current: &'a PipelineSnapshot,
    pub persist: &'a HistogramWindow,
}

impl fmt::Display for PipelineReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.window.as_secs_f64().max(0.001);
        let consumed = self
            .current
            .consumed_messages
            .saturating_sub(self.previous.consumed_messages);
        let blocked = self
            .current
            .sink_blocked
            .saturating_sub(self.previous.sink_blocked);
        #[allow(clippy::cast_precision_loss)]
        let consumed_per_second = (consumed as f64 / seconds).round();
        write!(
            f,
            "window_s={seconds:.1} consumed_msg_s={consumed_per_second} sink_blocked_pct={:.1} unconsumed=",
            blocked.as_secs_f64() / seconds * 100.0,
        )?;
        match self.current.unconsumed {
            Some(messages) => write!(f, "{messages}")?,
            None => write!(f, "unknown")?,
        }
        write!(f, " persist_ms={}", self.persist)
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
        assert_eq!((window.max_ms(), window.count()), (60_001, 6));
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
        assert_eq!((empty.count(), empty.max_ms()), (0, 0));
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
    fn counters_are_cumulative_and_persist_is_windowed() {
        let stats = PipelineStats::default();
        stats.add_consumed(10);
        stats.add_sink_blocked(Duration::from_millis(5));
        stats.record_settled(None);
        stats.record_settled(Some(unix_now_ms()));

        let first = stats.snapshot();
        assert_eq!(
            (
                first.consumed_messages,
                first.sink_blocked,
                first.unconsumed
            ),
            (10, Duration::from_millis(5), None)
        );
        assert_eq!(stats.take_persist().count(), 1, "no timestamp, no latency");

        stats.set_unconsumed(37);
        stats.add_consumed(5);
        let second = stats.snapshot();
        assert_eq!(second.consumed_messages, 15);
        assert_eq!(second.unconsumed, Some(37));
        assert_eq!(stats.take_persist().count(), 0);
    }

    #[test]
    fn report_line_uses_the_window_delta() {
        let stats = PipelineStats::default();
        let previous = stats.snapshot();
        stats.add_consumed(200);
        stats.add_sink_blocked(Duration::from_secs(1));
        let current = stats.snapshot();
        let persist = stats.take_persist();
        let line = PipelineReport {
            window: Duration::from_secs(10),
            previous: &previous,
            current: &current,
            persist: &persist,
        }
        .to_string();
        assert_eq!(
            line,
            "window_s=10.0 consumed_msg_s=20 sink_blocked_pct=10.0 unconsumed=unknown persist_ms=n=0"
        );
    }
}
