use super::write_metrics::{WriteMetricSnapshot, WriteMetrics};
use crate::pipeline_stats::{
    BUCKET_BOUNDS_MS, HistogramWindow, PipelineReport, PipelineSnapshot, PipelineStats,
    SAMPLE_INTERVAL,
};
use anyhow::{Context, Result};
use google_cloud_api::model::distribution::BucketOptions;
use google_cloud_api::model::distribution::bucket_options::Explicit;
use google_cloud_api::model::metric_descriptor::{MetricKind, ValueType};
use google_cloud_api::model::{Distribution, Metric, MonitoredResource};
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_monitoring_v3::client::MetricService;
use google_cloud_monitoring_v3::model::{Point, TimeInterval, TimeSeries, TypedValue};
use google_cloud_wkt::{Timestamp, TimestampError};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub const WRITE_LINES: &str = "custom.googleapis.com/chronoracer/influx_write_lines";
pub const WRITE_BYTES: &str = "custom.googleapis.com/chronoracer/influx_write_bytes";
pub const WRITE_REQUESTS: &str = "custom.googleapis.com/chronoracer/influx_write_requests";

pub const PIPELINE_CONSUMED_MESSAGES: &str =
    "custom.googleapis.com/chronoracer/pipeline_consumed_messages";
pub const PIPELINE_SINK_BLOCKED_MS: &str =
    "custom.googleapis.com/chronoracer/pipeline_sink_blocked_ms";
pub const PIPELINE_UNCONSUMED_MESSAGES: &str =
    "custom.googleapis.com/chronoracer/pipeline_unconsumed_messages";
pub const PIPELINE_PERSIST_LATENCY_MS: &str =
    "custom.googleapis.com/chronoracer/pipeline_persist_latency_ms";

const PUBLISH_INTERVAL: Duration = SAMPLE_INTERVAL;
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);
const METADATA_ROOT: &str = "http://metadata.google.internal/computeMetadata/v1/instance";
/// Must be in the correct format, not necessarily correct values
const UNKNOWN_GCE_IDENTITY_INSTANCE_ID: &str = "1111111111111111111";
const UNKNOWN_GCE_IDENTITY_ZONE: &str = "europe-west1-b";

#[derive(Debug)]
struct GceIdentity {
    instance_id: String,
    zone: String,
}

/// Publishes cumulative write counters and the pipeline series until `stop` is cancelled, then sends one final sample.
/// Metadata or client setup failure is logged and the task ends. A later
/// `CreateTimeSeries` failure is logged and the loop continues. Neither path
/// stops ingest.
pub async fn publish_write_metrics(
    project_id: String,
    max_retries: u16,
    metrics: Arc<WriteMetrics>,
    stats: Arc<PipelineStats>,
    allow_unknown_gce_metrics: bool,
    stop: CancellationToken,
) {
    let identity = match gce_identity().await {
        Ok(identity) => identity,
        Err(error) => {
            error!(%error, "GCE identity unavailable");
            if allow_unknown_gce_metrics {
                let identity = GceIdentity {
                    instance_id: UNKNOWN_GCE_IDENTITY_INSTANCE_ID.to_string(),
                    zone: UNKNOWN_GCE_IDENTITY_ZONE.to_string(),
                };
                warn!("Using unknown GCE identity for metrics: {identity:?}");
                identity
            } else {
                error!("Cloud Monitoring publisher skipped");
                return;
            }
        }
    };
    let client = match MetricService::builder()
        .with_retry_policy(AlwaysRetry.with_attempt_limit(u32::from(max_retries)))
        .build()
        .await
    {
        Ok(client) => client,
        Err(error) => {
            error!(%error, "Cloud Monitoring publisher skipped; client failed to build");
            return;
        }
    };
    let Ok(started) = timestamp_now() else {
        error!("Cloud Monitoring publisher skipped; system clock is before the unix epoch");
        return;
    };

    let mut ticker = tokio::time::interval(PUBLISH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    let name = format!("projects/{project_id}");
    let mut pipeline = PipelineSampler::new(stats);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => {
                publish_sample(&client, &name, &project_id, &identity, &started, &metrics, &mut pipeline).await;
                info!("Final Cloud Monitoring write-metrics sample sent");
                return;
            }
            _ = ticker.tick() => {
                publish_sample(&client, &name, &project_id, &identity, &started, &metrics, &mut pipeline).await;
            }
        }
    }
}

async fn publish_sample(
    client: &MetricService,
    name: &str,
    project_id: &str,
    identity: &GceIdentity,
    started: &Timestamp,
    metrics: &WriteMetrics,
    pipeline: &mut PipelineSampler,
) {
    let Ok(end) = timestamp_now() else {
        error!("Skipping Cloud Monitoring sample; system clock is before the unix epoch");
        return;
    };
    let snapshot = metrics.snapshot();
    let mut series = write_time_series(project_id, identity, started, &end, &snapshot);
    let sample = pipeline.sample(end);
    series.extend(pipeline_time_series(project_id, identity, started, &sample));
    let report = sample.report().to_string();
    match client
        .create_time_series()
        .set_name(name)
        .set_time_series(series)
        .send()
        .await
    {
        Ok(()) => {
            info!("{snapshot:?}");
            info!("Pipeline report: {report}");
        }
        Err(error) => {
            warn!(%error, "GCM write failed; the persist latency window of this sample is lost. Pipeline report: {report}");
        }
    }
}

fn timestamp_now() -> std::result::Result<Timestamp, TimestampError> {
    Timestamp::try_from(SystemTime::now())
}

async fn gce_identity() -> Result<GceIdentity> {
    let client = reqwest::Client::builder()
        .timeout(METADATA_TIMEOUT)
        .build()
        .context("failed to build metadata client")?;
    let instance_id = metadata_get(&client, "id").await?;
    let zone_path = metadata_get(&client, "zone").await?;
    let zone = zone_name(&zone_path)
        .context("GCE zone metadata did not contain a zone name")?
        .to_string();
    Ok(GceIdentity { instance_id, zone })
}

async fn metadata_get(client: &reqwest::Client, path: &str) -> Result<String> {
    let body = client
        .get(format!("{METADATA_ROOT}/{path}"))
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .with_context(|| format!("metadata request for {path} failed"))?
        .error_for_status()
        .with_context(|| format!("metadata request for {path} returned an error status"))?
        .text()
        .await
        .with_context(|| format!("metadata body for {path} was not text"))?;
    let trimmed = body.trim();
    if trimmed.is_empty() {
        anyhow::bail!("metadata {path} was empty");
    }
    Ok(trimmed.to_string())
}

#[must_use]
fn zone_name(metadata_zone: &str) -> Option<&str> {
    let zone = metadata_zone.rsplit('/').next()?.trim();
    if zone.is_empty() { None } else { Some(zone) }
}

#[must_use]
fn write_time_series(
    project_id: &str,
    identity: &GceIdentity,
    started: &Timestamp,
    end: &Timestamp,
    snapshot: &WriteMetricSnapshot,
) -> Vec<TimeSeries> {
    let resource = gce_resource(project_id, identity);
    vec![
        cumulative_series(
            WRITE_LINES,
            "1",
            &resource,
            started,
            end,
            snapshot.lines,
            None,
        ),
        cumulative_series(
            WRITE_BYTES,
            "By",
            &resource,
            started,
            end,
            snapshot.bytes,
            None,
        ),
        cumulative_series(
            WRITE_REQUESTS,
            "1",
            &resource,
            started,
            end,
            snapshot.requests_ok,
            Some("ok"),
        ),
        cumulative_series(
            WRITE_REQUESTS,
            "1",
            &resource,
            started,
            end,
            snapshot.requests_partial,
            Some("partial"),
        ),
        cumulative_series(
            WRITE_REQUESTS,
            "1",
            &resource,
            started,
            end,
            snapshot.requests_error,
            Some("error"),
        ),
        cumulative_series(
            WRITE_REQUESTS,
            "1",
            &resource,
            started,
            end,
            snapshot.requests_retry,
            Some("retry"),
        ),
    ]
}

/// Pipeline stats since the previous sample: cumulative counters, the gauge, and the persist
/// latency window (delta).
struct PipelineSampler {
    stats: Arc<PipelineStats>,
    previous: PipelineSnapshot,
    previous_at: Instant,
}

struct PipelineSample {
    previous: PipelineSnapshot,
    current: PipelineSnapshot,
    persist: HistogramWindow,
    window_end: Timestamp,
    window: Duration,
}

impl PipelineSampler {
    fn new(stats: Arc<PipelineStats>) -> Self {
        Self {
            previous: stats.snapshot(),
            stats,
            previous_at: Instant::now(),
        }
    }

    fn sample(&mut self, end: Timestamp) -> PipelineSample {
        let now = Instant::now();
        let sample = PipelineSample {
            previous: self.previous,
            current: self.stats.snapshot(),
            persist: self.stats.take_persist(),
            window_end: end,
            window: now.duration_since(self.previous_at),
        };
        self.previous = sample.current;
        self.previous_at = now;
        sample
    }
}

impl PipelineSample {
    fn report(&self) -> PipelineReport<'_> {
        PipelineReport {
            window: self.window,
            previous: &self.previous,
            current: &self.current,
            persist: &self.persist,
        }
    }
}

#[must_use]
fn pipeline_time_series(
    project_id: &str,
    identity: &GceIdentity,
    started: &Timestamp,
    sample: &PipelineSample,
) -> Vec<TimeSeries> {
    let resource = gce_resource(project_id, identity);
    let end = &sample.window_end;
    let current = &sample.current;
    let mut series = vec![
        cumulative_series(
            PIPELINE_CONSUMED_MESSAGES,
            "1",
            &resource,
            started,
            end,
            current.consumed_messages,
            None,
        ),
        cumulative_series(
            PIPELINE_SINK_BLOCKED_MS,
            "ms",
            &resource,
            started,
            end,
            u64::try_from(current.sink_blocked.as_millis()).unwrap_or(u64::MAX),
            None,
        ),
    ];
    // Unknown until the first librdkafka statistics arrive: no point rather than a false zero.
    if let Some(unconsumed) = current.unconsumed {
        series.push(series_with_point(
            PIPELINE_UNCONSUMED_MESSAGES,
            "1",
            &resource,
            MetricKind::Gauge,
            None,
            end,
            TypedValue::new().set_int64_value(unconsumed),
            None,
        ));
    }
    if sample.persist.count() > 0 {
        series.push(series_with_point(
            PIPELINE_PERSIST_LATENCY_MS,
            "ms",
            &resource,
            MetricKind::Gauge,
            None,
            end,
            TypedValue::new().set_distribution_value(latency_distribution(&sample.persist)),
            None,
        ));
    }
    series
}

/// Explicit buckets at the histogram bounds. Cloud Monitoring buckets are `[lower, upper)`
/// while ours are inclusive of the upper bound, so a value equal to a bound sits one bucket
/// higher there. The sum of squared deviation is not tracked and is reported as 0.
fn latency_distribution(window: &HistogramWindow) -> Distribution {
    let count = window.count();
    #[allow(clippy::cast_precision_loss)]
    let mean = window.sum_ms() as f64 / count.max(1) as f64;
    Distribution::new()
        .set_count(as_i64(count))
        .set_mean(mean)
        .set_sum_of_squared_deviation(0.0)
        .set_bucket_options(BucketOptions::new().set_explicit_buckets(
            #[allow(clippy::cast_precision_loss)]
            Explicit::new().set_bounds(BUCKET_BOUNDS_MS.iter().map(|bound| *bound as f64)),
        ))
        .set_bucket_counts(window.bucket_counts().iter().map(|count| as_i64(*count)))
}

fn gce_resource(project_id: &str, identity: &GceIdentity) -> MonitoredResource {
    MonitoredResource::new()
        .set_type("gce_instance")
        .set_labels([
            ("project_id", project_id),
            ("instance_id", identity.instance_id.as_str()),
            ("zone", identity.zone.as_str()),
        ])
}

fn cumulative_series(
    metric_type: &str,
    unit: &str,
    resource: &MonitoredResource,
    started: &Timestamp,
    end: &Timestamp,
    value: u64,
    outcome: Option<&str>,
) -> TimeSeries {
    series_with_point(
        metric_type,
        unit,
        resource,
        MetricKind::Cumulative,
        Some(started),
        end,
        TypedValue::new().set_int64_value(as_i64(value)),
        outcome,
    )
}

#[allow(clippy::too_many_arguments)]
fn series_with_point(
    metric_type: &str,
    unit: &str,
    resource: &MonitoredResource,
    kind: MetricKind,
    start: Option<&Timestamp>,
    end: &Timestamp,
    value: TypedValue,
    outcome: Option<&str>,
) -> TimeSeries {
    let mut metric = Metric::new().set_type(metric_type);
    if let Some(outcome) = outcome {
        metric = metric.set_labels([("outcome", outcome)]);
    }
    let value_type = if value.distribution_value().is_some() {
        ValueType::Distribution
    } else {
        ValueType::Int64
    };
    let mut interval = TimeInterval::new().set_end_time(*end);
    if let Some(start) = start {
        interval = interval.set_start_time(*start);
    }
    TimeSeries::new()
        .set_metric(metric)
        .set_resource(resource.clone())
        .set_metric_kind(kind)
        .set_value_type(value_type)
        .set_unit(unit)
        .set_points([Point::new().set_interval(interval).set_value(value)])
}

fn as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_identity() -> GceIdentity {
        GceIdentity {
            instance_id: "123".to_string(),
            zone: "europe-central2-a".to_string(),
        }
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("valid timestamp")
    }

    #[test]
    fn zone_name_uses_the_last_path_segment() {
        assert_eq!(
            zone_name("projects/99/zones/europe-central2-a"),
            Some("europe-central2-a")
        );
        assert_eq!(zone_name("europe-central2-a"), Some("europe-central2-a"));
        assert_eq!(zone_name("projects/99/zones/"), None);
        assert_eq!(zone_name(""), None);
    }

    #[test]
    fn write_time_series_emits_cumulative_counters() {
        let started = timestamp(1_700_000_000);
        let end = timestamp(1_700_000_060);
        let snapshot = WriteMetricSnapshot {
            lines: 12,
            bytes: 340,
            requests_ok: 2,
            requests_partial: 1,
            requests_error: 4,
            requests_retry: 7,
        };
        let series =
            write_time_series("chronoracer", &sample_identity(), &started, &end, &snapshot);

        assert_eq!(series.len(), 6);
        assert_eq!(
            series[0]
                .metric
                .as_ref()
                .map(|metric| metric.r#type.as_str()),
            Some(WRITE_LINES)
        );
        assert_eq!(
            series[1]
                .metric
                .as_ref()
                .map(|metric| metric.r#type.as_str()),
            Some(WRITE_BYTES)
        );
        assert_eq!(series[0].unit, "1");
        assert_eq!(series[1].unit, "By");
        assert_eq!(point_value(&series[0]), Some(12));
        assert_eq!(point_value(&series[1]), Some(340));

        let outcomes: Vec<(&str, i64)> = series[2..]
            .iter()
            .map(|series| {
                let outcome = series
                    .metric
                    .as_ref()
                    .and_then(|metric| metric.labels.get("outcome"))
                    .map_or("", String::as_str);
                (outcome, point_value(series).unwrap_or(-1))
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![("ok", 2), ("partial", 1), ("error", 4), ("retry", 7)]
        );

        let resource = series[0].resource.as_ref().expect("resource");
        assert_eq!(resource.r#type, "gce_instance");
        assert_eq!(
            resource.labels.get("project_id").map(String::as_str),
            Some("chronoracer")
        );
        assert_eq!(
            resource.labels.get("instance_id").map(String::as_str),
            Some("123")
        );
        assert_eq!(
            resource.labels.get("zone").map(String::as_str),
            Some("europe-central2-a")
        );
        assert_eq!(series[0].metric_kind, MetricKind::Cumulative);
        assert_eq!(series[0].value_type, ValueType::Int64);
        let interval = series[0].points[0].interval.as_ref().expect("interval");
        assert_eq!(interval.start_time, Some(started));
        assert_eq!(interval.end_time, Some(end));
    }

    #[test]
    fn pipeline_time_series_covers_counters_gauge_and_latency_distribution() {
        use crate::pipeline_stats::unix_now_ms;
        let stats = Arc::new(PipelineStats::default());
        let mut sampler = PipelineSampler::new(Arc::clone(&stats));
        stats.add_consumed(30);
        stats.add_sink_blocked(Duration::from_millis(1_500));
        stats.set_unconsumed(12);
        stats.record_settled(Some(unix_now_ms() - 7));
        stats.record_settled(Some(unix_now_ms() - 7));

        let sample = sampler.sample(timestamp(1_700_000_060));
        let series = pipeline_time_series(
            "chronoracer",
            &sample_identity(),
            &timestamp(1_700_000_000),
            &sample,
        );
        let types: Vec<&str> = series
            .iter()
            .map(|series| series.metric.as_ref().unwrap().r#type.as_str())
            .collect();
        assert_eq!(
            types,
            [
                PIPELINE_CONSUMED_MESSAGES,
                PIPELINE_SINK_BLOCKED_MS,
                PIPELINE_UNCONSUMED_MESSAGES,
                PIPELINE_PERSIST_LATENCY_MS
            ]
        );
        assert_eq!(point_value(&series[0]), Some(30));
        assert_eq!(point_value(&series[1]), Some(1_500));
        assert_eq!(series[2].metric_kind, MetricKind::Gauge);
        assert_eq!(point_value(&series[2]), Some(12));
        let gauge_interval = series[2].points[0].interval.as_ref().unwrap();
        assert_eq!(gauge_interval.start_time, None);

        let latency = &series[3];
        assert_eq!(latency.metric_kind, MetricKind::Gauge);
        assert_eq!(latency.value_type, ValueType::Distribution);
        let interval = latency.points[0].interval.as_ref().unwrap();
        assert_eq!(interval.start_time, None);
        let distribution = latency.points[0]
            .value
            .as_ref()
            .unwrap()
            .distribution_value()
            .unwrap();
        assert_eq!(distribution.count, 2);
        assert_eq!(distribution.bucket_counts.len(), BUCKET_BOUNDS_MS.len() + 1);
        assert_eq!(distribution.bucket_counts.iter().sum::<i64>(), 2);

        // Next window starts where this one ended; with nothing recorded, no latency series.
        let next = sampler.sample(timestamp(1_700_000_120));
        let next_end = timestamp(1_700_000_120);
        let series = pipeline_time_series(
            "chronoracer",
            &sample_identity(),
            &timestamp(1_700_000_000),
            &next,
        );
        assert_eq!(next.window_end, next_end);
        assert_eq!(series.len(), 3);
    }

    #[test]
    fn unknown_unconsumed_emits_no_gauge() {
        let stats = Arc::new(PipelineStats::default());
        let mut sampler = PipelineSampler::new(stats);
        let sample = sampler.sample(timestamp(2));
        let series = pipeline_time_series("p", &sample_identity(), &timestamp(1), &sample);
        assert_eq!(series.len(), 2);
    }

    fn point_value(series: &TimeSeries) -> Option<i64> {
        series
            .points
            .first()?
            .value
            .as_ref()?
            .int64_value()
            .copied()
    }
}
