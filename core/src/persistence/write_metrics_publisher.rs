use super::write_metrics::{WriteMetricSnapshot, WriteMetrics};
use anyhow::{Context, Result};
use google_cloud_api::model::metric_descriptor::{MetricKind, ValueType};
use google_cloud_api::model::{Metric, MonitoredResource};
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_monitoring_v3::client::MetricService;
use google_cloud_monitoring_v3::model::{Point, TimeInterval, TimeSeries, TypedValue};
use google_cloud_wkt::{Timestamp, TimestampError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub const WRITE_LINES: &str = "custom.googleapis.com/chronoracer/influx_write_lines";
pub const WRITE_BYTES: &str = "custom.googleapis.com/chronoracer/influx_write_bytes";
pub const WRITE_REQUESTS: &str = "custom.googleapis.com/chronoracer/influx_write_requests";

const PUBLISH_INTERVAL: Duration = Duration::from_mins(1);
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

/// Publishes cumulative write counters until `stop` is cancelled, then sends one final sample.
/// Metadata or client setup failure is logged and the task ends. A later
/// `CreateTimeSeries` failure is logged and the loop continues. Neither path
/// stops ingest.
pub async fn publish_write_metrics(
    project_id: String,
    max_retries: u16,
    metrics: Arc<WriteMetrics>,
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
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => {
                publish_sample(&client, &name, &project_id, &identity, &started, &metrics).await;
                info!("Final Cloud Monitoring write-metrics sample sent");
                return;
            }
            _ = ticker.tick() => {
                publish_sample(&client, &name, &project_id, &identity, &started, &metrics).await;
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
) {
    let Ok(end) = timestamp_now() else {
        error!("Skipping Cloud Monitoring sample; system clock is before the unix epoch");
        return;
    };
    let snapshot = metrics.snapshot();
    let series = write_time_series(project_id, identity, started, &end, &snapshot);
    match client
        .create_time_series()
        .set_name(name)
        .set_time_series(series)
        .send()
        .await
    {
        Ok(()) => {
            info!("{snapshot:?}");
        }
        Err(error) => {
            warn!(%error, "GCM write failed");
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
    let mut metric = Metric::new().set_type(metric_type);
    if let Some(outcome) = outcome {
        metric = metric.set_labels([("outcome", outcome)]);
    }
    TimeSeries::new()
        .set_metric(metric)
        .set_resource(resource.clone())
        .set_metric_kind(MetricKind::Cumulative)
        .set_value_type(ValueType::Int64)
        .set_unit(unit)
        .set_points([Point::new()
            .set_interval(
                TimeInterval::new()
                    .set_start_time(*started)
                    .set_end_time(*end),
            )
            .set_value(TypedValue::new().set_int64_value(as_i64(value)))])
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
        assert_eq!(outcomes, vec![("ok", 2), ("partial", 1), ("error", 4), ("retry", 7)]);

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
