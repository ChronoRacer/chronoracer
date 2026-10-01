use super::*;
use crate::processing::sensor_data_message::{AbsoluteSensorReading, UnwoundSensorDataMessage};
use crate::runtime_configuration::RetainReadTimeValue;
use crate::telemetry::proto::stream_event::Payload;
use crate::telemetry::proto::watch_request::Selection;
use crate::telemetry::proto::{AllSensors, SensorList, WatchRequest};
use crate::telemetry::proto::telemetry_client::TelemetryClient;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Endpoint;

fn preview(max_points: usize) -> PreviewHub {
    preview_with_slots(max_points, 4)
}

fn preview_with_slots(max_points: usize, max_subscribers: usize) -> PreviewHub {
    let driver = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
        "drv",
    ))));
    PreviewHub::new(
        driver,
        max_points,
        Duration::from_secs(30),
        max_subscribers,
    )
}

fn watch_all(client_id: &str) -> WatchRequest {
    WatchRequest {
        client_id: client_id.to_owned(),
        selection: Some(Selection::All(AllSensors {})),
    }
}

fn watch_sensors(client_id: &str, sensors: &[&str]) -> WatchRequest {
    WatchRequest {
        client_id: client_id.to_owned(),
        selection: Some(Selection::Sensors(SensorList {
            sensor_ids: sensors.iter().map(|sensor| (*sensor).to_owned()).collect(),
        })),
    }
}

fn message(sensor_id: &str, points: u64) -> Arc<UnwoundSensorDataMessage> {
    let readings = (0..points)
        .map(|index| AbsoluteSensorReading {
            value: 0.0,
            timestamp: index,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Arc::new(UnwoundSensorDataMessage {
        sensor_id: Arc::from(sensor_id),
        readings,
        source: Default::default(),
    })
}

async fn start(hub: PreviewHub) -> (CancellationToken, String) {
    let cancel = CancellationToken::new();
    let cancel_server = cancel.clone();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let race_id = CompactString::from("race-1");
    tokio::spawn(async move {
        accept_loop(
            hub,
            cancel_server,
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            race_id,
            Some(bound_tx),
        )
        .await;
    });
    let addr = bound_rx.await.unwrap();
    (cancel, format!("http://{addr}"))
}

fn fat_batch(index: usize) -> Arc<UnwoundSensorDataMessage> {
    let sensor_id = format!("{index:04}-{}", "s".repeat(80));
    let readings = (1..=1024)
        .map(|timestamp| AbsoluteSensorReading {
            value: 1.0,
            timestamp,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Arc::new(UnwoundSensorDataMessage {
        sensor_id: Arc::from(sensor_id),
        readings,
        source: Default::default(),
    })
}

#[test]
fn point_batch_emits_only_the_chunk_span() {
    let mut pending = VecDeque::new();
    push_ready(
        &mut pending,
        None,
        &[Chunk {
            message: message("speed", 3),
            driver_id: Arc::new(CompactString::from("drv")),
            first_sequence: 5,
            start: 1,
            len: 1,
        }],
        None,
    );
    let event = pending.pop_front().unwrap();
    let Payload::PointBatch(batch) = event.payload.unwrap() else {
        panic!("expected a point batch");
    };
    assert_eq!(batch.first_sequence, 5);
    assert_eq!(batch.points.len(), 1);
    assert_eq!(batch.points[0].sensor_id, "speed");
    assert_eq!(batch.points[0].timestamp_nanos, 1);
    assert!(pending.is_empty());
}

async fn connect(url: &str) -> TelemetryClient<tonic::transport::Channel> {
    connect_with_window(url, HTTP2_WINDOW_SIZE).await
}

async fn connect_with_window(url: &str, window: u32) -> TelemetryClient<tonic::transport::Channel> {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .connect()
        .await
        .unwrap();
    TelemetryClient::new(channel)
}

#[tokio::test]
async fn second_watch_gets_a_fresh_hello_and_not_the_old_queue() {
    let hub = preview(1000);
    let (_cancel, url) = start(hub.clone()).await;
    let mut first = connect(&url).await;
    let mut second_slot = connect(&url).await;
    let mut first_stream = first
        .watch(watch_all("telemetry"))
        .await
        .unwrap()
        .into_inner();
    let hello = first_stream.message().await.unwrap().unwrap();
    let Payload::Hello(hello) = hello.payload.unwrap() else {
        panic!("first event should be hello");
    };
    assert_eq!(hello.protocol_version, 1);
    assert_eq!(hello.race_id, "race-1");
    assert_eq!(hello.next_sequence, 1);
    let stream_id = hello.stream_id;
    hub.offer(&[message("old", 2)]);

    let mut second_stream = second_slot
        .watch(watch_all("telemetry"))
        .await
        .unwrap()
        .into_inner();
    let hello = second_stream.message().await.unwrap().unwrap();
    let Payload::Hello(hello) = hello.payload.unwrap() else {
        panic!("replacement should start with hello");
    };
    assert_eq!(hello.stream_id, stream_id);
    assert_eq!(hello.next_sequence, 1);

    // A batch already taken from the queue can still arrive. The call then ends.
    // Anything still queued is dropped and must not appear here or on the replacement.
    let mut ended = false;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(2), first_stream.message())
            .await
            .expect("replaced watch did not end")
        {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::Aborted);
                ended = true;
                break;
            }
            Ok(None) => {
                ended = true;
                break;
            }
            Ok(Some(event)) => {
                let Some(Payload::PointBatch(batch)) = event.payload else {
                    panic!("replaced watch sent {event:?}");
                };
                assert!(
                    batch.points.iter().all(|point| point.sensor_id == "old"),
                    "replaced watch sent {batch:?}"
                );
            }
        }
    }
    assert!(ended, "replaced watch did not end");

    hub.offer(&[message("new", 1)]);
    let event = tokio::time::timeout(Duration::from_secs(2), second_stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Payload::PointBatch(batch) = event.payload.unwrap() else {
        panic!("expected the new point");
    };
    assert_eq!(batch.first_sequence, 1);
    assert_eq!(batch.points.len(), 1);
    assert_eq!(batch.points[0].sensor_id, "new");
    assert_eq!(batch.points[0].driver_id, "drv");
}

#[tokio::test]
async fn offer_returns_while_the_socket_write_is_blocked() {
    let hub = preview(16_384);
    let (_cancel, url) = start(hub.clone()).await;
    let mut client = connect_with_window(&url, 32 * 1024).await;
    let mut stream = client
        .watch(watch_all("telemetry"))
        .await
        .unwrap()
        .into_inner();
    let hello = stream.message().await.unwrap().unwrap();
    assert!(matches!(hello.payload, Some(Payload::Hello(_))));

    let offered = 16 * 1024;
    for index in 0..16 {
        hub.offer(&[fat_batch(index)]);
    }
    let queued = wait_until_sender_stalls(&hub, offered).await;

    let hub_for_block = hub.clone();
    let started = Instant::now();
    let blocked_offer = tokio::task::spawn_blocking(move || {
        hub_for_block.offer(&[fat_batch(16)]);
    });
    tokio::time::timeout(Duration::from_millis(500), blocked_offer)
        .await
        .expect("offer waited on the socket")
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(500));
    let after = hub.queued_points();
    assert!(
        after > offered / 2,
        "backlog disappeared while the client was not reading, before {queued}, after {after}"
    );
}

async fn wait_until_sender_stalls(hub: &PreviewHub, offered: usize) -> usize {
    let mut previous = hub.queued_points();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let queued = hub.queued_points();
        if queued < offered && queued > offered / 2 && queued == previous {
            return queued;
        }
        previous = queued;
    }
    panic!("sender did not stall with a backlog, queued {previous}");
}

async fn next_point_batch(
    stream: &mut tonic::Streaming<StreamEvent>,
) -> crate::telemetry::proto::PointBatch {
    let event = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("no event arrived")
        .unwrap()
        .unwrap();
    let Some(Payload::PointBatch(batch)) = event.payload else {
        panic!("expected a point batch, got {event:?}");
    };
    batch
}

async fn hello_of(stream: &mut tonic::Streaming<StreamEvent>) -> Hello {
    let event = stream.message().await.unwrap().unwrap();
    let Some(Payload::Hello(hello)) = event.payload else {
        panic!("expected hello, got {event:?}");
    };
    hello
}

#[tokio::test]
async fn two_clients_each_receive_their_own_selection() {
    let hub = preview(1000);
    let (_cancel, url) = start(hub.clone()).await;
    let mut telemetry = connect(&url).await;
    let mut analysis = connect(&url).await;
    let mut telemetry_stream = telemetry
        .watch(watch_all("telemetry"))
        .await
        .unwrap()
        .into_inner();
    let mut analysis_stream = analysis
        .watch(watch_sensors("analysis", &["speed", "speed", "rpm"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hello_of(&mut telemetry_stream).await.selected_sensor_count, 0);
    assert_eq!(hello_of(&mut analysis_stream).await.selected_sensor_count, 2);

    hub.offer(&[message("speed", 2), message("temp", 1), message("rpm", 1)]);

    let batch = next_point_batch(&mut telemetry_stream).await;
    assert_eq!(batch.first_sequence, 1);
    let sensors: Vec<_> = batch.points.iter().map(|p| p.sensor_id.as_str()).collect();
    assert_eq!(sensors, ["speed", "speed", "temp", "rpm"]);

    let batch = next_point_batch(&mut analysis_stream).await;
    assert_eq!(batch.first_sequence, 1);
    let sensors: Vec<_> = batch.points.iter().map(|p| p.sensor_id.as_str()).collect();
    assert_eq!(sensors, ["speed", "speed", "rpm"]);
}

#[tokio::test]
async fn watch_rejects_invalid_requests() {
    let hub = preview(1000);
    let (_cancel, url) = start(hub.clone()).await;
    let mut client = connect(&url).await;
    let too_many: Vec<String> = (0..=MAX_SELECTED_SENSORS).map(|i| format!("s{i}")).collect();
    let too_many: Vec<&str> = too_many.iter().map(String::as_str).collect();
    let long_sensor = "x".repeat(MAX_SENSOR_ID_BYTES + 1);
    let long_client = "c".repeat(MAX_CLIENT_ID_BYTES + 1);
    let requests = [
        WatchRequest {
            client_id: "client".to_owned(),
            selection: None,
        },
        watch_all(""),
        watch_all(&long_client),
        watch_sensors("client", &[]),
        watch_sensors("client", &[""]),
        watch_sensors("client", &[long_sensor.as_str()]),
        watch_sensors("client", &too_many),
    ];
    for request in requests {
        let status = client.watch(request.clone()).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{request:?}");
    }
    // A full, valid list still passes: duplicates do not count twice.
    let mut at_limit: Vec<&str> = too_many[..MAX_SELECTED_SENSORS].to_vec();
    at_limit.push(too_many[0]);
    let mut stream = client
        .watch(watch_sensors("client", &at_limit))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        hello_of(&mut stream).await.selected_sensor_count,
        u32::try_from(MAX_SELECTED_SENSORS).unwrap()
    );
}

#[tokio::test]
async fn watch_beyond_the_subscriber_cap_is_refused_and_nobody_is_evicted() {
    let hub = preview_with_slots(1000, 2);
    let (_cancel, url) = start(hub.clone()).await;
    let mut first = connect(&url).await;
    let mut second = connect(&url).await;
    let mut third = connect(&url).await;
    let mut first_stream = first.watch(watch_all("one")).await.unwrap().into_inner();
    let _second_stream = second.watch(watch_all("two")).await.unwrap().into_inner();
    let status = third.watch(watch_all("three")).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    hello_of(&mut first_stream).await;
    hub.offer(&[message("still-here", 1)]);
    let batch = next_point_batch(&mut first_stream).await;
    assert_eq!(batch.points[0].sensor_id, "still-here");
}

#[tokio::test]
async fn replaced_watch_yields_aborted_and_then_ends() {
    let hub = preview(1000);
    let subscriber = hub
        .subscribe("telemetry", crate::telemetry::queue::Filter::All)
        .unwrap();
    let _replacement = hub
        .subscribe("telemetry", crate::telemetry::queue::Filter::All)
        .unwrap();
    let state = StreamState {
        race_id: "race-1".to_owned(),
        stream_id: hub.stream_id().to_owned(),
        next_sequence: FIRST_SEQUENCE,
        selected_sensor_count: 0,
        guard: SessionGuard {
            hub: hub.clone(),
            subscriber,
        },
        hello_sent: false,
        pending: VecDeque::new(),
        cancel: CancellationToken::new(),
    };

    let (hello, drive) = poll_step(Drive::Live(state)).await.unwrap();
    assert!(matches!(hello.unwrap().payload, Some(Payload::Hello(_))));

    let (aborted, drive) = poll_step(drive).await.unwrap();
    let status = aborted.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Aborted);
    assert_eq!(status.message(), "replaced by a new watch");

    assert!(poll_step(drive).await.is_none());
}
