use super::*;
use crate::processing::sensor_data_message::{AbsoluteSensorReading, UnwoundSensorDataMessage};
use crate::runtime_configuration::RetainReadTimeValue;
use crate::telemetry::proto::WatchRequest;
use crate::telemetry::proto::stream_event::Payload;
use crate::telemetry::proto::telemetry_client::TelemetryClient;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Endpoint;

fn preview(max_points: usize) -> PreviewHub {
    let driver = Box::leak(Box::new(RetainReadTimeValue::new(CompactString::from(
        "drv",
    ))));
    PreviewHub::new(driver, max_points, Duration::from_secs(30))
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
    let mut first_stream = first.watch(WatchRequest {}).await.unwrap().into_inner();
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
        .watch(WatchRequest {})
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
    let mut stream = client.watch(WatchRequest {}).await.unwrap().into_inner();
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
