use crate::processing::sensor_data_message::MAX_SENSOR_ID_BYTES;
use crate::telemetry::proto::telemetry_service_server::{TelemetryService, TelemetryServiceServer};
use crate::telemetry::proto::watch_request::Selection;
use crate::telemetry::proto::watch_response::Payload;
use crate::telemetry::proto::{Gap, Hello, Point, PointBatch, WatchRequest, WatchResponse};
use crate::telemetry::queue::{
    Chunk, Drain, FIRST_SEQUENCE, Filter, GapSpan, MAX_POINTS_PER_BATCH, PreviewHub,
    SubscribeError, Subscriber,
};
use compact_str::CompactString;
use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{error, info};

const HTTP2_WINDOW_SIZE: u32 = 256 * 1024;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_STREAMS: u32 = 2;
const PROTOCOL_VERSION: u32 = 1;
const BIND_RETRY: Duration = Duration::from_secs(1);
/// First bind failure is logged immediately; later ones every this many retries.
const BIND_LOG_EVERY: u32 = 30;

/// `client_id` limit in bytes.
const MAX_CLIENT_ID_BYTES: usize = 64;
/// Distinct sensors one `Watch` may select.
const MAX_SELECTED_SENSORS: usize = 100;
/// Largest `WatchRequest` the server decodes: a full sensor list plus headroom.
const MAX_REQUEST_BYTES: usize = 64 * 1024;

struct SessionGuard {
    hub: PreviewHub,
    subscriber: Arc<Subscriber>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.hub.unsubscribe(&self.subscriber);
    }
}

struct StreamState {
    guard: SessionGuard,
    race_id: String,
    stream_id: String,
    next_sequence: u64,
    selected_sensor_count: u32,
    hello_sent: bool,
    pending: VecDeque<WatchResponse>,
    cancel: CancellationToken,
}

/// Plaintext h2c `Watch` server. Not supervised by the data pipeline or the control tasks (but has the `CancellationToken` of the data pipeline)
pub async fn serve(hub: PreviewHub, cancel: CancellationToken, port: u16, race_id: CompactString) {
    accept_loop(
        hub,
        cancel,
        SocketAddr::from(([0, 0, 0, 0], port)),
        race_id,
        None,
    )
    .await;
}

async fn accept_loop(
    hub: PreviewHub,
    cancel: CancellationToken,
    addr: SocketAddr,
    race_id: CompactString,
    mut bound_tx: Option<oneshot::Sender<SocketAddr>>,
) {
    let pump_hub = hub.clone();
    let pump_cancel = cancel.clone();
    tokio::spawn(async move {
        run_pump(pump_hub, pump_cancel).await;
    });
    let mut failures: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match run_server(&hub, &cancel, addr, &race_id, &mut bound_tx).await {
            Ok(()) => return,
            Err(error) => {
                if failures.is_multiple_of(BIND_LOG_EVERY) {
                    error!(
                        %error,
                        %addr,
                        attempt = failures.saturating_add(1),
                        "telemetry listener stopped; retrying"
                    );
                }
                failures = failures.saturating_add(1);
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(BIND_RETRY) => {}
                }
            }
        }
    }
}

async fn run_server(
    hub: &PreviewHub,
    cancel: &CancellationToken,
    addr: SocketAddr,
    race_id: &CompactString,
    bound_tx: &mut Option<oneshot::Sender<SocketAddr>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    if let Some(tx) = bound_tx.take() {
        let _ = tx.send(bound);
    }
    info!(%bound, "gRPC server started - listener bound");
    // tonic's `tcp_nodelay` applies only to its own listener. This server accepts on
    // `TcpListenerStream`, so Nagle would hold small preview frames unless set here.
    let incoming = TcpListenerStream::new(listener).map(|accepted| {
        accepted.inspect(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                error!(%error, "failed to set TCP_NODELAY on telemetry connection");
            }
        })
    });
    let service = WatchService {
        hub: hub.clone(),
        race_id: race_id.clone(),
        cancel: cancel.clone(),
    };
    // No compression codec is registered.
    Server::builder()
        .initial_stream_window_size(HTTP2_WINDOW_SIZE)
        .initial_connection_window_size(HTTP2_WINDOW_SIZE)
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .add_service(
            TelemetryServiceServer::new(service).max_decoding_message_size(MAX_REQUEST_BYTES),
        )
        .serve_with_incoming_shutdown(incoming, cancel.clone().cancelled_owned())
        .await?;
    Ok(())
}

/// Sequence the handoff even when HTTP/2 is not polling any stream, so age and the point cap
/// still apply while a client is stuck. `pull_ingress` wakes the subscribers it changed.
async fn run_pump(hub: PreviewHub, cancel: CancellationToken) {
    let tick = hub.max_age().max(Duration::from_millis(1));
    let notify = hub.notifier();
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        hub.pull_ingress();
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            () = &mut notified => {}
            () = tokio::time::sleep(tick) => {}
        }
    }
}

struct WatchService {
    hub: PreviewHub,
    race_id: CompactString,
    cancel: CancellationToken,
}

#[tonic::async_trait]
impl TelemetryService for WatchService {
    type WatchStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<WatchResponse, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (client_id, filter) = parse_request(request.into_inner())?;
        let subscriber =
            self.hub
                .subscribe(&client_id, filter)
                .map_err(|SubscribeError::Full| {
                    Status::resource_exhausted("every telemetry subscriber slot is in use")
                })?;
        info!(
            client_id = subscriber.client_id(),
            selected_sensors = subscriber.selected_sensor_count(),
            "New subscriber Watch started"
        );
        let state = StreamState {
            race_id: self.race_id.to_string(),
            stream_id: self.hub.stream_id().to_owned(),
            next_sequence: FIRST_SEQUENCE,
            selected_sensor_count: u32::try_from(subscriber.selected_sensor_count())
                .unwrap_or(u32::MAX),
            guard: SessionGuard {
                hub: self.hub.clone(),
                subscriber,
            },
            hello_sent: false,
            pending: VecDeque::new(),
            cancel: self.cancel.clone(),
        };
        let stream = futures_util::stream::unfold(Drive::Live(state), poll_step);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Check the request before any subscriber state changes.
fn parse_request(request: WatchRequest) -> Result<(String, Filter), Status> {
    let WatchRequest {
        client_id,
        selection,
    } = request;
    if client_id.is_empty() || client_id.len() > MAX_CLIENT_ID_BYTES {
        return Err(Status::invalid_argument(format!(
            "client_id must be 1..={MAX_CLIENT_ID_BYTES} bytes"
        )));
    }
    let filter = match selection {
        None => {
            return Err(Status::invalid_argument(
                "selection is required: choose all sensors or a sensor list",
            ));
        }
        Some(Selection::All(_)) => Filter::All,
        Some(Selection::Sensors(list)) => {
            let mut distinct = HashSet::new();
            for sensor_id in list.sensor_ids {
                if sensor_id.is_empty() || sensor_id.len() > MAX_SENSOR_ID_BYTES {
                    return Err(Status::invalid_argument(format!(
                        "sensor ids must be 1..={MAX_SENSOR_ID_BYTES} bytes"
                    )));
                }
                distinct.insert(CompactString::from(sensor_id));
                if distinct.len() > MAX_SELECTED_SENSORS {
                    return Err(Status::invalid_argument(format!(
                        "at most {MAX_SELECTED_SENSORS} distinct sensors per watch"
                    )));
                }
            }
            if distinct.is_empty() {
                return Err(Status::invalid_argument(
                    "sensor list is empty: select all sensors instead",
                ));
            }
            Filter::sensors(distinct)
        }
    };
    Ok((client_id, filter))
}

enum Step {
    Event(WatchResponse),
    Replaced,
    Stopped,
}

/// `unfold` polls again with the state returned alongside an item. `End` makes that
/// next poll finish the stream after the `ABORTED` status, instead of waiting on the hub.
enum Drive {
    Live(StreamState),
    End,
}

async fn poll_step(drive: Drive) -> Option<(Result<WatchResponse, Status>, Drive)> {
    let Drive::Live(mut state) = drive else {
        return None;
    };
    if let Some(event) = state.pending.pop_front() {
        return Some((Ok(event), Drive::Live(state)));
    }
    if !state.hello_sent {
        state.hello_sent = true;
        return Some((Ok(hello_event(&state)), Drive::Live(state)));
    }
    match wait_for_chunk(&mut state).await {
        Step::Event(event) => Some((Ok(event), Drive::Live(state))),
        Step::Replaced => Some((Err(Status::aborted("replaced by a new watch")), Drive::End)),
        Step::Stopped => None,
    }
}

async fn wait_for_chunk(state: &mut StreamState) -> Step {
    loop {
        let notified = state.guard.subscriber.notify().notified();
        tokio::pin!(notified);
        match state
            .guard
            .hub
            .try_drain(&state.guard.subscriber, MAX_POINTS_PER_BATCH)
        {
            Drain::Replaced => return Step::Replaced,
            Drain::Ready { gap, tail, chunks } => {
                push_ready(&mut state.pending, gap, &chunks, tail);
                if let Some(event) = state.pending.pop_front() {
                    return Step::Event(event);
                }
            }
            Drain::Idle => {}
        }
        tokio::select! {
            biased;
            () = state.cancel.cancelled() => return Step::Stopped,
            () = &mut notified => {}
        }
    }
}

fn hello_event(state: &StreamState) -> WatchResponse {
    WatchResponse {
        payload: Some(Payload::Hello(Hello {
            protocol_version: PROTOCOL_VERSION,
            race_id: state.race_id.clone(),
            stream_id: state.stream_id.clone(),
            next_sequence: state.next_sequence,
            selected_sensor_count: state.selected_sensor_count,
        })),
    }
}

fn push_gap(pending: &mut VecDeque<WatchResponse>, gap: GapSpan) {
    pending.push_back(WatchResponse {
        payload: Some(Payload::Gap(Gap {
            first_sequence: gap.first_sequence,
            dropped_point_count: gap.dropped,
        })),
    });
}

fn push_ready(
    pending: &mut VecDeque<WatchResponse>,
    gap: Option<GapSpan>,
    chunks: &[Chunk],
    tail: Option<GapSpan>,
) {
    if let Some(gap) = gap {
        push_gap(pending, gap);
    }
    if let Some(batch) = point_batch(chunks) {
        pending.push_back(batch);
    }
    if let Some(tail) = tail {
        push_gap(pending, tail);
    }
}

fn point_batch(chunks: &[Chunk]) -> Option<WatchResponse> {
    if chunks.is_empty() {
        return None;
    }
    let first_sequence = chunks[0].first_sequence;
    let mut points = Vec::with_capacity(chunks.iter().map(|chunk| chunk.len).sum());
    for chunk in chunks {
        let sensor_id = chunk.message.sensor_id.as_ref();
        let driver_id = chunk.driver_id.as_str();
        let end = chunk.start + chunk.len;
        for reading in &chunk.message.readings[chunk.start..end] {
            points.push(Point {
                sensor_id: sensor_id.to_owned(),
                timestamp_nanos: reading.timestamp,
                value: reading.value,
                driver_id: driver_id.to_owned(),
            });
        }
    }
    if points.is_empty() {
        return None;
    }
    Some(WatchResponse {
        payload: Some(Payload::PointBatch(PointBatch {
            first_sequence,
            points,
        })),
    })
}

#[cfg(test)]
mod tests;
