use crate::telemetry::proto::stream_event::Payload;
use crate::telemetry::proto::telemetry_server::{Telemetry, TelemetryServer};
use crate::telemetry::proto::{Gap, Hello, Point, PointBatch, StreamEvent, WatchRequest};
use crate::telemetry::queue::{Chunk, Drain, GapSpan, MAX_POINTS_PER_BATCH, PreviewHub};
use compact_str::CompactString;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
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

struct SessionGuard {
    hub: PreviewHub,
    generation: u64,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.hub.end_stream(self.generation);
    }
}

struct StreamState {
    guard: SessionGuard,
    race_id: String,
    stream_id: String,
    next_sequence: u64,
    hello_sent: bool,
    pending: VecDeque<StreamEvent>,
    cancel: CancellationToken,
    notify: Arc<Notify>,
}

/// Plaintext h2c `Watch` server. Not supervised by the data pipeline or the control tasks (but has the CancellationToken of the data pipeline)
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
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match run_server(&hub, &cancel, addr, &race_id, &mut bound_tx).await {
            Ok(()) => return,
            Err(error) => {
                error!(%error, %addr, "telemetry listener stopped");
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(BIND_RETRY) => {}
                }
            }
        }
    }
}

/// [IMPORTANT!]: The server only supports a single client. Any new client will eject the existing one.
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
    info!(%bound, "Telemetry gRPC listener bound");
    let incoming = TcpListenerStream::new(listener);
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
        .add_service(TelemetryServer::new(service))
        .serve_with_incoming_shutdown(incoming, cancel.clone().cancelled_owned())
        .await?;
    Ok(())
}

/// Sequence the handoff even when HTTP/2 is not polling the stream, so age and the point cap
/// still apply while a client is stuck.
async fn run_pump(hub: PreviewHub, cancel: CancellationToken) {
    let tick = hub.max_age().max(Duration::from_millis(1));
    let notify = hub.notifier();
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        if hub.pull_ingress() {
            notify.notify_waiters();
        }
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
impl Telemetry for WatchService {
    type WatchStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<StreamEvent, Status>> + Send>>;

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (generation, next_sequence) = self.hub.begin_stream();
        info!(generation, "Telemetry watch started");
        let state = StreamState {
            guard: SessionGuard {
                hub: self.hub.clone(),
                generation,
            },
            race_id: self.race_id.to_string(),
            stream_id: self.hub.stream_id().to_owned(),
            next_sequence,
            hello_sent: false,
            pending: VecDeque::new(),
            cancel: self.cancel.clone(),
            notify: self.hub.notifier(),
        };
        let stream = futures_util::stream::unfold(state, poll_step);
        Ok(Response::new(Box::pin(stream)))
    }
}

enum Step {
    Event(StreamEvent),
    Replaced,
    Stopped,
}

async fn poll_step(mut state: StreamState) -> Option<(Result<StreamEvent, Status>, StreamState)> {
    if let Some(event) = state.pending.pop_front() {
        return Some((Ok(event), state));
    }
    if !state.hello_sent {
        state.hello_sent = true;
        return Some((Ok(hello_event(&state)), state));
    }
    match wait_for_chunk(&mut state).await {
        Step::Event(event) => Some((Ok(event), state)),
        Step::Replaced => Some((Err(Status::aborted("replaced by a new watch")), state)),
        Step::Stopped => None,
    }
}

async fn wait_for_chunk(state: &mut StreamState) -> Step {
    loop {
        let notified = state.notify.notified();
        tokio::pin!(notified);
        match state
            .guard
            .hub
            .try_drain(state.guard.generation, MAX_POINTS_PER_BATCH)
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

fn hello_event(state: &StreamState) -> StreamEvent {
    StreamEvent {
        payload: Some(Payload::Hello(Hello {
            protocol_version: PROTOCOL_VERSION,
            race_id: state.race_id.clone(),
            stream_id: state.stream_id.clone(),
            next_sequence: state.next_sequence,
        })),
    }
}

fn push_gap(pending: &mut VecDeque<StreamEvent>, gap: GapSpan) {
    pending.push_back(StreamEvent {
        payload: Some(Payload::Gap(Gap {
            first_sequence: gap.first_sequence,
            dropped_point_count: gap.dropped,
        })),
    });
}

fn push_ready(
    pending: &mut VecDeque<StreamEvent>,
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

fn point_batch(chunks: &[Chunk]) -> Option<StreamEvent> {
    if chunks.is_empty() {
        return None;
    }
    let first_sequence = chunks[0].first_sequence;
    let mut points = Vec::new();
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
    Some(StreamEvent {
        payload: Some(Payload::PointBatch(PointBatch {
            first_sequence,
            points,
        })),
    })
}

#[cfg(test)]
mod tests;
