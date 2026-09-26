# ADR - Core telemetry stream protocol

Status: accepted

Problem: Telemetry pulls a binary Protobuf preview from Core across the private race VPC. Application data moves only
from Core to Telemetry. Core has no public address, so this hop does not need TLS. The consumer can be slow or absent.
Core must be able to drop preview data without a second, hand-rolled framing layer, and without a duplex API that can
later carry commands into the ingest process.

This ADR chooses the protocol. What the stream carries, and how loss works, stays in
[Core](component-core.md#telemetry-stream). Why the preview is not read from Kafka, and why Core does not push
outbound, stays in [ADR - Dashboard and live telemetry](adr-dashboard.md).

## Decision

Core serves **one gRPC server-streaming RPC** over plaintext HTTP/2 (h2c).

- Package `chronoracer.telemetry.v1`, service `Telemetry`, rpc `Watch(WatchRequest) returns (stream StreamEvent)`.
- `WatchRequest` is empty. One gRPC message is one `StreamEvent` (`hello`, `point_batch`, or `gap`), not one sensor point.
- No TLS. The client target is an `http://` URL (h2c prior knowledge).
- No message compression.
- HTTP/2 stream window and connection window are each 256 KiB.
- A `point_batch` holds at most 1024 points, small enough to fit in that window. The sender may emit a smaller batch
  when the queue has less. It does not wait to fill 1024.
- One active `Watch`. `max_concurrent_streams` is 2 so a replacement call can start before the previous one is cancelled.
- HTTP/2 keepalive interval is 15 seconds.

This replaces the WebSocket transport named in [ADR - Dashboard and live telemetry](adr-dashboard.md).

## Why gRPC server streaming

Server streaming is the RPC shape for this hop: the client opens the call, Core sends many messages, and the client
does not send application data after the request.

Protobuf is already the event encoding. gRPC frames those messages, and it supplies keepalive, status codes, and flow
control. A length prefix in front of the same bytes would be a second protocol Core would have to keep correct.

The client on this hop is the Telemetry service, not a browser. Browser sockets stay on Telemetry's own public
surface ([Telemetry](component-telemetry.md)). gRPC-web is not required here.

HTTP/2 flow control holds bytes after they leave Core's preview queue. Those bytes can no longer be dropped. A large
window would pin stale preview in the HTTP/2 stack. 256 KiB is about one batch. On a same-zone private path a client
that is keeping up refills that window in under a millisecond, so a multi-megabyte window is not what provides
throughput.

Compression stays off. Points are mostly floats and timestamps, which compress poorly. gzip or zstd would spend CPU on
the Core VM next to Influx. The private hop is not bandwidth-bound.

## Rejected

**WebSocket.** It runs without TLS and already has binary frames, ping/pong, and a subprotocol for version. The socket
is full duplex. This hop never sends application data toward Core. Ignoring client data frames would be a policy on
top of a protocol that still allows them. The dashboard ADR chose WebSocket so Core would not invent framing. gRPC
provides that framing as a one-way RPC.

**WebTransport, server-initiated unidirectional stream.** The direction matches: the client connects, Core writes one
stream, the client does not write application data on it. WebTransport rides QUIC, and QUIC always uses TLS 1.3. A
private subnet does not remove that. Core would hold a certificate, Telemetry would have to trust it, and the firewall
would allow UDP. No browser on this hop needs the WebTransport API.

**Server-Sent Events.** One-way and plaintext, with no RPC stack. The framing is UTF-8 text. Protobuf would be base64,
or the body would no longer be SSE.

**HTTP GET with a length-prefixed body.** Plaintext TCP, binary, and one application direction: the client sends `GET`
and then only reads. Core would own the `u32` length prefix and the keepalive. gRPC already defines both for Protobuf.

**Raw TCP.** The same custom framing and keepalive problem, without a request that names the method.

**gRPC over TLS, client streaming, or bidirectional streaming.** TLS adds certificates this hop does not need. Client
streaming and bidirectional streaming send application data toward Core, or both ways.
