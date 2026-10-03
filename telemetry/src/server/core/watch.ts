import { ChannelCredentials } from "@grpc/grpc-js";
import { createChannel, createClient, ClientError, Status } from "nice-grpc";
import { TelemetryServiceDefinition } from "../gen/telemetry_v1.js";
import type { SensorSelection } from "../env.js";
import type { FanoutHub } from "../fanout/hub.js";
import type { PreviewStore } from "../store/ring-buffer.js";
import type { PreviewPoint } from "./types.js";
import { coreTelemetryGrpcAddress } from "./grpc-address.js";

const MIN_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;
const KEEPALIVE_TIME_MS = 15_000;
const KEEPALIVE_TIMEOUT_MS = 5_000;

/** Identifies this service to Core. Another Watch with the same id replaces this one. */
export const CLIENT_ID = "telemetry";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function u64Json(value: bigint): string {
  return value.toString();
}

function logError(context: string, err: unknown): void {
  const payload: Record<string, unknown> = { type: "telemetry_error", context };
  if (err instanceof ClientError) {
    payload.message = err.message;
    payload.code = Status[err.code] ?? err.code;
    payload.details = err.details;
  } else if (err instanceof Error) {
    payload.message = err.message;
  } else {
    payload.message = String(err);
  }
  console.error(JSON.stringify(payload));
}

export type CoreWatchOptions = {
  coreTelemetryUrl: string;
  raceId: string | undefined;
  sensors: SensorSelection;
  store: PreviewStore;
  hub: FanoutHub;
  abortSignal: AbortSignal;
};

export async function runCoreWatchLoop(options: CoreWatchOptions): Promise<void> {
  const { coreTelemetryUrl, raceId, sensors, store, hub, abortSignal } = options;
  const request =
    sensors === undefined
      ? { clientId: CLIENT_ID, all: {} }
      : { clientId: CLIENT_ID, sensors: { sensorIds: sensors } };
  const address = coreTelemetryGrpcAddress(coreTelemetryUrl);
  let backoffMs = MIN_BACKOFF_MS;
  let sawStream = false;

  while (!abortSignal.aborted) {
    // h2c: plaintext HTTP/2 — ChannelCredentials.createInsecure() is required (no TLS on this hop).
    const channel = createChannel(address, ChannelCredentials.createInsecure(), {
      "grpc.keepalive_time_ms": KEEPALIVE_TIME_MS,
      "grpc.keepalive_timeout_ms": KEEPALIVE_TIMEOUT_MS,
      "grpc.keepalive_permit_without_calls": 1,
      "grpc.http2.min_time_between_pings_ms": KEEPALIVE_TIME_MS,
      "grpc.max_receive_message_length": 4 * 1024 * 1024,
    });
    const client = createClient(TelemetryServiceDefinition, channel);

    try {
      process.stdout.write(`telemetry connecting Watch to ${address}\n`);
      backoffMs = MIN_BACKOFF_MS;
      store.setConnected(false);

      for await (const event of client.watch(request, { signal: abortSignal })) {
        if (event.hello !== undefined) {
          const hello = event.hello;
          const wire = {
            protocolVersion: hello.protocolVersion,
            raceId: hello.raceId,
            streamId: hello.streamId,
            nextSequence: u64Json(hello.nextSequence),
            selectedSensorCount: hello.selectedSensorCount,
          };
          store.setHello(wire);
          store.setConnected(true);
          sawStream = true;
          hub.onHello(wire);
          console.log(JSON.stringify({ type: "telemetry_hello", ...wire }));
          if (raceId !== undefined && hello.raceId !== raceId) {
            logError("hello_race_id_mismatch", new Error(`expected race_id ${raceId}, got ${hello.raceId}`));
            break;
          }
          continue;
        }

        if (event.pointBatch !== undefined) {
          const batch = event.pointBatch;
          const points: PreviewPoint[] = batch.points.map((p) => ({
            sensorId: p.sensorId,
            timestampNanos: p.timestampNanos,
            value: p.value,
            driverId: p.driverId,
          }));
          store.ingestPoints(points);
          hub.onPoints(points);
          continue;
        }

        if (event.gap !== undefined) {
          const gap = event.gap;
          store.recordCoreGap(gap.firstSequence, gap.droppedPointCount);
          hub.onGaps([
            {
              kind: "core",
              firstSequence: gap.firstSequence,
              droppedPointCount: gap.droppedPointCount,
              atMs: Date.now(),
            },
          ]);
          console.log(
            JSON.stringify({
              type: "telemetry_gap",
              firstSequence: u64Json(gap.firstSequence),
              droppedPointCount: u64Json(gap.droppedPointCount),
            }),
          );
        }
      }

      if (!abortSignal.aborted) {
        process.stdout.write("telemetry Watch stream ended; reconnecting\n");
      }
    } catch (err) {
      if (abortSignal.aborted) {
        break;
      }
      logError("watch_stream", err);
    } finally {
      store.setConnected(false);
      channel.close();
    }

    if (abortSignal.aborted) {
      break;
    }

    if (sawStream) {
      store.recordReconnect();
      hub.onGaps([{ kind: "reconnect", droppedPointCount: 0, atMs: Date.now() }]);
    }

    process.stdout.write(`telemetry reconnect in ${backoffMs}ms\n`);
    await sleep(backoffMs);
    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
  }
}
