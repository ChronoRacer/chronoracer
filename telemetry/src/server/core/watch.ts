import { setTimeout as sleep } from "node:timers/promises";
import { ChannelCredentials } from "@grpc/grpc-js";
import { ClientError, createChannel, createClient, Status } from "nice-grpc";
import type { SensorSelection } from "../env/index.js";
import { TelemetryServiceDefinition, type WatchRequest } from "../gen/telemetry_v1.js";
import { log, logError } from "../log.js";
import type { PreviewStore } from "../store/index.js";
import { coreTelemetryGrpcAddress } from "./grpc-address.js";

export const CLIENT_ID = "telemetry";

const MIN_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;

const CHANNEL_OPTIONS = {
  "grpc.keepalive_time_ms": 15_000,
  "grpc.keepalive_timeout_ms": 5_000,
  "grpc.keepalive_permit_without_calls": 1,
  "grpc.http2.min_time_between_pings_ms": 15_000,
  "grpc.max_receive_message_length": 4 * 1024 * 1024,
};

export type CoreWatchOptions = {
  coreTelemetryUrl: string;
  raceId: string | undefined;
  sensors: SensorSelection;
  store: PreviewStore;
  signal: AbortSignal;
};

export async function runCoreWatchLoop(options: CoreWatchOptions): Promise<void> {
  const { coreTelemetryUrl, raceId, sensors, store, signal } = options;
  const address = coreTelemetryGrpcAddress(coreTelemetryUrl);
  const request: WatchRequest = sensors
    ? { clientId: CLIENT_ID, sensors: { sensorIds: sensors } }
    : { clientId: CLIENT_ID, all: {} };
  let backoffMs = MIN_BACKOFF_MS;

  while (!signal.aborted) {
    const channel = createChannel(address, ChannelCredentials.createInsecure(), CHANNEL_OPTIONS);
    try {
      log("telemetry_connecting", { address });
      const stream = createClient(TelemetryServiceDefinition, channel).watch(request, { signal });
      for await (const { hello, pointBatch, gap } of stream) {
        if (hello) {
          if (raceId !== undefined && hello.raceId !== raceId) {
            throw new Error(`Core hello has race_id ${hello.raceId}, expected ${raceId}`);
          }
          store.connect(hello);
          backoffMs = MIN_BACKOFF_MS;
          log("telemetry_hello", { ...hello });
        } else if (pointBatch) {
          store.ingest(pointBatch.points);
        } else if (gap) {
          store.recordCoreGap(gap);
          log("telemetry_gap", { ...gap });
        }
      }
      log("telemetry_stream_ended");
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      if (err instanceof ClientError && err.code === Status.INVALID_ARGUMENT) {
        throw err;
      }
      logError("watch_stream", err);
    } finally {
      store.disconnect();
      channel.close();
    }

    const delayMs = Math.round(backoffMs * (0.5 + Math.random() / 2));
    log("telemetry_reconnect_wait", { delayMs });
    await sleep(delayMs, undefined, { signal }).catch(() => {});
    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
  }
}
