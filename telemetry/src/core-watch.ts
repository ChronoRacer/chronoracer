import { ChannelCredentials } from "@grpc/grpc-js";
import { createChannel, createClient, ClientError, Status } from "nice-grpc";
import { TelemetryDefinition } from "./gen/telemetry_v1.js";
import { coreTelemetryGrpcAddress } from "./grpc-address.js";
import type { StreamStats } from "./stats.js";

const MIN_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;

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

/** Identifies this service to Core. Another Watch with the same id replaces this one. */
export const CLIENT_ID = "telemetry";

/** Sensors to watch, or `undefined` for all sensors. Core owns the limits. */
export type SensorSelection = string[] | undefined;

/** `TELEMETRY_SENSORS`: unset or `*` selects all sensors, otherwise a comma-separated list. */
export function parseSensorSelection(raw: string | undefined): SensorSelection {
  if (raw === undefined || raw.trim() === "*") {
    return undefined;
  }
  const sensors = raw
    .split(",")
    .map((sensor) => sensor.trim())
    .filter((sensor) => sensor !== "");
  if (sensors.length === 0) {
    throw new Error(`TELEMETRY_SENSORS has no sensor ids: "${raw}" (use * for all sensors)`);
  }
  return sensors;
}

export type CoreWatchOptions = {
  coreTelemetryUrl: string;
  raceId: string | undefined;
  sensors: SensorSelection;
  stats: StreamStats;
  abortSignal: AbortSignal;
};

export async function runCoreWatchLoop(options: CoreWatchOptions): Promise<void> {
  const { coreTelemetryUrl, raceId, sensors, stats, abortSignal } = options;
  const request =
    sensors === undefined
      ? { clientId: CLIENT_ID, all: {} }
      : { clientId: CLIENT_ID, sensors: { sensorIds: sensors } };
  const address = coreTelemetryGrpcAddress(coreTelemetryUrl);
  let backoffMs = MIN_BACKOFF_MS;

  while (!abortSignal.aborted) {
    const channel = createChannel(address, ChannelCredentials.createInsecure());
    const client = createClient(TelemetryDefinition, channel);

    try {
      process.stdout.write(`telemetry connecting Watch to ${address}\n`);
      backoffMs = MIN_BACKOFF_MS;

      for await (const event of client.watch(request, { signal: abortSignal })) {
        if (event.hello !== undefined) {
          const hello = event.hello;
          stats.recordHello();
          console.log(
            JSON.stringify({
              type: "telemetry_hello",
              protocolVersion: hello.protocolVersion,
              raceId: hello.raceId,
              streamId: hello.streamId,
              nextSequence: u64Json(hello.nextSequence),
              selectedSensorCount: hello.selectedSensorCount,
            }),
          );
          if (raceId !== undefined && hello.raceId !== raceId) {
            logError("hello_race_id_mismatch", new Error(`expected race_id ${raceId}, got ${hello.raceId}`));
            stats.recordError();
            break;
          }
          continue;
        }

        if (event.pointBatch !== undefined) {
          const batch = event.pointBatch;
          stats.recordBatch(batch.points);
          continue;
        }

        if (event.gap !== undefined) {
          const gap = event.gap;
          stats.recordGap();
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
      stats.recordError();
      logError("watch_stream", err);
    } finally {
      channel.close();
    }

    if (abortSignal.aborted) {
      break;
    }

    process.stdout.write(`telemetry reconnect in ${backoffMs}ms\n`);
    await sleep(backoffMs);
    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
  }
}
