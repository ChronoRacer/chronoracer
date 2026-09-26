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

export type CoreWatchOptions = {
  coreTelemetryUrl: string;
  raceId: string | undefined;
  stats: StreamStats;
  abortSignal: AbortSignal;
};

export async function runCoreWatchLoop(options: CoreWatchOptions): Promise<void> {
  const { coreTelemetryUrl, raceId, stats, abortSignal } = options;
  const address = coreTelemetryGrpcAddress(coreTelemetryUrl);
  let backoffMs = MIN_BACKOFF_MS;

  while (!abortSignal.aborted) {
    const channel = createChannel(address, ChannelCredentials.createInsecure());
    const client = createClient(TelemetryDefinition, channel);

    try {
      process.stdout.write(`telemetry connecting Watch to ${address}\n`);
      backoffMs = MIN_BACKOFF_MS;

      for await (const event of client.watch({}, { signal: abortSignal })) {
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
