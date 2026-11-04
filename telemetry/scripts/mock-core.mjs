#!/usr/bin/env node
/**
 * Local-only mock of Core's plaintext h2c TelemetryService.Watch.
 * Feeds Telemetry with synthetic points so the UI can be exercised without Core: MOCK_CORE_RATE points/s spread
 * evenly over the contract sensors, in batches of at most 1024 like Core. Each value is a slow smooth wave
 * (~6 s) plus a gentle secondary (~2 s); no sub-tick noise. `brake_prs` still gets a short full-scale spike every 2 s.
 *
 *   node scripts/mock-core.mjs
 *   CORE_TELEMETRY_URL=http://127.0.0.1:9470 TELEMETRY_AUTH_DISABLED=1 pnpm start
 */
import { createServer } from 'nice-grpc';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { TelemetryServiceDefinition } from '../dist/server/gen/telemetry_v1.js';

const PORT = Number(process.env.MOCK_CORE_PORT || 9470);
const RACE_ID = process.env.RACE_ID || 'local-dev';
// Default: every contract sensor at 10 Hz.
const RATE = Number(process.env.MOCK_CORE_RATE || 380);
const MAX_POINTS_PER_BATCH = 1024;
const STEP_MS = 10;
const REPORT_EVERY_MS = 5000;
const SPIKE_EVERY_MS = 2000;
const SPIKE_MS = 2;

if (!Number.isFinite(RATE) || RATE <= 0) {
  throw new Error(`MOCK_CORE_RATE must be a positive number of points per second, got ${process.env.MOCK_CORE_RATE}`);
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const contract = JSON.parse(readFileSync(resolve(root, 'infra/contract/sensors.json'), 'utf8'));
const ALL_SENSORS = contract.sensors.map((s) => s.name);

const ranges = Object.fromEntries(
  contract.sensors.map((s) => [s.name, { min: s.min ?? 0, max: s.max ?? 100 }]),
);
const phases = Object.fromEntries(ALL_SENSORS.map((id) => [id, [...id].reduce((a, c) => a + c.charCodeAt(0), 0)]));

function pickSensors(request) {
  if (request.sensors?.sensorIds?.length) {
    return request.sensors.sensorIds;
  }
  return ALL_SENSORS;
}

function sample(sensorId, tMs) {
  const { min, max } = ranges[sensorId] ?? { min: 0, max: 100 };
  if (sensorId === 'brake_prs' && tMs % SPIKE_EVERY_MS < SPIKE_MS) {
    return max;
  }
  const mid = (min + max) / 2;
  const amp = (max - min) / 2;
  const phase = phases[sensorId] ?? 0;
  // Periods ~6.3 s and ~2.1 s — above the fanout tick, so instruments glide instead of looking noisy.
  return mid + amp * (0.7 * Math.sin(tMs / 1000 + phase / 50) + 0.15 * Math.sin(tMs / 333 + phase / 40));
}

let sent = 0;
setInterval(() => {
  console.log(JSON.stringify({ msg: 'mock_core_rate', pointsPerSec: Math.round((sent * 1000) / REPORT_EVERY_MS) }));
  sent = 0;
}, REPORT_EVERY_MS).unref();

async function* watch(request) {
  const sensors = pickSensors(request);
  let seq = 1n;
  const streamId = `mock-${Date.now().toString(36)}`;
  yield {
    hello: {
      protocolVersion: 1,
      raceId: RACE_ID,
      streamId,
      nextSequence: seq,
      selectedSensorCount: request.sensors ? sensors.length : 0,
    },
  };

  // Points are generated for the wall-clock time that has passed, so the rate holds whatever the step jitter.
  let n = 0;
  let doneUntilMs = Date.now();
  while (true) {
    const count = Math.floor(((Date.now() - doneUntilMs) * RATE) / 1000);
    if (count === 0) {
      await sleep(STEP_MS);
      continue;
    }
    const spanMs = (count * 1000) / RATE;
    for (let offset = 0; offset < count; offset += MAX_POINTS_PER_BATCH) {
      const points = [];
      for (let i = offset; i < Math.min(count, offset + MAX_POINTS_PER_BATCH); i++, n++) {
        const sensorId = sensors[n % sensors.length];
        const tMs = doneUntilMs + (i * spanMs) / count;
        points.push({
          sensorId,
          timestampNanos: BigInt(Math.round(tMs * 1e6)),
          value: sample(sensorId, tMs),
          driverId: 'local-driver',
        });
      }
      const firstSequence = seq;
      seq += BigInt(points.length);
      sent += points.length;
      yield { pointBatch: { firstSequence, points } };
    }
    doneUntilMs += spanMs;
  }
}

const server = createServer();
server.add(TelemetryServiceDefinition, { watch });
await server.listen(`0.0.0.0:${PORT}`);
console.log(
  JSON.stringify({ msg: 'mock_core_listening', port: PORT, raceId: RACE_ID, sensors: ALL_SENSORS.length, rate: RATE }),
);
