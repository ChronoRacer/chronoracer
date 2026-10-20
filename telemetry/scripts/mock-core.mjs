#!/usr/bin/env node
/**
 * Local-only mock of Core's plaintext h2c TelemetryService.Watch.
 * Feeds Telemetry with synthetic points so the UI can be exercised without Core.
 *
 *   node scripts/mock-core.mjs
 *   CORE_TELEMETRY_URL=http://127.0.0.1:9470 TELEMETRY_AUTH_DISABLED=1 pnpm start
 */
import { createServer } from 'nice-grpc';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  TelemetryServiceDefinition,
} from '../dist/server/gen/telemetry_v1.js';

const PORT = Number(process.env.MOCK_CORE_PORT || 9470);
const RACE_ID = process.env.RACE_ID || 'local-dev';
const INTERVAL_MS = Number(process.env.MOCK_CORE_INTERVAL_MS || 100);

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const contract = JSON.parse(readFileSync(resolve(root, 'infra/contract/sensors.json'), 'utf8'));
const ALL_SENSORS = contract.sensors.map((s) => s.name);

const ranges = Object.fromEntries(
  contract.sensors.map((s) => [s.name, { min: s.min ?? 0, max: s.max ?? 100 }]),
);

function pickSensors(request) {
  if (request.sensors?.sensorIds?.length) {
    return request.sensors.sensorIds;
  }
  return ALL_SENSORS;
}

function sample(sensorId, t) {
  const { min, max } = ranges[sensorId] ?? { min: 0, max: 100 };
  const mid = (min + max) / 2;
  const amp = (max - min) / 2;
  const phase = [...sensorId].reduce((a, c) => a + c.charCodeAt(0), 0);
  return mid + amp * Math.sin(t / 1000 + phase / 50) * 0.7;
}

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

  while (true) {
    const now = Date.now();
    const points = sensors.map((sensorId) => ({
      sensorId,
      timestampNanos: BigInt(now) * 1_000_000n,
      value: sample(sensorId, now),
      driverId: 'local-driver',
    }));
    const firstSequence = seq;
    seq += BigInt(points.length);
    yield { pointBatch: { firstSequence, points } };
    await new Promise((r) => setTimeout(r, INTERVAL_MS));
  }
}

const server = createServer();
server.add(TelemetryServiceDefinition, { watch });
await server.listen(`0.0.0.0:${PORT}`);
console.log(JSON.stringify({ msg: 'mock_core_listening', port: PORT, raceId: RACE_ID, sensors: ALL_SENSORS.length }));
