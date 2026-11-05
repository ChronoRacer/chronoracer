#!/usr/bin/env node
/**
 * Local-only mock of Core's plaintext h2c TelemetryService.Watch.
 * Feeds Telemetry with synthetic points so the UI can be exercised without Core: MOCK_CORE_RATE points/s spread
 * evenly over the contract sensors, in batches of at most 1024 like Core. Each value is a slow smooth wave
 * (~6 s) plus a gentle secondary (~2 s); no sub-tick noise. `brake_prs` still gets a short full-scale spike every 2 s.
 * GPS position, heading, speed, and fix follow a lap of Tor Poznań at constant speed, so the map has a real track.
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

// Tor Poznań, traced from the GUGiK orthophoto (~4.1 km, clockwise), so the map shows a real track.
const LAP = [
  [52.420351, 16.801529], [52.420377, 16.80346], [52.420325, 16.805606], [52.420246, 16.807752],
  [52.420167, 16.809897], [52.420089, 16.811829], [52.419958, 16.812901], [52.419605, 16.813438],
  [52.419016, 16.813502], [52.418519, 16.813116], [52.418335, 16.812258], [52.418296, 16.81097],
  [52.418362, 16.809254], [52.418492, 16.807537], [52.418754, 16.80625], [52.419147, 16.805177],
  [52.419304, 16.804533], [52.419212, 16.804104], [52.418623, 16.804211], [52.417838, 16.80464],
  [52.41738, 16.805177], [52.417472, 16.806679], [52.417576, 16.808395], [52.417498, 16.810112],
  [52.417419, 16.811829], [52.417249, 16.812901], [52.416922, 16.813116], [52.416529, 16.812472],
  [52.416333, 16.811507], [52.415875, 16.811078], [52.415221, 16.81097], [52.414763, 16.810648],
  [52.414697, 16.809254], [52.414802, 16.807537], [52.415351, 16.80625], [52.41594, 16.805177],
  [52.416071, 16.803675], [52.41581, 16.801958], [52.415221, 16.800456], [52.415482, 16.799812],
  [52.416398, 16.799705], [52.417576, 16.79992], [52.418754, 16.800349], [52.419801, 16.800885],
];
const LAP_KMH = 110;
const M_PER_DEG_LAT = 111_320;
const lapLegs = LAP.map(([lat, lon], i) => {
  const [lat2, lon2] = LAP[(i + 1) % LAP.length];
  const north = (lat2 - lat) * M_PER_DEG_LAT;
  const east = (lon2 - lon) * M_PER_DEG_LAT * Math.cos((lat * Math.PI) / 180);
  return { lat, lon, dLat: lat2 - lat, dLon: lon2 - lon, m: Math.hypot(north, east), heading: (Math.atan2(east, north) * 180) / Math.PI };
});
const lapM = lapLegs.reduce((sum, leg) => sum + leg.m, 0);

function lapSample(sensorId, tMs) {
  let at = ((tMs / 3.6e6) * LAP_KMH * 1000) % lapM;
  const leg = lapLegs.find((l) => (at -= l.m) < 0) ?? lapLegs[0];
  const f = 1 + at / leg.m;
  switch (sensorId) {
    case 'gps_lat':
      return leg.lat + f * leg.dLat;
    case 'gps_lon':
      return leg.lon + f * leg.dLon;
    case 'gps_heading':
      return (leg.heading + 360) % 360;
    case 'gps_speed':
      return LAP_KMH;
    case 'gps_fix':
      return 3;
    default:
      return undefined;
  }
}

function sample(sensorId, tMs) {
  const { min, max } = ranges[sensorId] ?? { min: 0, max: 100 };
  if (sensorId === 'brake_prs' && tMs % SPIKE_EVERY_MS < SPIKE_MS) {
    return max;
  }
  const onLap = lapSample(sensorId, tMs);
  if (onLap !== undefined) {
    return onLap;
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
