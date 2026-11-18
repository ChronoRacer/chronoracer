#!/usr/bin/env node
/**
 * Local-only mock of the analysis module: posts predictions and alerts to Telemetry's analysis ingest.
 *
 *   TELEMETRY_AUTH_DISABLED=1 pnpm start            # ingest is open without ANALYSIS_INGEST_TOKEN
 *   node scripts/mock-analysis.mjs
 */
const BASE = `${process.env.TELEMETRY_URL || 'http://127.0.0.1:8080'}/analysis/v1`;
const TOKEN = process.env.ANALYSIS_INGEST_TOKEN;
const INTERVAL_MS = Number(process.env.MOCK_ANALYSIS_INTERVAL_MS || 2000);

const nowUs = () => Date.now() * 1000;

async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(TOKEN ? { authorization: `Bearer ${TOKEN}` } : {}) },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    console.error(JSON.stringify({ msg: 'mock_analysis_rejected', path, status: res.status, body: await res.text() }));
  }
}

const PREDICTIONS = [
  { prediction_id: 'tyre-wear', name: 'Tyre wear (laps left)', refresh_rate: 'every 2 laps', sensors: ['wheel_fl', 'wheel_fr'] },
  { prediction_id: 'pit-window', name: 'Pit window', refresh_rate: null, sensors: ['fuel_l'] },
  { prediction_id: 'oil-trend', name: 'Oil temperature trend', refresh_rate: 'every 2 laps', sensors: ['oil_temp'] },
];

for (const p of PREDICTIONS) {
  await post('/predictions/running', {
    timestamp: nowUs(),
    prediction_id: p.prediction_id,
    name: p.name,
    refresh_rate: p.refresh_rate,
  });
}
console.log(JSON.stringify({ msg: 'mock_analysis_running', base: BASE, predictions: PREDICTIONS.length }));

let tick = 0;
setInterval(async () => {
  tick += 1;
  const timestamp = nowUs();
  await post('/predictions', {
    timestamp,
    prediction_id: 'tyre-wear',
    sensors_id: PREDICTIONS[0].sensors,
    instruction: null,
    value: Math.max(0, 30 - tick * 0.1),
  });

  if (tick % 5 === 0) {
    await post('/predictions', {
      timestamp,
      prediction_id: 'pit-window',
      sensors_id: PREDICTIONS[1].sensors,
      instruction: tick % 10 === 0 ? 'Box this lap' : 'Stay out',
      value: null,
    });
  }

  if (tick % 15 < 3) {
    await post('/predictions/unavailable', { timestamp, prediction_id: 'oil-trend', info: 'oil_temp not responding' });
  } else {
    await post('/predictions', {
      timestamp,
      prediction_id: 'oil-trend',
      sensors_id: PREDICTIONS[2].sensors,
      instruction: 'Rising',
      value: 118 + Math.sin(tick / 4) * 6,
    });
  }

  if (tick % 8 === 0) {
    await post('/alerts', {
      timestamp,
      alert_id: 'oil-overheat',
      sensors: [{ sensor_id: 'oil_temp', value: 142.3 }],
      analysis_message: 'Oil temperature above limit',
      analysis_reason: 'RED-FLAG: oil_temp higher than 140 °C over a 5 s window',
    });
  }
}, INTERVAL_MS);
