import { describe, expect, it } from 'vitest';
import { AnalysisStore } from './AnalysisStore.js';
import { analysisRoutes, MAX_BODY_BYTES, type AnalysisIngestAuth } from './routes.js';
import { MIN_TIMESTAMP_US } from './schema.js';

const TOKEN = 't'.repeat(32);
const ts = MIN_TIMESTAMP_US + 1;

function setup(auth: AnalysisIngestAuth = { mode: 'token', token: TOKEN }, store = new AnalysisStore()) {
  const app = analysisRoutes(store, auth);
  const post = (path: string, body: unknown, token: string | null = TOKEN) =>
    app.request(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
      body: typeof body === 'string' ? body : JSON.stringify(body),
    });
  return { store, post };
}

const running = { timestamp: ts, prediction_id: 'p1', name: 'Tyre wear', refresh_rate: null };

describe('analysis ingest', () => {
  it('accepts the four documented bodies and folds them into one prediction state', async () => {
    const { store, post } = setup();
    expect((await post('/predictions/running', running)).status).toBe(204);
    expect((await post('/predictions/unavailable', { timestamp: ts + 1, prediction_id: 'p1', info: 'no rpm' })).status).toBe(204);
    expect(store.snapshot().predictions[0]).toMatchObject({ name: 'Tyre wear', unavailable: { info: 'no rpm' } });

    const prediction = { timestamp: ts + 2, prediction_id: 'p1', sensors_id: ['rpm'], instruction: 'Box now', value: null };
    expect((await post('/predictions', prediction)).status).toBe(204);
    const [state] = store.snapshot().predictions;
    expect(state.unavailable).toBeUndefined();
    expect(state.latest).toEqual({ timestampUs: ts + 2, sensorIds: ['rpm'], instruction: 'Box now', value: null });

    const alert = {
      timestamp: ts,
      alert_id: 'overheat',
      sensors: [{ sensor_id: 'oil_temp', value: 141.5 }],
      analysis_message: 'Oil too hot',
      analysis_reason: 'RED-FLAG: above 140 over 5 s',
    };
    expect((await post('/alerts', alert)).status).toBe(204);
    expect(store.snapshot().alerts).toMatchObject([{ alertId: 'overheat', sensors: [{ sensorId: 'oil_temp', value: 141.5 }] }]);
  });

  it('keeps an older unavailable report when a stale prediction arrives after it', async () => {
    const { store, post } = setup();
    await post('/predictions/unavailable', { timestamp: ts + 5, prediction_id: 'p1', info: 'no rpm' });
    await post('/predictions', { timestamp: ts, prediction_id: 'p1', sensors_id: [], instruction: null, value: 1 });
    expect(store.snapshot().predictions[0].unavailable?.info).toBe('no rpm');
  });

  it('rejects bodies that break the schema with the failing field', async () => {
    const { post } = setup();
    const bad = async (path: string, body: unknown) => {
      const res = await post(path, body);
      expect(res.status).toBe(400);
      return (await res.json()) as { issues?: { path: string }[] };
    };
    expect((await bad('/predictions/running', { ...running, timestamp: MIN_TIMESTAMP_US - 1 })).issues?.[0].path).toBe('timestamp');
    expect((await bad('/predictions/running', { ...running, refresh_rate: 0 })).issues?.[0].path).toBe('refresh_rate');
    expect((await bad('/predictions/running', { timestamp: ts, prediction_id: 'p1', name: 'x' })).issues?.[0].path).toBe('refresh_rate');
    expect((await bad('/predictions/running', { ...running, prediction_id: '' })).issues?.[0].path).toBe('prediction_id');
    expect((await bad('/predictions', { timestamp: ts, prediction_id: 'p', sensors_id: [''], instruction: null, value: null })).issues?.[0].path).toBe('sensors_id.0');
    const alert = { timestamp: ts, alert_id: 'a', sensors: [], analysis_message: 'm', analysis_reason: 'r' };
    expect((await bad('/alerts', alert)).issues?.[0].path).toBe('sensors');
    expect((await bad('/alerts', { ...alert, sensors: [{ sensor_id: 's', value: 1 }], analysis_message: 'x'.repeat(256) })).issues?.[0].path).toBe('analysis_message');
    await bad('/alerts', 'not json');
  });

  it('counts the 255 character limit in code points', async () => {
    const { post } = setup();
    const alert = { timestamp: ts, alert_id: 'a', sensors: [{ sensor_id: 's', value: 1 }], analysis_message: '🔥'.repeat(255), analysis_reason: 'r' };
    expect((await post('/alerts', alert)).status).toBe(204);
  });

  it('requires the bearer token and refuses when no token is configured', async () => {
    const { post } = setup();
    expect((await post('/predictions/running', running, null)).status).toBe(401);
    expect((await post('/predictions/running', running, 'x'.repeat(32))).status).toBe(401);
    expect((await setup({ mode: 'disabled' }).post('/predictions/running', running)).status).toBe(503);
    expect((await setup({ mode: 'open' }).post('/predictions/running', running, null)).status).toBe(204);
  });

  it('limits body size and the number of prediction ids', async () => {
    const { post } = setup(undefined, new AnalysisStore({ maxPredictions: 1, maxAlerts: 10 }));
    expect((await post('/predictions/running', { ...running, name: 'x'.repeat(MAX_BODY_BYTES) })).status).toBe(413);
    expect((await post('/predictions/running', running)).status).toBe(204);
    expect((await post('/predictions/running', { ...running, prediction_id: 'p2' })).status).toBe(422);
    expect((await post('/predictions/running', running)).status).toBe(204);
  });
});
