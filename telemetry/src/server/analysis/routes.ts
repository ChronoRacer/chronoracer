/**
 * @fileoverview Machine-to-Machine (M2M) Analytics Ingestion Router
 *
 * Handles HTTP POST endpoints (`/analysis/v1/*`) for external analytics engines.
 * Security & Reliability Features:
 * - Constant-time Bearer token authorization using `crypto.timingSafeEqual`.
 * - Request payload size capping (64 KB limit).
 * - Automatic Zod validation returning structured error issues (400) or HTTP 204 on success.
 */

import { timingSafeEqual } from 'node:crypto';
import { Hono, type Context, type MiddlewareHandler } from 'hono';
import { bodyLimit } from 'hono/body-limit';
import type { z } from 'zod';
import type { AnalysisStore } from './AnalysisStore.js';
import { newAlert, newPrediction, predictionRunning, predictionUnavailable } from './schema.js';

export const MAX_BODY_BYTES = 64 * 1024;

export type AnalysisIngestAuth = { mode: 'token'; token: string } | { mode: 'open' } | { mode: 'disabled' };

export function analysisRoutes(store: AnalysisStore, auth: AnalysisIngestAuth) {
  const accept =
    <T>(schema: z.ZodType<T>, apply: (body: T) => boolean | void) =>
    async (c: Context) => {
      let raw: unknown;
      try {
        raw = await c.req.json();
      } catch {
        return c.json({ error: 'body is not JSON' }, 400);
      }
      const parsed = schema.safeParse(raw);
      if (!parsed.success) {
        const issues = parsed.error.issues.map(({ path, message }) => ({ path: path.join('.'), message }));
        return c.json({ error: 'invalid body', issues }, 400);
      }
      if (apply(parsed.data) === false) {
        return c.json({ error: 'prediction id limit reached' }, 422);
      }
      return c.body(null, 204);
    };

  return new Hono()
    .use('*', requireIngestToken(auth))
    .use('*', bodyLimit({ maxSize: MAX_BODY_BYTES, onError: (c) => c.json({ error: 'body too large' }, 413) }))
    .post(
      '/predictions/running',
      accept(predictionRunning, (b) => store.running(b)),
    )
    .post(
      '/predictions/unavailable',
      accept(predictionUnavailable, (b) => store.unavailable(b)),
    )
    .post(
      '/predictions',
      accept(newPrediction, (b) => store.prediction(b)),
    )
    .post(
      '/alerts',
      accept(newAlert, (b) => store.alert(b)),
    );
}

/**
 * Middleware validating Bearer token authentication against timing side-channel attacks.
 * Bypassed in `open` mode, returns 503 Service Unavailable if `disabled`.
 */
function requireIngestToken(auth: AnalysisIngestAuth): MiddlewareHandler {
  if (auth.mode === 'open') {
    return (_c, next) => next();
  }
  if (auth.mode === 'disabled') {
    return async (c) => c.json({ error: 'analysis ingest disabled: ANALYSIS_INGEST_TOKEN not set' }, 503);
  }
  const expected = Buffer.from(auth.token);
  return async (c, next) => {
    const header = c.req.header('authorization') ?? '';
    const given = Buffer.from(header.startsWith('Bearer ') ? header.slice('Bearer '.length) : '');
    if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
      c.header('WWW-Authenticate', 'Bearer');
      return c.json({ error: 'unauthorized' }, 401);
    }
    return next();
  };
}
