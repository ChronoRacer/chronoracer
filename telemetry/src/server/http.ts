/**
 * @fileoverview HTTP & WebSocket Server Setup (Hono + Node.js)
 *
 * Configures the primary application server:
 * - Security & Authentication (OIDC, Session Checks, CSRF / Same-Origin)
 * - REST API Routes (Health checks, Sensor metadata, Analysis ingestion)
 * - WebSocket Streaming endpoint (`/ws`) bridged to FanoutHub
 * - Static SPA Asset Serving with client-side routing fallback (`index.html`)
 */

import { createServer, type Server } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getRequestListener } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import { createNodeWebSocket } from '@hono/node-ws';
import { Hono } from 'hono';
import type WebSocket from 'ws';
import { analysisRoutes, type AnalysisIngestAuth, type AnalysisStore } from './analysis/index.js';
import { authRoutes, oidcConfig, readSession, requireSameOrigin, requireSession } from './auth/index.js';
import { driverRoutes, type DriverMessages, type DriverPublisher } from './driver/index.js';
import type { AuthConfig } from './env/index.js';
import type { FanoutHub } from './fanout/hub.js';
import { log, logError } from './log.js';
import type { PreviewStore } from './store/index.js';

const BUILT_CLIENT_DIR = fileURLToPath(new URL('../client', import.meta.url));

export type HttpDeps = {
  port: number;
  auth: AuthConfig;
  clientDir: string | undefined;
  analysisIngestToken: string | undefined;
  store: PreviewStore;
  analysis: AnalysisStore;
  driver: DriverMessages;
  driverPublisher: DriverPublisher | undefined;
  hub: FanoutHub;
};

export function startHttpServer({
  port,
  auth,
  analysisIngestToken,
  clientDir,
  store,
  analysis,
  driver,
  driverPublisher,
  hub,
}: HttpDeps): Server {
  const root = path.resolve(clientDir ?? BUILT_CLIENT_DIR);
  const app = new Hono();
  const { upgradeWebSocket, injectWebSocket } = createNodeWebSocket({ app });
  const signedIn = requireSession(auth);
  app.onError((err, c) => {
    logError('http', err);
    return c.text('internal error', 500);
  });

  app.get('/healthz', (c) => c.text('ok\n'));
  app.use('*', oidcConfig(auth));
  app.route('/auth', authRoutes(auth));
  app.get('/api/me', async (c) => {
    const session = await readSession(c, auth);
    return session ? c.json({ authenticated: true, ...session }) : c.json({ authenticated: false }, 401);
  });

  app.route('/analysis/v1', analysisRoutes(analysis, ingestAuth(auth, analysisIngestToken)));
  app.get('/api/sensors', signedIn, (c) => c.json({ sensorIds: store.sensorIds(), stats: store.stats() }));
  app.get('/api/analysis', signedIn, (c) => c.json(analysis.snapshot()));
  app.use('/api/driver/*', requireSameOrigin(auth), signedIn);
  app.route('/api/driver', driverRoutes(driver, driverPublisher, auth));
  app.get(
    '/ws',
    requireSameOrigin(auth),
    signedIn,
    upgradeWebSocket(() => ({
      onOpen: (_event, ws) => hub.addClient(ws.raw as WebSocket),
    })),
  );
  app.use('*', signedIn, serveStatic({ root }));
  app.get('*', serveStatic({ root, path: 'index.html' }));

  const server = createServer(getRequestListener(app.fetch));
  injectWebSocket(server);
  const analysisIngest = ingestAuth(auth, analysisIngestToken).mode;
  const driverMessages = driverPublisher ? 'kafka' : 'disabled';
  server.listen(port, () =>
    log('telemetry_listening', { port, ui: root, auth: auth.enabled, analysisIngest, driverMessages }),
  );
  return server;
}

function ingestAuth(auth: AuthConfig, token: string | undefined): AnalysisIngestAuth {
  if (token) {
    return { mode: 'token', token };
  }
  return auth.enabled ? { mode: 'disabled' } : { mode: 'open' };
}
