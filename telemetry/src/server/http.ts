// This file is the entry point for the HTTP server.

import { createServer, type Server } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getRequestListener } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import { createNodeWebSocket } from '@hono/node-ws';
import { Hono } from 'hono';
import type WebSocket from 'ws';
import { authRoutes, oidcConfig, readSession, requireSameOrigin, requireSession } from './auth/index.js';
import type { AuthConfig } from './env/index.js';
import type { FanoutHub } from './fanout/hub.js';
import { log, logError } from './log.js';
import type { PreviewStore } from './store/index.js';

const BUILT_CLIENT_DIR = fileURLToPath(new URL('../client', import.meta.url));

export type HttpDeps = {
  port: number;
  auth: AuthConfig;
  clientDir: string | undefined;
  store: PreviewStore;
  hub: FanoutHub;
};

export function startHttpServer({ port, auth, clientDir, store, hub }: HttpDeps): Server {
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

  app.get('/api/sensors', signedIn, (c) => c.json({ sensorIds: store.sensorIds(), stats: store.stats() }));
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
  server.listen(port, () => log('telemetry_listening', { port, ui: root, auth: auth.enabled }));
  return server;
}
