/**
 * @fileoverview Service Entrypoint & Lifecycle Orchestration
 *
 * Main application bootstrap that initializes core stores, hooks up event fanout,
 * starts the HTTP/WebSocket server, executes the telemetry watch loop,
 * and handles graceful process termination.
 */

import { AnalysisStore } from './analysis/index.js';
import { runCoreWatchLoop } from './core/watch.js';
import { loadEnv } from './env/index.js';
import { FanoutHub } from './fanout/hub.js';
import { startHttpServer } from './http.js';
import { log, logError } from './log.js';
import { PreviewStore } from './store/index.js';

const env = loadEnv();
const store = new PreviewStore(env.ring);
const analysis = new AnalysisStore();
const hub = new FanoutHub(store, analysis, env.fanout);
const server = startHttpServer({ ...env, store, analysis, hub });
const shutdown = new AbortController();

hub.start();
runCoreWatchLoop({ ...env, store, signal: shutdown.signal }).catch((err: unknown) => {
  logError('fatal', err);
  process.exit(1);
});

for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.once(signal, () => {
    log('telemetry_shutdown', { signal });
    shutdown.abort();
    hub.stop();
    server.close();
    server.closeAllConnections();
  });
}
