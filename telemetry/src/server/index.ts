import { runCoreWatchLoop } from "./core/watch.js";
import { loadEnv } from "./env/index.js";
import { FanoutHub } from "./fanout/hub.js";
import { startHttpServer } from "./http.js";
import { log, logError } from "./log.js";
import { PreviewStore } from "./store/index.js";

const env = loadEnv();
const store = new PreviewStore(env.ring);
const hub = new FanoutHub(store, env.fanout);
const server = startHttpServer({ ...env, store, hub });
const shutdown = new AbortController();

hub.start();
runCoreWatchLoop({ ...env, store, signal: shutdown.signal }).catch(
  (err: unknown) => {
    logError("fatal", err);
    process.exit(1);
  },
);

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.once(signal, () => {
    log("telemetry_shutdown", { signal });
    shutdown.abort();
    hub.stop();
    server.close();
    server.closeAllConnections();
  });
}
