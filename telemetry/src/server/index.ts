import { loadEnv } from "./env/index.js";
import { startHttpServer } from "./http.js";
import { runCoreWatchLoop } from "./core/watch.js";
import { PreviewStore } from "./store/index.js";
import { FanoutHub } from "./fanout/hub.js";

const env = loadEnv();
const store = new PreviewStore(env.ring);
const hub = new FanoutHub(store, env.fanout);
hub.start();

startHttpServer({
  port: env.port,
  auth: env.auth,
  clientDir: env.clientDir,
  store,
  hub,
});

const abort = new AbortController();
process.on("SIGTERM", () => {
  process.stdout.write("telemetry SIGTERM; shutting down\n");
  abort.abort();
  hub.stop();
});
process.on("SIGINT", () => {
  abort.abort();
  hub.stop();
});

void runCoreWatchLoop({
  coreTelemetryUrl: env.coreTelemetryUrl,
  raceId: env.raceId,
  sensors: env.sensors,
  store,
  hub,
  abortSignal: abort.signal,
}).catch((err) => {
  console.error(JSON.stringify({ type: "telemetry_fatal", message: String(err) }));
  process.exit(1);
});
