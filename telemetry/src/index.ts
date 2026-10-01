import { startHealthServer } from "./health.js";
import { parseSensorSelection, runCoreWatchLoop } from "./core-watch.js";
import { StreamStats } from "./stats.js";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`missing required env ${name}`);
  }
  return value;
}

const port = Number.parseInt(process.env.PORT ?? "8080", 10);
if (!Number.isInteger(port) || port <= 0) {
  throw new Error(`invalid PORT: ${process.env.PORT}`);
}

const coreTelemetryUrl = requiredEnv("CORE_TELEMETRY_URL");
const raceId = process.env.RACE_ID?.trim() || undefined;
const sensors = parseSensorSelection(process.env.TELEMETRY_SENSORS);

startHealthServer(port);

const stats = new StreamStats();
stats.start();

const abort = new AbortController();
process.on("SIGTERM", () => {
  process.stdout.write("telemetry SIGTERM; shutting down\n");
  abort.abort();
  stats.stop();
  stats.flush();
});

void runCoreWatchLoop({
  coreTelemetryUrl,
  raceId,
  sensors,
  stats,
  abortSignal: abort.signal,
}).catch((err) => {
  console.error(JSON.stringify({ type: "telemetry_fatal", message: String(err) }));
  process.exit(1);
});
