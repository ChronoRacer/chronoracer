import { buildAuth } from "./auth.js";
import type { RawEnv } from "./schema.js";
import type { TelemetryEnv } from "./types.js";

export type TelemetryEnvResolution = {
  env: TelemetryEnv;
  notice?: string;
};

export function toTelemetryEnv(raw: RawEnv): TelemetryEnvResolution {
  const { auth, notice } = buildAuth(raw);
  return {
    env: {
      port: raw.PORT,
      coreTelemetryUrl: raw.CORE_TELEMETRY_URL,
      raceId: raw.RACE_ID || undefined,
      sensors: raw.TELEMETRY_SENSORS,
      ring: {
        maxPointsPerSensor: raw.TELEMETRY_MAX_POINTS_PER_SENSOR,
        maxAgeMs: raw.TELEMETRY_RETENTION_MS,
        maxSensors: raw.TELEMETRY_MAX_SENSORS,
        maxTotalPoints: raw.TELEMETRY_MAX_POINTS,
      },
      fanout: {
        tickMs: raw.TELEMETRY_FANOUT_TICK_MS,
        maxBufferedBytes: raw.TELEMETRY_WS_MAX_BUFFERED_BYTES,
      },
      clientDir: raw.TELEMETRY_CLIENT_DIR || "",
      auth,
    },
    notice,
  };
}
