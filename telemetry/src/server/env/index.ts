import { createEnv } from "@t3-oss/env-core";
import { serverEnvShape, type RawEnv } from "./schema.js";
import type { AuthConfig, TelemetryEnv } from "./types.js";

export type {
  AuthConfig,
  FanoutConfig,
  OAuthConfig,
  RingLimits,
  SensorSelection,
  TelemetryEnv,
} from "./types.js";

export function loadEnv(): TelemetryEnv {
  const raw = createEnv({
    server: serverEnvShape,
    runtimeEnv: process.env,
    emptyStringAsUndefined: true,
    onValidationError: (issues) => {
      const details = issues.map((issue) => `${issue.path?.join(".")}: ${issue.message}`);
      throw new Error(`invalid environment: ${details.join("; ")}`);
    },
  });
  return toTelemetryEnv(raw);
}

function toTelemetryEnv(raw: RawEnv): TelemetryEnv {
  return {
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
    clientDir: raw.TELEMETRY_CLIENT_DIR || undefined,
    auth: toAuthConfig(raw),
  };
}

function toAuthConfig(raw: RawEnv): AuthConfig {
  if (raw.TELEMETRY_AUTH_DISABLED) {
    return { enabled: false };
  }
  const {
    OAUTH_CLIENT_ID: clientId,
    OAUTH_CLIENT_SECRET: clientSecret,
    OAUTH_ISSUER: issuer,
    TELEMETRY_ROOT_URL: rootUrl,
  } = raw;
  if (!clientId || !clientSecret || !issuer || !rootUrl) {
    throw new Error(
      "OAuth config incomplete: set OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_ISSUER and TELEMETRY_ROOT_URL" +
        " (or TELEMETRY_AUTH_DISABLED=1 for local runs)",
    );
  }
  return {
    enabled: true,
    clientId,
    clientSecret,
    issuer,
    rootUrl: new URL(rootUrl).origin,
    sessionSecret: raw.SESSION_SECRET || clientSecret,
  };
}
