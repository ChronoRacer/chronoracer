import { z } from "zod";
import {
  CORE_TELEMETRY_URL_REQUIRED_MESSAGE,
} from "./constants.js";
import { isTruthyEnv, optStr, parseSensorSelection, positiveInt } from "./helpers.js";

export const serverEnvShape = {
  PORT: positiveInt("PORT", 8080),
  CORE_TELEMETRY_URL: z
    .string({ error: CORE_TELEMETRY_URL_REQUIRED_MESSAGE })
    .min(1, CORE_TELEMETRY_URL_REQUIRED_MESSAGE),
  RACE_ID: optStr,
  TELEMETRY_SENSORS: z.string().optional().transform(parseSensorSelection),
  TELEMETRY_MAX_POINTS_PER_SENSOR: positiveInt(
    "TELEMETRY_MAX_POINTS_PER_SENSOR",
    1000,
  ),
  TELEMETRY_RETENTION_MS: positiveInt("TELEMETRY_RETENTION_MS", 5 * 60_000),
  TELEMETRY_MAX_SENSORS: positiveInt("TELEMETRY_MAX_SENSORS", 500),
  TELEMETRY_MAX_POINTS: positiveInt("TELEMETRY_MAX_POINTS", 50_000),
  TELEMETRY_FANOUT_TICK_MS: positiveInt("TELEMETRY_FANOUT_TICK_MS", 200),
  TELEMETRY_WS_MAX_BUFFERED_BYTES: positiveInt(
    "TELEMETRY_WS_MAX_BUFFERED_BYTES",
    1_048_576,
  ),
  TELEMETRY_CLIENT_DIR: optStr,
  TELEMETRY_AUTH_DISABLED: z.string().optional().transform(isTruthyEnv),
  OAUTH_CLIENT_ID: optStr,
  OAUTH_CLIENT_SECRET: optStr,
  OAUTH_ISSUER: optStr,
  TELEMETRY_ROOT_URL: optStr,
  SESSION_SECRET: optStr,
};

export type RawEnv = z.infer<z.ZodObject<typeof serverEnvShape>>;
