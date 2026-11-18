/**
 * @fileoverview Zod Validation Schemas & Parsers for Environment Variables
 *
 * Defines type-safe parsing rules, fallbacks, and coercions for all application
 * environment variables prior to application bootstrapping.
 */

import { z } from 'zod';

const positiveInt = (fallback: number) => z.coerce.number().int().positive().default(fallback);
const optionalString = z.string().trim().optional();

// Core's Watch selection limits (docs/component-core.md#clients-and-sensor-selection), checked at startup so a bad
// TELEMETRY_SENSORS fails fast instead of every Watch call ending in INVALID_ARGUMENT.
const CORE_MAX_SELECTED_SENSORS = 100;
const CORE_MAX_SENSOR_ID_BYTES = 24;
const sensorId = z
  .string()
  .refine((id) => Buffer.byteLength(id, 'utf8') <= CORE_MAX_SENSOR_ID_BYTES, {
    message: `sensor ids are at most ${CORE_MAX_SENSOR_ID_BYTES} bytes`,
  });

function parseSensorList(raw: string | undefined): string[] | undefined {
  const value = raw?.trim();
  if (!value || value === '*') {
    return undefined;
  }
  return [
    ...new Set(
      value
        .split(',')
        .map((id) => id.trim())
        .filter(Boolean),
    ),
  ];
}

export const serverEnvShape = {
  PORT: z.coerce.number().int().min(1).max(65_535).default(8080),
  CORE_TELEMETRY_URL: z.url(),
  RACE_ID: optionalString,
  TELEMETRY_SENSORS: z
    .string()
    .optional()
    .transform(parseSensorList)
    .pipe(z.array(sensorId).min(1).max(CORE_MAX_SELECTED_SENSORS).optional()),
  TELEMETRY_BUCKET_MS: positiveInt(50),
  TELEMETRY_RETENTION_MS: positiveInt(60_000),
  TELEMETRY_MAX_BUCKETS: positiveInt(100_000),
  TELEMETRY_FANOUT_TICK_MS: positiveInt(100),
  TELEMETRY_WS_MAX_BUFFERED_BYTES: positiveInt(1_048_576),
  TELEMETRY_CLIENT_DIR: optionalString,
  TELEMETRY_AUTH_DISABLED: z.stringbool().default(false),
  OAUTH_CLIENT_ID: optionalString,
  OAUTH_CLIENT_SECRET: optionalString,
  OAUTH_ISSUER: z.url().optional(),
  TELEMETRY_ROOT_URL: z.url().optional(),
  SESSION_SECRET: z.string().trim().min(32).optional(),
  ANALYSIS_INGEST_TOKEN: z.string().trim().min(32).optional(),
  KAFKA_PRIVATE_BOOTSTRAP: optionalString,
  KAFKA_EVENT_TOPIC: optionalString,
};

export type RawEnv = z.infer<z.ZodObject<typeof serverEnvShape>>;
