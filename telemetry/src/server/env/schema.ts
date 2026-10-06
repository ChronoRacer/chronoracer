// Environment variables validation schema for the telemetry server.

import { z } from 'zod';

const positiveInt = (fallback: number) => z.coerce.number().int().positive().default(fallback);
const optionalString = z.string().trim().optional();

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
  TELEMETRY_SENSORS: z.string().optional().transform(parseSensorList).pipe(z.array(z.string()).min(1).optional()),
  TELEMETRY_MAX_POINTS_PER_SENSOR: positiveInt(1000),
  TELEMETRY_RETENTION_MS: positiveInt(5 * 60_000),
  TELEMETRY_MAX_SENSORS: positiveInt(500),
  TELEMETRY_MAX_POINTS: positiveInt(50_000),
  TELEMETRY_FANOUT_TICK_MS: positiveInt(200),
  TELEMETRY_WS_MAX_BUFFERED_BYTES: positiveInt(1_048_576),
  TELEMETRY_CLIENT_DIR: optionalString,
  TELEMETRY_AUTH_DISABLED: z.stringbool().default(false),
  OAUTH_CLIENT_ID: optionalString,
  OAUTH_CLIENT_SECRET: optionalString,
  OAUTH_ISSUER: z.url().optional(),
  TELEMETRY_ROOT_URL: z.url().optional(),
  SESSION_SECRET: optionalString,
};

export type RawEnv = z.infer<z.ZodObject<typeof serverEnvShape>>;
