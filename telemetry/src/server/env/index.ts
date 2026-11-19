/**
 * @fileoverview Environment Loader & Configuration Mapper
 *
 * Loads, validates, and transforms process environment variables into a strongly-typed
 * `TelemetryEnv` config object. Handles fail-fast validation errors and constructs
 * conditional authentication configurations (OAuth vs disabled).
 */

import { createEnv } from '@t3-oss/env-core';
import { serverEnvShape, type RawEnv } from './schema.js';
import type { AuthConfig, DriverKafkaConfig, TelemetryEnv } from './types.js';

export type {
  AuthConfig,
  DriverKafkaConfig,
  FanoutConfig,
  OAuthConfig,
  RingLimits,
  SensorSelection,
  TelemetryEnv,
} from './types.js';

export function loadEnv(): TelemetryEnv {
  const raw = createEnv({
    server: serverEnvShape,
    runtimeEnv: process.env,
    emptyStringAsUndefined: true,
    onValidationError: (issues) => {
      const details = issues.map((issue) => `${issue.path?.join('.')}: ${issue.message}`);
      throw new Error(`invalid environment: ${details.join('; ')}`);
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
      bucketMs: raw.TELEMETRY_BUCKET_MS,
      maxAgeMs: raw.TELEMETRY_RETENTION_MS,
      maxTotalBuckets: raw.TELEMETRY_MAX_BUCKETS,
    },
    fanout: {
      tickMs: raw.TELEMETRY_FANOUT_TICK_MS,
      maxBufferedBytes: raw.TELEMETRY_WS_MAX_BUFFERED_BYTES,
    },
    clientDir: raw.TELEMETRY_CLIENT_DIR || undefined,
    auth: toAuthConfig(raw),
    analysisIngestToken: raw.ANALYSIS_INGEST_TOKEN || undefined,
    driverKafka: toDriverKafka(raw),
  };
}

function toDriverKafka({ KAFKA_PRIVATE_BOOTSTRAP: bootstrap, KAFKA_EVENT_TOPIC: topic }: RawEnv): DriverKafkaConfig {
  if (!bootstrap && !topic) {
    return undefined;
  }
  if (!bootstrap || !topic) {
    throw new Error('Driver messages need both KAFKA_PRIVATE_BOOTSTRAP and KAFKA_EVENT_TOPIC, or neither');
  }
  return { bootstrap, topic };
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
      'OAuth config incomplete: set OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_ISSUER and TELEMETRY_ROOT_URL' +
        ' (or TELEMETRY_AUTH_DISABLED=1 for local runs)',
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
