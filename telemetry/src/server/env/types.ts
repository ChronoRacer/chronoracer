/**
 * @fileoverview Telemetry Server Environment & Runtime Configuration Types
 *
 * Defines parsed environment settings including storage retention rules,
 * WebSocket fanout rate-limiting, and optional OAuth/OIDC authentication.
 */

export type SensorSelection = string[] | undefined;

export type RingLimits = {
  bucketMs: number;
  maxAgeMs: number;
  maxTotalBuckets: number;
};

export type FanoutConfig = {
  tickMs: number;
  maxBufferedBytes: number;
};

export type OAuthConfig = {
  clientId: string;
  clientSecret: string;
  issuer: string;
  rootUrl: string;
  sessionSecret: string;
};

export type AuthConfig = { enabled: false } | ({ enabled: true } & OAuthConfig);

/** Kafka private listener for driver messages; undefined disables sending. */
export type DriverKafkaConfig = { bootstrap: string; topic: string } | undefined;

export type TelemetryEnv = {
  port: number;
  coreTelemetryUrl: string;
  raceId: string | undefined;
  sensors: SensorSelection;
  ring: RingLimits;
  fanout: FanoutConfig;
  auth: AuthConfig;
  analysisIngestToken: string | undefined;
  driverKafka: DriverKafkaConfig;
  clientDir: string | undefined;
};
