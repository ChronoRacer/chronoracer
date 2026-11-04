/**
 * @fileoverview Telemetry Protocol & WebSocket API Types
 * @description Defines data structures for bidirectional communication
 * between the telemetry server and the client interface/dashboard.
 *
 * @template U64 Represents 64-bit integers. Defaults to `string` to prevent
 * precision loss with standard JS numbers above `Number.MAX_SAFE_INTEGER`.
 * Can be overridden with `bigint` if supported by the client application.
 */

export type Hello<U64 = string> = {
  protocolVersion: number;
  raceId: string;
  streamId: string;
  nextSequence: U64;
  selectedSensorCount: number;
};

/**
 * One sensor's buckets on the shared time grid, flat as `index, a, b` per bucket, ascending by `index`. Bucket
 * `index` covers source time `[index, index + 1) × bucketMs`. `a` and `b` are its minimum and maximum in the order
 * they occurred (equal for one sample); both are `null` when the bucket held a non-finite sample, which is a hole.
 */
export type Buckets = (number | null)[];

/** Newest sample per sensor by source time, for instruments; `null` when it is not finite. */
export type Latest = Record<string, number | null>;

export type Gap<U64 = string> =
  | { kind: 'core'; atMs: number; firstSequence: U64; droppedPointCount: U64 }
  | { kind: 'reconnect'; atMs: number }
  | { kind: 'stream_restart'; atMs: number; detail: string };

export type CoreStatus<U64 = string> = {
  connected: boolean;
  hello: Hello<U64> | undefined;
};

export type RingStats = {
  bucketsInStore: number;
  sensorCount: number;
  approxBytes: number;
  evictedBuckets: number;
};

export type Snapshot<U64 = string> = CoreStatus<U64> & {
  sensorIds: string[];
  bucketMs: number;
  buckets: Record<string, Buckets>;
  latest: Latest;
  gaps: Gap<U64>[];
  stats: RingStats;
};

export type PredictionValue = {
  timestampUs: number;
  sensorIds: string[];
  instruction: string | null;
  value: number | null;
};

export type Prediction = {
  predictionId: string;
  name?: string;
  refreshRate?: number | null;
  announcedAtUs?: number;
  unavailable?: { timestampUs: number; info: string };
  latest?: PredictionValue;
  receivedAtMs: number;
};

export type Alert = {
  timestampUs: number;
  alertId: string;
  sensors: { sensorId: string; value: number }[];
  message: string;
  reason: string;
  receivedAtMs: number;
};

export type AnalysisSnapshot = {
  predictions: Prediction[];
  alerts: Alert[];
};

/**
 * Kafka keys of driver messages on the `event` topic; the body is the UTF-8 text. Contract:
 * docs/component-kafka.md#event.
 */
export const DRIVER_KEYS = ['MSG', 'BOX', 'FLAG', 'PACE'] as const;
export type DriverKey = (typeof DRIVER_KEYS)[number];
/** Characters, not bytes. */
export const DRIVER_TEXT_MAX = 200;

export type DriverMessageRequest = { key: DriverKey; text: string };

/** `delivered` means Kafka acknowledged the write, not that the driver read it. */
export type DriverMessageStatus = 'sending' | 'delivered' | 'failed';

export type DriverMessage = DriverMessageRequest & {
  id: string;
  sentBy: string;
  sentAtMs: number;
  status: DriverMessageStatus;
  error?: string;
};

/** `driverEnabled` is false when the server has no Kafka configuration; sending then answers 503. */
export type DriverSnapshot = { driverEnabled: boolean; driverMessages: DriverMessage[] };

export type ServerMessage<U64 = string> =
  /** `tickMs`: interval between pushes; the browser animates each update over one tick. */
  | ({ type: 'snapshot'; tickMs: number } & Snapshot<U64> & AnalysisSnapshot & DriverSnapshot)
  | ({ type: 'status' } & CoreStatus<U64>)
  /** Buckets that changed since the last push; a bucket the browser already has replaces it. */
  | { type: 'buckets'; buckets: Record<string, Buckets>; latest: Latest }
  | { type: 'gap'; gaps: Gap<U64>[] }
  | ({ type: 'analysis' } & AnalysisSnapshot)
  /** Driver messages that are new or changed status; the browser replaces its copy by `id`. */
  | { type: 'driver'; messages: DriverMessage[] }
  | { type: 'overloaded'; atMs: number };

export type ClientMessage = { type: 'subscribe'; all: true } | { type: 'subscribe'; sensors: string[] };
