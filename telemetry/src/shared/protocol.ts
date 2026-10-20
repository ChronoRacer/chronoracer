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

export type Point<U64 = string> = {
  sensorId: string;
  timestampNanos: U64;
  value: number;
  driverId: string;
};

export type SnapshotPoint<U64 = string> = Omit<Point<U64>, 'sensorId'>;

export type Gap<U64 = string> =
  | { kind: 'core'; atMs: number; firstSequence: U64; droppedPointCount: U64 }
  | { kind: 'reconnect'; atMs: number }
  | { kind: 'stream_restart'; atMs: number; detail: string };

export type CoreStatus<U64 = string> = {
  connected: boolean;
  hello: Hello<U64> | undefined;
};

export type RingStats = {
  pointsInStore: number;
  sensorCount: number;
  approxBytes: number;
  evictedPoints: number;
};

export type Snapshot<U64 = string> = CoreStatus<U64> & {
  sensorIds: string[];
  pointsBySensor: Record<string, SnapshotPoint<U64>[]>;
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

export type ServerMessage<U64 = string> =
  | ({ type: 'snapshot' } & Snapshot<U64> & AnalysisSnapshot)
  | ({ type: 'status' } & CoreStatus<U64>)
  | { type: 'points'; points: Point<U64>[] }
  | { type: 'gap'; gaps: Gap<U64>[] }
  | ({ type: 'analysis' } & AnalysisSnapshot)
  | { type: 'overloaded'; atMs: number };

export type ClientMessage = { type: 'subscribe'; all: true } | { type: 'subscribe'; sensors: string[] };
