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
 * Operator → driver flags and messages: Kafka record keys on the `event` topic. Wire contract:
 * docs/component-kafka.md (driver display records).
 */
export const DRIVER_FLAG_TYPES = ['YELLOW', 'RED', 'GREEN', 'BLUE', 'SAFETY', 'CHECKERED'] as const;
/** Types whose `show` requires `text`. */
export const DRIVER_TEXT_TYPES = ['BOX', 'PACE', 'MSG'] as const;
export const DRIVER_TYPES = [...DRIVER_FLAG_TYPES, ...DRIVER_TEXT_TYPES] as const;
export type DriverFlagType = (typeof DRIVER_FLAG_TYPES)[number];
export type DriverType = (typeof DRIVER_TYPES)[number];
/** Key of a `clear` that removes every active message. */
export const DRIVER_ALL = 'ALL';
export type DriverTarget = DriverType | typeof DRIVER_ALL;

/** String length, not bytes. */
export const DRIVER_TEXT_MAX = 200;
export const DRIVER_DURATION_DEFAULT_S = 5;
export const DRIVER_DURATION_MAX_S = 3600;
/** `durationS` of a message that stays until the operator clears it. */
export const DRIVER_UNTIL_CLEARED = -1;

export const isDriverFlag = (type: DriverTarget): type is DriverFlagType =>
  (DRIVER_FLAG_TYPES as readonly string[]).includes(type);

/** Browser → server. `durationS` defaults to {@link DRIVER_DURATION_DEFAULT_S}. */
export type DriverRequest =
  | { action: 'show'; type: DriverType; text?: string; durationS?: number }
  | { action: 'clear'; type: DriverTarget };

/** One operator action as the server accepted it; `durationS` is resolved on `show`. */
export type DriverCommand =
  | { action: 'show'; type: DriverType; text?: string; durationS: number }
  | { action: 'clear'; type: DriverTarget };

/** `delivered` means Kafka acknowledged the write, not that the driver read it. */
export type DriverMessageStatus = 'sending' | 'delivered' | 'failed';

export type DriverMessage = DriverCommand & {
  id: string;
  sentBy: string;
  sentAtMs: number;
  status: DriverMessageStatus;
  error?: string;
};

/**
 * A type the operators last showed, did not clear, and whose time has not run out, counted from the Kafka ack. The
 * operators' own view: the car sends nothing back.
 */
export type DriverActive = {
  type: DriverType;
  text?: string;
  durationS: number;
  shownAtMs: number;
  /** `null` until cleared. */
  expiresAtMs: number | null;
  sentBy: string;
};

/**
 * `driverEnabled` is false when the server has no Kafka configuration; sending then answers 503. `driverActive` is
 * oldest first.
 */
export type DriverSnapshot = { driverEnabled: boolean; driverMessages: DriverMessage[]; driverActive: DriverActive[] };

export type ServerMessage<U64 = string> =
  /** `tickMs`: interval between pushes; the browser animates each update over one tick. */
  | ({ type: 'snapshot'; tickMs: number } & Snapshot<U64> & AnalysisSnapshot & DriverSnapshot)
  | ({ type: 'status' } & CoreStatus<U64>)
  /** Buckets that changed since the last push; a bucket the browser already has replaces it. */
  | { type: 'buckets'; buckets: Record<string, Buckets>; latest: Latest }
  | { type: 'gap'; gaps: Gap<U64>[] }
  | ({ type: 'analysis' } & AnalysisSnapshot)
  /**
   * Driver messages that are new or changed status; the browser replaces its copy by `id`. `active` is the whole
   * active set, present only when it changed.
   */
  | { type: 'driver'; messages: DriverMessage[]; active?: DriverActive[] }
  | { type: 'overloaded'; atMs: number };

export type ClientMessage = { type: 'subscribe'; all: true } | { type: 'subscribe'; sensors: string[] };
