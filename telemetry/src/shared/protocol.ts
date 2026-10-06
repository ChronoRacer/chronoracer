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

export type SnapshotPoint<U64 = string> = Omit<Point<U64>, "sensorId">;

export type Gap<U64 = string> =
  | { kind: "core"; atMs: number; firstSequence: U64; droppedPointCount: U64 }
  | { kind: "reconnect"; atMs: number }
  | { kind: "stream_restart"; atMs: number; detail: string };

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

export type ServerMessage<U64 = string> =
  | ({ type: "snapshot" } & Snapshot<U64>)
  | ({ type: "status" } & CoreStatus<U64>)
  | { type: "points"; points: Point<U64>[] }
  | { type: "gap"; gaps: Gap<U64>[] }
  | { type: "overloaded"; atMs: number };

export type ClientMessage =
  | { type: "subscribe"; all: true }
  | { type: "subscribe"; sensors: string[] };
