import type { GapEvent, StreamHello } from "../core/types.js";

export type RingStats = {
  pointsInStore: number;
  sensorCount: number;
  approxBytes: number;
  droppedPoints: number;
  droppedSensors: number;
};

export type SnapshotPoint = {
  timestampNanos: string;
  value: number;
  driverId: string;
};

export type StoreSnapshot = {
  hello: StreamHello | undefined;
  connected: boolean;
  pointsBySensor: Record<string, SnapshotPoint[]>;
  gaps: GapEvent[];
  sensorIds: string[];
  stats: RingStats;
};

export type CoreGapJson = {
  kind: "core";
  firstSequence: string;
  droppedPointCount: string;
  atMs: number;
};

export type LocalGapJson = {
  kind: "local_drop" | "reconnect" | "stream_restart";
  droppedPointCount: number;
  atMs: number;
  detail?: string;
};

export type GapJson = CoreGapJson | LocalGapJson;

export type SnapshotPayload = {
  type: "snapshot";
  connected: boolean;
  hello: StreamHello | undefined;
  sensorIds: string[];
  pointsBySensor: Record<string, SnapshotPoint[]>;
  gaps: GapJson[];
  stats: RingStats;
};
