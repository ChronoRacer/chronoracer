export type PreviewPoint = {
  sensorId: string;
  timestampNanos: bigint;
  value: number;
  driverId: string;
};

export type CoreGap = {
  kind: "core";
  firstSequence: bigint;
  droppedPointCount: bigint;
  atMs: number;
};

export type LocalGap = {
  kind: "local_drop" | "reconnect" | "stream_restart";
  droppedPointCount: number;
  atMs: number;
  detail?: string;
};

export type GapEvent = CoreGap | LocalGap;

export type StreamHello = {
  protocolVersion: number;
  raceId: string;
  streamId: string;
  nextSequence: string;
  selectedSensorCount: number;
};
