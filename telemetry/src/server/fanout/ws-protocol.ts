import { z } from "zod";
import type { GapEvent, PreviewPoint } from "../core/types.js";
import { gapsToJson } from "../store/index.js";

export type PointWire = {
  sensorId: string;
  timestampNanos: string;
  value: number;
  driverId: string;
};

export type ServerMessage =
  | { type: "points"; points: PointWire[] }
  | { type: "gap"; gaps: ReturnType<typeof gapsToJson> }
  | { type: "status"; [key: string]: unknown }
  | { type: "overloaded"; atMs: number }
  | { type: "hello"; hello: object };

export const encode = (message: ServerMessage): string =>
  JSON.stringify(message);

export const pointsWire = (points: PreviewPoint[]): PointWire[] =>
  points.map((p) => ({
    sensorId: p.sensorId,
    timestampNanos: p.timestampNanos.toString(),
    value: p.value,
    driverId: p.driverId,
  }));

export const encodePoints = (points: PreviewPoint[]): string =>
  encode({ type: "points", points: pointsWire(points) });

export const encodeGaps = (gaps: GapEvent[]): string =>
  encode({ type: "gap", gaps: gapsToJson(gaps) });

const clientMessageSchema = z.union([
  z.object({ type: z.literal("ping") }),
  z.object({ type: z.literal("subscribe"), all: z.literal(true) }),
  z.object({ type: z.literal("subscribe"), sensors: z.array(z.string()) }),
]);

export type ClientToServer = z.infer<typeof clientMessageSchema>;

export function parseClientMessage(raw: string): ClientToServer | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  const result = clientMessageSchema.safeParse(parsed);
  return result.success ? result.data : undefined;
}

/** Stable cache key for a sensor filter; `undefined` / empty → all sensors (`*`). */
export const subscriptionKey = (sensors: Set<string> | undefined): string =>
  sensors === undefined || sensors.size === 0
    ? "*"
    : [...sensors].sort().join(",");
