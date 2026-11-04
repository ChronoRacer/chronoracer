import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Point } from "../core/types.js";
import { PreviewStore } from "./PreviewStore.js";

const hello = (streamId: string) => ({
  protocolVersion: 1,
  raceId: "race",
  streamId,
  nextSequence: 1n,
  selectedSensorCount: 0,
});
const point = (sensorId: string, ms: number, value = ms): Point => ({
  sensorId,
  timestampNanos: BigInt(ms) * 1_000_000n,
  value,
  driverId: "d",
});
const gapKinds = (store: PreviewStore) => store.drain().gaps.map((gap) => gap.kind);
const limits = { bucketMs: 10, maxAgeMs: 10_000, maxTotalBuckets: 100 };

describe("PreviewStore", () => {
  beforeEach(() => vi.useFakeTimers());

  it("folds points into each bucket's min and max in the order they occurred; non-finite is a hole", () => {
    const store = new PreviewStore(limits);
    store.ingest([point("a", 0, 5), point("a", 3, 9), point("a", 6, 1), point("a", 12, 4), point("a", 21, 7)]);
    store.ingest([point("a", 22, Number.NaN)]);
    expect(store.snapshot().buckets).toEqual({ a: [0, 9, 1, 1, 4, 4, 2, null, null] });
  });

  it("keeps the newest sample by source time for instruments", () => {
    const store = new PreviewStore(limits);
    store.ingest([point("a", 20, 2), point("a", 10, 1)]);
    expect(store.snapshot().latest).toEqual({ a: 2 });
  });

  it("drains only changed buckets, and a late point resends its bucket", () => {
    const store = new PreviewStore(limits);
    store.ingest([point("a", 0, 1), point("a", 10, 2), point("b", 10, 3)]);
    expect(store.drain()).toEqual({ buckets: { a: [0, 1, 1, 1, 2, 2], b: [1, 3, 3] }, latest: { a: 2, b: 3 }, gaps: [] });

    store.ingest([point("a", 5, 9)]);
    expect(store.drain()).toEqual({ buckets: { a: [0, 1, 9] }, latest: {}, gaps: [] });
    expect(store.drain()).toEqual({ buckets: {}, latest: {}, gaps: [] });
  });

  it("evicts silently for retention: per-sensor cap, total cap, age", () => {
    const store = new PreviewStore({ bucketMs: 10, maxAgeMs: 30, maxTotalBuckets: 5 });
    store.ingest([point("a", 0), point("a", 10), point("a", 20), point("a", 30)]);
    store.ingest([point("b", 0), point("b", 10), point("b", 20)]);
    expect(store.stats()).toMatchObject({ bucketsInStore: 5, evictedBuckets: 2 });

    vi.advanceTimersByTime(31);
    store.ingest([point("b", 40)]);
    expect(store.stats()).toMatchObject({ bucketsInStore: 1, sensorCount: 1 });
    expect(gapKinds(store)).toEqual([]);
  });

  it("under the total cap, drops the globally oldest arrival, not round-robin", () => {
    const store = new PreviewStore({ bucketMs: 10, maxAgeMs: 60_000, maxTotalBuckets: 1 });
    store.ingest([point("a", 0), point("a", 10)]);
    vi.advanceTimersByTime(1_000);
    store.ingest([point("b", 0)]);
    expect(store.stats()).toMatchObject({ bucketsInStore: 1, sensorCount: 1 });
    expect(store.sensorIds()).toEqual(["b"]);
    expect(store.drain().buckets).toEqual({ b: [0, 0, 0] });
  });

  it("tracks every new sensor without a sensor-count cap", () => {
    const store = new PreviewStore(limits);
    store.ingest([point("a", 0), point("b", 0), point("c", 0)]);
    expect(store.sensorIds()).toEqual(["a", "b", "c"]);
    expect(gapKinds(store)).toEqual([]);
  });

  it("keeps buckets across a Core restart and marks reconnect and restart", () => {
    const store = new PreviewStore(limits);
    store.connect(hello("A"));
    store.ingest([point("a", 0)]);
    store.disconnect();
    store.connect(hello("B"));
    expect(gapKinds(store)).toEqual(["reconnect", "stream_restart"]);
    expect(store.snapshot()).toMatchObject({ connected: true, hello: { streamId: "B" }, sensorIds: ["a"] });
  });

  it("filters snapshots by sensor", () => {
    const store = new PreviewStore(limits);
    store.ingest([point("a", 0), point("b", 0)]);
    const snapshot = store.snapshot(new Set(["b"]));
    expect(Object.keys(snapshot.buckets)).toEqual(["b"]);
    expect(snapshot).toMatchObject({ bucketMs: 10, latest: { b: 0 } });
  });
});
