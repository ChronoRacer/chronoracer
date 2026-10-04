import type { GapEvent } from "../core/types.js";
import type { PreviewStore } from "./PreviewStore.js";
import type { GapJson, SnapshotPayload, StoreSnapshot } from "./types.js";

export function gapsToJson(gaps: GapEvent[]): GapJson[] {
  return gaps.map((gap) => {
    if (gap.kind === "core") {
      return {
        kind: gap.kind,
        firstSequence: gap.firstSequence.toString(),
        droppedPointCount: gap.droppedPointCount.toString(),
        atMs: gap.atMs,
      };
    }
    return {
      kind: gap.kind,
      droppedPointCount: gap.droppedPointCount,
      atMs: gap.atMs,
      detail: gap.detail,
    };
  });
}

export function buildSnapshotPayload(
  store: PreviewStore,
  sensorFilter: string[] | undefined,
): SnapshotPayload {
  const snap: StoreSnapshot = store.snapshot(sensorFilter);
  return {
    type: "snapshot",
    connected: snap.connected,
    hello: snap.hello,
    sensorIds: snap.sensorIds,
    pointsBySensor: snap.pointsBySensor,
    gaps: gapsToJson(snap.gaps),
    stats: snap.stats,
  };
}
