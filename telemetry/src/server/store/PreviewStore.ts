import Denque from "denque";
import type { GapEvent, PreviewPoint, StreamHello } from "../core/types.js";
import type { RingLimits } from "../env/index.js";
import { BYTES_PER_POINT_ESTIMATE, MAX_GAPS_COUNT } from "./constants.js";
import type { RingStats, StoreSnapshot } from "./types.js";

export class PreviewStore {
  private readonly points = new Map<string, Denque<PreviewPoint>>();
  private readonly gaps = new Denque<GapEvent>();
  private hello: StreamHello | undefined;
  private connected = false;
  private totalPoints = 0;
  private droppedPoints = 0;
  private droppedSensors = 0;

  constructor(private readonly limits: RingLimits) {}

  setConnected(connected: boolean): void {
    this.connected = connected;
  }

  setHello(hello: StreamHello): void {
    if (this.hello !== undefined && this.hello.streamId !== hello.streamId) {
      this.recordGap({
        kind: "stream_restart",
        droppedPointCount: 0,
        atMs: Date.now(),
        detail: `stream_id ${this.hello.streamId} -> ${hello.streamId}`,
      });
      this.clearPoints("stream_restart");
    }
    this.hello = hello;
  }

  recordReconnect(): void {
    this.recordGap({
      kind: "reconnect",
      droppedPointCount: 0,
      atMs: Date.now(),
    });
  }

  ingestPoints(points: PreviewPoint[]): number {
    let localDrops = 0;
    const now = Date.now();

    for (const point of points) {
      if (
        !this.points.has(point.sensorId) &&
        this.points.size >= this.limits.maxSensors
      ) {
        this.droppedSensors += 1;
        localDrops += 1;
        this.droppedPoints += 1;
        continue;
      }

      let series = this.points.get(point.sensorId);
      if (series === undefined) {
        series = new Denque<PreviewPoint>();
        this.points.set(point.sensorId, series);
      }

      series.push(point);
      this.totalPoints += 1;
    }

    localDrops += this.trimAll(now);

    if (localDrops > 0) {
      this.recordGap({
        kind: "local_drop",
        droppedPointCount: localDrops,
        atMs: now,
        detail: "ring_cap",
      });
    }
    return localDrops;
  }

  recordCoreGap(firstSequence: bigint, droppedPointCount: bigint): void {
    this.recordGap({
      kind: "core",
      firstSequence,
      droppedPointCount,
      atMs: Date.now(),
    });
  }

  recordGap(gap: GapEvent): void {
    this.gaps.push(gap);
    while (this.gaps.length > MAX_GAPS_COUNT) {
      this.gaps.shift();
    }
  }

  sensorIds(): string[] {
    return [...this.points.keys()].sort();
  }

  stats(): RingStats {
    return {
      pointsInStore: this.totalPoints,
      sensorCount: this.points.size,
      approxBytes: this.totalPoints * BYTES_PER_POINT_ESTIMATE,
      droppedPoints: this.droppedPoints,
      droppedSensors: this.droppedSensors,
    };
  }

  snapshot(sensorFilter: string[] | undefined): StoreSnapshot {
    const filter =
      sensorFilter !== undefined ? new Set(sensorFilter) : undefined;
    const pointsBySensor: StoreSnapshot["pointsBySensor"] = {};
    for (const [sensorId, series] of this.points) {
      if (filter !== undefined && !filter.has(sensorId)) {
        continue;
      }
      pointsBySensor[sensorId] = series.toArray().map((p) => ({
        timestampNanos: p.timestampNanos.toString(),
        value: p.value,
        driverId: p.driverId,
      }));
    }
    return {
      hello: this.hello,
      connected: this.connected,
      pointsBySensor,
      gaps: this.gaps.toArray(),
      sensorIds: this.sensorIds(),
      stats: this.stats(),
    };
  }

  recentPoints(
    sensorFilter: string[] | undefined,
    sinceMs: number,
  ): PreviewPoint[] {
    const filter =
      sensorFilter !== undefined ? new Set(sensorFilter) : undefined;
    const sinceNanos = BigInt(sinceMs) * 1_000_000n;
    const out: PreviewPoint[] = [];

    for (const [sensorId, series] of this.points) {
      if (filter !== undefined && !filter.has(sensorId)) {
        continue;
      }
      const len = series.length;
      let i = 0;
      for (; i < len; i += 1) {
        const p = series.get(i);
        if (p === undefined || p.timestampNanos >= sinceNanos) {
          break;
        }
      }
      for (; i < len; i += 1) {
        const p = series.get(i);
        if (p !== undefined) {
          out.push(p);
        }
      }
    }
    return out;
  }

  private clearPoints(reason: string): void {
    const n = this.totalPoints;
    this.points.clear();
    this.totalPoints = 0;
    if (n > 0) {
      this.droppedPoints += n;
      this.recordGap({
        kind: "local_drop",
        droppedPointCount: n,
        atMs: Date.now(),
        detail: reason,
      });
    }
  }

  private trimAll(nowMs: number): number {
    let dropped = 0;
    const cutoffNanos = BigInt(nowMs - this.limits.maxAgeMs) * 1_000_000n;

    for (const series of this.points.values()) {
      dropped += this.trimSeries(series, cutoffNanos);
    }

    dropped += this.trimOverflowRoundRobin();
    this.pruneEmptySeries();
    return dropped;
  }

  private trimSeries(
    series: Denque<PreviewPoint>,
    cutoffNanos: bigint,
  ): number {
    let dropped = 0;

    while (
      !series.isEmpty() &&
      series.peekFront()!.timestampNanos < cutoffNanos
    ) {
      series.shift();
      this.totalPoints -= 1;
      this.droppedPoints += 1;
      dropped += 1;
    }

    const excess = series.length - this.limits.maxPointsPerSensor;
    if (excess > 0) {
      series.remove(0, excess);
      this.totalPoints -= excess;
      this.droppedPoints += excess;
      dropped += excess;
    }
    return dropped;
  }

  private trimOverflowRoundRobin(): number {
    let dropped = 0;

    while (this.totalPoints > this.limits.maxTotalPoints) {
      let progressed = false;
      for (const series of this.points.values()) {
        if (this.totalPoints <= this.limits.maxTotalPoints) {
          break;
        }
        if (series.isEmpty()) {
          continue;
        }
        series.shift();
        this.totalPoints -= 1;
        this.droppedPoints += 1;
        dropped += 1;
        progressed = true;
      }
      if (!progressed) {
        break;
      }
    }

    return dropped;
  }

  private pruneEmptySeries(): void {
    for (const [sensorId, series] of this.points) {
      if (series.isEmpty()) {
        this.points.delete(sensorId);
      }
    }
  }
}
