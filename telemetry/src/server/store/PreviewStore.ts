/**
 * @fileoverview In-Memory Ring Buffer Store for Live Telemetry
 *
 * High-performance, time-series ring buffer using double-ended queues (`Denque`).
 * Handles point ingestion, stream gap detection, memory eviction rules (TTL + capacity limits),
 * and incremental delta accumulation for downstream websockets.
 */

import Denque from 'denque';
import type { CoreGap, Hello, Point } from '../core/types.js';
import type { RingLimits } from '../env/index.js';
import { BYTES_PER_POINT_ESTIMATE, MAX_GAPS_COUNT } from './constants.js';
import type { Delta, GapEvent, RingStats, StoreSnapshot, StoreStatus } from './types.js';

type Entry = { point: Point; receivedAtMs: number };

export class PreviewStore {
  private readonly series = new Map<string, Denque<Entry>>();
  private readonly gaps = new Denque<GapEvent>();
  private delta: Delta = { points: [], gaps: [] };
  private hello: Hello | undefined;
  private connected = false;
  private pointCount = 0;
  private evictedPoints = 0;

  constructor(private readonly limits: RingLimits) {}

  connect(hello: Hello): void {
    if (this.hello && this.hello.streamId !== hello.streamId) {
      const detail = `stream ${this.hello.streamId} -> ${hello.streamId}`;
      this.recordGap({ kind: 'stream_restart', atMs: Date.now(), detail });
    }
    this.hello = hello;
    this.connected = true;
  }

  disconnect(): void {
    if (this.connected) {
      this.connected = false;
      this.recordGap({ kind: 'reconnect', atMs: Date.now() });
    }
  }

  ingest(points: Point[]): void {
    const now = Date.now();
    for (const point of points) {
      this.seriesFor(point.sensorId).push({ point, receivedAtMs: now });
      this.pointCount += 1;
      this.delta.points.push(point);
    }
    this.evict(now);
  }

  recordCoreGap({ firstSequence, droppedPointCount }: CoreGap): void {
    this.recordGap({ kind: 'core', atMs: Date.now(), firstSequence, droppedPointCount });
  }

  status(): StoreStatus {
    return { connected: this.connected, hello: this.hello };
  }

  sensorIds(): string[] {
    return [...this.series.keys()].sort();
  }

  stats(): RingStats {
    return {
      pointsInStore: this.pointCount,
      sensorCount: this.series.size,
      approxBytes: this.pointCount * BYTES_PER_POINT_ESTIMATE,
      evictedPoints: this.evictedPoints,
    };
  }

  snapshot(sensors?: ReadonlySet<string>): StoreSnapshot {
    const pointsBySensor: StoreSnapshot['pointsBySensor'] = {};
    for (const [sensorId, series] of this.series) {
      if (sensors === undefined || sensors.has(sensorId)) {
        pointsBySensor[sensorId] = series
          .toArray()
          .map(({ point: { timestampNanos, value, driverId } }) => ({ timestampNanos, value, driverId }));
      }
    }
    return {
      ...this.status(),
      sensorIds: this.sensorIds(),
      pointsBySensor,
      gaps: this.gaps.toArray(),
      stats: this.stats(),
    };
  }

  drain(): Delta {
    const delta = this.delta;
    this.delta = { points: [], gaps: [] };
    return delta;
  }

  private recordGap(gap: GapEvent): void {
    this.gaps.push(gap);
    if (this.gaps.length > MAX_GAPS_COUNT) {
      this.gaps.shift();
    }
    this.delta.gaps.push(gap);
  }

  private seriesFor(sensorId: string): Denque<Entry> {
    const existing = this.series.get(sensorId);
    if (existing) {
      return existing;
    }
    const series = new Denque<Entry>();
    this.series.set(sensorId, series);
    return series;
  }

  private evict(nowMs: number): void {
    const { maxAgeMs, maxPointsPerSensor, maxTotalPoints } = this.limits;
    const cutoffMs = nowMs - maxAgeMs;
    const tooOld = (series: Denque<Entry>) => (series.peekFront()?.receivedAtMs ?? Infinity) < cutoffMs;

    for (const series of this.series.values()) {
      while (series.length > maxPointsPerSensor || tooOld(series)) {
        this.evictOldest(series);
      }
    }
    while (this.pointCount > maxTotalPoints) {
      for (const series of this.series.values()) {
        if (this.pointCount > maxTotalPoints && !series.isEmpty()) {
          this.evictOldest(series);
        }
      }
    }
    for (const [sensorId, series] of this.series) {
      if (series.isEmpty()) {
        this.series.delete(sensorId);
      }
    }
  }

  private evictOldest(series: Denque<Entry>): void {
    series.shift();
    this.pointCount -= 1;
    this.evictedPoints += 1;
  }
}
