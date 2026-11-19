/**
 * @fileoverview In-Memory Bucket Store for Live Telemetry
 *
 * Folds every point into its sensor's bucket of `bucketMs` source time, on a grid shared by all sensors, keeping the
 * bucket's minimum and maximum. The browser draws about one bucket per pixel, so its work stays bounded at any input
 * rate and peaks are not lost. Handles stream gap detection, retention (arrival age + caps), and collects what
 * changed since the last drain for downstream websockets.
 */

import Denque from 'denque';
import type { CoreGap, Hello, Point } from '../core/types.js';
import type { RingLimits } from '../env/index.js';
import { BYTES_PER_BUCKET_ESTIMATE, MAX_GAPS_COUNT } from './constants.js';
import type { Buckets, Delta, GapEvent, Latest, RingStats, StoreSnapshot, StoreStatus } from './types.js';

type Bucket = {
  index: number;
  /** Over finite samples. */
  min: number;
  max: number;
  /** Source time of `min` and `max` (ns as a float), only to order them. */
  minAt: number;
  maxAt: number;
  /** Held a non-finite sample: sent as a hole. */
  hole: boolean;
  /** Changed since the last drain. */
  dirty: boolean;
  /** Arrival of its first sample, for retention. */
  receivedAtMs: number;
};

type Sensor = {
  /** By bucket index. Iteration order is the arrival order of each bucket's first sample. */
  buckets: Map<number, Bucket>;
  /** Newest sample by source time, for instruments. */
  lastAt: number;
  lastValue: number;
  lastDirty: boolean;
};

export class PreviewStore {
  private readonly sensors = new Map<string, Sensor>();
  private readonly gaps = new Denque<GapEvent>();
  private readonly bucketNanos: number;
  /** A sensor with a sane clock fills at most one bucket per `bucketMs` within the retention age. */
  private readonly maxBucketsPerSensor: number;
  private changedBuckets: [string, Bucket][] = [];
  private changedLatest: string[] = [];
  private newGaps: GapEvent[] = [];
  private hello: Hello | undefined;
  private connected = false;
  private bucketCount = 0;
  private evictedBuckets = 0;

  constructor(private readonly limits: RingLimits) {
    this.bucketNanos = limits.bucketMs * 1_000_000;
    this.maxBucketsPerSensor = Math.ceil(limits.maxAgeMs / limits.bucketMs);
  }

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
    for (const { sensorId, timestampNanos, value } of points) {
      // A float loses sub-microsecond precision at today's epoch, which cannot move a sample across a bucket.
      const at = Number(timestampNanos);
      const sensor = this.sensorFor(sensorId);
      fold(this.bucketFor(sensorId, sensor, Math.floor(at / this.bucketNanos), now), at, value);
      if (at >= sensor.lastAt) {
        sensor.lastAt = at;
        sensor.lastValue = value;
        if (!sensor.lastDirty) {
          sensor.lastDirty = true;
          this.changedLatest.push(sensorId);
        }
      }
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
    return [...this.sensors.keys()].sort();
  }

  stats(): RingStats {
    return {
      bucketsInStore: this.bucketCount,
      sensorCount: this.sensors.size,
      approxBytes: this.bucketCount * BYTES_PER_BUCKET_ESTIMATE,
      evictedBuckets: this.evictedBuckets,
    };
  }

  snapshot(sensors?: ReadonlySet<string>): StoreSnapshot {
    const buckets: Record<string, Buckets> = {};
    const latest: Latest = {};
    for (const [sensorId, sensor] of this.sensors) {
      if (sensors === undefined || sensors.has(sensorId)) {
        buckets[sensorId] = encode([...sensor.buckets.values()]);
        latest[sensorId] = sensor.lastValue;
      }
    }
    return {
      ...this.status(),
      sensorIds: this.sensorIds(),
      bucketMs: this.limits.bucketMs,
      buckets,
      latest,
      gaps: this.gaps.toArray(),
      stats: this.stats(),
    };
  }

  /** Buckets and newest samples changed since the previous drain, and new gaps. */
  drain(): Delta {
    const changed = new Map<string, Bucket[]>();
    for (const [sensorId, bucket] of this.changedBuckets) {
      bucket.dirty = false;
      if (this.sensors.get(sensorId)?.buckets.get(bucket.index) === bucket) {
        const list = changed.get(sensorId);
        if (list) {
          list.push(bucket);
        } else {
          changed.set(sensorId, [bucket]);
        }
      }
    }
    const buckets: Record<string, Buckets> = {};
    for (const [sensorId, list] of changed) {
      buckets[sensorId] = encode(list);
    }
    const latest: Latest = {};
    for (const sensorId of this.changedLatest) {
      const sensor = this.sensors.get(sensorId);
      if (sensor) {
        sensor.lastDirty = false;
        latest[sensorId] = sensor.lastValue;
      }
    }
    const delta = { buckets, latest, gaps: this.newGaps };
    this.changedBuckets = [];
    this.changedLatest = [];
    this.newGaps = [];
    return delta;
  }

  private recordGap(gap: GapEvent): void {
    this.gaps.push(gap);
    if (this.gaps.length > MAX_GAPS_COUNT) {
      this.gaps.shift();
    }
    this.newGaps.push(gap);
  }

  private sensorFor(sensorId: string): Sensor {
    const existing = this.sensors.get(sensorId);
    if (existing) {
      return existing;
    }
    const sensor: Sensor = { buckets: new Map(), lastAt: -Infinity, lastValue: Number.NaN, lastDirty: false };
    this.sensors.set(sensorId, sensor);
    return sensor;
  }

  private bucketFor(sensorId: string, sensor: Sensor, index: number, nowMs: number): Bucket {
    let bucket = sensor.buckets.get(index);
    if (bucket === undefined) {
      bucket = {
        index,
        min: Infinity,
        max: -Infinity,
        minAt: 0,
        maxAt: 0,
        hole: false,
        dirty: false,
        receivedAtMs: nowMs,
      };
      sensor.buckets.set(index, bucket);
      this.bucketCount += 1;
    }
    if (!bucket.dirty) {
      bucket.dirty = true;
      this.changedBuckets.push([sensorId, bucket]);
    }
    return bucket;
  }

  private evict(nowMs: number): void {
    const cutoffMs = nowMs - this.limits.maxAgeMs;
    for (const [sensorId, sensor] of this.sensors) {
      for (const bucket of sensor.buckets.values()) {
        if (sensor.buckets.size <= this.maxBucketsPerSensor && bucket.receivedAtMs >= cutoffMs) {
          break;
        }
        this.evictBucket(sensorId, sensor, bucket);
      }
    }
    while (this.bucketCount > this.limits.maxTotalBuckets) {
      let oldest: [string, Sensor, Bucket] | undefined;
      for (const [sensorId, sensor] of this.sensors) {
        const first = sensor.buckets.values().next().value;
        if (first && (oldest === undefined || first.receivedAtMs < oldest[2].receivedAtMs)) {
          oldest = [sensorId, sensor, first];
        }
      }
      if (oldest === undefined) {
        return;
      }
      this.evictBucket(...oldest);
    }
  }

  private evictBucket(sensorId: string, sensor: Sensor, bucket: Bucket): void {
    sensor.buckets.delete(bucket.index);
    this.bucketCount -= 1;
    this.evictedBuckets += 1;
    if (sensor.buckets.size === 0) {
      this.sensors.delete(sensorId);
    }
  }
}

function fold(bucket: Bucket, at: number, value: number): void {
  if (!Number.isFinite(value)) {
    bucket.hole = true;
    return;
  }
  if (value < bucket.min) {
    bucket.min = value;
    bucket.minAt = at;
  }
  if (value > bucket.max) {
    bucket.max = value;
    bucket.maxAt = at;
  }
}

/** Wire form of `Buckets`: ascending by index, extremes in the order they occurred. */
function encode(buckets: Bucket[]): Buckets {
  const out: Buckets = [];
  for (const { index, min, max, minAt, maxAt, hole } of buckets.sort((a, b) => a.index - b.index)) {
    if (hole) {
      out.push(index, null, null);
    } else if (minAt <= maxAt) {
      out.push(index, min, max);
    } else {
      out.push(index, max, min);
    }
  }
  return out;
}
