import type { Point, SnapshotPoint } from '../../shared/protocol';

export const WINDOW_SEC = 60;
const MAX_POINTS_PER_SENSOR = 5000;

export type SensorSeries = {
  times: number[];
  values: (number | null)[];
  last: number;
  lastArrivalMs: number;
};

type Listener = () => void;

export class SeriesStore {
  #series = new Map<string, SensorSeries>();
  #versions = new Map<string, number>();
  #listeners = new Map<string, Set<Listener>>();
  #latest = 0;
  #latestAtMs = 0;

  get latest(): number {
    return this.#latest;
  }

  get(id: string): SensorSeries | undefined {
    return this.#series.get(id);
  }

  version(id: string): number {
    return this.#versions.get(id) ?? 0;
  }

  /**
   * Right edge of the chart window at `nowMs` (`performance.now()`): the newest timestamp played back at wall-clock
   * speed, `delayMs` behind its arrival. With `delayMs` one fanout tick, the edge reaches the newest point as the
   * next push lands, so charts scroll continuously instead of jumping once per push.
   */
  viewEnd(nowMs: number, delayMs: number): number {
    return Math.min(this.#latest, this.#latest + (nowMs - this.#latestAtMs - delayMs) / 1000);
  }

  replace(pointsBySensor: Record<string, SnapshotPoint[]>): void {
    const touched = new Set([...this.#series.keys(), ...Object.keys(pointsBySensor)]);
    this.#series.clear();
    this.#latest = 0;
    for (const [id, points] of Object.entries(pointsBySensor)) {
      for (const point of points) {
        this.#add(id, point);
      }
    }
    this.#latestAtMs = performance.now();
    this.#publish(touched);
  }

  append(points: Point[]): void {
    const latest = this.#latest;
    for (const point of points) {
      this.#add(point.sensorId, point);
    }
    if (this.#latest > latest) {
      this.#latestAtMs = performance.now();
    }
    this.#publish(new Set(points.map((p) => p.sensorId)));
  }

  subscribe(id: string, listener: Listener): () => void {
    const listeners = this.#listeners.get(id) ?? new Set();
    this.#listeners.set(id, listeners.add(listener));
    return () => listeners.delete(listener);
  }

  #add(id: string, point: SnapshotPoint): void {
    let series = this.#series.get(id);
    if (!series) {
      series = { times: [], values: [], last: point.value, lastArrivalMs: 0 };
      this.#series.set(id, series);
    }
    const t = nanosToSec(point.timestampNanos);
    let i = series.times.length;
    while (i > 0 && series.times[i - 1] > t) {
      i--;
    }
    series.times.splice(i, 0, t);
    series.values.splice(i, 0, Number.isFinite(point.value) ? point.value : null);
    if (i === series.times.length - 1) {
      series.last = point.value;
    }
    series.lastArrivalMs = Date.now();
    this.#latest = Math.max(this.#latest, t);
  }

  #publish(touched: Set<string>): void {
    const from = this.#latest - WINDOW_SEC;
    for (const id of touched) {
      const series = this.#series.get(id);
      if (series) {
        let drop = series.times.findIndex((t) => t >= from);
        drop = Math.max(drop === -1 ? series.times.length : drop, series.times.length - MAX_POINTS_PER_SENSOR);
        series.times.splice(0, drop);
        series.values.splice(0, drop);
      }
      this.#versions.set(id, this.version(id) + 1);
      this.#listeners.get(id)?.forEach((listener) => listener());
    }
  }
}

function nanosToSec(nanos: string): number {
  const n = BigInt(nanos);
  return Number(n / 1_000_000_000n) + Number(n % 1_000_000_000n) / 1e9;
}
