import type { Buckets, Latest } from '../../shared/protocol';

export const WINDOW_SEC = 60;
const MAX_POINTS_PER_SENSOR = 5000;

// Where a bucket's two extremes are drawn, as fractions of the bucket, so the line keeps their order.
const FIRST_AT = 0.25;
const SECOND_AT = 0.75;

export type SensorSeries = {
  /** Seconds; two points per bucket, its extremes in the order they occurred. */
  times: number[];
  values: (number | null)[];
  /** Newest sample; NaN when it was not finite. */
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
  #bucketSec = 0;

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

  replace(bucketMs: number, buckets: Record<string, Buckets>, latest: Latest): void {
    const touched = new Set([...this.#series.keys(), ...Object.keys(buckets)]);
    this.#series.clear();
    this.#latest = 0;
    this.#bucketSec = bucketMs / 1000;
    this.#merge(buckets, latest);
    this.#latestAtMs = performance.now();
    this.#publish(touched);
  }

  /** Adds new buckets and replaces ones already held, e.g. when a late point changed them. */
  apply(buckets: Record<string, Buckets>, latest: Latest): void {
    const before = this.#latest;
    this.#merge(buckets, latest);
    if (this.#latest > before) {
      this.#latestAtMs = performance.now();
    }
    this.#publish(new Set([...Object.keys(buckets), ...Object.keys(latest)]));
  }

  subscribe(id: string, listener: Listener): () => void {
    const listeners = this.#listeners.get(id) ?? new Set();
    this.#listeners.set(id, listeners.add(listener));
    return () => listeners.delete(listener);
  }

  #merge(buckets: Record<string, Buckets>, latest: Latest): void {
    for (const [id, flat] of Object.entries(buckets)) {
      const series = this.#seriesFor(id);
      for (let i = 0; i < flat.length; i += 3) {
        this.#upsert(series, flat[i] as number, flat[i + 1], flat[i + 2]);
      }
    }
    const now = Date.now();
    for (const [id, value] of Object.entries(latest)) {
      const series = this.#seriesFor(id);
      series.last = value ?? Number.NaN;
      series.lastArrivalMs = now;
    }
  }

  #seriesFor(id: string): SensorSeries {
    let series = this.#series.get(id);
    if (!series) {
      series = { times: [], values: [], last: Number.NaN, lastArrivalMs: 0 };
      this.#series.set(id, series);
    }
    return series;
  }

  #upsert({ times, values }: SensorSeries, index: number, a: number | null, b: number | null): void {
    const first = (index + FIRST_AT) * this.#bucketSec;
    const second = (index + SECOND_AT) * this.#bucketSec;
    let i = times.length;
    while (i > 0 && times[i - 1] > second) {
      i--;
    }
    if (i > 0 && times[i - 1] === second) {
      values[i - 2] = a;
      values[i - 1] = b;
    } else {
      times.splice(i, 0, first, second);
      values.splice(i, 0, a, b);
    }
    this.#latest = Math.max(this.#latest, second);
  }

  #publish(touched: Set<string>): void {
    const from = this.#latest - WINDOW_SEC;
    for (const id of touched) {
      const series = this.#series.get(id);
      if (series) {
        let drop = series.times.findIndex((t) => t >= from);
        drop = Math.max(drop === -1 ? series.times.length : drop, series.times.length - MAX_POINTS_PER_SENSOR);
        drop -= drop % 2; // whole buckets only
        series.times.splice(0, drop);
        series.values.splice(0, drop);
      }
      this.#versions.set(id, this.version(id) + 1);
      this.#listeners.get(id)?.forEach((listener) => listener());
    }
  }
}
