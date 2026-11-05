import { WINDOW_SEC, recentPairs, type SeriesStore } from './series-store';

export type LatLon = [lat: number, lon: number];

const MAX_POINTS = 6000;
/** Dropped at once when full, so the map redraws the whole line rarely. */
const TRIM_POINTS = 1000;
const MIN_STEP_M = 3;
const M_PER_DEG = 111_320;

/**
 * Car positions for the map. Lives as long as the store (the page), not the map component, so switching tabs does
 * not lose the drawn lap; the store itself only keeps the 60 s chart window.
 */
export class Trail {
  readonly points: LatLon[] = [];
  /** Bumped when old points are trimmed; listeners then redraw instead of appending. */
  epoch = 0;
  #listeners = new Set<() => void>();
  #store: SeriesStore;

  constructor(store: SeriesStore) {
    this.#store = store;
    for (const [lat, lon] of recentPairs(store, 'gps_lat', 'gps_lon', WINDOW_SEC)) {
      this.#add(lat, lon);
    }
    store.subscribe('gps_lat', this.#onSample);
    store.subscribe('gps_lon', this.#onSample);
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  #onSample = () => {
    const lat = this.#store.get('gps_lat')?.last;
    const lon = this.#store.get('gps_lon')?.last;
    if (lat !== undefined && lon !== undefined && this.#add(lat, lon)) {
      this.#listeners.forEach((listener) => listener());
    }
  };

  #add(lat: number, lon: number): boolean {
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return false;
    }
    const prev = this.points.at(-1);
    if (prev) {
      const north = (lat - prev[0]) * M_PER_DEG;
      const east = (lon - prev[1]) * M_PER_DEG * Math.cos((lat * Math.PI) / 180);
      if (Math.hypot(north, east) < MIN_STEP_M) {
        return false;
      }
    }
    this.points.push([lat, lon]);
    if (this.points.length > MAX_POINTS) {
      this.points.splice(0, TRIM_POINTS);
      this.epoch++;
    }
    return true;
  }
}

const trails = new WeakMap<SeriesStore, Trail>();

export function trailOf(store: SeriesStore): Trail {
  let trail = trails.get(store);
  if (!trail) {
    trail = new Trail(store);
    trails.set(store, trail);
  }
  return trail;
}
