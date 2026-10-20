import { useCallback, useSyncExternalStore } from 'react';
import type { SeriesStore } from '../lib/series-store';
import { sensorSpec, type SensorSpec } from '../sensors/catalog';
import { toneOf, type Tone } from '../sensors/tones';

export const STALE_MS = 5000;

export type Reading = {
  id: string;
  spec: SensorSpec | undefined;
  /** Newest sample; may be NaN or infinite. */
  value: number | undefined;
  /** Newest sample when finite, for instruments. */
  finite: number | undefined;
  ageMs: number | undefined;
  stale: boolean;
  outOfRange: boolean;
  tone: Tone;
};

/** Re-renders only for this sensor's points and the shared 1 s staleness clock. */
export function useSensor(store: SeriesStore, id: string): Reading {
  const subscribe = useCallback((onChange: () => void) => store.subscribe(id, onChange), [store, id]);
  useSyncExternalStore(subscribe, () => store.version(id));
  const now = useNow();

  const spec = sensorSpec(id);
  const series = store.get(id);
  const value = series?.last;
  const ageMs = series ? Math.max(0, now - series.lastArrivalMs) : undefined;
  const finite = value !== undefined && Number.isFinite(value) ? value : undefined;
  return {
    id,
    spec,
    value,
    finite,
    ageMs,
    stale: ageMs === undefined || ageMs > STALE_MS,
    outOfRange:
      spec !== undefined &&
      finite !== undefined &&
      ((spec.min !== null && finite < spec.min) || (spec.max !== null && finite > spec.max)),
    tone: toneOf(id, value),
  };
}

let now = Date.now();
let clock: ReturnType<typeof setInterval> | undefined;
const clockListeners = new Set<() => void>();

function subscribeClock(listener: () => void): () => void {
  clockListeners.add(listener);
  clock ??= setInterval(() => {
    now = Date.now();
    clockListeners.forEach((l) => l());
  }, 1000);
  return () => {
    clockListeners.delete(listener);
    if (clockListeners.size === 0) {
      clearInterval(clock);
      clock = undefined;
    }
  };
}

const useNow = () => useSyncExternalStore(subscribeClock, () => now);
