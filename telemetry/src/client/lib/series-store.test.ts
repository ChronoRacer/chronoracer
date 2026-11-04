import { afterEach, describe, expect, it, vi } from 'vitest';
import { SeriesStore, WINDOW_SEC } from './series-store';

// 1 s buckets keep the expected times readable: bucket i is drawn at i + 0.25 and i + 0.75.
const BUCKET_MS = 1000;
const withBuckets = (buckets: Record<string, (number | null)[]>, latest = {}) => {
  const store = new SeriesStore();
  store.replace(BUCKET_MS, buckets, latest);
  return store;
};

describe('SeriesStore', () => {
  afterEach(() => vi.restoreAllMocks());

  it('draws each bucket as its two extremes in order, and a resent bucket replaces it', () => {
    const store = withBuckets({ rpm: [10, 1, 5, 11, 7, 7] }, { rpm: 7 });
    store.apply({ rpm: [11, 7, 2, 9, 3, 3] }, {});
    const rpm = store.get('rpm')!;
    expect(rpm.times).toEqual([9.25, 9.75, 10.25, 10.75, 11.25, 11.75]);
    expect(rpm.values).toEqual([3, 3, 1, 5, 7, 2]);
    expect(rpm.last).toBe(7);
    expect(store.latest).toBe(11.75);
  });

  it('keeps holes and a non-finite newest sample', () => {
    const store = withBuckets({ rpm: [1, null, null] }, { rpm: null });
    expect(store.get('rpm')!.values).toEqual([null, null]);
    expect(store.get('rpm')!.last).toBeNaN();
  });

  it('notifies only the sensors in a message', () => {
    const store = withBuckets({});
    const calls: string[] = [];
    store.subscribe('rpm', () => calls.push('rpm'));
    store.subscribe('speed', () => calls.push('speed'));
    store.apply({ rpm: [1, 900, 900] }, { rpm: 900 });
    expect(calls).toEqual(['rpm']);
    expect(store.version('rpm')).toBe(1);
    expect(store.version('speed')).toBe(0);
  });

  it('plays the window edge back one delay behind the newest arrival, never past the newest point', () => {
    vi.spyOn(performance, 'now').mockReturnValue(1_000);
    const store = withBuckets({});
    store.apply({ rpm: [9, 1, 1] }, {});
    expect(store.viewEnd(1_000, 100)).toBeCloseTo(9.65);
    expect(store.viewEnd(1_050, 100)).toBeCloseTo(9.7);
    expect(store.viewEnd(5_000, 100)).toBe(9.75);

    vi.spyOn(performance, 'now').mockReturnValue(1_100);
    store.apply({ rpm: [8, 2, 2] }, {});
    expect(store.viewEnd(1_100, 100)).toBeCloseTo(9.75);
  });

  it('drops whole buckets older than the window behind the newest point', () => {
    const store = withBuckets({ rpm: [0, 1, 1, 5, 2, 2] });
    store.apply({ rpm: [WINDOW_SEC + 5, 3, 3] }, {});
    expect(store.get('rpm')!.times).toEqual([5.25, 5.75, WINDOW_SEC + 5.25, WINDOW_SEC + 5.75]);
  });

  it('replaces everything on a snapshot and notifies removed sensors too', () => {
    const store = withBuckets({ old: [1, 1, 1] });
    const removed = vi.fn();
    store.subscribe('old', removed);
    store.replace(BUCKET_MS, { rpm: [2, 800, 800] }, { rpm: 800 });
    expect(store.get('old')).toBeUndefined();
    expect(store.get('rpm')!.values).toEqual([800, 800]);
    expect(removed).toHaveBeenCalledOnce();
  });
});
