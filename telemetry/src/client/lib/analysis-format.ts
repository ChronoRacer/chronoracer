import type { Prediction } from '../../shared/protocol';

export type PredictionStatus = 'unavailable' | 'live' | 'waiting';

export const predictionStatus = (p: Prediction): PredictionStatus =>
  p.unavailable ? 'unavailable' : p.latest ? 'live' : 'waiting';

/** Analysis timestamps are Unix microseconds. */
export const formatTimeUs = (timestampUs: number): string =>
  new Date(timestampUs / 1000).toLocaleTimeString('pl-PL');

export function formatRefreshRate(refreshRate: number | null | undefined): string {
  if (refreshRate === undefined) {
    return 'nie ogłoszono';
  }
  return refreshRate === null ? 'nieregularne odświeżanie' : `częstotliwość ${refreshRate}`;
}
