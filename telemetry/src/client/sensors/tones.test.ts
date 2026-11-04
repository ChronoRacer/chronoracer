import { describe, expect, it } from 'vitest';
import { SENSORS } from './catalog';
import { NORMAL_BANDS, TONE_RULES, toneOf } from './tones';

describe('tones', () => {
  it('only rates catalog sensors', () => {
    for (const id of [...Object.keys(TONE_RULES), ...Object.keys(NORMAL_BANDS)]) {
      expect(SENSORS.some((s) => s.name === id), id).toBe(true);
    }
  });

  it('keeps normal bands inside the contract range', () => {
    for (const [id, band] of Object.entries(NORMAL_BANDS)) {
      const [lo, hi] = band!;
      const spec = SENSORS.find((s) => s.name === id)!;
      expect(lo >= (spec.min ?? -Infinity) && hi <= (spec.max ?? Infinity) && lo < hi, id).toBe(true);
    }
  });

  it('rates the documented examples', () => {
    expect(toneOf('gps_fix', 3)).toBe('ok');
    expect(toneOf('gps_pdop', 1.4)).toBe('ok');
    expect(toneOf('gps_pdop', 6)).toBe('bad');
    expect(toneOf('hv_soc', 20)).toBe('bad');
    expect(toneOf('stft', -20)).toBe('bad');
    expect(toneOf('lambda', 1.01)).toBe('ok');
  });

  it('is neutral without a rule or a finite value', () => {
    expect(toneOf('speed', 80)).toBe('neutral');
    expect(toneOf('hv_soc', Number.NaN)).toBe('neutral');
    expect(toneOf('hv_soc', undefined)).toBe('neutral');
  });
});
