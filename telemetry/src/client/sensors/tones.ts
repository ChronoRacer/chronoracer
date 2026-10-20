/**
 * Presentation thresholds: which color a live value gets. These are UI judgements for this car,
 * not contract ranges; the contract `min`–`max` still decides what counts as out of range.
 */

export type Tone = 'ok' | 'warn' | 'bad' | 'neutral';

type Rule = (value: number) => Tone;

const above = (warn: number, bad: number): Rule => (v) => (v > bad ? 'bad' : v > warn ? 'warn' : 'ok');
const below = (warn: number, bad: number): Rule => (v) => (v < bad ? 'bad' : v < warn ? 'warn' : 'ok');
const offZero = (warn: number, bad: number): Rule => (v) => above(warn, bad)(Math.abs(v));

export const TONE_RULES: Readonly<Partial<Record<string, Rule>>> = {
  gps_fix: (v) => (v === 3 ? 'ok' : v === 2 ? 'warn' : 'bad'),
  gps_sats: below(6, 4),
  gps_pdop: (v) => (v < 2 ? 'ok' : v > 5 ? 'bad' : 'warn'),
  batt_volt: (v) => (v < 11.8 || v > 15 ? 'bad' : v < 12.2 || v > 14.8 ? 'warn' : 'ok'),
  fuel_l: below(8, 4),
  hv_soc: below(50, 30),
  rpm: above(5500, 6500),
  coolant_temp: above(100, 110),
  oil_temp: above(110, 125),
  intake_temp: above(55, 70),
  stft: offZero(8, 15),
  ltft: offZero(8, 15),
  lambda: (v) => offZero(0.05, 0.15)(v - 1),
  brake_prs: above(40, 75),
  brake_prs_b: above(40, 75),
};

export function toneOf(id: string, value: number | undefined): Tone {
  const rule = TONE_RULES[id];
  return rule && value !== undefined && Number.isFinite(value) ? rule(value) : 'neutral';
}

/** Nominal operating band drawn on thermometers. */
export const NORMAL_BANDS: Readonly<Partial<Record<string, readonly [number, number]>>> = {
  coolant_temp: [85, 95],
  oil_temp: [90, 105],
  intake_temp: [10, 45],
};
