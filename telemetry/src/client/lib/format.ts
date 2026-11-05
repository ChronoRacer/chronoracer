import { STATUS_LABELS } from '../sensors/layout';

/** Exact readout of a sample: status label, NaN/Infinity as-is, otherwise up to 6 significant digits. */
export function formatValue(id: string, value: number | undefined): string {
  if (value === undefined) {
    return '—';
  }
  const labels = STATUS_LABELS[id];
  if (labels) {
    return labels[value] ?? String(value);
  }
  if (!Number.isFinite(value)) {
    return String(value);
  }
  return String(Number(Number(value.toPrecision(6)).toFixed(4)));
}

/** Fixed decimals for live readouts; non-finite values pass through. */
export function formatFixed(value: number | undefined, digits: number): string {
  if (value === undefined) {
    return '—';
  }
  return Number.isFinite(value) ? value.toFixed(digits) : String(value);
}

export function formatSigned(value: number | undefined, digits: number): string {
  const text = formatFixed(value, digits);
  return value !== undefined && value > 0 && Number.isFinite(value) ? `+${text}` : text;
}

export function formatAge(ms: number | undefined): string {
  if (ms === undefined) {
    return 'no data';
  }
  const sec = Math.floor(ms / 1000);
  return sec < 60 ? `${sec} s ago` : `${Math.floor(sec / 60)} min ago`;
}

const CARDINALS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'] as const;

export function cardinal(degrees: number): string {
  const heading = ((degrees % 360) + 360) % 360;
  return CARDINALS[Math.round(heading / 45) % 8];
}
