import type { ReactNode } from 'react';
import { useMotionCenterScale, useMotionScale } from '../hooks/useSmoothedNumber';
import type { Reading } from '../hooks/useSensor';
import { formatAge, formatFixed, formatSigned, formatValue } from '../lib/format';
import { clamp01 } from '../lib/svg';

export function cx(...names: (string | false | null | undefined)[]): string {
  return names.filter(Boolean).join(' ');
}

/** Tooltip: sensor id, contract description, age of the last point. */
export function describe(...readings: Reading[]): string {
  return readings
    .map((r) => `${r.id} — ${r.spec?.description ?? 'brak w kontrakcie sensorów'} (${formatAge(r.ageMs)})`)
    .join('\n');
}

/** Classes every live element shares: tone color, stale dimming, out-of-range red. */
export function liveClass(r: Reading): string {
  return cx(`tone-${r.tone}`, r.stale && 'stale', r.outOfRange && 'out-of-range');
}

export function Card({
  title,
  aside,
  className,
  children,
}: {
  title: string;
  aside?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={cx('card', className)}>
      <header className="card-head">
        <h3 className="card-title">{title}</h3>
        {aside}
      </header>
      {children}
    </section>
  );
}

type ReadoutProps = {
  r: Reading;
  /** Fixed decimals; omitted = exact value. */
  digits?: number;
  signed?: boolean;
  unit?: string;
  className?: string;
};

/** Numeric value with unit, from the raw sample (never smoothed). */
export function Readout({ r, digits, signed, unit = r.spec?.unit, className }: ReadoutProps) {
  const text =
    digits === undefined ? formatValue(r.id, r.value) : signed ? formatSigned(r.value, digits) : formatFixed(r.value, digits);
  return (
    <span className={cx('readout', liveClass(r), className)} title={describe(r)}>
      <span className="readout-value">{text}</span>
      {unit && r.value !== undefined ? <span className="readout-unit">{unit}</span> : null}
    </span>
  );
}

/** Label above a readout; the building block of the status bar and card stats. */
export function Metric({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="metric">
      <span className="metric-label">{label}</span>
      {children}
    </div>
  );
}

/** Vertical fill bar (pedals, brake channels). */
export function VBar({ r, label, variant }: { r: Reading; label: string; variant: 'accel' | 'brake' }) {
  const fill = useMotionScale(r.finite ?? 0, r.spec?.min ?? 0, r.spec?.max ?? 100, 'Y');
  return (
    <div className={cx('vbar', `vbar-${variant}`, liveClass(r))} title={describe(r)}>
      <span className="vbar-value">{formatFixed(r.value, 0)}</span>
      <div className="vbar-track">
        <div className="vbar-fill" ref={fill} />
      </div>
      <span className="vbar-label">{label}</span>
    </div>
  );
}

/** Horizontal fill bar with label and readout on one line. */
export function HBar({ r, label, digits = 0 }: { r: Reading; label: string; digits?: number }) {
  return (
    <div className={cx('hbar', liveClass(r))} title={describe(r)}>
      <div className="hbar-meta">
        <span className="hbar-label">{label}</span>
        <Readout r={r} digits={digits} />
      </div>
      <RangeTrack r={r} />
    </div>
  );
}

/** Bare fill track over the contract range, with the range ends below. */
export function RangeTrack({ r, scale = false }: { r: Reading; scale?: boolean }) {
  const fill = useMotionScale(r.finite ?? 0, r.spec?.min ?? 0, r.spec?.max ?? 100, 'X');
  return (
    <div className={cx('range', liveClass(r))}>
      <div className="bar-track">
        <div className="hbar-fill" ref={fill} />
      </div>
      {scale ? (
        <div className="bar-scale">
          <span>{r.spec?.min}</span>
          <span>{r.spec?.max}</span>
        </div>
      ) : null}
    </div>
  );
}

/** Bar centered on zero; the fill grows left for negative values and right for positive ones. */
export function CenterBar({
  r,
  label,
  digits = 1,
  negLabel = '−',
  posLabel = '+',
}: {
  r: Reading;
  label: string;
  digits?: number;
  negLabel?: string;
  posLabel?: string;
}) {
  const span = Math.max(Math.abs(r.spec?.min ?? 1), Math.abs(r.spec?.max ?? 1), 1e-9);
  const fill = useMotionCenterScale(r.finite ?? 0, span);
  return (
    <div className={cx('cbar', liveClass(r))} title={describe(r)}>
      <div className="hbar-meta">
        <span className="hbar-label">{label}</span>
        <Readout r={r} digits={digits} signed />
      </div>
      <div className="bar-track">
        <div className="cbar-zero" />
        <div className="cbar-fill" ref={fill} />
      </div>
      <div className="bar-scale">
        <span>{negLabel}</span>
        <span>0</span>
        <span>{posLabel}</span>
      </div>
    </div>
  );
}

/** Vertical thermometer over the contract range, with the nominal band shaded. */
export function Thermometer({ r, band }: { r: Reading; band?: readonly [number, number] }) {
  const min = r.spec?.min ?? 0;
  const max = r.spec?.max ?? 100;
  const fill = useMotionScale(r.finite ?? min, min, max, 'Y');
  const bandLo = band ? clamp01((band[0] - min) / (max - min)) : 0;
  const bandHi = band ? clamp01((band[1] - min) / (max - min)) : 0;
  return (
    <div className={cx('thermo', liveClass(r))} title={describe(r)}>
      <div className="thermo-stem">
        {band ? <div className="thermo-band" style={{ bottom: `${bandLo * 100}%`, height: `${(bandHi - bandLo) * 100}%` }} /> : null}
        <div className="thermo-fill" ref={fill} />
      </div>
      <div className="thermo-scale">
        <span>{max}</span>
        <span>{min}</span>
      </div>
    </div>
  );
}
