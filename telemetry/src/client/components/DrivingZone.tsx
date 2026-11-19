import { useCallback } from 'react';
import { useSensor, type Reading } from '../hooks/useSensor';
import { useMotionSvgArc, useMotionSvgDot, useMotionSvgRotate } from '../hooks/useSmoothedNumber';
import { recentPairs, type SeriesStore } from '../lib/series-store';
import { formatFixed, formatSigned } from '../lib/format';
import { describeArc, polar } from '../lib/svg';
import { CenterBar, Card, Metric, Readout, VBar, cx, describe, liveClass } from './instruments';

type ZoneProps = { store: SeriesStore };

/** Shared by the speed and rpm dials: 270° sweep in a 200×200 box. */
export const DIAL = { c: 100, r: 84, sweep: 135 } as const;

/** Tick marks every `step`, labelled every `labelEvery` steps, from `min` to `max`. */
export function DialScale({
  min,
  max,
  step,
  labelEvery,
  label = (v) => String(v),
}: {
  min: number;
  max: number;
  step: number;
  labelEvery: number;
  label?: (v: number) => string;
}) {
  const steps = Math.round((max - min) / step);
  return (
    <>
      {Array.from({ length: steps + 1 }, (_, i) => {
        const deg = -DIAL.sweep + (2 * DIAL.sweep * i) / steps;
        const major = i % labelEvery === 0;
        const outer = polar(DIAL.c, DIAL.c, DIAL.r + 13, deg);
        const inner = polar(DIAL.c, DIAL.c, DIAL.r + (major ? 7 : 9), deg);
        const text = polar(DIAL.c, DIAL.c, DIAL.r - 19, deg);
        return (
          <g key={i}>
            <line x1={inner.x} y1={inner.y} x2={outer.x} y2={outer.y} className={cx('dial-tick', major && 'major')} />
            {major ? (
              <text x={text.x} y={text.y} className="dial-label" textAnchor="middle" dominantBaseline="central">
                {label(min + i * step)}
              </text>
            ) : null}
          </g>
        );
      })}
    </>
  );
}

export function SpeedGauge({ store }: ZoneProps) {
  const speed = useSensor(store, 'speed');
  const min = speed.spec?.min ?? 0;
  const max = speed.spec?.max ?? 220;
  const arc = useMotionSvgArc(speed.finite ?? min, min, max, DIAL.c, DIAL.c, DIAL.r, -DIAL.sweep, DIAL.sweep, describeArc);

  return (
    <div className={cx('gauge gauge-speed', liveClass(speed))} title={describe(speed)}>
      <svg viewBox="0 0 200 200" aria-hidden>
        <path d={describeArc(DIAL.c, DIAL.c, DIAL.r, -DIAL.sweep, DIAL.sweep)} className="dial-track" />
        <path ref={arc} className="dial-value" />
        <DialScale min={min} max={max} step={20} labelEvery={2} />
      </svg>
      <div className="gauge-center">
        <span className="gauge-num">{formatFixed(speed.value, 0)}</span>
        <span className="gauge-unit">{speed.spec?.unit ?? 'km/h'}</span>
      </div>
    </div>
  );
}

const BRAKE_SPLIT = 5;

export function PedalsCard({ store }: ZoneProps) {
  const accel = useSensor(store, 'accel_pedal');
  const brakeA = useSensor(store, 'brake_prs');
  const brakeB = useSensor(store, 'brake_prs_b');
  const split =
    brakeA.finite !== undefined && brakeB.finite !== undefined ? Math.abs(brakeA.finite - brakeB.finite) : undefined;

  return (
    <Card
      title="PEDALS"
      className="area-pedals"
      aside={
        <span
          className={cx('chip', split !== undefined && split > BRAKE_SPLIT ? 'tone-bad' : 'tone-neutral')}
          title={`Brake A/B channel delta; alarm above ${BRAKE_SPLIT}`}
        >
          A/B Δ {formatFixed(split, 1)}
        </span>
      }
    >
      <div className="pedals grow">
        <VBar r={accel} label="GAS" variant="accel" />
        <VBar r={brakeA} label="BRAKE A" variant="brake" />
        <VBar r={brakeB} label="BRAKE B" variant="brake" />
      </div>
    </Card>
  );
}

export function SteeringCard({ store }: ZoneProps) {
  const angle = useSensor(store, 'steer_angle');
  const torque = useSensor(store, 'steer_torque');
  // Contract: left = plus; SVG rotates clockwise for plus, so negate for a driver's view.
  const wheel = useMotionSvgRotate(angle.finite ?? 0, 70, 70, (v) => -v);
  const side = angle.finite === undefined || Math.round(angle.finite) === 0 ? '' : angle.finite > 0 ? 'L' : 'R';

  return (
    <Card
      title="STEERING"
      className="area-steer"
      aside={
        <span className={cx('card-value', liveClass(angle))} title={describe(angle)}>
          <span className="readout-value">{formatSigned(angle.value, 0)}°</span> {side}
        </span>
      }
    >
      <svg viewBox="0 0 140 140" className={cx('wheel-svg grow', angle.stale && 'stale')} aria-hidden>
        <circle cx="70" cy="70" r="66" className="wheel-guide" />
        <g ref={wheel}>
          <circle cx="70" cy="70" r="54" className="wheel-rim" />
          <circle cx="70" cy="70" r="15" className="wheel-hub" />
          <path d="M 55 74 L 20 82 M 85 74 L 120 82 M 70 85 L 70 124" className="wheel-spoke" />
          <rect x="64" y="12" width="12" height="7" rx="2" className="wheel-mark" />
        </g>
      </svg>
      <CenterBar r={torque} label="TORQUE" digits={0} />
    </Card>
  );
}

const WHEEL_RECTS = [
  [20, 34],
  [88, 34],
  [20, 136],
  [88, 136],
] as const;

/** A wheel this far from the four-wheel mean is highlighted as slipping or locking. */
const SLIP_KMH = 3;
const SLIP_RATIO = 0.08;

export function WheelsCard({ store }: ZoneProps) {
  const readings = [
    useSensor(store, 'wheel_fl'),
    useSensor(store, 'wheel_fr'),
    useSensor(store, 'wheel_rl'),
    useSensor(store, 'wheel_rr'),
  ];
  const live = readings.flatMap((r) => (r.finite === undefined ? [] : [r.finite]));
  const mean = live.length ? live.reduce((a, b) => a + b, 0) / live.length : undefined;
  const slips = readings.map(
    (r) =>
      mean !== undefined &&
      r.finite !== undefined &&
      Math.abs(r.finite - mean) > Math.max(SLIP_KMH, mean * SLIP_RATIO),
  );
  const [fl, fr, rl, rr] = readings;

  return (
    <Card title="WHEEL SPEEDS" className="area-wheels" aside={<span className="card-unit">km/h</span>}>
      <div className="chassis grow">
        <WheelValue r={fl} label="FL" slip={slips[0]} className="w-fl" />
        <WheelValue r={fr} label="FR" slip={slips[1]} className="w-fr" />
        <svg viewBox="0 0 120 200" className="chassis-svg" aria-hidden>
          <path d="M 34 30 Q 34 10 60 10 Q 86 10 86 30 L 88 170 Q 88 192 60 192 Q 32 192 32 170 Z" className="car-body" />
          <path d="M 40 58 Q 60 48 80 58 L 78 82 Q 60 76 42 82 Z" className="car-glass" />
          <path d="M 42 140 Q 60 146 78 140 L 80 158 Q 60 166 40 158 Z" className="car-glass" />
          {WHEEL_RECTS.map(([x, y], i) => (
            <rect
              key={readings[i].id}
              x={x}
              y={y}
              width="12"
              height="32"
              rx="3"
              className={cx('car-wheel', readings[i].stale && 'stale', slips[i] && 'slip')}
            />
          ))}
        </svg>
        <WheelValue r={rl} label="RL" slip={slips[2]} className="w-rl" />
        <WheelValue r={rr} label="RR" slip={slips[3]} className="w-rr" />
      </div>
    </Card>
  );
}

function WheelValue({ r, label, slip, className }: { r: Reading; label: string; slip: boolean; className: string }) {
  return (
    <div className={cx('wheel-value', className, slip && 'slip')}>
      <span className="metric-label">{label}</span>
      <Readout r={r} digits={0} unit="" />
    </div>
  );
}

const G_LIMIT = 12;
const G_RADIUS = 88;
const TRAIL_SEC = 1.5;
const STANDARD_G = 9.80665;

export function GForceCard({ store }: ZoneProps) {
  const long = useSensor(store, 'accel_long');
  const lat = useSensor(store, 'accel_lat');
  const k = G_RADIUS / G_LIMIT;
  const clampG = (v: number) => Math.max(-G_LIMIT, Math.min(G_LIMIT, v));
  // Contract: lateral left = plus, longitudinal accelerating = plus. Screen: left is left, accelerating is up.
  const toX = useCallback((latV: number) => 100 - clampG(latV) * k, [k]);
  const toY = useCallback((longV: number) => 100 - clampG(longV) * k, [k]);
  const dot = useMotionSvgDot(lat.finite ?? 0, long.finite ?? 0, toX, toY);
  const trail = recentPairs(store, 'accel_long', 'accel_lat', TRAIL_SEC)
    .map(([l, a]) => `${toX(a).toFixed(1)},${toY(l).toFixed(1)}`)
    .join(' ');
  const combined =
    long.finite !== undefined && lat.finite !== undefined ? Math.hypot(long.finite, lat.finite) / STANDARD_G : undefined;

  return (
    <Card
      title="G-FORCE"
      className="area-gforce"
      aside={
        <span className="card-value">
          <span className="readout-value">{formatFixed(combined, 2)}</span> g
        </span>
      }
    >
      <svg viewBox="0 0 200 200" className={cx('gforce-svg grow', (long.stale || lat.stale) && 'stale')} aria-hidden>
        {[4, 8, 12].map((ring) => (
          <circle key={ring} cx="100" cy="100" r={ring * k} className={ring === 12 ? 'g-ring outer' : 'g-ring'} />
        ))}
        <line x1="100" y1="12" x2="100" y2="188" className="g-axis" />
        <line x1="12" y1="100" x2="188" y2="100" className="g-axis" />
        <text x="100" y="20" className="g-label" textAnchor="middle">
          ACCEL
        </text>
        <text x="100" y="186" className="g-label" textAnchor="middle">
          BRAKE
        </text>
        <text x="18" y="97" className="g-label">
          L
        </text>
        <text x="182" y="97" className="g-label" textAnchor="end">
          R
        </text>
        {trail ? <polyline points={trail} className="g-trail" /> : null}
        <circle ref={dot} r="6" className="g-dot" />
      </svg>
      <div className="stats">
        <Metric label="LONG">
          <Readout r={long} digits={1} signed />
        </Metric>
        <Metric label="LAT">
          <Readout r={lat} digits={1} signed />
        </Metric>
      </div>
    </Card>
  );
}
