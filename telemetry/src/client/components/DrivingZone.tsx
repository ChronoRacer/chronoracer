import { useCallback } from 'react';
import { useSensor, type Reading } from '../hooks/useSensor';
import {
  useMotionSvgAngleRotate,
  useMotionSvgArc,
  useMotionSvgDot,
  useMotionSvgRotate,
} from '../hooks/useSmoothedNumber';
import type { SeriesStore } from '../lib/series-store';
import { cardinal, formatFixed, formatSigned } from '../lib/format';
import { describeArc, polar } from '../lib/svg';
import { CenterBar, Card, Metric, Readout, VBar, cx, describe, liveClass } from './instruments';

type ZoneProps = { store: SeriesStore };

export function DrivingZone({ store }: ZoneProps) {
  return (
    <section className="zone zone-driving" aria-label="Jazda i dynamika">
      <h2 className="zone-title">Jazda i dynamika</h2>
      <div className="driving-grid">
        <SpeedHero store={store} />
        <Pedals store={store} />
        <Steering store={store} />
        <Wheels store={store} />
        <GForce store={store} />
      </div>
    </section>
  );
}

const SPEED_SWEEP = 130;

function SpeedHero({ store }: ZoneProps) {
  const speed = useSensor(store, 'speed');
  const gpsSpeed = useSensor(store, 'gps_speed');
  const heading = useSensor(store, 'gps_heading');
  const arc = useMotionSvgArc(
    speed.finite ?? 0,
    speed.spec?.min ?? 0,
    speed.spec?.max ?? 220,
    120,
    120,
    72,
    -SPEED_SWEEP,
    SPEED_SWEEP,
    describeArc,
  );
  const headingMark = useMotionSvgAngleRotate(heading.finite ?? 0, 120, 120);

  return (
    <Card title="Prędkość i kierunek" className="area-hero">
      <div className="hero">
        <svg viewBox="0 0 240 240" className="hero-svg" aria-hidden>
          <circle cx="120" cy="120" r="110" className="compass-ring" />
          {Array.from({ length: 36 }, (_, i) => i * 10).map((deg) => {
            const major = deg % 90 === 0;
            const outer = polar(120, 120, 110, deg);
            const inner = polar(120, 120, major ? 100 : deg % 30 === 0 ? 104 : 107, deg);
            return <line key={deg} x1={inner.x} y1={inner.y} x2={outer.x} y2={outer.y} className={major ? 'compass-tick major' : 'compass-tick'} />;
          })}
          {(['N', 'E', 'S', 'W'] as const).map((label, i) => {
            const p = polar(120, 120, 90, i * 90);
            return (
              <text key={label} x={p.x} y={p.y} className="compass-cardinal" textAnchor="middle" dominantBaseline="central">
                {label}
              </text>
            );
          })}
          <path d={describeArc(120, 120, 72, -SPEED_SWEEP, SPEED_SWEEP)} className="speed-track" />
          <path ref={arc} className={cx('speed-value', speed.stale && 'stale')} />
          <g ref={headingMark} className={cx('heading-marker', heading.stale && 'stale')}>
            <path d="M 120 4 L 128 20 L 120 16 L 112 20 Z" />
          </g>
        </svg>
        <div className="hero-center">
          <Readout r={speed} digits={0} unit="" className="hero-speed" />
          <span className="hero-unit">{speed.spec?.unit ?? 'km/h'}</span>
          <span className={cx('hero-sub', liveClass(gpsSpeed))} title={describe(gpsSpeed)}>
            GPS {formatFixed(gpsSpeed.value, 0)}
          </span>
        </div>
      </div>
      <div className={cx('hero-heading', liveClass(heading))} title={describe(heading)}>
        <span className="metric-label">Kurs</span>
        <span className="readout-value">
          {formatFixed(heading.value, 0)}° {heading.finite === undefined ? '' : cardinal(heading.finite)}
        </span>
      </div>
    </Card>
  );
}

const BRAKE_SPLIT = 5;

function Pedals({ store }: ZoneProps) {
  const accel = useSensor(store, 'accel_pedal');
  const brakeA = useSensor(store, 'brake_prs');
  const brakeB = useSensor(store, 'brake_prs_b');
  const split =
    brakeA.finite !== undefined && brakeB.finite !== undefined ? Math.abs(brakeA.finite - brakeB.finite) : undefined;

  return (
    <Card
      title="Pedały"
      className="area-pedals"
      aside={
        <span
          className={cx('chip', split !== undefined && split > BRAKE_SPLIT ? 'tone-bad' : 'tone-neutral')}
          title={`Różnica kanałów hamulca A/B; alarm powyżej ${BRAKE_SPLIT}`}
        >
          A/B Δ {formatFixed(split, 1)}
        </span>
      }
    >
      <div className="pedals">
        <VBar r={accel} label="Gaz" variant="accel" />
        <VBar r={brakeA} label="Hamulec A" variant="brake" />
        <VBar r={brakeB} label="Hamulec B" variant="brake" />
      </div>
    </Card>
  );
}

function Steering({ store }: ZoneProps) {
  const angle = useSensor(store, 'steer_angle');
  const torque = useSensor(store, 'steer_torque');
  // Contract: left = plus; SVG rotates clockwise for plus, so negate for a driver's view.
  const wheel = useMotionSvgRotate(angle.finite ?? 0, 70, 70, (v) => -v);
  const side = angle.finite === undefined || Math.round(angle.finite) === 0 ? '' : angle.finite > 0 ? 'L' : 'P';

  return (
    <Card title="Kierownica" className="area-steer">
      <div className="steering">
        <svg viewBox="0 0 140 140" className={cx('wheel-svg', angle.stale && 'stale')} aria-hidden>
          <circle cx="70" cy="70" r="66" className="wheel-guide" />
          <g ref={wheel}>
            <circle cx="70" cy="70" r="54" className="wheel-rim" />
            <circle cx="70" cy="70" r="15" className="wheel-hub" />
            <path d="M 55 74 L 20 82 M 85 74 L 120 82 M 70 85 L 70 124" className="wheel-spoke" />
            <rect x="64" y="12" width="12" height="7" rx="2" className="wheel-mark" />
          </g>
        </svg>
      </div>
      <div className={cx('steering-readout', liveClass(angle))} title={describe(angle)}>
        <span className="readout-value">{formatSigned(angle.value, 0)}°</span>
        <span className="steering-side">{side}</span>
      </div>
      <CenterBar r={torque} label="Moment" digits={0} />
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

function Wheels({ store }: ZoneProps) {
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
    <Card title="Prędkości kół" className="area-wheels" aside={<span className="card-unit">km/h</span>}>
      <div className="chassis">
        <WheelValue r={fl} label="PL" slip={slips[0]} className="w-fl" />
        <WheelValue r={fr} label="PP" slip={slips[1]} className="w-fr" />
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
        <WheelValue r={rl} label="TL" slip={slips[2]} className="w-rl" />
        <WheelValue r={rr} label="TP" slip={slips[3]} className="w-rr" />
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

function GForce({ store }: ZoneProps) {
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
    <Card title="Przeciążenia" className="area-gforce" aside={<span className="card-unit">{formatFixed(combined, 2)} g</span>}>
      <svg viewBox="0 0 200 200" className={cx('gforce-svg', (long.stale || lat.stale) && 'stale')} aria-hidden>
        {[4, 8, 12].map((ring) => (
          <circle key={ring} cx="100" cy="100" r={ring * k} className={ring === 12 ? 'g-ring outer' : 'g-ring'} />
        ))}
        <line x1="100" y1="8" x2="100" y2="192" className="g-axis" />
        <line x1="8" y1="100" x2="192" y2="100" className="g-axis" />
        <text x="100" y="5" className="g-label" textAnchor="middle" dominantBaseline="hanging">
          PRZYS
        </text>
        <text x="100" y="196" className="g-label" textAnchor="middle">
          HAM
        </text>
        <text x="4" y="98" className="g-label">
          L
        </text>
        <text x="196" y="98" className="g-label" textAnchor="end">
          P
        </text>
        {[4, 8].map((ring) => (
          <text key={ring} x={100 + ring * k * 0.71 + 2} y={100 - ring * k * 0.71 - 2} className="g-ring-label">
            {ring}
          </text>
        ))}
        {trail ? <polyline points={trail} className="g-trail" /> : null}
        <circle ref={dot} r="6" className="g-dot" />
      </svg>
      <div className="stats">
        <Metric label="Wzdłużne">
          <Readout r={long} digits={1} signed />
        </Metric>
        <Metric label="Poprzeczne">
          <Readout r={lat} digits={1} signed />
        </Metric>
      </div>
    </Card>
  );
}

/** Recent samples of `a`, each paired with the newest `b` at or before it. */
function recentPairs(store: SeriesStore, a: string, b: string, windowSec: number): [number, number][] {
  const sa = store.get(a);
  const sb = store.get(b);
  if (!sa || !sb || sa.times.length === 0) {
    return [];
  }
  const from = sa.times[sa.times.length - 1] - windowSec;
  const pairs: [number, number][] = [];
  let j = sb.times.length - 1;
  for (let i = sa.times.length - 1; i >= 0 && sa.times[i] >= from; i--) {
    while (j >= 0 && sb.times[j] > sa.times[i]) {
      j--;
    }
    const va = sa.values[i];
    const vb = j >= 0 ? sb.values[j] : null;
    if (va !== null && vb !== null && Number.isFinite(va) && Number.isFinite(vb)) {
      pairs.push([va, vb]);
    }
  }
  return pairs.reverse();
}
