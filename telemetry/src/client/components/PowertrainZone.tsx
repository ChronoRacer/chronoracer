import { useSensor, type Reading } from '../hooks/useSensor';
import { useMotionSvgArc } from '../hooks/useSmoothedNumber';
import type { SeriesStore } from '../lib/series-store';
import { formatFixed } from '../lib/format';
import { describeArc, norm } from '../lib/svg';
import { DIAL, DialScale } from './DrivingZone';
import { HBar, cx, describe, liveClass } from './instruments';

type ZoneProps = { store: SeriesStore };

/** The combustion engine is off (EV drive) when the hybrid ECU reports zero rpm. */
const EV_RPM = 1;
const REDLINE_RPM = 6500;

const isEv = (rpm: Reading) => rpm.finite !== undefined && rpm.finite < EV_RPM;

export function ModeChip({ store }: ZoneProps) {
  const rpm = useSensor(store, 'rpm');
  const ev = isEv(rpm);
  return (
    <span className={cx('mode-chip', rpm.finite === undefined ? 'unknown' : ev ? 'ev' : 'ice')} title={describe(rpm)}>
      {rpm.finite === undefined ? 'MODE —' : ev ? 'EV DRIVE' : 'ENGINE ON'}
    </span>
  );
}

/** RPM dial over throttle and engine load. */
export function EnginePanel({ store }: ZoneProps) {
  const throttle = useSensor(store, 'throttle');
  const load = useSensor(store, 'engine_load');
  return (
    <div className="engine">
      <RpmGauge store={store} />
      <div className="bars">
        <HBar r={throttle} label="THROTTLE" />
        <HBar r={load} label="ENGINE LOAD" />
      </div>
    </div>
  );
}

function RpmGauge({ store }: ZoneProps) {
  const rpm = useSensor(store, 'rpm');
  const ev = isEv(rpm);
  const min = rpm.spec?.min ?? 0;
  const max = rpm.spec?.max ?? 8000;
  const arc = useMotionSvgArc(rpm.finite ?? min, min, max, DIAL.c, DIAL.c, DIAL.r, -DIAL.sweep, DIAL.sweep, describeArc);
  const redFrom = -DIAL.sweep + 2 * DIAL.sweep * norm(REDLINE_RPM, min, max);

  return (
    <div className={cx('gauge gauge-rpm', ev && 'ev', liveClass(rpm))} title={describe(rpm)}>
      <svg viewBox="0 0 200 200" aria-hidden>
        <path d={describeArc(DIAL.c, DIAL.c, DIAL.r, -DIAL.sweep, DIAL.sweep)} className="dial-track" />
        <path d={describeArc(DIAL.c, DIAL.c, DIAL.r + 10, redFrom, DIAL.sweep)} className="dial-redline" />
        <path ref={arc} className="dial-value" />
        <DialScale min={min} max={max} step={500} labelEvery={2} label={(v) => String(v / 1000)} />
      </svg>
      <div className="gauge-center">
        <span className="gauge-num">{formatFixed(rpm.value, 0)}</span>
        <span className="gauge-unit">{ev ? 'RPM · EV' : 'RPM'}</span>
      </div>
    </div>
  );
}
