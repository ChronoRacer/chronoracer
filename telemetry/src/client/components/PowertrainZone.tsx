import { useSensor } from '../hooks/useSensor';
import { useSmoothedNumber } from '../hooks/useSmoothedNumber';
import type { SeriesStore } from '../lib/series-store';
import { formatFixed } from '../lib/format';
import { describeArc, norm, polar } from '../lib/svg';
import { Card, HBar, Readout, cx, describe, liveClass } from './instruments';

type ZoneProps = { store: SeriesStore };

/** The combustion engine is off (EV drive) when the hybrid ECU reports zero rpm. */
const EV_RPM = 1;
const REDLINE_RPM = 6500;

export function PowertrainZone({ store }: ZoneProps) {
  const rpm = useSensor(store, 'rpm');
  const throttle = useSensor(store, 'throttle');
  const load = useSensor(store, 'engine_load');
  const ev = rpm.finite !== undefined && rpm.finite < EV_RPM;
  return (
    <section className="zone zone-powertrain" aria-label="Hybryda i napęd">
      <h2 className="zone-title">
        Hybryda i napęd
        <span className={cx('mode-chip', rpm.finite === undefined ? 'unknown' : ev ? 'ev' : 'ice')}>
          {rpm.finite === undefined ? 'tryb —' : ev ? 'napęd EV' : 'silnik włączony'}
        </span>
      </h2>
      <div className="powertrain-grid">
        <SocRing store={store} />
        <RpmGauge store={store} ev={ev} />
        <Card title="Przepustnica i obciążenie" className="area-bars">
          <div className="bars">
            <HBar r={throttle} label="Przepustnica (ECU)" />
            <HBar r={load} label="Obciążenie silnika" />
          </div>
        </Card>
      </div>
    </section>
  );
}

const SOC_SWEEP = 135;

function SocRing({ store }: ZoneProps) {
  const soc = useSensor(store, 'hv_soc');
  const t = norm(useSmoothedNumber(soc.finite ?? 0), soc.spec?.min ?? 0, soc.spec?.max ?? 100);
  return (
    <Card title="Bateria HV" className="area-soc">
      <div className={cx('ring', liveClass(soc))} title={describe(soc)}>
        <svg viewBox="0 0 160 160" aria-hidden>
          <path d={describeArc(80, 80, 64, -SOC_SWEEP, SOC_SWEEP)} className="ring-track" />
          <path d={describeArc(80, 80, 64, -SOC_SWEEP, -SOC_SWEEP + 2 * SOC_SWEEP * t)} className="ring-value" />
        </svg>
        <div className="ring-center">
          <Readout r={soc} digits={0} unit="" className="ring-readout" />
          <span className="ring-unit">% SOC</span>
        </div>
      </div>
    </Card>
  );
}

const RPM_SWEEP = 120;

function RpmGauge({ store, ev }: ZoneProps & { ev: boolean }) {
  const rpm = useSensor(store, 'rpm');
  const min = rpm.spec?.min ?? 0;
  const max = rpm.spec?.max ?? 8000;
  const t = norm(useSmoothedNumber(rpm.finite ?? 0), min, max);
  const angle = -RPM_SWEEP + 2 * RPM_SWEEP * t;
  const redFrom = -RPM_SWEEP + 2 * RPM_SWEEP * norm(REDLINE_RPM, min, max);
  const steps = Math.round((max - min) / 1000);

  return (
    <Card title="Obroty silnika" className={cx('area-rpm', ev && 'ev')}>
      <div className={cx('tacho', liveClass(rpm))} title={describe(rpm)}>
        <svg viewBox="0 0 200 170" aria-hidden>
          <path d={describeArc(100, 100, 82, -RPM_SWEEP, RPM_SWEEP)} className="tacho-track" />
          <path d={describeArc(100, 100, 82, redFrom, RPM_SWEEP)} className="tacho-redline" />
          <path d={describeArc(100, 100, 82, -RPM_SWEEP, angle)} className="tacho-value" />
          {Array.from({ length: steps + 1 }, (_, i) => {
            const deg = -RPM_SWEEP + (2 * RPM_SWEEP * i) / steps;
            const outer = polar(100, 100, 74, deg);
            const inner = polar(100, 100, 68, deg);
            const label = polar(100, 100, 58, deg);
            return (
              <g key={i}>
                <line x1={inner.x} y1={inner.y} x2={outer.x} y2={outer.y} className="tacho-tick" />
                <text x={label.x} y={label.y} className="tacho-label" textAnchor="middle" dominantBaseline="central">
                  {Math.round((min + ((max - min) * i) / steps) / 1000)}
                </text>
              </g>
            );
          })}
          <g transform={`rotate(${angle} 100 100)`}>
            <line x1="100" y1="108" x2="100" y2="26" className="tacho-needle" />
          </g>
          <circle cx="100" cy="100" r="6" className="tacho-hub" />
          <text x="100" y="160" className="tacho-caption" textAnchor="middle">
            ×1000 rpm
          </text>
        </svg>
        <div className="tacho-center">
          <span className="readout-value">{formatFixed(rpm.value, 0)}</span>
          <span className={cx('ev-badge', ev && 'on')}>EV</span>
        </div>
      </div>
    </Card>
  );
}
