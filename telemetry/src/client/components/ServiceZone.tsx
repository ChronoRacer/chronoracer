import { useSensor, type Reading } from '../hooks/useSensor';
import type { SeriesStore } from '../lib/series-store';
import { formatSigned } from '../lib/format';
import { NORMAL_BANDS } from '../sensors/tones';
import { SensorChart } from './SensorChart';
import { CenterBar, Card, Metric, RangeTrack, Readout, Thermometer, cx, liveClass } from './instruments';

type ZoneProps = { store: SeriesStore };

const SERIES_COLORS = { primary: '#22d3ee', secondary: '#fbbf24', reference: '#8792a6' } as const;

type Props = ZoneProps & {
  variant?: 'strip' | 'full';
  onExpand?: () => void;
  extraIds?: readonly string[];
};

/** Compact dock values always visible; full cards with charts live in the drawer. */
export function ServiceZone({ store, variant = 'full', onExpand, extraIds = [] }: Props) {
  if (variant === 'strip') {
    return (
      <button type="button" className="service-dock" onClick={onExpand} aria-label="Rozwiń serwis i wykresy">
        <span className="zone-title">Serwis</span>
        <div className="dock-metrics">
          <DockTemp store={store} id="coolant_temp" label="Płyn" />
          <DockTemp store={store} id="oil_temp" label="Olej" />
          <DockTemp store={store} id="intake_temp" label="Dolot" />
          <DockMetric store={store} id="lambda" label="λ" digits={3} />
          <DockMetric store={store} id="stft" label="STFT" digits={1} />
          <DockMetric store={store} id="ltft" label="LTFT" digits={1} />
          <DockMetric store={store} id="maf" label="MAF" digits={1} />
          <DockMetric store={store} id="ign_timing" label="Zapłon" digits={1} />
          <DockMetric store={store} id="o2_current" label="O₂" digits={2} />
          {extraIds.map((id) => (
            <DockMetric key={id} store={store} id={id} label={id} />
          ))}
        </div>
        <span className="dock-hint">rozwiń wykresy</span>
      </button>
    );
  }

  return (
    <section className="zone zone-service" aria-label="Serwis i telemetria silnika">
      <div className="service-grid">
        <TempCard store={store} id="coolant_temp" title="Płyn chłodzący" />
        <TempCard store={store} id="oil_temp" title="Olej" />
        <TempCard store={store} id="intake_temp" title="Powietrze dolotowe" />
        <LambdaCard store={store} />
        <TrimsCard store={store} />
        <SensorCard store={store} id="maf" title="Masowy przepływ powietrza" digits={1} bar />
        <SensorCard store={store} id="ign_timing" title="Wyprzedzenie zapłonu" digits={1} bar />
        <SensorCard store={store} id="o2_current" title="Prąd sondy O₂" digits={2} />
        {extraIds.map((id) => (
          <SensorCard key={id} store={store} id={id} />
        ))}
      </div>
    </section>
  );
}

function DockTemp({ store, id, label }: ZoneProps & { id: string; label: string }) {
  const r = useSensor(store, id);
  return (
    <span className={cx('dock-item', liveClass(r))}>
      <span className="metric-label">{label}</span>
      <Readout r={r} digits={0} />
    </span>
  );
}

function DockMetric({
  store,
  id,
  label,
  digits,
}: ZoneProps & { id: string; label: string; digits?: number }) {
  const r = useSensor(store, id);
  return (
    <span className={cx('dock-item', liveClass(r))}>
      <span className="metric-label">{label}</span>
      <Readout r={r} digits={digits} />
    </span>
  );
}

function rangeOf(r: Reading): [number | null, number | null] {
  return [r.spec?.min ?? null, r.spec?.max ?? null];
}

function TempCard({ store, id, title }: ZoneProps & { id: string; title: string }) {
  const r = useSensor(store, id);
  const band = NORMAL_BANDS[id];
  const [min, max] = rangeOf(r);
  return (
    <Card title={title} className="card-temp">
      <div className="temp-body">
        <Thermometer r={r} band={band} />
        <div className="temp-readouts">
          <Readout r={r} digits={0} className="big" />
          {band ? (
            <span className="hint">
              norma {band[0]}–{band[1]} {r.spec?.unit}
            </span>
          ) : null}
        </div>
      </div>
      <SensorChart store={store} series={[{ id, color: SERIES_COLORS.primary }]} min={min} max={max} height={88} />
    </Card>
  );
}

function LambdaCard({ store }: ZoneProps) {
  const lambda = useSensor(store, 'lambda');
  const cmd = useSensor(store, 'lambda_cmd');
  const delta = lambda.finite !== undefined && cmd.finite !== undefined ? lambda.finite - cmd.finite : undefined;
  const [min, max] = rangeOf(lambda);
  return (
    <Card
      title="Lambda vs cel"
      className="card-wide"
      aside={<Legend items={[['zmierzona', SERIES_COLORS.primary], ['cel', SERIES_COLORS.reference]]} />}
    >
      <div className="stats">
        <Metric label="Zmierzona">
          <Readout r={lambda} digits={3} className="big" />
        </Metric>
        <Metric label="Cel">
          <Readout r={cmd} digits={3} className="big" />
        </Metric>
        <Metric label="Δ">
          <span className={cx('readout', delta !== undefined && Math.abs(delta) > 0.05 ? 'tone-warn' : 'tone-neutral')}>
            <span className="readout-value">{formatSigned(delta, 3)}</span>
          </span>
        </Metric>
      </div>
      <SensorChart
        store={store}
        series={[
          { id: 'lambda_cmd', color: SERIES_COLORS.reference, dash: [4, 3] },
          { id: 'lambda', color: SERIES_COLORS.primary },
        ]}
        min={min}
        max={max}
      />
    </Card>
  );
}

function TrimsCard({ store }: ZoneProps) {
  const stft = useSensor(store, 'stft');
  const ltft = useSensor(store, 'ltft');
  const [min, max] = rangeOf(stft);
  return (
    <Card
      title="Korekty paliwa"
      className="card-wide"
      aside={<Legend items={[['STFT', SERIES_COLORS.primary], ['LTFT', SERIES_COLORS.secondary]]} />}
    >
      <div className="bars two">
        <CenterBar r={stft} label="Krótkoterminowa" digits={1} negLabel="−25" posLabel="+25" />
        <CenterBar r={ltft} label="Długoterminowa" digits={1} negLabel="−25" posLabel="+25" />
      </div>
      <SensorChart
        store={store}
        series={[
          { id: 'stft', color: SERIES_COLORS.primary },
          { id: 'ltft', color: SERIES_COLORS.secondary },
        ]}
        min={min}
        max={max}
      />
    </Card>
  );
}

/** Generic card: readout, optional range bar, 60 s chart. Also used for sensors outside the contract. */
export function SensorCard({
  store,
  id,
  title = id,
  digits,
  bar = false,
}: ZoneProps & { id: string; title?: string; digits?: number; bar?: boolean }) {
  const r = useSensor(store, id);
  const [min, max] = rangeOf(r);
  return (
    <Card title={title} aside={title === id ? null : <span className="card-unit">{id}</span>}>
      <Readout r={r} digits={digits} className="big" />
      {bar && r.spec?.min != null && r.spec.max != null ? <RangeTrack r={r} scale /> : null}
      <SensorChart store={store} series={[{ id, color: SERIES_COLORS.primary }]} min={min} max={max} height={88} />
    </Card>
  );
}

function Legend({ items }: { items: [string, string][] }) {
  return (
    <span className="legend">
      {items.map(([label, color]) => (
        <span key={label} className="legend-item">
          <span className="legend-swatch" style={{ background: color }} />
          {label}
        </span>
      ))}
    </span>
  );
}
