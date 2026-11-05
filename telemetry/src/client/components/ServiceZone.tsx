import { useSensor, type Reading } from '../hooks/useSensor';
import type { SeriesStore } from '../lib/series-store';
import { formatSigned } from '../lib/format';
import { NORMAL_BANDS } from '../sensors/tones';
import { SensorChart } from './SensorChart';
import { Card, Metric, Readout, cx } from './instruments';

type ZoneProps = { store: SeriesStore };

const SERIES_COLORS = { primary: '#22d3ee', secondary: '#fbbf24', reference: '#8792a6' } as const;

/** CHARTS tab: one 60 s chart per card, sized to fill the stage. */
export function ServiceZone({ store, extraIds = [] }: ZoneProps & { extraIds?: readonly string[] }) {
  return (
    <section className="charts" aria-label="Service charts">
      <TempCard store={store} id="coolant_temp" title="COOLANT" />
      <TempCard store={store} id="oil_temp" title="OIL" />
      <TempCard store={store} id="intake_temp" title="INTAKE AIR" />
      <SensorCard store={store} id="maf" title="MASS AIR FLOW" digits={1} />
      <SensorCard store={store} id="ign_timing" title="IGNITION TIMING" digits={1} />
      <LambdaCard store={store} />
      <TrimsCard store={store} />
      <SensorCard store={store} id="o2_current" title="O₂ SENSOR CURRENT" digits={2} />
      {extraIds.map((id) => (
        <SensorCard key={id} store={store} id={id} />
      ))}
    </section>
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
    <Card title={title} aside={band ? <span className="card-unit">NORMAL {band[0]}–{band[1]}</span> : null}>
      <Readout r={r} digits={0} className="big" />
      <SensorChart store={store} series={[{ id, color: SERIES_COLORS.primary }]} min={min} max={max} />
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
      title="LAMBDA VS TARGET"
      className="card-wide"
      aside={<Legend items={[['MEASURED', SERIES_COLORS.primary], ['TARGET', SERIES_COLORS.reference]]} />}
    >
      <div className="stats">
        <Metric label="MEASURED">
          <Readout r={lambda} digits={3} className="big" />
        </Metric>
        <Metric label="TARGET">
          <Readout r={cmd} digits={3} className="big" />
        </Metric>
        <Metric label="DELTA">
          <span className={cx('readout big', delta !== undefined && Math.abs(delta) > 0.05 ? 'tone-warn' : 'tone-neutral')}>
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
      title="FUEL TRIMS"
      className="card-wide"
      aside={<Legend items={[['SHORT TERM', SERIES_COLORS.primary], ['LONG TERM', SERIES_COLORS.secondary]]} />}
    >
      <div className="stats">
        <Metric label="SHORT TERM">
          <Readout r={stft} digits={1} signed className="big" />
        </Metric>
        <Metric label="LONG TERM">
          <Readout r={ltft} digits={1} signed className="big" />
        </Metric>
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

/** Generic card: readout and 60 s chart. Also used for sensors outside the contract. */
export function SensorCard({
  store,
  id,
  title = id.toUpperCase(),
  digits,
}: ZoneProps & { id: string; title?: string; digits?: number }) {
  const r = useSensor(store, id);
  const [min, max] = rangeOf(r);
  return (
    <Card title={title} aside={<span className="card-unit">{id}</span>}>
      <Readout r={r} digits={digits} className="big" />
      <SensorChart store={store} series={[{ id, color: SERIES_COLORS.primary }]} min={min} max={max} />
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
