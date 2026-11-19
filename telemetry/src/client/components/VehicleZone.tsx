import type { ReactNode } from 'react';
import { useSensor, type Reading } from '../hooks/useSensor';
import type { SeriesStore } from '../lib/series-store';
import { NORMAL_BANDS } from '../sensors/tones';
import { Card, RangeTrack, Readout, TempBar, cx, describe, liveClass } from './instruments';

type ZoneProps = { store: SeriesStore };

export function EnergyCard({ store }: ZoneProps) {
  const soc = useSensor(store, 'hv_soc');
  const fuel = useSensor(store, 'fuel_l');
  const batt = useSensor(store, 'batt_volt');
  return (
    <Card title="ENERGY" className="area-energy">
      <div className="rows grow">
        <Row r={soc} label="HV BATTERY" digits={0}>
          <RangeTrack r={soc} />
        </Row>
        <Row r={fuel} label="FUEL" digits={1}>
          <RangeTrack r={fuel} />
        </Row>
        <Row r={batt} label="12V SYSTEM" digits={1} />
      </div>
    </Card>
  );
}

const TEMPS = [
  ['coolant_temp', 'COOLANT'],
  ['oil_temp', 'OIL'],
  ['intake_temp', 'INTAKE AIR'],
  ['ambient_temp', 'AMBIENT'],
] as const;

export function TemperaturesCard({ store }: ZoneProps) {
  return (
    <Card title="TEMPERATURES" className="area-temps" aside={<span className="card-unit">°C</span>}>
      <div className="rows grow">
        {TEMPS.map(([id, label]) => (
          <TempRow key={id} store={store} id={id} label={label} />
        ))}
      </div>
    </Card>
  );
}

function TempRow({ store, id, label }: ZoneProps & { id: string; label: string }) {
  const r = useSensor(store, id);
  const band = NORMAL_BANDS[id];
  return (
    <Row r={r} label={label} digits={0} unit="" hint={band ? `NORMAL ${band[0]}–${band[1]}` : undefined}>
      <TempBar r={r} band={band} />
    </Row>
  );
}

function Row({
  r,
  label,
  digits,
  unit,
  hint,
  children,
}: {
  r: Reading;
  label: string;
  digits: number;
  unit?: string;
  hint?: string;
  children?: ReactNode;
}) {
  return (
    <div className={cx('row', liveClass(r))} title={describe(r)}>
      <div className="row-head">
        <span className="row-label">
          {label}
          {hint ? <span className="row-hint">{hint}</span> : null}
        </span>
        <Readout r={r} digits={digits} unit={unit} className="row-value" />
      </div>
      {children}
    </div>
  );
}
