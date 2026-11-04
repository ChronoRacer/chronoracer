import type { ReactNode } from 'react';
import type { TelemetryState } from '../hooks/useTelemetrySocket';
import { useSensor, type Reading } from '../hooks/useSensor';
import { useMotionScale } from '../hooks/useSmoothedNumber';
import type { SeriesStore } from '../lib/series-store';
import { formatFixed, formatValue } from '../lib/format';
import { CAR } from '../sensors/catalog';
import { ConnectionStatus } from './ConnectionStatus';
import { Metric, Readout, cx, describe, liveClass } from './instruments';

export function StatusBar({ state }: { state: TelemetryState }) {
  const { store } = state;
  const batt = useSensor(store, 'batt_volt');
  const fuel = useSensor(store, 'fuel_l');
  const ambient = useSensor(store, 'ambient_temp');
  const baro = useSensor(store, 'baro');
  return (
    <header className="topbar">
      <div className="topbar-brand">
        <h1 className="brand">Chronoracer</h1>
        <span className="brand-car">{CAR}</span>
      </div>
      <ConnectionStatus connection={state.connection} coreConnected={state.coreConnected} hello={state.hello} />

      <div className="topbar-groups">
        <GpsGroup store={store} />
        <Group>
          <Metric label="Akumulator 12 V">
            <Readout r={batt} digits={1} />
          </Metric>
          <FuelMetric r={fuel} />
        </Group>
        <Group>
          <Metric label="Otoczenie">
            <Readout r={ambient} digits={1} />
          </Metric>
          <Metric label="Baro">
            <Readout r={baro} digits={1} />
          </Metric>
        </Group>
        <PositionGroup store={store} />
      </div>

      <a className="logout" href="/auth/logout">
        Wyloguj
      </a>
    </header>
  );
}

function Group({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cx('topbar-group', className)}>{children}</div>;
}

function GpsGroup({ store }: { store: SeriesStore }) {
  const fix = useSensor(store, 'gps_fix');
  const sats = useSensor(store, 'gps_sats');
  const pdop = useSensor(store, 'gps_pdop');
  return (
    <Group>
      <span className={cx('badge', liveClass(fix))} title={describe(fix)}>
        GPS {formatValue(fix.id, fix.value)}
      </span>
      <Metric label="Satelity">
        <Readout r={sats} digits={0} />
      </Metric>
      <Metric label="PDOP">
        <Readout r={pdop} digits={1} />
      </Metric>
    </Group>
  );
}

function FuelMetric({ r }: { r: Reading }) {
  const fill = useMotionScale(r.finite ?? 0, r.spec?.min ?? 0, r.spec?.max ?? 60, 'X');
  return (
    <Metric label="Paliwo">
      <span className="fuel">
        <span className={cx('fuel-gauge', liveClass(r))} title={describe(r)}>
          <span className="fuel-fill" ref={fill} />
        </span>
        <Readout r={r} digits={1} />
      </span>
    </Metric>
  );
}

function PositionGroup({ store }: { store: SeriesStore }) {
  const lat = useSensor(store, 'gps_lat');
  const lon = useSensor(store, 'gps_lon');
  const alt = useSensor(store, 'gps_alt');
  return (
    <Group className="topbar-position">
      <span className={cx('coord', liveClass(lat))} title={describe(lat)}>
        {formatFixed(lat.value, 6)}°
      </span>
      <span className={cx('coord', liveClass(lon))} title={describe(lon)}>
        {formatFixed(lon.value, 6)}°
      </span>
      <span className={cx('coord', liveClass(alt))} title={describe(alt)}>
        {formatFixed(alt.value, 0)} m
      </span>
    </Group>
  );
}
