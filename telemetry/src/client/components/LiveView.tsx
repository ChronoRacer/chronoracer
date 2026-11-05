import type { SeriesStore } from '../lib/series-store';
import { GForceCard, PedalsCard, SpeedGauge, SteeringCard, WheelsCard } from './DrivingZone';
import { GpsTrack } from './GpsTrack';
import { Card } from './instruments';
import { EnginePanel, ModeChip } from './PowertrainZone';
import { EnergyCard, TemperaturesCard } from './VehicleZone';

/** LIVE tab: driving instruments | track map | energy and temperatures, all in one viewport. */
export function LiveView({ store }: { store: SeriesStore }) {
  return (
    <div className="live">
      <Card title="SPEED · ENGINE" className="area-cluster" aside={<ModeChip store={store} />}>
        <div className="cluster grow">
          <SpeedGauge store={store} />
          <EnginePanel store={store} />
        </div>
      </Card>
      <PedalsCard store={store} />
      <SteeringCard store={store} />
      <GForceCard store={store} />
      <WheelsCard store={store} />
      <GpsTrack store={store} />
      <EnergyCard store={store} />
      <TemperaturesCard store={store} />
    </div>
  );
}
