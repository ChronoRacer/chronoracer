import { useEffect, useState } from 'react';
import { AlertsPanel } from './components/AlertsPanel';
import { Drawer } from './components/Drawer';
import { DriverPanel } from './components/DriverPanel';
import { DrivingZone } from './components/DrivingZone';
import { GapMarkers } from './components/GapMarkers';
import { PowertrainZone } from './components/PowertrainZone';
import { PredictionsPanel } from './components/PredictionsPanel';
import { ServiceZone } from './components/ServiceZone';
import { StatusBar } from './components/StatusBar';
import { useTelemetrySocket } from './hooks/useTelemetrySocket';
import { FanoutTick } from './lib/fanout-tick';
import { unknownSensors } from './sensors/layout';

type DrawerKind = 'analysis' | 'driver' | 'service' | null;

export function App() {
  const state = useTelemetrySocket();
  const other = unknownSensors(state.sensorIds);
  const [drawer, setDrawer] = useState<DrawerKind>(null);

  useEffect(() => {
    if (drawer === null) {
      return;
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setDrawer(null);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [drawer]);

  return (
    <FanoutTick value={state.tickMs}>
      <div className="dash">
        <StatusBar state={state} />
        <div className="dash-chrome">
          <GapMarkers gaps={state.gaps} />
          <PredictionsPanel
            predictions={state.predictions}
            variant="strip"
            onExpand={() => setDrawer('analysis')}
          />
          <AlertsPanel alerts={state.alerts} variant="strip" onExpand={() => setDrawer('analysis')} />
          <DriverPanel
            enabled={state.driverEnabled}
            messages={state.driverMessages}
            variant="strip"
            onExpand={() => setDrawer('driver')}
          />
        </div>

        <div className="dash-stage">
          <div className="zones-main">
            <DrivingZone store={state.store} />
            <PowertrainZone store={state.store} />
          </div>

          <Drawer open={drawer === 'analysis'} title="Predykcje i alerty" onClose={() => setDrawer(null)}>
            <div className="drawer-split">
              <PredictionsPanel predictions={state.predictions} />
              <AlertsPanel alerts={state.alerts} />
            </div>
          </Drawer>

          <Drawer open={drawer === 'driver'} title="Komunikacja z kierowcą" onClose={() => setDrawer(null)}>
            <DriverPanel enabled={state.driverEnabled} messages={state.driverMessages} />
          </Drawer>

          <Drawer
            open={drawer === 'service'}
            title={other.length > 0 ? 'Serwis, wykresy i pozostałe sensory' : 'Serwis i wykresy'}
            onClose={() => setDrawer(null)}
          >
            <ServiceZone store={state.store} extraIds={other} />
          </Drawer>
        </div>

        <ServiceZone
          store={state.store}
          variant="strip"
          onExpand={() => setDrawer('service')}
          extraIds={other}
        />
      </div>
    </FanoutTick>
  );
}
