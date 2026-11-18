import { useEffect, useState } from 'react';
import { AlertToast } from './components/AlertToast';
import { AlertsPanel } from './components/AlertsPanel';
import { Drawer } from './components/Drawer';
import { DriverPanel } from './components/DriverPanel';
import { LiveView } from './components/LiveView';
import { PredictionsPanel } from './components/PredictionsPanel';
import { ServiceZone } from './components/ServiceZone';
import { TopBar, type Tab } from './components/TopBar';
import { useTelemetrySocket } from './hooks/useTelemetrySocket';
import { FanoutTick } from './lib/fanout-tick';
import { unknownSensors } from './sensors/layout';

export function App() {
  const state = useTelemetrySocket();
  const other = unknownSensors(state.sensorIds);
  const [tab, setTab] = useState<Tab>('live');
  const [driverOpen, setDriverOpen] = useState(false);
  const newestAlertUs = state.alerts.reduce((max, a) => Math.max(max, a.timestampUs), 0);
  // Like the toast, the history in the first snapshot counts as seen.
  const [seenAlertUs, setSeenAlertUs] = useState<number>();
  const unseenAlerts =
    seenAlertUs === undefined ? 0 : state.alerts.filter((a) => a.timestampUs > seenAlertUs).length;

  useEffect(() => {
    if (tab === 'analysis' || (seenAlertUs === undefined && newestAlertUs > 0)) {
      setSeenAlertUs(newestAlertUs);
    }
  }, [tab, newestAlertUs, seenAlertUs]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setDriverOpen(false);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  return (
    <FanoutTick value={state.tickMs}>
      <div className="dash">
        <TopBar
          state={state}
          tab={tab}
          onTab={setTab}
          unseenAlerts={unseenAlerts}
          onDriver={() => setDriverOpen(true)}
        />

        <main className="stage">
          {tab === 'live' ? <LiveView store={state.store} /> : null}
          {tab === 'charts' ? <ServiceZone store={state.store} extraIds={other} /> : null}
          {tab === 'analysis' ? (
            <div className="analysis">
              <PredictionsPanel predictions={state.predictions} />
              <AlertsPanel alerts={state.alerts} />
            </div>
          ) : null}

          <Drawer open={driverOpen} title="DRIVER RADIO" onClose={() => setDriverOpen(false)}>
            <DriverPanel enabled={state.driverEnabled} messages={state.driverMessages} active={state.driverActive} />
          </Drawer>
        </main>

        <AlertToast alerts={state.alerts} />
      </div>
    </FanoutTick>
  );
}
