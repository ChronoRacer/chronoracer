import { useEffect, useState } from 'react';
import { AlertToast } from './components/AlertToast';
import { AlertsPanel } from './components/AlertsPanel';
import { ChartsView, type ChartsSet } from './components/ChartsView';
import { DriverPanel } from './components/DriverPanel';
import { LiveView } from './components/LiveView';
import { PredictionsPanel } from './components/PredictionsPanel';
import { TopBar, type Tab } from './components/TopBar';
import { useTelemetrySocket } from './hooks/useTelemetrySocket';
import { FanoutTick } from './lib/fanout-tick';

export function App() {
  const state = useTelemetrySocket();
  const [tab, setTab] = useState<Tab>('live');
  const [chartsSet, setChartsSet] = useState<ChartsSet>('main');
  const [chartsPage, setChartsPage] = useState(0);
  // The alert list is on screen: its alerts count as seen and need no toast.
  const alertsVisible = tab === 'analysis';
  const newestAlertUs = state.alerts.reduce((max, a) => Math.max(max, a.timestampUs), 0);
  // Like the toast, the history in the first snapshot counts as seen.
  const [seenAlertUs, setSeenAlertUs] = useState<number>();
  const unseenAlerts =
    seenAlertUs === undefined ? 0 : state.alerts.filter((a) => a.timestampUs > seenAlertUs).length;

  useEffect(() => {
    if (alertsVisible || (seenAlertUs === undefined && newestAlertUs > 0)) {
      setSeenAlertUs(newestAlertUs);
    }
  }, [alertsVisible, newestAlertUs, seenAlertUs]);

  return (
    <FanoutTick value={state.tickMs}>
      <div className="dash">
        <TopBar state={state} tab={tab} onTab={setTab} unseenAlerts={unseenAlerts} />

        <main className="stage">
          {tab === 'live' ? <LiveView store={state.store} /> : null}
          {tab === 'charts' ? (
            <ChartsView
              store={state.store}
              sensorIds={state.sensorIds}
              set={chartsSet}
              onSet={setChartsSet}
              page={chartsPage}
              onPage={setChartsPage}
            />
          ) : null}
          {tab === 'analysis' ? (
            <div className="analysis">
              <PredictionsPanel predictions={state.predictions} />
              <AlertsPanel alerts={state.alerts} />
            </div>
          ) : null}
          {tab === 'driver' ? (
            <div className="driver-view">
              <DriverPanel enabled={state.driverEnabled} messages={state.driverMessages} active={state.driverActive} />
            </div>
          ) : null}
        </main>

        <AlertToast alerts={state.alerts} muted={alertsVisible} />
      </div>
    </FanoutTick>
  );
}
