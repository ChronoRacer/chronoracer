import { ConnectionStatus } from './components/ConnectionStatus';
import { GapMarkers } from './components/GapMarkers';
import { SensorPanel } from './components/SensorPanel';
import { useTelemetrySocket } from './hooks/useTelemetrySocket';
import { CAR } from './sensors/catalog';
import { withUnknownSensors } from './sensors/layout';

export function App() {
  const state = useTelemetrySocket();

  return (
    <div className="app">
      <header>
        <h1 className="brand">Chronoracer Telemetry</h1>
        <p className="lede">{CAR}. Near-real-time preview of Core-accepted sensor values.</p>
      </header>

      <div className="toolbar">
        <ConnectionStatus connection={state.connection} coreConnected={state.coreConnected} hello={state.hello} />
        <a className="auth-link" href="/auth/logout">
          Log out
        </a>
      </div>

      <GapMarkers gaps={state.gaps} />

      {withUnknownSensors(state.sensorIds).map((section) => (
        <section key={section.title} className="sensor-section">
          <h2>{section.title}</h2>
          <div className="sensor-grid">
            {section.sensors.map((id) => (
              <SensorPanel key={id} store={state.store} id={id} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
