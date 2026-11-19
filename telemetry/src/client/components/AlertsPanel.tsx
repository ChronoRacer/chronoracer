import type { Alert } from '../../shared/protocol';
import { formatTimeUs } from '../lib/analysis-format';
import { formatValue } from '../lib/format';
import { sensorSpec } from '../sensors/catalog';

type Props = { alerts: Alert[] };

const SHOWN = 50;

export function AlertsPanel({ alerts }: Props) {
  const recent = alerts.slice(-SHOWN).reverse();

  return (
    <section className="zone zone-alerts" aria-label="Alerts">
      <h2 className="zone-title">
        ALERTS {alerts.length > 0 ? <span className="chip tone-bad">{alerts.length}</span> : null}
      </h2>
      {recent.length === 0 ? (
        <p className="empty-note">NO ALERTS FROM ANALYSIS</p>
      ) : (
        <ul className="alerts">
          {recent.map((a, i) => (
            <li key={`${a.timestampUs}-${a.alertId}-${i}`} className="alert">
              <div className="alert-head">
                <span className="alert-time">{formatTimeUs(a.timestampUs)}</span>
                <span className="card-unit">{a.alertId}</span>
              </div>
              <p className="alert-message">{a.message}</p>
              <p className="alert-reason">{a.reason}</p>
              <div className="chips">
                {a.sensors.map(({ sensorId, value }) => (
                  <span key={sensorId} className="chip" title={sensorSpec(sensorId)?.description}>
                    {sensorId.toUpperCase()} {formatValue(sensorId, value)}
                    {sensorSpec(sensorId)?.unit ? ` ${sensorSpec(sensorId)?.unit}` : ''}
                  </span>
                ))}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
