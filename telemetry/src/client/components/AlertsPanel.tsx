import type { Alert } from '../../shared/protocol';
import { formatTimeUs } from '../lib/analysis-format';
import { formatValue } from '../lib/format';
import { sensorSpec } from '../sensors/catalog';

type Props = {
  alerts: Alert[];
  variant?: 'strip' | 'full';
  onExpand?: () => void;
};

const SHOWN = 50;

export function AlertsPanel({ alerts, variant = 'full', onExpand }: Props) {
  const recent = alerts.slice(-SHOWN).reverse();
  const latest = recent[0];

  if (variant === 'strip') {
    return (
      <button type="button" className="meta-strip" onClick={onExpand} aria-label="Rozwiń alerty">
        <span className="zone-title">
          Alerty {alerts.length > 0 ? <span className="chip tone-bad">{alerts.length}</span> : null}
        </span>
        {latest ? (
          <span className="meta-latest">
            <span className="alert-time">{formatTimeUs(latest.timestampUs)}</span>
            <span className="alert-message">{latest.message}</span>
          </span>
        ) : (
          <span className="hint">brak</span>
        )}
      </button>
    );
  }

  return (
    <section className="zone" aria-label="Alerty">
      {recent.length === 0 ? (
        <div className="gaps-strip empty">Brak alertów z analysis</div>
      ) : (
        <ul className="alerts">
          {recent.map((a, i) => (
            <li key={`${a.timestampUs}-${a.alertId}-${i}`} className="alert">
              <div className="alert-head">
                <span className="alert-time">{formatTimeUs(a.timestampUs)}</span>
                <span className="alert-message">{a.message}</span>
                <span className="card-unit">{a.alertId}</span>
              </div>
              <div className="hint">{a.reason}</div>
              <div className="chips">
                {a.sensors.map(({ sensorId, value }) => (
                  <span key={sensorId} className="chip" title={sensorSpec(sensorId)?.description}>
                    {sensorId} {formatValue(sensorId, value)}
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
