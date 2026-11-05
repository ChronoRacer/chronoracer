import type { Prediction } from '../../shared/protocol';
import { formatRefreshRate, formatTimeUs, predictionStatus } from '../lib/analysis-format';
import { formatValue } from '../lib/format';
import { sensorSpec } from '../sensors/catalog';
import { Card, cx } from './instruments';

type Props = { predictions: Prediction[] };

const STATUS_LABEL = { live: 'LIVE', waiting: 'WAITING', unavailable: 'UNAVAILABLE' } as const;
const STATUS_TONE = { live: 'ok', waiting: 'warn', unavailable: 'bad' } as const;

export function PredictionsPanel({ predictions }: Props) {
  const sorted = [...predictions].sort((a, b) => title(a).localeCompare(title(b), 'en'));

  return (
    <section className="zone zone-predictions" aria-label="Predictions">
      <h2 className="zone-title">PREDICTIONS</h2>
      {sorted.length === 0 ? (
        <p className="empty-note">NO PREDICTIONS FROM ANALYSIS</p>
      ) : (
        <div className="predictions">
          {sorted.map((p) => (
            <PredictionCard key={p.predictionId} prediction={p} />
          ))}
        </div>
      )}
    </section>
  );
}

const title = (p: Prediction) => (p.name ?? p.predictionId).toUpperCase();

function PredictionCard({ prediction: p }: { prediction: Prediction }) {
  const status = predictionStatus(p);
  const latest = p.latest;
  return (
    <Card
      title={title(p)}
      className={`prediction status-${status}`}
      aside={<span className={`badge tone-${STATUS_TONE[status]}`}>{STATUS_LABEL[status]}</span>}
    >
      {p.unavailable ? (
        <p className="prediction-info">
          {p.unavailable.info} <span className="hint">SINCE {formatTimeUs(p.unavailable.timestampUs)}</span>
        </p>
      ) : null}

      {latest?.instruction !== null && latest?.instruction !== undefined ? (
        <p className="prediction-instruction">{latest.instruction}</p>
      ) : null}

      {latest ? (
        <div className={cx('prediction-body', p.unavailable && 'stale')}>
          {latest.value !== null ? (
            <span className="prediction-value">
              <span className="prediction-value-num">{formatValue('', latest.value)}</span>
            </span>
          ) : null}
        </div>
      ) : null}

      {latest && latest.sensorIds.length > 0 ? (
        <div className="chips">
          {latest.sensorIds.map((id) => (
            <span key={id} className="chip" title={sensorSpec(id)?.description}>
              {id.toUpperCase()}
            </span>
          ))}
        </div>
      ) : null}

      <footer className="prediction-meta" title={`prediction_id ${p.predictionId}`}>
        <span>{formatRefreshRate(p.refreshRate)}</span>
        {latest ? <span>AT {formatTimeUs(latest.timestampUs)}</span> : null}
      </footer>
    </Card>
  );
}
