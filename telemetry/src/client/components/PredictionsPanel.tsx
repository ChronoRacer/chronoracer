import type { Prediction } from '../../shared/protocol';
import { formatRefreshRate, formatTimeUs, predictionStatus } from '../lib/analysis-format';
import { formatValue } from '../lib/format';
import { sensorSpec } from '../sensors/catalog';
import { cx } from './instruments';

type Props = { predictions: Prediction[] };

const STATUS_LABEL = { live: 'LIVE', waiting: 'WAITING', unavailable: 'UNAVAILABLE' } as const;
const STATUS_TONE = { live: 'ok', waiting: 'warn', unavailable: 'bad' } as const;

/** One full-width row per prediction_id, in a stable order, so each result reads on its own line. */
export function PredictionsPanel({ predictions }: Props) {
  const sorted = [...predictions].sort((a, b) => title(a).localeCompare(title(b), 'en'));

  return (
    <section className="zone zone-predictions" aria-label="Predictions">
      <h2 className="zone-title">
        PREDICTIONS {sorted.length > 0 ? <span className="chip">{sorted.length}</span> : null}
      </h2>
      {sorted.length === 0 ? (
        <p className="empty-note">NO PREDICTIONS FROM ANALYSIS</p>
      ) : (
        <ul className="predictions">
          {sorted.map((p) => (
            <PredictionRow key={p.predictionId} prediction={p} />
          ))}
        </ul>
      )}
    </section>
  );
}

const title = (p: Prediction) => (p.name ?? p.predictionId).toUpperCase();

function PredictionRow({ prediction: p }: { prediction: Prediction }) {
  const status = predictionStatus(p);
  const latest = p.latest;
  const instruction = latest?.instruction ?? null;
  const value = latest?.value ?? null;
  return (
    <li className={`prediction status-${status}`}>
      <div className="prediction-name" title={`prediction_id ${p.predictionId}`}>
        <h3 className="prediction-title">{title(p)}</h3>
        <span className="prediction-meta">{formatRefreshRate(p.refreshRate)}</span>
      </div>

      <div className="prediction-result">
        {latest && (instruction !== null || value !== null) ? (
          <div className={cx('prediction-body', p.unavailable && 'stale')}>
            {instruction !== null ? <span className="prediction-instruction">{instruction}</span> : null}
            {value !== null ? <span className="prediction-value-num">{formatValue('', value)}</span> : null}
          </div>
        ) : p.unavailable ? null : (
          <span className="hint">NO RESULT YET</span>
        )}
        {p.unavailable ? (
          <p className="prediction-info">
            {p.unavailable.info} <span className="hint">SINCE {formatTimeUs(p.unavailable.timestampUs)}</span>
          </p>
        ) : null}
      </div>

      <div className="chips prediction-sensors">
        {latest?.sensorIds.map((id) => (
          <span key={id} className="chip" title={sensorSpec(id)?.description}>
            {id.toUpperCase()}
          </span>
        ))}
      </div>

      <div className="prediction-side">
        <span className={`badge tone-${STATUS_TONE[status]}`}>{STATUS_LABEL[status]}</span>
        {latest ? <span className="prediction-meta">AT {formatTimeUs(latest.timestampUs)}</span> : null}
      </div>
    </li>
  );
}
