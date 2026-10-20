import type { Prediction } from '../../shared/protocol';
import { formatRefreshRate, formatTimeUs, predictionStatus } from '../lib/analysis-format';
import { formatValue } from '../lib/format';
import { sensorSpec } from '../sensors/catalog';
import { Card, cx } from './instruments';

type Props = {
  predictions: Prediction[];
  /** Compact strip in the chrome; full cards go in the drawer. */
  variant?: 'strip' | 'full';
  onExpand?: () => void;
};

const STATUS_LABEL = { live: 'na żywo', waiting: 'oczekiwanie', unavailable: 'niedostępna' } as const;
const STATUS_TONE = { live: 'ok', waiting: 'warn', unavailable: 'bad' } as const;

export function PredictionsPanel({ predictions, variant = 'full', onExpand }: Props) {
  const sorted = [...predictions].sort((a, b) => title(a).localeCompare(title(b), 'pl'));

  if (variant === 'strip') {
    return (
      <button type="button" className="meta-strip" onClick={onExpand} aria-label="Rozwiń predykcje">
        <span className="zone-title">Predykcje</span>
        {sorted.length === 0 ? (
          <span className="hint">brak</span>
        ) : (
          <div className="meta-chips">
            {sorted.map((p) => {
              const status = predictionStatus(p);
              return (
                <span key={p.predictionId} className={cx('chip', `tone-${STATUS_TONE[status]}`)}>
                  <span className="meta-chip-name">{title(p)}</span>
                  {p.latest?.value !== null && p.latest?.value !== undefined
                    ? formatValue('', p.latest.value)
                    : p.latest?.instruction ?? STATUS_LABEL[status]}
                </span>
              );
            })}
          </div>
        )}
      </button>
    );
  }

  return (
    <section className="zone" aria-label="Predykcje">
      {sorted.length === 0 ? (
        <div className="gaps-strip empty">Brak predykcji z analysis</div>
      ) : (
        <div className="service-grid">
          {sorted.map((p) => (
            <PredictionCard key={p.predictionId} prediction={p} />
          ))}
        </div>
      )}
    </section>
  );
}

const title = (p: Prediction) => p.name ?? p.predictionId;

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
          {p.unavailable.info} <span className="hint">od {formatTimeUs(p.unavailable.timestampUs)}</span>
        </p>
      ) : null}

      {latest ? (
        <div className={cx('prediction-body', p.unavailable && 'stale')}>
          {latest.value !== null ? (
            <span className="readout big">
              <span className="readout-value">{formatValue('', latest.value)}</span>
            </span>
          ) : null}
          {latest.instruction !== null ? <p className="prediction-instruction">{latest.instruction}</p> : null}
        </div>
      ) : null}

      {latest && latest.sensorIds.length > 0 ? (
        <div className="chips">
          {latest.sensorIds.map((id) => (
            <span key={id} className="chip" title={sensorSpec(id)?.description}>
              {id}
            </span>
          ))}
        </div>
      ) : null}

      <footer className="prediction-meta" title={`prediction_id ${p.predictionId}`}>
        <span>{formatRefreshRate(p.refreshRate)}</span>
        {latest ? <span>o {formatTimeUs(latest.timestampUs)}</span> : null}
      </footer>
    </Card>
  );
}
