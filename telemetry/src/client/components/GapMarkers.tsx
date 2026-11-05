import type { Gap } from '../../shared/protocol';

type Props = {
  gaps: Gap[];
};

function formatGap(gap: Gap): string {
  const t = new Date(gap.atMs).toLocaleTimeString('en-GB');
  if (gap.kind === 'core') {
    return `${t} Core dropped ${gap.droppedPointCount} points from seq ${gap.firstSequence}`;
  }
  if (gap.kind === 'reconnect') {
    return `${t} Telemetry ↔ Core reconnected`;
  }
  return `${t} Core stream restarted (${gap.detail})`;
}

/** Nothing until the first gap; then a chip that opens the recent list. */
export function GapMarkers({ gaps }: Props) {
  if (gaps.length === 0) {
    return null;
  }
  const recent = gaps.slice(-20).reverse();
  return (
    <details className="gaps">
      <summary className="chip tone-warn" title={`Latest: ${formatGap(recent[0])}`}>
        GAPS {gaps.length}
      </summary>
      <ul className="gaps-list">
        {recent.map((g, i) => (
          <li key={`${g.atMs}-${g.kind}-${i}`}>{formatGap(g)}</li>
        ))}
      </ul>
    </details>
  );
}
