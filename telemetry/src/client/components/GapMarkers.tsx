import type { Gap } from "../../shared/protocol";

type Props = {
  gaps: Gap[];
};

function formatGap(gap: Gap): string {
  const t = new Date(gap.atMs).toLocaleTimeString("pl-PL");
  if (gap.kind === "core") {
    return `${t} Luka Core seq ${gap.firstSequence}, utracono ${gap.droppedPointCount}`;
  }
  if (gap.kind === "reconnect") {
    return `${t} Ponowne połączenie Telemetry↔Core`;
  }
  return `${t} Restart strumienia Core (${gap.detail})`;
}

export function GapMarkers({ gaps }: Props) {
  const recent = gaps.slice(-20).reverse();
  if (recent.length === 0) {
    return <div className="gaps-strip empty">Luki i granice: brak zgłoszeń</div>;
  }
  return (
    <details className="gaps-strip">
      <summary>
        <span className="chip tone-warn">{gaps.length}</span>
        Luki i granice · ostatnia: {formatGap(recent[0])}
      </summary>
      <ul className="gaps">
        {recent.map((g, i) => (
          <li key={`${g.atMs}-${g.kind}-${i}`}>{formatGap(g)}</li>
        ))}
      </ul>
    </details>
  );
}
