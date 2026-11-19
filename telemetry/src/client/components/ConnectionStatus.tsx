import type { Hello } from '../../shared/protocol';
import type { ConnectionKind } from '../hooks/useTelemetrySocket';

type Props = {
  connection: ConnectionKind;
  coreConnected: boolean;
  hello: Hello | undefined;
};

type Status = { label: string; detail: string; dot: 'ok' | 'warn' | 'bad' };

function status({ connection, coreConnected }: Props): Status {
  if (connection === 'closed') {
    return { label: 'OFFLINE', detail: 'WebSocket closed; reconnecting', dot: 'bad' };
  }
  if (connection === 'overloaded') {
    return { label: 'OVERLOADED', detail: 'This browser falls behind; updates are skipped until a resync', dot: 'warn' };
  }
  if (connection === 'open' && coreConnected) {
    return { label: 'LIVE', detail: 'Receiving from Core', dot: 'ok' };
  }
  if (connection === 'open') {
    return { label: 'WAITING FOR CORE', detail: 'Connected to Telemetry; Core watch not connected', dot: 'warn' };
  }
  return { label: 'CONNECTING…', detail: 'Opening WebSocket', dot: 'warn' };
}

export function ConnectionStatus(props: Props) {
  const { label, detail, dot } = status(props);
  const { hello } = props;
  return (
    <div className={`conn conn-${dot}`} title={hello ? `${detail}\nrace ${hello.raceId} · stream ${hello.streamId}` : detail}>
      <span className={`dot ${dot}`} />
      <span className="conn-label">{label}</span>
      {hello ? <span className="conn-race">{hello.raceId}</span> : null}
    </div>
  );
}
