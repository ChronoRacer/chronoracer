import type { ConnectionKind } from "../hooks/useTelemetrySocket";
import type { Hello } from "../../shared/protocol";

type Props = {
  connection: ConnectionKind;
  coreConnected: boolean;
  hello: Hello | undefined;
};

export function ConnectionStatus({ connection, coreConnected, hello }: Props) {
  let label = "Łączenie…";
  let dot: "ok" | "warn" | "bad" | "" = "warn";

  if (connection === "closed") {
    label = "WebSocket rozłączony — ponawianie";
    dot = "bad";
  } else if (connection === "overloaded") {
    label = "Klient przeciążony — podgląd na tej przeglądarce gubi dane";
    dot = "warn";
  } else if (connection === "open" && coreConnected) {
    label = "Na żywo";
    dot = "ok";
  } else if (connection === "open") {
    label = "UI połączone — oczekiwanie na Core Watch";
    dot = "warn";
  }

  return (
    <div className="status" title={hello ? `stream ${hello.streamId}` : undefined}>
      <span className={`dot ${dot}`} />
      <span>{label}</span>
      {hello ? (
        <span>
          · wyścig {hello.raceId} · stream {hello.streamId.slice(0, 8)}…
        </span>
      ) : null}
    </div>
  );
}
