import { useEffect, useState } from 'react';
import type { Alert, Gap, Hello, Point, Prediction, ServerMessage } from '../../shared/protocol';
import { SeriesStore } from '../lib/series-store';
import { wsUrl } from '../lib/ws-client';

export type ConnectionKind = 'connecting' | 'open' | 'closed' | 'overloaded';

export type TelemetryState = {
  connection: ConnectionKind;
  coreConnected: boolean;
  hello: Hello | undefined;
  sensorIds: string[];
  gaps: Gap[];
  predictions: Prediction[];
  alerts: Alert[];
  store: SeriesStore;
};

const MAX_ALERTS = 200;

export function useTelemetrySocket(): TelemetryState {
  const [store] = useState(() => new SeriesStore());
  const [connection, setConnection] = useState<ConnectionKind>('connecting');
  const [coreConnected, setCoreConnected] = useState(false);
  const [hello, setHello] = useState<Hello | undefined>();
  const [sensorIds, setSensorIds] = useState<string[]>([]);
  const [gaps, setGaps] = useState<Gap[]>([]);
  const [predictions, setPredictions] = useState<Prediction[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);

  useEffect(() => {
    let cancelled = false;
    let retryMs = 1000;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let ws: WebSocket | undefined;

    const connect = () => {
      if (cancelled) {
        return;
      }
      setConnection('connecting');
      ws = new WebSocket(wsUrl());

      ws.onopen = () => {
        setConnection('open');
        retryMs = 1000;
        ws?.send(JSON.stringify({ type: 'subscribe', all: true }));
      };

      ws.onclose = () => {
        setConnection('closed');
        if (!cancelled) {
          timer = setTimeout(connect, retryMs);
          retryMs = Math.min(retryMs * 2, 15_000);
        }
      };

      ws.onerror = () => {
        ws?.close();
      };

      ws.onmessage = (ev) => {
        let msg: ServerMessage;
        try {
          msg = JSON.parse(String(ev.data)) as ServerMessage;
        } catch {
          return;
        }
        if (msg.type === 'snapshot') {
          setConnection('open');
          setCoreConnected(msg.connected);
          setHello(msg.hello);
          setSensorIds(msg.sensorIds);
          setGaps(msg.gaps);
          setPredictions(msg.predictions);
          setAlerts(msg.alerts);
          store.replace(msg.pointsBySensor);
          return;
        }
        if (msg.type === 'analysis') {
          if (msg.predictions.length > 0) {
            setPredictions((prev) => mergePredictions(prev, msg.predictions));
          }
          if (msg.alerts.length > 0) {
            setAlerts((prev) => [...prev, ...msg.alerts].slice(-MAX_ALERTS));
          }
          return;
        }
        if (msg.type === 'points') {
          store.append(msg.points);
          setSensorIds((known) => withNewSensors(known, msg.points));
          return;
        }
        if (msg.type === 'gap') {
          setGaps((prev) => [...prev, ...msg.gaps].slice(-100));
          return;
        }
        if (msg.type === 'status') {
          setCoreConnected(msg.connected);
          setHello(msg.hello);
          return;
        }
        if (msg.type === 'overloaded') {
          setConnection('overloaded');
        }
      };
    };

    connect();
    return () => {
      cancelled = true;
      if (timer) {
        clearTimeout(timer);
      }
      ws?.close();
    };
  }, [store]);

  return { connection, coreConnected, hello, sensorIds, gaps, predictions, alerts, store };
}

function mergePredictions(known: Prediction[], changed: Prediction[]): Prediction[] {
  const byId = new Map(known.map((p) => [p.predictionId, p]));
  for (const p of changed) {
    byId.set(p.predictionId, p);
  }
  return [...byId.values()];
}

function withNewSensors(known: string[], points: Point[]): string[] {
  const seen = new Set(known);
  const added = [...new Set(points.map((p) => p.sensorId))].filter((id) => !seen.has(id));
  return added.length === 0 ? known : [...known, ...added];
}
