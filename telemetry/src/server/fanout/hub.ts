/**
 * @fileoverview WebSocket Fanout & Broadcast Hub
 *
 * Manages active client WebSocket connections, handles sensor subscription messaging,
 * and streams telemetry deltas/snapshots in periodic tick intervals.
 *
 * Performance Features:
 * - Backpressure handling: Sends `overloaded` frame and drops deltas for lagging clients.
 * - Frame memoization: Encodes JSON payloads once per unique sensor subscription key per tick.
 */

import WebSocket from 'ws';
import type { AnalysisStore } from '../analysis/index.js';
import type { DriverMessages } from '../driver/index.js';
import type { FanoutConfig } from '../env/index.js';
import { log } from '../log.js';
import type { PreviewStore, StoreStatus } from '../store/index.js';
import { encode, parseClientMessage } from './ws-protocol.js';

const STATS_INTERVAL_MS = 20_000;

type Client = {
  socket: WebSocket;
  sensors: ReadonlySet<string> | undefined;
  key: string;
  needsSnapshot: boolean;
};

export class FanoutHub {
  private readonly clients = new Set<Client>();
  private sentStatus: StoreStatus | undefined;
  private timers: NodeJS.Timeout[] = [];

  constructor(
    private readonly store: PreviewStore,
    private readonly analysis: AnalysisStore,
    private readonly driver: DriverMessages,
    private readonly config: FanoutConfig,
  ) {}

  start(): void {
    const logStats = () => log('telemetry_stats', { clients: this.clients.size, ...this.store.stats() });
    this.timers = [setInterval(() => this.flush(), this.config.tickMs), setInterval(logStats, STATS_INTERVAL_MS)];
  }

  stop(): void {
    this.timers.forEach(clearInterval);
    for (const { socket } of this.clients) {
      socket.terminate();
    }
  }

  addClient(socket: WebSocket): void {
    const client: Client = { socket, sensors: undefined, key: '*', needsSnapshot: true };
    this.clients.add(client);
    socket.on('close', () => this.clients.delete(client));
    socket.on('message', (data) => {
      const message = parseClientMessage(data.toString());
      if (message === undefined) {
        return;
      }
      const sensors = 'sensors' in message ? new Set(message.sensors) : undefined;
      const key = sensors ? [...sensors].sort().join(',') : '*';
      // The same selection is already covered by the snapshot sent or queued for it.
      if (key === client.key) {
        return;
      }
      client.sensors = sensors;
      client.key = key;
      client.needsSnapshot = true;
    });
  }

  /**
   * Main broadcast loop executed on every tick:
   * 1. Drains new deltas from PreviewStore and AnalysisStore.
   * 2. Handles backpressure (notifies slow sockets and flags them for full snapshot upon recovery).
   * 3. Broadcasts updates using a cache map (`frames`) to prevent duplicate JSON serialization.
   */
  private flush(): void {
    const delta = this.store.drain();
    const analysis = this.analysis.drain();
    const driver = this.driver.drain();
    const status = this.store.status();
    const statusChanged = status.connected !== this.sentStatus?.connected || status.hello !== this.sentStatus?.hello;
    this.sentStatus = status;
    if (this.clients.size === 0) {
      return;
    }

    const frames = new Map<string, string | undefined>();
    const frame = (key: string, encodeFrame: () => string | undefined) => {
      if (!frames.has(key)) {
        frames.set(key, encodeFrame());
      }
      return frames.get(key);
    };

    for (const client of this.clients) {
      const { socket, sensors, key } = client;
      const send = (data: string | undefined) => {
        if (data !== undefined) {
          socket.send(data);
        }
      };
      if (socket.readyState !== WebSocket.OPEN) {
        continue;
      }

      if (socket.bufferedAmount > this.config.maxBufferedBytes) {
        if (!client.needsSnapshot) {
          client.needsSnapshot = true;
          send(encode({ type: 'overloaded', atMs: Date.now() }));
        }
        continue;
      }

      if (client.needsSnapshot) {
        client.needsSnapshot = false;
        send(
          frame(`snapshot:${key}`, () =>
            encode({
              type: 'snapshot',
              tickMs: this.config.tickMs,
              ...this.store.snapshot(sensors),
              ...this.analysis.snapshot(),
              ...this.driver.snapshot(),
            }),
          ),
        );
        continue;
      }

      if (statusChanged) {
        send(frame('status', () => encode({ type: 'status', ...status })));
      }
      send(
        frame(`buckets:${key}`, () => {
          const buckets = only(delta.buckets, sensors);
          const latest = only(delta.latest, sensors);
          const changed = Object.keys(buckets).length > 0 || Object.keys(latest).length > 0;
          return changed ? encode({ type: 'buckets', buckets, latest }) : undefined;
        }),
      );
      if (delta.gaps.length > 0) {
        send(frame('gaps', () => encode({ type: 'gap', gaps: delta.gaps })));
      }
      if (analysis.predictions.length > 0 || analysis.alerts.length > 0) {
        send(frame('analysis', () => encode({ type: 'analysis', ...analysis })));
      }
      if (driver.messages.length > 0 || driver.active !== undefined) {
        send(frame('driver', () => encode({ type: 'driver', ...driver })));
      }
    }
  }
}

function only<T>(bySensor: Record<string, T>, sensors: ReadonlySet<string> | undefined): Record<string, T> {
  if (sensors === undefined) {
    return bySensor;
  }
  return Object.fromEntries(Object.entries(bySensor).filter(([sensorId]) => sensors.has(sensorId)));
}
