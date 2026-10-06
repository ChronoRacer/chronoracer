// This file contains the FanoutHub class, which is responsible for managing the WebSocket connections and the data flow.

import WebSocket from 'ws';
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
      client.sensors = 'sensors' in message ? new Set(message.sensors) : undefined;
      client.key = client.sensors ? [...client.sensors].sort().join(',') : '*';
      client.needsSnapshot = true;
    });
  }

  private flush(): void {
    const delta = this.store.drain();
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
        send(frame(`snapshot:${key}`, () => encode({ type: 'snapshot', ...this.store.snapshot(sensors) })));
        continue;
      }

      if (statusChanged) {
        send(frame('status', () => encode({ type: 'status', ...status })));
      }
      send(
        frame(`points:${key}`, () => {
          const points = sensors ? delta.points.filter((p) => sensors.has(p.sensorId)) : delta.points;
          return points.length > 0 ? encode({ type: 'points', points }) : undefined;
        }),
      );
      if (delta.gaps.length > 0) {
        send(frame('gaps', () => encode({ type: 'gap', gaps: delta.gaps })));
      }
    }
  }
}
