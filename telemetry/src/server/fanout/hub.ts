import WebSocket from "ws";
import type { FanoutConfig } from "../env/index.js";
import type { GapEvent, PreviewPoint, StreamHello } from "../core/types.js";
import type { PreviewStore } from "../store/index.js";
import { buildSnapshotPayload } from "../store/index.js";
import {
  encode,
  encodeGaps,
  encodePoints,
  parseClientMessage,
  subscriptionKey,
} from "./ws-protocol.js";

type Client = {
  socket: WebSocket;
  sensors: Set<string> | undefined;
  overloaded: boolean;
};

export class FanoutHub {
  private readonly clients = new Set<Client>();
  private pendingPoints: PreviewPoint[] = [];
  private pendingGaps: GapEvent[] = [];
  private timer: ReturnType<typeof setInterval> | undefined;
  private statsTimer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly store: PreviewStore,
    private readonly config: FanoutConfig,
  ) {}

  start(): void {
    if (this.timer) {
      return;
    }
    this.timer = setInterval(() => this.flush(), this.config.tickMs);
    this.statsTimer = setInterval(() => this.logStats(), 20_000);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    if (this.statsTimer) {
      clearInterval(this.statsTimer);
      this.statsTimer = undefined;
    }
    this.flush();
  }

  addClient(socket: WebSocket): void {
    const client: Client = { socket, sensors: undefined, overloaded: false };
    this.clients.add(client);

    socket.send(JSON.stringify(buildSnapshotPayload(this.store, undefined)));
    socket.send(
      encode({ type: "status", connected: true, clients: this.clients.size }),
    );

    socket.on("message", (data) => {
      const text = typeof data === "string" ? data : data.toString("utf8");
      const msg = parseClientMessage(text);
      if (msg === undefined) {
        return;
      }
      if (msg.type === "ping") {
        socket.send(encode({ type: "status", pong: true, atMs: Date.now() }));
        return;
      }
      if (msg.type === "subscribe") {
        client.sensors = "all" in msg ? undefined : new Set(msg.sensors);
        const filter = client.sensors ? [...client.sensors] : undefined;
        socket.send(JSON.stringify(buildSnapshotPayload(this.store, filter)));
      }
    });

    const cleanup = () => {
      this.clients.delete(client);
    };
    socket.on("close", cleanup);
    socket.on("error", cleanup);
  }

  onHello(hello: StreamHello): void {
    this.broadcastRaw(encode({ type: "hello", hello }));
  }

  onPoints(points: PreviewPoint[]): void {
    if (points.length === 0) {
      return;
    }
    this.pendingPoints.push(...points);
  }

  onGaps(gaps: GapEvent[]): void {
    if (gaps.length === 0) {
      return;
    }
    this.pendingGaps.push(...gaps);
  }

  private flush(): void {
    const points = this.pendingPoints;
    const gaps = this.pendingGaps;
    this.pendingPoints = [];
    this.pendingGaps = [];

    if (points.length === 0 && gaps.length === 0) {
      return;
    }

    const gapsFrame = gaps.length > 0 ? encodeGaps(gaps) : undefined;
    const allPointsFrame = points.length > 0 ? encodePoints(points) : undefined;
    const pointsFrameCache = new Map<string, string | undefined>();

    for (const client of this.clients) {
      if (client.socket.readyState !== WebSocket.OPEN) {
        continue;
      }

      const frames: string[] = [];
      if (client.sensors === undefined) {
        if (allPointsFrame) {
          frames.push(allPointsFrame);
        }
      } else if (points.length > 0) {
        const key = subscriptionKey(client.sensors);
        let frame = pointsFrameCache.get(key);
        if (frame === undefined && !pointsFrameCache.has(key)) {
          const filtered = points.filter((p) =>
            client.sensors!.has(p.sensorId),
          );
          frame = filtered.length > 0 ? encodePoints(filtered) : undefined;
          pointsFrameCache.set(key, frame);
        }
        if (frame) {
          frames.push(frame);
        }
      }

      if (gapsFrame) {
        frames.push(gapsFrame);
      }
      if (frames.length === 0) {
        continue;
      }
      this.sendFrames(client, frames);
    }
  }

  private sendFrames(client: Client, frames: string[]): void {
    const { socket } = client;
    if (socket.bufferedAmount > this.config.maxBufferedBytes) {
      if (!client.overloaded) {
        client.overloaded = true;
        try {
          socket.send(encode({ type: "overloaded", atMs: Date.now() }));
        } catch {}
      }
      return;
    }
    client.overloaded = false;
    for (const frame of frames) {
      try {
        socket.send(frame);
      } catch {}
    }
  }

  private broadcastRaw(frame: string): void {
    for (const client of this.clients) {
      if (client.socket.readyState !== WebSocket.OPEN) {
        continue;
      }
      this.sendFrames(client, [frame]);
    }
  }

  private logStats(): void {
    const stats = this.store.stats();
    console.log(
      JSON.stringify({
        type: "telemetry_stats",
        windowSeconds: 20,
        clients: this.clients.size,
        ...stats,
      }),
    );
  }
}
