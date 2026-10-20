import { EventEmitter } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type WebSocket from "ws";
import type { ServerMessage } from "../../shared/protocol.js";
import type { Point } from "../core/types.js";
import { PreviewStore } from "../store/index.js";
import { FanoutHub } from "./hub.js";

const TICK_MS = 100;
const MAX_BUFFERED = 1_000;

class FakeSocket extends EventEmitter {
  readyState = 1;
  bufferedAmount = 0;
  private sent: ServerMessage[] = [];

  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  terminate() {
    this.readyState = 3;
    this.emit("close");
  }
  subscribe(message: object) {
    this.emit("message", Buffer.from(JSON.stringify(message)));
  }
  take(): ServerMessage[] {
    const sent = this.sent;
    this.sent = [];
    return sent;
  }
}

let seq = 0;
const point = (sensorId: string): Point => ({ sensorId, timestampNanos: BigInt(++seq), value: seq, driverId: "d" });
const hello = { protocolVersion: 1, raceId: "race", streamId: "A", nextSequence: 1n, selectedSensorCount: 0 };

describe("FanoutHub", () => {
  let store: PreviewStore;
  let hub: FanoutHub;
  const tick = () => vi.advanceTimersByTime(TICK_MS);
  const connect = () => {
    const socket = new FakeSocket();
    hub.addClient(socket as unknown as WebSocket);
    return socket;
  };

  beforeEach(() => {
    vi.useFakeTimers();
    store = new PreviewStore({ maxPointsPerSensor: 100, maxAgeMs: 60_000, maxSensors: 10, maxTotalPoints: 1_000 });
    hub = new FanoutHub(store, { tickMs: TICK_MS, maxBufferedBytes: MAX_BUFFERED });
    hub.start();
    store.connect(hello);
  });

  it("answers a subscribe with one snapshot, then deltas without repeating it", () => {
    const all = connect();
    const onlyA = connect();
    onlyA.subscribe({ type: "subscribe", sensors: ["a"] });
    store.ingest([point("a"), point("b")]);
    tick();

    const [snapshot] = all.take();
    expect(snapshot).toMatchObject({ type: "snapshot", connected: true, hello: { nextSequence: "1" } });
    const [filtered] = onlyA.take();
    expect(filtered.type === "snapshot" && Object.keys(filtered.pointsBySensor)).toEqual(["a"]);

    tick();
    expect(all.take()).toEqual([]);

    store.ingest([point("a"), point("b")]);
    tick();
    expect(all.take()).toMatchObject([{ type: "points", points: [{ sensorId: "a" }, { sensorId: "b" }] }]);
    expect(onlyA.take()).toMatchObject([{ type: "points", points: [{ sensorId: "a" }] }]);
  });

  it("pushes Core status changes and gaps live", () => {
    const socket = connect();
    tick();
    socket.take();
    store.disconnect();
    tick();
    expect(socket.take()).toMatchObject([
      { type: "status", connected: false },
      { type: "gap", gaps: [{ kind: "reconnect" }] },
    ]);
  });

  it("skips updates for an overloaded browser and resyncs it with a snapshot", () => {
    const socket = connect();
    tick();
    socket.take();

    socket.bufferedAmount = MAX_BUFFERED + 1;
    store.ingest([point("a")]);
    tick();
    store.ingest([point("a")]);
    tick();
    expect(socket.take().map((m) => m.type)).toEqual(["overloaded"]);

    socket.bufferedAmount = 0;
    tick();
    expect(socket.take()).toMatchObject([{ type: "snapshot", pointsBySensor: { a: [{}, {}] } }]);
  });

  it("ignores invalid client messages", () => {
    const socket = connect();
    tick();
    socket.take();
    socket.subscribe({ type: "subscribe", sensors: [] });
    socket.emit("message", Buffer.from("not json"));
    tick();
    expect(socket.take()).toEqual([]);
  });

  it("closes browsers on stop", () => {
    const socket = connect();
    hub.stop();
    expect(socket.readyState).toBe(3);
  });
});
