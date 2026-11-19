import { EventEmitter } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type WebSocket from "ws";
import type { ServerMessage } from "../../shared/protocol.js";
import { AnalysisStore } from "../analysis/index.js";
import { DriverMessages } from "../driver/index.js";
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
// Each point lands in its own bucket.
const point = (sensorId: string): Point => ({ sensorId, timestampNanos: BigInt(++seq) * 10_000_000n, value: seq, driverId: "d" });
const changedSensors = (messages: ServerMessage[]) =>
  messages.map((m) => (m.type === "buckets" ? [Object.keys(m.buckets), Object.keys(m.latest)] : m.type));
const hello = { protocolVersion: 1, raceId: "race", streamId: "A", nextSequence: 1n, selectedSensorCount: 0 };

describe("FanoutHub", () => {
  let store: PreviewStore;
  let analysis: AnalysisStore;
  let driver: DriverMessages;
  let hub: FanoutHub;
  const tick = () => vi.advanceTimersByTime(TICK_MS);
  const connect = () => {
    const socket = new FakeSocket();
    hub.addClient(socket as unknown as WebSocket);
    return socket;
  };

  beforeEach(() => {
    vi.useFakeTimers();
    store = new PreviewStore({ bucketMs: 10, maxAgeMs: 60_000, maxTotalBuckets: 1_000 });
    analysis = new AnalysisStore();
    driver = new DriverMessages(true);
    hub = new FanoutHub(store, analysis, driver, { tickMs: TICK_MS, maxBufferedBytes: MAX_BUFFERED });
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
    expect(filtered.type === "snapshot" && Object.keys(filtered.buckets)).toEqual(["a"]);

    tick();
    expect(all.take()).toEqual([]);

    store.ingest([point("a"), point("b")]);
    tick();
    expect(changedSensors(all.take())).toEqual([[["a", "b"], ["a", "b"]]]);
    expect(changedSensors(onlyA.take())).toEqual([[["a"], ["a"]]]);
  });

  it("sends the tick in the snapshot and ignores a subscribe that repeats the selection", () => {
    const socket = connect();
    tick();
    expect(socket.take()).toMatchObject([{ type: "snapshot", tickMs: TICK_MS }]);

    socket.subscribe({ type: "subscribe", all: true });
    store.ingest([point("a")]);
    tick();
    expect(socket.take().map((m) => m.type)).toEqual(["buckets"]);
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
    const messages = socket.take();
    expect(messages.map((m) => m.type === "snapshot" && m.buckets.a.length)).toEqual([6]);
  });

  it("includes analysis state in the snapshot and pushes changes to every subscription", () => {
    const ts = 1_767_225_600_000_001;
    analysis.running({ timestamp: ts, prediction_id: "p1", name: "Tyre wear", refresh_rate: "every 5 laps" });
    const onlyA = connect();
    onlyA.subscribe({ type: "subscribe", sensors: ["a"] });
    tick();
    expect(onlyA.take()).toMatchObject([
      { type: "snapshot", predictions: [{ predictionId: "p1", name: "Tyre wear" }], alerts: [] },
    ]);

    analysis.prediction({ timestamp: ts + 1, prediction_id: "p1", sensors_id: ["a"], instruction: null, value: 0.4 });
    analysis.alert({
      timestamp: ts + 2,
      alert_id: "hot",
      sensors: [{ sensor_id: "a", value: 1 }],
      analysis_message: "m",
      analysis_reason: "r",
    });
    tick();
    expect(onlyA.take()).toMatchObject([
      { type: "analysis", predictions: [{ predictionId: "p1", latest: { value: 0.4 } }], alerts: [{ alertId: "hot" }] },
    ]);
    tick();
    expect(onlyA.take()).toEqual([]);
  });

  it("includes driver messages in the snapshot and pushes status and active set changes to every browser", () => {
    const sent = driver.add({ action: "show", type: "BOX", text: "BOX BOX", durationS: -1 }, "op");
    const all = connect();
    const onlyA = connect();
    onlyA.subscribe({ type: "subscribe", sensors: ["a"] });
    tick();
    expect(all.take()).toMatchObject([
      { type: "snapshot", driverEnabled: true, driverMessages: [{ id: sent.id }], driverActive: [] },
    ]);
    onlyA.take();

    driver.settle(sent);
    tick();
    const update = [
      {
        type: "driver",
        messages: [{ id: sent.id, status: "delivered" }],
        active: [{ type: "BOX", text: "BOX BOX", expiresAtMs: null }],
      },
    ];
    expect(all.take()).toMatchObject(update);
    expect(onlyA.take()).toMatchObject(update);
    tick();
    expect(all.take()).toEqual([]);
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
