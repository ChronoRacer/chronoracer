const FLUSH_INTERVAL_MS = 20_000;

export type StatsSnapshot = {
  windowSeconds: number;
  pointsTotal: number;
  batches: number;
  gaps: number;
  hellos: number;
  errors: number;
  pointsBySensor: Record<string, number>;
};

export class StreamStats {
  private pointsBySensor = new Map<string, number>();
  private pointsTotal = 0;
  private batches = 0;
  private gaps = 0;
  private hellos = 0;
  private errors = 0;
  private timer: ReturnType<typeof setInterval> | undefined;

  start(): void {
    if (this.timer !== undefined) {
      return;
    }
    this.timer = setInterval(() => this.flush(), FLUSH_INTERVAL_MS);
  }

  stop(): void {
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  recordHello(): void {
    this.hellos += 1;
  }

  recordBatch(points: { sensorId: string }[]): void {
    this.batches += 1;
    for (const point of points) {
      this.pointsTotal += 1;
      const prev = this.pointsBySensor.get(point.sensorId) ?? 0;
      this.pointsBySensor.set(point.sensorId, prev + 1);
    }
  }

  recordGap(): void {
    this.gaps += 1;
  }

  recordError(): void {
    this.errors += 1;
  }

  flush(): void {
    const pointsBySensor: Record<string, number> = {};
    for (const [sensorId, count] of this.pointsBySensor) {
      pointsBySensor[sensorId] = count;
    }

    const snapshot: StatsSnapshot = {
      windowSeconds: FLUSH_INTERVAL_MS / 1000,
      pointsTotal: this.pointsTotal,
      batches: this.batches,
      gaps: this.gaps,
      hellos: this.hellos,
      errors: this.errors,
      pointsBySensor,
    };

    console.log(JSON.stringify({ type: "telemetry_stats", ...snapshot }));

    this.pointsBySensor.clear();
    this.pointsTotal = 0;
    this.batches = 0;
    this.gaps = 0;
    this.hellos = 0;
    this.errors = 0;
  }
}
