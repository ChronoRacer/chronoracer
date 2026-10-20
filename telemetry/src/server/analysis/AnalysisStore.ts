/**
 * @fileoverview In-Memory Store for Analytics Predictions and Alerts
 *
 * Maintains state for active prediction models and recent alerts in memory.
 * Accumulates dirty states and new alerts between broadcast ticks for downstream WebSocket fanout.
 */

import Denque from 'denque';
import type { Alert, AnalysisSnapshot, Prediction } from '../../shared/protocol.js';
import type { NewAlertBody, NewPredictionBody, PredictionRunningBody, PredictionUnavailableBody } from './schema.js';

export const MAX_PREDICTIONS = 200;
export const MAX_ALERTS = 200;

export type AnalysisLimits = { maxPredictions: number; maxAlerts: number };

export class AnalysisStore {
  private readonly predictions = new Map<string, Prediction>();
  private readonly alerts = new Denque<Alert>();
  private changed = new Set<string>();
  private newAlerts: Alert[] = [];

  constructor(private readonly limits: AnalysisLimits = { maxPredictions: MAX_PREDICTIONS, maxAlerts: MAX_ALERTS }) {}

  /** Updates or registers a running prediction model. Returns `false` if capacity limit is reached. */
  running(body: PredictionRunningBody): boolean {
    return this.update(body.prediction_id, (p) => {
      p.name = body.name;
      p.refreshRate = body.refresh_rate;
      p.announcedAtUs = body.timestamp;
      clearUnavailableUpTo(p, body.timestamp);
    });
  }

  /** Marks a prediction model as temporarily unavailable or suspended. */
  unavailable(body: PredictionUnavailableBody): boolean {
    return this.update(body.prediction_id, (p) => {
      p.unavailable = { timestampUs: body.timestamp, info: body.info };
    });
  }

  /** Ingests a new predicted value payload and clears outdated unavailable status if present. */
  prediction(body: NewPredictionBody): boolean {
    return this.update(body.prediction_id, (p) => {
      p.latest = {
        timestampUs: body.timestamp,
        sensorIds: body.sensors_id,
        instruction: body.instruction,
        value: body.value,
      };
      clearUnavailableUpTo(p, body.timestamp);
    });
  }

  /** Appends a new alert to the ring buffer and tracks it for the next delta broadcast. */
  alert(body: NewAlertBody): void {
    const alert: Alert = {
      timestampUs: body.timestamp,
      alertId: body.alert_id,
      sensors: body.sensors.map(({ sensor_id, value }) => ({ sensorId: sensor_id, value })),
      message: body.analysis_message,
      reason: body.analysis_reason,
      receivedAtMs: Date.now(),
    };
    this.alerts.push(alert);
    if (this.alerts.length > this.limits.maxAlerts) {
      this.alerts.shift();
    }
    this.newAlerts.push(alert);
  }

  /** Generates a full snapshot of all current predictions and historical alerts in store. */
  snapshot(): AnalysisSnapshot {
    return { predictions: [...this.predictions.values()], alerts: this.alerts.toArray() };
  }

  /**
   * Flushes and returns only modified predictions and new alerts since the last call.
   * Used by `FanoutHub` for efficient incremental WebSocket updates.
   */
  drain(): AnalysisSnapshot {
    const predictions = [...this.changed].flatMap((id) => this.predictions.get(id) ?? []);
    const delta = { predictions, alerts: this.newAlerts.slice(-this.limits.maxAlerts) };
    this.changed = new Set();
    this.newAlerts = [];
    return delta;
  }

  /** Helper method that mutates state and enforces maximum distinct prediction model limits. */
  private update(predictionId: string, apply: (p: Prediction) => void): boolean {
    let prediction = this.predictions.get(predictionId);
    if (!prediction) {
      if (this.predictions.size >= this.limits.maxPredictions) {
        return false;
      }
      prediction = { predictionId, receivedAtMs: 0 };
      this.predictions.set(predictionId, prediction);
    }
    apply(prediction);
    prediction.receivedAtMs = Date.now();
    this.changed.add(predictionId);
    return true;
  }
}

function clearUnavailableUpTo(p: Prediction, timestampUs: number): void {
  if (p.unavailable && p.unavailable.timestampUs <= timestampUs) {
    p.unavailable = undefined;
  }
}
