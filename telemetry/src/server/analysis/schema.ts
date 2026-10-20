/**
 * @fileoverview Network Ingestion Schemas for External Analytics Services
 *
 * Defines strict Zod validation schemas for incoming analytics payloads (in `snake_case` JSON).
 * Used by external ML models or analysis pipelines pushing predictions and alerts over HTTP.
 */

import { z } from 'zod';

export const MIN_TIMESTAMP_US = 1_767_225_600_000_000;

const timestampUs = z.number().int().min(MIN_TIMESTAMP_US);
const id = z.string().min(1);
const nonEmpty = z.string().min(1);
const shortText = nonEmpty.refine((s) => [...s].length <= 255, 'at most 255 characters');

export const predictionRunning = z.object({
  timestamp: timestampUs,
  prediction_id: id,
  name: nonEmpty,
  refresh_rate: z.number().int().min(1).nullable(),
});

export const predictionUnavailable = z.object({
  timestamp: timestampUs,
  prediction_id: id,
  info: nonEmpty,
});

export const newPrediction = z.object({
  timestamp: timestampUs,
  prediction_id: id,
  sensors_id: z.array(id),
  instruction: z.string().nullable(),
  value: z.number().nullable(),
});

export const newAlert = z.object({
  timestamp: timestampUs,
  alert_id: id,
  sensors: z.array(z.object({ sensor_id: id, value: z.number() })).min(1),
  analysis_message: shortText,
  analysis_reason: shortText,
});

export type PredictionRunningBody = z.infer<typeof predictionRunning>;
export type PredictionUnavailableBody = z.infer<typeof predictionUnavailable>;
export type NewPredictionBody = z.infer<typeof newPrediction>;
export type NewAlertBody = z.infer<typeof newAlert>;
