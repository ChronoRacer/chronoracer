import { z } from "zod";
import {
  emptySensorSelectionMessage,
  invalidEnvIntMessage,
  TRUTHY_ENV_VALUES,
} from "./constants.js";
import type { SensorSelection } from "./types.js";

export const positiveInt = (name: string, fallback: number) => {
  const error = invalidEnvIntMessage(name);
  return z.coerce
    .number({ error })
    .int()
    .positive({ error })
    .default(fallback);
};

export const optStr = z.string().trim().optional();

export function parseSensorSelection(raw: string | undefined): SensorSelection {
  const trimmed = raw?.trim();
  if (!trimmed || trimmed === "*") {
    return undefined;
  }
  const sensors = trimmed
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (sensors.length === 0) {
    throw new Error(emptySensorSelectionMessage(raw));
  }
  return sensors;
}

export function isTruthyEnv(value: string | undefined): boolean {
  return TRUTHY_ENV_VALUES.has((value ?? "").trim().toLowerCase());
}
