import { createEnv } from "@t3-oss/env-core";
import { z } from "zod";
import { INVALID_ENV_FALLBACK_MESSAGE } from "./constants.js";
import { toTelemetryEnv } from "./map.js";
import { serverEnvShape } from "./schema.js";
import type { TelemetryEnv } from "./types.js";

export type {
  AuthConfig,
  FanoutConfig,
  RingLimits,
  SensorSelection,
  TelemetryEnv,
} from "./types.js";

export function loadEnv(): TelemetryEnv {
  return createEnv({
    server: serverEnvShape,
    runtimeEnv: { ...process.env },
    emptyStringAsUndefined: true,
    createFinalSchema: (shape) =>
      z.object(shape).transform((raw) => {
        const { env, notice } = toTelemetryEnv(raw);
        if (notice) process.stdout.write(notice);
        return env;
      }),
    onValidationError: (issues) => {
      throw new Error(issues[0]?.message ?? INVALID_ENV_FALLBACK_MESSAGE);
    },
  });
}
