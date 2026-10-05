import { toJson } from "./json.js";

export function log(type: string, fields: Record<string, unknown> = {}): void {
  console.log(toJson({ severity: "INFO", type, ...fields }));
}

export function logError(context: string, err: unknown): void {
  const message = err instanceof Error ? err.message : String(err);
  console.error(toJson({ severity: "ERROR", type: "telemetry_error", context, message }));
}
