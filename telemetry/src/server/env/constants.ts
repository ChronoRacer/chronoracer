export const AUTH_DISABLED_NOTICE =
  "telemetry auth disabled (no OAUTH_* / TELEMETRY_ROOT_URL); set TELEMETRY_AUTH_DISABLED=1 to silence\n";

export const OAUTH_INCOMPLETE_MESSAGE =
  "OAuth incomplete: need OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_ISSUER, TELEMETRY_ROOT_URL (or TELEMETRY_AUTH_DISABLED=1)";

export const INVALID_ENV_FALLBACK_MESSAGE = "Invalid environment variables";

export const CORE_TELEMETRY_URL_REQUIRED_MESSAGE =
  "missing required env CORE_TELEMETRY_URL";

export const TRUTHY_ENV_VALUES = new Set(["1", "true", "yes"]);

export const invalidEnvIntMessage = (name: string) => `invalid ${name}`;

export const emptySensorSelectionMessage = (raw: string | undefined) =>
  `TELEMETRY_SENSORS has no sensor ids: "${raw}" (use * for all sensors)`;
