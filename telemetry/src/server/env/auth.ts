import {
  AUTH_DISABLED_NOTICE,
  OAUTH_INCOMPLETE_MESSAGE,
} from "./constants.js";
import type { RawEnv } from "./schema.js";
import type { AuthConfig } from "./types.js";

export type AuthResolution = {
  auth: AuthConfig;
  notice?: string;
};

const stripSlash = (v: string) => v.replace(/\/$/, "");

export function buildAuth(raw: RawEnv): AuthResolution {
  if (raw.TELEMETRY_AUTH_DISABLED) {
    return { auth: { enabled: false } };
  }

  const {
    OAUTH_CLIENT_ID: clientId,
    OAUTH_CLIENT_SECRET: clientSecret,
    OAUTH_ISSUER: issuer,
    TELEMETRY_ROOT_URL: rootUrl,
    SESSION_SECRET: sessionSecret,
  } = raw;

  if (!clientId && !clientSecret && !issuer && !rootUrl) {
    return { auth: { enabled: false }, notice: AUTH_DISABLED_NOTICE };
  }

  if (!clientId || !clientSecret || !issuer || !rootUrl) {
    throw new Error(OAUTH_INCOMPLETE_MESSAGE);
  }

  return {
    auth: {
      enabled: true,
      clientId,
      clientSecret,
      issuer: stripSlash(issuer),
      rootUrl: stripSlash(rootUrl),
      sessionSecret: sessionSecret || clientSecret,
    },
  };
}
