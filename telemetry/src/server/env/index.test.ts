import { afterEach, describe, expect, it, vi } from "vitest";
import { loadEnv } from "./index.js";

const withEnv = (env: Record<string, string>) => {
  vi.stubEnv("CORE_TELEMETRY_URL", "http://core:50051");
  for (const [name, value] of Object.entries(env)) {
    vi.stubEnv(name, value);
  }
  return loadEnv();
};

describe("loadEnv", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("requires OAuth unless auth is explicitly disabled", () => {
    expect(() => withEnv({})).toThrow(/OAuth config incomplete/);
    expect(withEnv({ TELEMETRY_AUTH_DISABLED: "1" }).auth).toEqual({ enabled: false });
  });

  it("normalizes the root URL and falls back to the client secret for sessions", () => {
    const { auth } = withEnv({
      OAUTH_CLIENT_ID: "id",
      OAUTH_CLIENT_SECRET: "secret",
      OAUTH_ISSUER: "https://tenant.auth0.com/",
      TELEMETRY_ROOT_URL: "https://telemetry.test/",
    });
    expect(auth).toMatchObject({ enabled: true, rootUrl: "https://telemetry.test", sessionSecret: "secret" });
  });

  it("parses the sensor selection with Core's limits", () => {
    expect(withEnv({ TELEMETRY_AUTH_DISABLED: "1", TELEMETRY_SENSORS: "*" }).sensors).toBeUndefined();
    expect(withEnv({ TELEMETRY_AUTH_DISABLED: "1", TELEMETRY_SENSORS: "a, b,a" }).sensors).toEqual(["a", "b"]);
    expect(() => withEnv({ TELEMETRY_AUTH_DISABLED: "1", TELEMETRY_SENSORS: " , " })).toThrow(/TELEMETRY_SENSORS/);
    expect(() => withEnv({ TELEMETRY_AUTH_DISABLED: "1", TELEMETRY_SENSORS: "x".repeat(25) })).toThrow(/24 bytes/);
  });

  it("names the invalid variable", () => {
    expect(() => withEnv({ TELEMETRY_AUTH_DISABLED: "1", PORT: "abc" })).toThrow(/PORT/);
    expect(() => withEnv({ TELEMETRY_AUTH_DISABLED: "maybe" })).toThrow(/TELEMETRY_AUTH_DISABLED/);
  });
});
