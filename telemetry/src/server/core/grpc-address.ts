export function coreTelemetryGrpcAddress(urlString: string): string {
  const url = new URL(urlString);
  if (url.protocol !== "http:") {
    throw new Error(`CORE_TELEMETRY_URL must use http: (h2c), got ${url.protocol}`);
  }
  return `dns:///${url.hostname}:${url.port || "80"}`;
}
