/**
 * Normalizes a URL string into a native gRPC target address (`dns:///hostname:port`).
 *
 * Enforces unencrypted HTTP/2 (`h2c`) transport via `http:` protocol and applies
 * default port 80 if unspecified.
 *
 * @throws {Error} If protocol is not `http:`.
 */

export function coreTelemetryGrpcAddress(urlString: string): string {
  const url = new URL(urlString);
  if (url.protocol !== 'http:') {
    throw new Error(`CORE_TELEMETRY_URL must use http: (h2c), got ${url.protocol}`);
  }
  return `dns:///${url.hostname}:${url.port || '80'}`;
}
