function grpcAuthority(hostname: string, port: string): string {
  if (hostname.includes(":")) {
    return `[${hostname}]:${port}`;
  }
  return `${hostname}:${port}`;
}

export function coreTelemetryGrpcAddress(urlString: string): string {
  const url = new URL(urlString);
  if (url.protocol !== "http:") {
    throw new Error(
      `CORE_TELEMETRY_URL must use http: (h2c), got ${url.protocol}`,
    );
  }
  const port = url.port || "80";
  return `dns:///${grpcAuthority(url.hostname, port)}`;
}
