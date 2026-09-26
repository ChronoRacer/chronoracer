import http from "node:http";

export function startHealthServer(port: number): http.Server {
  const server = http.createServer((req, res) => {
    const path = req.url?.split("?", 1)[0] ?? "";
    if (req.method === "GET" && (path === "/healthz" || path === "/health" || path === "/")) {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("ok\n");
      return;
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found\n");
  });

  server.listen(port, "0.0.0.0", () => {
    process.stdout.write(`telemetry health listening on ${port}\n`);
  });

  return server;
}
