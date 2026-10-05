import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getRequestListener } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { createNodeWebSocket } from "@hono/node-ws";
import { Hono } from "hono";
import type WebSocket from "ws";
import type { AuthConfig } from "./env/index.js";
import type { FanoutHub } from "./fanout/hub.js";
import type { PreviewStore } from "./store/index.js";
import { requireSession } from "./auth/guard.js";
import {
  beginLoginCookies,
  exchangeCode,
  finishLoginCookies,
  loginRedirectUrl,
  logoutCookies,
  logoutRedirectUrl,
  oauthEnabled,
  readPkceCookie,
  readStateCookie,
  sessionFromRequest,
} from "./auth/oauth.js";

export type HttpDeps = {
  port: number;
  auth: AuthConfig;
  clientDir: string;
  store: PreviewStore;
  hub: FanoutHub;
};

function isSecureRequest(
  rootUrl: string | undefined,
  protoHeader: string | undefined,
): boolean {
  if (rootUrl?.startsWith("https:")) {
    return true;
  }
  return protoHeader === "https";
}

export function resolveClientDir(configured: string): string {
  if (path.isAbsolute(configured)) {
    return configured;
  }
  const here = path.dirname(fileURLToPath(import.meta.url));
  return path.resolve(here, "..", "client");
}

export function startHttpServer(
  deps: HttpDeps,
): ReturnType<typeof createServer> {
  const clientRoot = resolveClientDir(deps.clientDir);
  const app = new Hono();
  const { upgradeWebSocket, injectWebSocket } = createNodeWebSocket({ app });

  app.get("/healthz", (c) => c.text("ok\n"));
  app.get("/health", (c) => c.text("ok\n"));

  app.get("/api/me", async (c) => {
    const session = await sessionFromRequest(deps.auth, c.req.header("cookie"));
    if (!session) {
      return c.json({ authenticated: false }, 401);
    }
    return c.json({
      authenticated: true,
      sub: session.sub,
      email: session.email,
    });
  });

  app.get("/api/sensors", requireSession(deps.auth), (c) => {
    return c.json({
      sensorIds: deps.store.sensorIds(),
      stats: deps.store.stats(),
    });
  });

  app.get(
    "/ws",
    async (c, next) => {
      if (!(await sessionFromRequest(deps.auth, c.req.header("cookie")))) {
        return c.text("unauthorized", 401);
      }
      await next();
    },
    upgradeWebSocket(() => ({
      onOpen(_event, ws) {
        deps.hub.addClient(ws.raw as WebSocket);
      },
    })),
  );

  if (oauthEnabled(deps.auth)) {
    const auth = deps.auth;

    app.get("/auth/login", async (c) => {
      const secure = isSecureRequest(
        auth.rootUrl,
        c.req.header("x-forwarded-proto"),
      );
      const { state, codeVerifier, headers } = await beginLoginCookies(secure);
      for (const h of headers) {
        c.header("set-cookie", h, { append: true });
      }
      const target = await loginRedirectUrl(auth, state, codeVerifier);
      return c.redirect(target, 302);
    });

    app.get("/auth/callback", async (c) => {
      const code = c.req.query("code");
      const state = c.req.query("state");
      const expected = readStateCookie(c.req.header("cookie"));
      const codeVerifier = readPkceCookie(c.req.header("cookie"));
      if (!code || !state || !expected || state !== expected || !codeVerifier) {
        return c.text("invalid oauth state", 400);
      }
      try {
        const identity = await exchangeCode(
          auth,
          new URL(c.req.url),
          state,
          codeVerifier,
        );
        const secure = isSecureRequest(
          auth.rootUrl,
          c.req.header("x-forwarded-proto"),
        );
        for (const h of await finishLoginCookies(auth, identity, secure)) {
          c.header("set-cookie", h, { append: true });
        }
        return c.redirect("/", 302);
      } catch (err) {
        console.error(
          JSON.stringify({
            type: "telemetry_error",
            context: "oauth_callback",
            message: String(err),
          }),
        );
        return c.text("oauth failed", 500);
      }
    });

    app.get("/auth/logout", async (c) => {
      const secure = isSecureRequest(
        auth.rootUrl,
        c.req.header("x-forwarded-proto"),
      );
      for (const h of logoutCookies(secure)) {
        c.header("set-cookie", h, { append: true });
      }
      const target = await logoutRedirectUrl(auth);
      return c.redirect(target, 302);
    });
  } else {
    app.get("/auth/login", (c) => c.redirect("/", 302));
    app.get("/auth/logout", (c) => c.redirect("/", 302));
  }

  app.use("/*", async (c, next) => {
    const pathName = new URL(c.req.url).pathname;

    const isPublicRoute =
      pathName.startsWith("/health") ||
      pathName.startsWith("/auth/") ||
      pathName.startsWith("/api/") ||
      pathName === "/ws";

    if (!isPublicRoute && deps.auth.enabled) {
      const session = await sessionFromRequest(deps.auth, c.req.header("cookie"));
      if (!session) {
        return c.redirect("/auth/login", 302);
      }
    }

    await next();
  });

  app.use(
    "/*",
    serveStatic({
      root: clientRoot,
      rewriteRequestPath: (p) => (p === "/" ? "/index.html" : p),
    }),
  );

  let cachedHtml: string | undefined;
  app.get("*", requireSession(deps.auth), async (c) => {
    try {
      cachedHtml ??= await readFile(
        path.join(clientRoot, "index.html"),
        "utf8",
      );
      return c.html(cachedHtml);
    } catch {
      return c.text("ui not built — run pnpm build", 503);
    }
  });

  const server = createServer(getRequestListener(app.fetch));
  injectWebSocket(server);

  server.listen(deps.port, "0.0.0.0", () => {
    process.stdout.write(
      `telemetry listening on ${deps.port} (ui ${clientRoot})\n`,
    );
  });

  return server;
}
