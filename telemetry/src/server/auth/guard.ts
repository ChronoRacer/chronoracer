import type { MiddlewareHandler } from "hono";
import type { AuthConfig } from "../env/index.js";
import { readSession } from "./session.js";

export function requireSession(auth: AuthConfig): MiddlewareHandler {
  return async (c, next) => {
    if (await readSession(c, auth)) {
      return next();
    }
    if (c.req.header("accept")?.includes("text/html")) {
      return c.redirect("/auth/login");
    }
    return c.text("unauthorized", 401);
  };
}

export function requireSameOrigin(auth: AuthConfig): MiddlewareHandler {
  return async (c, next) => {
    if (auth.enabled && c.req.header("origin") !== auth.rootUrl) {
      return c.text("forbidden origin", 403);
    }
    return next();
  };
}
