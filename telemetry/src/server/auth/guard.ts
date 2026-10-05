import type { Context, Next } from "hono";
import type { AuthConfig } from "../env/index.js";
import { sessionFromRequest } from "./oauth.js";

export type SessionVars = {
  session: { sub: string; email?: string };
};

export function requireSession(auth: AuthConfig) {
  return async (c: Context, next: Next) => {
    const session = await sessionFromRequest(auth, c.req.header("cookie"));
    if (!session) {
      if (c.req.header("accept")?.includes("text/html")) {
        return c.redirect("/auth/login", 302);
      }
      return c.text("unauthorized", 401);
    }
    c.set("session", session);
    await next();
  };
}
