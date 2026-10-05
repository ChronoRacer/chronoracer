import type { Context } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";
import { sign, verify } from "hono/jwt";
import type { AuthConfig, OAuthConfig } from "../env/index.js";

export type Session = { sub: string; email?: string };

const SESSION_COOKIE = "telemetry_session";
const SESSION_MAX_AGE_SEC = 12 * 60 * 60;

export const cookieOptions = (auth: OAuthConfig, maxAge: number) => ({
  path: "/",
  httpOnly: true,
  sameSite: "Lax" as const,
  secure: auth.rootUrl.startsWith("https:"),
  maxAge,
});

export async function startSession(c: Context, auth: OAuthConfig, session: Session): Promise<void> {
  const exp = Math.floor(Date.now() / 1000) + SESSION_MAX_AGE_SEC;
  const token = await sign({ ...session, exp }, auth.sessionSecret, "HS256");
  setCookie(c, SESSION_COOKIE, token, cookieOptions(auth, SESSION_MAX_AGE_SEC));
}

export function endSession(c: Context, auth: OAuthConfig): void {
  deleteCookie(c, SESSION_COOKIE, cookieOptions(auth, 0));
}

export async function readSession(c: Context, auth: AuthConfig): Promise<Session | undefined> {
  if (!auth.enabled) {
    return { sub: "anonymous" };
  }
  const token = getCookie(c, SESSION_COOKIE);
  if (!token) {
    return undefined;
  }
  try {
    const { sub, email } = await verify(token, auth.sessionSecret, "HS256");
    if (typeof sub !== "string") {
      return undefined;
    }
    return { sub, email: typeof email === "string" ? email : undefined };
  } catch {
    return undefined;
  }
}
