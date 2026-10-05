import { SignJWT, jwtVerify } from "jose";

export const SESSION_COOKIE = "telemetry_session";
const MAX_AGE_SEC = 60 * 60 * 12;

export type SessionPayload = {
  sub: string;
  email?: string;
  exp: number;
};

export type CookieSerializeOptions = {
  maxAge?: number;
  secure?: boolean;
};

export function serializeCookie(
  name: string,
  value: string,
  opts?: CookieSerializeOptions,
): string {
  const parts = [`${name}=${value}`, "Path=/", "HttpOnly", "SameSite=Lax"];
  if (opts?.maxAge !== undefined) {
    parts.push(`Max-Age=${opts.maxAge}`);
  }
  if (opts?.secure) {
    parts.push("Secure");
  }
  return parts.join("; ");
}

function sessionKey(secret: string): Uint8Array {
  return new TextEncoder().encode(secret);
}

export async function createSessionToken(
  payload: Omit<SessionPayload, "exp">,
  secret: string,
): Promise<string> {
  const key = sessionKey(secret);
  return new SignJWT({
    sub: payload.sub,
    ...(payload.email !== undefined ? { email: payload.email } : {}),
  })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(`${MAX_AGE_SEC}s`)
    .sign(key);
}

export async function verifySessionToken(
  token: string,
  secret: string,
): Promise<SessionPayload | undefined> {
  try {
    const { payload } = await jwtVerify(token, sessionKey(secret), {
      algorithms: ["HS256"],
    });
    if (typeof payload.sub !== "string") {
      return undefined;
    }
    const exp = payload.exp;
    if (typeof exp !== "number") {
      return undefined;
    }
    return {
      sub: payload.sub,
      email: typeof payload.email === "string" ? payload.email : undefined,
      exp,
    };
  } catch {
    return undefined;
  }
}

export function sessionCookieHeader(token: string, secure: boolean): string {
  return serializeCookie(SESSION_COOKIE, token, { maxAge: MAX_AGE_SEC, secure });
}

export function clearSessionCookieHeader(secure: boolean): string {
  return serializeCookie(SESSION_COOKIE, "", { maxAge: 0, secure });
}

export function parseCookieHeader(header: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!header) {
    return out;
  }
  for (const part of header.split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) {
      continue;
    }
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    out[key] = value;
  }
  return out;
}
