import * as client from "openid-client";
import type { AuthConfig } from "../env/index.js";
import {
  clearSessionCookieHeader,
  createSessionToken,
  serializeCookie,
  sessionCookieHeader,
  verifySessionToken,
  SESSION_COOKIE,
  parseCookieHeader,
} from "./session.js";

const STATE_COOKIE = "telemetry_oauth_state";
const PKCE_COOKIE = "telemetry_oauth_pkce";

const configCache = new Map<string, Promise<client.Configuration>>();

export function oauthEnabled(auth: AuthConfig): auth is Extract<AuthConfig, { enabled: true }> {
  return auth.enabled;
}

function redirectUri(auth: Extract<AuthConfig, { enabled: true }>): string {
  return `${auth.rootUrl}/auth/callback`;
}

function oauthConfiguration(auth: Extract<AuthConfig, { enabled: true }>): Promise<client.Configuration> {
  const cacheKey = `${auth.issuer}\0${auth.clientId}`;
  let pending = configCache.get(cacheKey);
  if (!pending) {
    pending = client.discovery(new URL(auth.issuer), auth.clientId, auth.clientSecret);
    configCache.set(cacheKey, pending);
  }
  return pending;
}

export async function loginRedirectUrl(
  auth: Extract<AuthConfig, { enabled: true }>,
  state: string,
  codeVerifier: string,
): Promise<string> {
  const config = await oauthConfiguration(auth);
  const codeChallenge = await client.calculatePKCECodeChallenge(codeVerifier);
  const url = client.buildAuthorizationUrl(config, {
    redirect_uri: redirectUri(auth),
    scope: "openid profile email",
    state,
    code_challenge: codeChallenge,
    code_challenge_method: "S256",
  });
  return url.href;
}

export async function logoutRedirectUrl(auth: Extract<AuthConfig, { enabled: true }>): Promise<string> {
  const config = await oauthConfiguration(auth);
  const url = client.buildEndSessionUrl(config, {
    returnTo: auth.rootUrl,
  });
  return url.href;
}

export async function exchangeCode(
  auth: Extract<AuthConfig, { enabled: true }>,
  callbackUrl: URL,
  expectedState: string,
  codeVerifier: string,
): Promise<{ sub: string; email?: string }> {
  const config = await oauthConfiguration(auth);
  const tokens = await client.authorizationCodeGrant(config, callbackUrl, {
    expectedState,
    pkceCodeVerifier: codeVerifier,
  });
  const claims = tokens.claims();
  const sub = claims?.sub;
  if (!sub) {
    throw new Error("id_token missing sub");
  }
  const email = typeof claims.email === "string" ? claims.email : undefined;
  return { sub, email };
}

function oauthFlowCookie(name: string, value: string, secure: boolean): string {
  return serializeCookie(name, value, { maxAge: 600, secure });
}

function clearOAuthFlowCookie(name: string, secure: boolean): string {
  return serializeCookie(name, "", { maxAge: 0, secure });
}

export async function beginLoginCookies(
  secure: boolean,
): Promise<{ state: string; codeVerifier: string; headers: string[] }> {
  const state = client.randomState();
  const codeVerifier = client.randomPKCECodeVerifier();
  return {
    state,
    codeVerifier,
    headers: [
      oauthFlowCookie(STATE_COOKIE, state, secure),
      oauthFlowCookie(PKCE_COOKIE, codeVerifier, secure),
    ],
  };
}

export function readStateCookie(cookieHeader: string | undefined): string | undefined {
  return parseCookieHeader(cookieHeader)[STATE_COOKIE];
}

export function readPkceCookie(cookieHeader: string | undefined): string | undefined {
  return parseCookieHeader(cookieHeader)[PKCE_COOKIE];
}

export async function finishLoginCookies(
  auth: Extract<AuthConfig, { enabled: true }>,
  identity: { sub: string; email?: string },
  secure: boolean,
): Promise<string[]> {
  const token = await createSessionToken(
    { sub: identity.sub, email: identity.email },
    auth.sessionSecret,
  );
  return [
    sessionCookieHeader(token, secure),
    clearOAuthFlowCookie(STATE_COOKIE, secure),
    clearOAuthFlowCookie(PKCE_COOKIE, secure),
  ];
}

export function logoutCookies(secure: boolean): string[] {
  return [
    clearSessionCookieHeader(secure),
    clearOAuthFlowCookie(STATE_COOKIE, secure),
    clearOAuthFlowCookie(PKCE_COOKIE, secure),
  ];
}

export async function sessionFromRequest(
  auth: AuthConfig,
  cookieHeader: string | undefined,
): Promise<{ sub: string; email?: string } | undefined> {
  if (!auth.enabled) {
    return { sub: "anonymous" };
  }
  const token = parseCookieHeader(cookieHeader)[SESSION_COOKIE];
  if (!token) {
    return undefined;
  }
  const payload = await verifySessionToken(token, auth.sessionSecret);
  if (!payload) {
    return undefined;
  }
  return { sub: payload.sub, email: payload.email };
}
