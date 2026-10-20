/**
 * @fileoverview OpenID Connect (OIDC) & Session Auth Middleware (Auth0 + @hono/oidc-auth)
 *
 * Implements browser authentication using OAuth 2.0 Authorization Code Flow + PKCE:
 * - OIDC middleware initialization & HTTP-only session cookie management (12h TTL).
 * - Auth routes (`/login`, `/callback`, `/logout` with OIDC RP-Initiated Logout).
 * - Development fallback (`auth.enabled: false`) yielding an `anonymous` dummy session.
 * - Same-Origin verification middleware protecting WebSocket upgrade endpoints against CSWSH.
 */

import {
  getAuth,
  getAuthorizationServer,
  initOidcAuthMiddleware,
  oidcAuthMiddleware,
  processOAuthCallback,
  revokeSession,
  setClientAuth,
} from '@hono/oidc-auth';
import { Hono, type Context, type MiddlewareHandler } from 'hono';
import { ClientSecretPost } from 'oauth4webapi';
import type { AuthConfig } from '../env/index.js';

export type Session = { sub: string; email?: string };

const SESSION_MAX_AGE_SEC = `${12 * 60 * 60}`;
const open: MiddlewareHandler = (_c, next) => next();

export function oidcConfig(auth: AuthConfig): MiddlewareHandler {
  if (!auth.enabled) {
    return open;
  }
  const init = initOidcAuthMiddleware({
    OIDC_ISSUER: auth.issuer,
    OIDC_CLIENT_ID: auth.clientId,
    OIDC_CLIENT_SECRET: auth.clientSecret,
    OIDC_AUTH_SECRET: auth.sessionSecret,
    OIDC_SCOPES: 'openid profile email',
    OIDC_REDIRECT_URI: `${auth.rootUrl}/auth/callback`,
    OIDC_AUTH_EXTERNAL_URL: auth.rootUrl,
    OIDC_AUTH_EXPIRES: SESSION_MAX_AGE_SEC,
    OIDC_AUTH_REFRESH_INTERVAL: SESSION_MAX_AGE_SEC,
  });
  const clientAuth = ClientSecretPost(auth.clientSecret);
  return (c, next) =>
    init(c, () => {
      setClientAuth(c, clientAuth);
      return next();
    });
}

export function authRoutes(auth: AuthConfig) {
  if (!auth.enabled) {
    return new Hono().get('/login', (c) => c.redirect('/')).get('/logout', (c) => c.redirect('/'));
  }
  return new Hono()
    .get('/login', oidcAuthMiddleware(), (c) => c.redirect('/'))
    .get('/callback', (c) => processOAuthCallback(c))
    .get('/logout', async (c) => {
      await revokeSession(c);
      const { end_session_endpoint } = await getAuthorizationServer(c);
      if (!end_session_endpoint) {
        throw new Error('Auth0 discovery has no end_session_endpoint');
      }
      const url = new URL(end_session_endpoint);
      url.searchParams.set('client_id', auth.clientId);
      url.searchParams.set('post_logout_redirect_uri', auth.rootUrl);
      return c.redirect(url.href);
    });
}

export const requireSession = (auth: AuthConfig): MiddlewareHandler => (auth.enabled ? oidcAuthMiddleware() : open);

export async function readSession(c: Context, auth: AuthConfig): Promise<Session | undefined> {
  if (!auth.enabled) {
    return { sub: 'anonymous' };
  }
  const session = await getAuth(c);
  return session?.sub ? { sub: session.sub, email: session.email || undefined } : undefined;
}

/** Cross-Site WebSocket Hijacking (CSWSH) protection: verifies `Origin` header matches `auth.rootUrl`. */
export function requireSameOrigin(auth: AuthConfig): MiddlewareHandler {
  return async (c, next) => {
    if (auth.enabled && c.req.header('origin') !== auth.rootUrl) {
      return c.text('forbidden origin', 403);
    }
    return next();
  };
}
