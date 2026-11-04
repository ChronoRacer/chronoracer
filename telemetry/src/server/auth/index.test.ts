import { Hono } from 'hono';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AuthConfig } from '../env/index.js';
import { authRoutes, oidcConfig, readSession, requireSameOrigin, requireSession } from './index.js';

const ISSUER = 'https://issuer.test/';
const auth = {
  enabled: true,
  clientId: 'client-1',
  clientSecret: 'client-secret',
  issuer: ISSUER,
  rootUrl: 'https://telemetry.test',
  sessionSecret: 'session-secret-session-secret-32',
} satisfies AuthConfig;

const base64 = (value: object) => Buffer.from(JSON.stringify(value)).toString('base64url');
const idToken = (nonce: string) => {
  const now = Math.floor(Date.now() / 1000);
  const claims = { iss: ISSUER, aud: auth.clientId, sub: 'auth0|op', email: 'op@x.test', nonce, iat: now, exp: now + 60 };
  return `${base64({ alg: 'RS256' })}.${base64(claims)}.signature`;
};

function fakeAuth0() {
  const idp = { nonce: '', tokenRequests: [] as URLSearchParams[] };
  vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input instanceof Request ? input.url : input);
    if (url === `${ISSUER}.well-known/openid-configuration`) {
      return Response.json({
        issuer: ISSUER,
        authorization_endpoint: `${ISSUER}authorize`,
        token_endpoint: `${ISSUER}oauth/token`,
        end_session_endpoint: `${ISSUER}oidc/logout`,
        scopes_supported: ['openid', 'profile', 'email'],
      });
    }
    if (url === `${ISSUER}oauth/token`) {
      idp.tokenRequests.push(new URLSearchParams(await new Response(init?.body).text()));
      return Response.json({ access_token: 'at', token_type: 'Bearer', id_token: idToken(idp.nonce) });
    }
    throw new Error(`unexpected fetch ${url}`);
  });
  return idp;
}

function app() {
  const app = new Hono();
  app.onError((_err, c) => c.text('internal error', 500));
  app.use('*', oidcConfig(auth));
  app.route('/auth', authRoutes(auth));
  app.get('/me', async (c) => c.json((await readSession(c, auth)) ?? null));
  app.get('/private', requireSession(auth), (c) => c.text('ok'));
  app.get('/ws', requireSameOrigin(auth), (c) => c.text('ok'));
  return app;
}

const cookiePairs = (res: Response) =>
  res.headers
    .getSetCookie()
    .map((c) => c.split(';')[0])
    .join('; ');

async function login(server: Hono, idp: ReturnType<typeof fakeAuth0>) {
  const start = await server.request('/auth/login');
  const authorize = new URL(start.headers.get('location')!);
  idp.nonce = authorize.searchParams.get('nonce')!;
  const callback = await server.request(
    `http://telemetry.test/auth/callback?code=abc&state=${authorize.searchParams.get('state')}`,
    { headers: { cookie: cookiePairs(start) } },
  );
  const session = callback.headers.getSetCookie().find((c) => c.startsWith('oidc-auth='));
  return { authorize, callback, session, cookie: session?.split(';')[0] ?? '' };
}

describe('auth', () => {
  beforeEach(() => vi.unstubAllGlobals());

  it('logs in with PKCE, client_secret_post and the registered https redirect_uri', async () => {
    const idp = fakeAuth0();
    const { authorize, callback, session } = await login(app(), idp);

    expect(authorize.searchParams.get('redirect_uri')).toBe('https://telemetry.test/auth/callback');
    expect(authorize.searchParams.get('code_challenge_method')).toBe('S256');
    expect(authorize.searchParams.get('scope')).toBe('openid profile email');
    expect(idp.tokenRequests[0].get('redirect_uri')).toBe('https://telemetry.test/auth/callback');
    expect(idp.tokenRequests[0].get('client_secret')).toBe('client-secret');
    expect(callback.headers.get('location')).toBe('https://telemetry.test/auth/login');
    expect(session).toMatch(/HttpOnly; Secure/);
  });

  it('serves pages and the session only with a valid session cookie', async () => {
    const server = app();
    const { cookie } = await login(server, fakeAuth0());

    expect((await server.request('/private', { headers: { cookie } })).status).toBe(200);
    expect(await (await server.request('/me', { headers: { cookie } })).json()).toEqual({
      sub: 'auth0|op',
      email: 'op@x.test',
    });
    const anonymous = await server.request('/private', { headers: { cookie: 'oidc-auth=x.y.z' } });
    expect(anonymous.headers.get('location')).toMatch(new RegExp(`^${ISSUER}authorize\\?`));
    expect(await (await server.request('/me')).json()).toBeNull();
  });

  it('logs out of Auth0 and returns to the service root', async () => {
    const server = app();
    const { cookie } = await login(server, fakeAuth0());
    const logout = await server.request('/auth/logout', { headers: { cookie } });
    const end = new URL(logout.headers.get('location')!);
    expect(`${end.origin}${end.pathname}`).toBe(`${ISSUER}oidc/logout`);
    expect(end.searchParams.get('post_logout_redirect_uri')).toBe('https://telemetry.test');
    expect(end.searchParams.get('client_id')).toBe('client-1');
    expect(logout.headers.getSetCookie().some((c) => /^oidc-auth=;.*Max-Age=0/.test(c))).toBe(true);
  });

  it('refuses WebSocket handshakes from other origins', async () => {
    const server = app();
    expect((await server.request('/ws', { headers: { origin: 'https://evil.test' } })).status).toBe(403);
    expect((await server.request('/ws', { headers: { origin: 'https://telemetry.test' } })).status).toBe(200);
  });
});
