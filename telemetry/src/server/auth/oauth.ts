import { Hono } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";
import * as oidc from "openid-client";
import type { AuthConfig } from "../env/index.js";
import { logError } from "../log.js";
import { cookieOptions, endSession, startSession } from "./session.js";

const STATE_COOKIE = "telemetry_oauth_state";
const PKCE_COOKIE = "telemetry_oauth_pkce";
const FLOW_MAX_AGE_SEC = 10 * 60;

export function oauthRoutes(auth: AuthConfig) {
  if (!auth.enabled) {
    return new Hono().get("/login", (c) => c.redirect("/")).get("/logout", (c) => c.redirect("/"));
  }

  const callbackUrl = `${auth.rootUrl}/auth/callback`;
  const flowCookie = cookieOptions(auth, FLOW_MAX_AGE_SEC);

  let config: Promise<oidc.Configuration> | undefined;
  const discover = () =>
    (config ??= oidc.discovery(new URL(auth.issuer), auth.clientId, auth.clientSecret).catch((err: unknown) => {
      config = undefined;
      throw err;
    }));

  return new Hono()
    .get("/login", async (c) => {
      const state = oidc.randomState();
      const verifier = oidc.randomPKCECodeVerifier();
      setCookie(c, STATE_COOKIE, state, flowCookie);
      setCookie(c, PKCE_COOKIE, verifier, flowCookie);
      const url = oidc.buildAuthorizationUrl(await discover(), {
        redirect_uri: callbackUrl,
        scope: "openid profile email",
        state,
        code_challenge: await oidc.calculatePKCECodeChallenge(verifier),
        code_challenge_method: "S256",
      });
      return c.redirect(url.href);
    })
    .get("/callback", async (c) => {
      const state = getCookie(c, STATE_COOKIE);
      const verifier = getCookie(c, PKCE_COOKIE);
      deleteCookie(c, STATE_COOKIE, flowCookie);
      deleteCookie(c, PKCE_COOKIE, flowCookie);

      const error = c.req.query("error");
      if (error) {
        return c.text(`login failed: ${c.req.query("error_description") ?? error}`, 401);
      }
      if (!state || !verifier) {
        return c.text("login expired; open /auth/login again", 400);
      }

      const responseUrl = new URL(callbackUrl);
      responseUrl.search = new URL(c.req.url).search;
      try {
        const tokens = await oidc.authorizationCodeGrant(await discover(), responseUrl, {
          expectedState: state,
          pkceCodeVerifier: verifier,
        });
        const claims = tokens.claims();
        if (!claims) {
          throw new Error("token response has no id_token");
        }
        const email = typeof claims.email === "string" ? claims.email : undefined;
        await startSession(c, auth, { sub: claims.sub, email });
        return c.redirect("/");
      } catch (err) {
        logError("oauth_callback", err);
        return c.text("login failed", 500);
      }
    })
    .get("/logout", async (c) => {
      endSession(c, auth);
      const url = oidc.buildEndSessionUrl(await discover(), { post_logout_redirect_uri: auth.rootUrl });
      return c.redirect(url.href);
    });
}
