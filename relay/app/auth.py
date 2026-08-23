from typing import Any

import jwt
from auth0_fastapi.auth.auth_client import AuthClient
from auth0_fastapi.config import Auth0Config
from fastapi import HTTPException, Request, Response
from jwt import PyJWKClient
from loguru import logger


class Auth0Authenticator:
    def __init__(self, config: Auth0Config):
        self.config = config
        self.client = AuthClient(config)

    async def require_session(self, request: Request, response: Response) -> dict[str, Any]:
        logger.debug("Validating authenticated browser session")
        return await self.client.require_session(request, response)

    def validate_token(self, token: str) -> dict[str, Any]:
        """Validate an Okta/Auth0 access token for API calls."""
        try:
            jwks = PyJWKClient(f"https://{self.config.domain}/.well-known/jwks.json")
            key = jwks.get_signing_key_from_jwt(token).key
            return jwt.decode(token, key, algorithms=["RS256"], audience=self.config.audience,
                              issuer=f"https://{self.config.domain}/")
        except Exception as exc:
            logger.warning("Okta token validation failed: {}", type(exc).__name__)
            raise HTTPException(status_code=401, detail="Invalid Okta access token") from exc


async def auth0_user(request: Request, response: Response) -> dict[str, Any]:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() == "bearer" and token:
        return request.app.state.auth0.validate_token(token)
    return await request.app.state.auth0.require_session(request, response)


class OIDCAuthenticator:
    """Validates Google/Pub/Sub push JWTs against Google's public signing keys."""

    GOOGLE_CERTS = "https://www.googleapis.com/oauth2/v3/certs"

    def __init__(self, audience: str):
        self.audience = audience
        self._jwks = PyJWKClient(self.GOOGLE_CERTS)

    def validate(self, token: str) -> dict[str, Any]:
        try:
            key = self._jwks.get_signing_key_from_jwt(token).key
            return jwt.decode(token, key, algorithms=["RS256"], audience=self.audience,
                              issuer="https://accounts.google.com")
        except Exception as exc:
            logger.warning("Pub/Sub OIDC token validation failed: {}", type(exc).__name__)
            raise HTTPException(status_code=401, detail="Invalid Pub/Sub OIDC token") from exc


async def pubsub_push(request: Request, audience: str) -> dict[str, Any]:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not audience:
        logger.warning("Rejected Pub/Sub request with incomplete authorization")
        raise HTTPException(status_code=401, detail="Pub/Sub OIDC token required")
    authenticator = request.app.state.pubsub_auth.get(audience)
    if authenticator is None:
        authenticator = OIDCAuthenticator(audience)
        request.app.state.pubsub_auth[audience] = authenticator
    return authenticator.validate(token)
