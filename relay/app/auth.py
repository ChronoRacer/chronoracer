from typing import Any

import jwt
from fastapi import HTTPException, Request
from jwt import PyJWKClient
from loguru import logger


class OktaAuthenticator:
    def __init__(self, issuer: str, audience: str):
        self.issuer = issuer.rstrip('/')
        self.audience = audience
        self._jwks = PyJWKClient(f"{self.issuer}/v1/keys")

    def validate_token(self, token: str) -> dict[str, Any]:
        """Validate an Okta access token for operator API calls."""
        try:
            key = self._jwks.get_signing_key_from_jwt(token).key
            return jwt.decode(token, key, algorithms=["RS256"], audience=self.audience, issuer=self.issuer)
        except Exception as exc:
            logger.warning("Okta token validation failed: {}", type(exc).__name__)
            raise HTTPException(status_code=401, detail="Invalid Okta access token") from exc


async def okta_user(request: Request) -> dict[str, Any]:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() == "bearer" and token:
        return request.app.state.okta.validate_token(token)
    raise HTTPException(status_code=401, detail="Okta bearer token required")


class OIDCAuthenticator:
    """Validates Google/Pub/Sub push JWTs against Google's public signing keys."""

    GOOGLE_CERTS = "https://www.googleapis.com/oauth2/v3/certs"

    def __init__(self, audience: str, service_account: str):
        self.audience = audience
        self.service_account = service_account
        self._jwks = PyJWKClient(self.GOOGLE_CERTS)

    def validate(self, token: str) -> dict[str, Any]:
        try:
            key = self._jwks.get_signing_key_from_jwt(token).key
            claims = jwt.decode(token, key, algorithms=["RS256"], audience=self.audience,
                                issuer="https://accounts.google.com")
            if claims.get("email") != self.service_account:
                raise HTTPException(status_code=401, detail="Unexpected Pub/Sub service identity")
            return claims
        except Exception as exc:
            logger.warning("Pub/Sub OIDC token validation failed: {}", type(exc).__name__)
            raise HTTPException(status_code=401, detail="Invalid Pub/Sub OIDC token") from exc


async def pubsub_push(request: Request, audience: str, service_account: str) -> dict[str, Any]:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not audience or not service_account:
        logger.warning("Rejected Pub/Sub request with incomplete authorization")
        raise HTTPException(status_code=401, detail="Pub/Sub OIDC token required")
    cache_key = (audience, service_account)
    authenticator = request.app.state.pubsub_auth.get(cache_key)
    if authenticator is None:
        authenticator = OIDCAuthenticator(audience, service_account)
        request.app.state.pubsub_auth[cache_key] = authenticator
    return authenticator.validate(token)
