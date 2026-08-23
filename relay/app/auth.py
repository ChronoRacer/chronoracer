from typing import Any

import jwt
from fastapi import HTTPException, Request, status
from jwt import PyJWKClient

from .config import OktaConfig


class OktaAuthenticator:
    def __init__(self, config: OktaConfig):
        self.config = config
        self._jwks = PyJWKClient(config.jwks_url)

    def validate(self, token: str) -> dict[str, Any]:
        try:
            signing_key = self._jwks.get_signing_key_from_jwt(token).key
            return jwt.decode(
                token,
                signing_key,
                algorithms=self.config.algorithms,
                audience=self.config.audience,
                issuer=self.config.issuer,
            )
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Okta token") from exc


async def okta_user(request: Request) -> dict[str, Any]:
    authenticator: OktaAuthenticator = request.app.state.okta
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Bearer token required")
    return authenticator.validate(token)


class OIDCAuthenticator:
    """Validates Google/Pub/Sub push JWTs against Google's public signing keys."""

    GOOGLE_CERTS = "https://www.googleapis.com/oauth2/v3/certs"

    def __init__(self, audience: str):
        self.audience = audience
        self._jwks = PyJWKClient(self.GOOGLE_CERTS)

    def validate(self, token: str) -> dict[str, Any]:
        try:
            key = self._jwks.get_signing_key_from_jwt(token).key
            return jwt.decode(token, key, algorithms=["RS256"], audience=self.audience, issuer="https://accounts.google.com")
        except Exception as exc:
            raise HTTPException(status_code=401, detail="Invalid Pub/Sub OIDC token") from exc


async def pubsub_push(request: Request, audience: str) -> dict[str, Any]:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not audience:
        raise HTTPException(status_code=401, detail="Pub/Sub OIDC token required")
    authenticator = request.app.state.pubsub_auth.get(audience)
    if authenticator is None:
        authenticator = OIDCAuthenticator(audience)
        request.app.state.pubsub_auth[audience] = authenticator
    return authenticator.validate(token)
