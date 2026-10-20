import asyncio
import jwt
from fastapi import HTTPException, Request
from jwt import PyJWKClient
from loguru import logger
from typing import Any
from pydantic import BaseModel

class OktaUser(BaseModel):
    sub: str

    def __str__(self):
        return self.sub

    def __repr__(self):
        return self.sub

def _mock_user() -> OktaUser:
    return OktaUser(sub="auth-disabled@chronoracer.live")

def _user_from_okta_dict(okta_dict: dict[str, Any]) -> OktaUser:
    return OktaUser(sub=okta_dict.get('sub', 'unknown'))

class OktaAuthenticator:
    def __init__(self, issuer: str, audience: str):
        self.issuer = issuer.rstrip('/') + '/'
        self.audience = audience
        self._jwks = PyJWKClient(f"{self.issuer}.well-known/jwks.json")

    def validate_token(self, token: str) -> dict[str, Any]:
        """Validate an Auth0 access token for API calls."""
        try:
            key = self._jwks.get_signing_key_from_jwt(token).key
            return jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer,
            )
        except Exception as exc:
            logger.warning("Token validation failed: {}", type(exc).__name__)
            raise HTTPException(status_code=401, detail="Invalid access token") from exc

async def okta_user(request: Request) -> OktaUser:
    okta = request.app.state.okta
    # None if auth is disabled
    if not okta:
        return _mock_user()
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() == "bearer" and token:
        valid = await asyncio.to_thread(okta.validate_token, token)
        return _user_from_okta_dict(valid)
    raise HTTPException(status_code=401, detail="Bearer token required")


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
            identity = claims.get("email")
            if identity != self.service_account:
                raise HTTPException(status_code=401, detail=f"Unexpected push SA identity: {identity}")
            return claims
        except Exception as exc:
            logger.warning("Pub/Sub token validation failed: {}", exc)
            raise HTTPException(status_code=401, detail="Invalid access token") from exc


async def pubsub_push(request: Request, audience: str, service_account: str) -> dict[str, Any]:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not audience or not service_account:
        logger.warning("Rejected Pub/Sub request with incomplete authorization")
        raise HTTPException(status_code=401, detail="Bearer token required")
    cache_key = (audience, service_account)
    authenticator = request.app.state.pubsub_auth.get(cache_key)
    if authenticator is None:
        authenticator = OIDCAuthenticator(audience, service_account)
        request.app.state.pubsub_auth[cache_key] = authenticator
    return await asyncio.to_thread(authenticator.validate, token)
