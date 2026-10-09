from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, ConfigDict, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
APP_TITLE = "chrono-analysis"
APP_VERSION = "0.0.1"

"""
app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
    docs_url=None,
    redoc_url=None,
    openapi_url=None
)
"""

app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
)

# TODO HOW TO GET DOMAIN NAME AND ETC IN RUN TIME

limiter = Limiter(key_func=get_remote_address,default_limits=["120/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded,_rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(TrustedHostMiddleware,allowed_hosts=["*"])
app.add_middleware(
    CORSMiddleware,
    allow_origin=["*"],
    allow_methods=["GET","POST"],
    allow_headers=["Content-Type"]
)

@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


