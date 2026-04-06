"""
Stateless OTP Service — FastAPI Application
--------------------------------------------
Run:
    uvicorn api.main:app --reload
"""

import os
import sys
import base64
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from core.service  import OTPService, OTPServiceConfig
from core.delivery import ConsoleBackend, SMTPBackend, SMTPConfig, TwilioBackend, TwilioConfig
from core.ratelimit import RateLimitConfig
from core.token    import TokenExpiredError, TokenInvalidError

from api.routes import build_router

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Config from environment variables
# ──────────────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")
    return val


def _build_service() -> OTPService:
    otp_secret_raw = os.environ.get("OTP_SECRET_KEY", "")
    if not otp_secret_raw:
        # Auto-generate in dev — warn loudly
        logger.warning("OTP_SECRET_KEY not set — generating ephemeral key (dev only!)")
        import secrets as _s
        otp_secret_raw = base64.urlsafe_b64encode(_s.token_bytes(32)).decode()

    jwt_secret = os.environ.get("JWT_SECRET", "")
    if not jwt_secret:
        logger.warning("JWT_SECRET not set — using insecure default (dev only!)")
        jwt_secret = "CHANGE_ME_IN_PRODUCTION"

    try:
        otp_secret_bytes = base64.urlsafe_b64decode(otp_secret_raw + "==")
    except Exception:
        otp_secret_bytes = otp_secret_raw.encode()

    cfg = OTPServiceConfig(
        otp_secret_key=otp_secret_bytes,
        jwt_secret=jwt_secret,
        otp_digits=int(os.environ.get("OTP_DIGITS", "6")),
        otp_window_seconds=int(os.environ.get("OTP_WINDOW_SECONDS", "300")),
        otp_drift_tolerance=int(os.environ.get("OTP_DRIFT_TOLERANCE", "1")),
        access_token_ttl=int(os.environ.get("ACCESS_TOKEN_TTL", "900")),
        token_issuer=os.environ.get("TOKEN_ISSUER", "stateless-otp"),
        token_audience=os.environ.get("TOKEN_AUDIENCE", "api"),
        rate_limit=RateLimitConfig(
            request_otp_max=int(os.environ.get("RATE_REQUEST_OTP_MAX", "5")),
            request_otp_window=int(os.environ.get("RATE_REQUEST_OTP_WINDOW", "600")),
            verify_otp_max=int(os.environ.get("RATE_VERIFY_OTP_MAX", "10")),
            verify_otp_window=int(os.environ.get("RATE_VERIFY_OTP_WINDOW", "600")),
        ),
    )

    # Choose delivery backend
    delivery_mode = os.environ.get("DELIVERY_BACKEND", "console").lower()

    if delivery_mode == "smtp":
        delivery = SMTPBackend(SMTPConfig(
            host=_require_env("SMTP_HOST"),
            port=int(os.environ.get("SMTP_PORT", "587")),
            username=os.environ.get("SMTP_USERNAME", ""),
            password=os.environ.get("SMTP_PASSWORD", ""),
            from_address=_require_env("SMTP_FROM"),
            use_tls=os.environ.get("SMTP_USE_TLS", "true").lower() == "true",
        ))
    elif delivery_mode == "twilio":
        delivery = TwilioBackend(TwilioConfig(
            account_sid=_require_env("TWILIO_ACCOUNT_SID"),
            auth_token=_require_env("TWILIO_AUTH_TOKEN"),
            from_number=_require_env("TWILIO_FROM_NUMBER"),
        ))
    else:
        delivery = ConsoleBackend()

    return OTPService(cfg, delivery)


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Stateless OTP Service",
    description=(
        "Zero-database OTP authentication service. "
        "Integrates with any tech stack via REST API."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — restrict origins in production via CORS_ORIGINS env var
_cors_origins = os.environ.get("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Build and attach the OTP service
_service = _build_service()
app.include_router(build_router(_service), prefix="/api/v1")


# ──────────────────────────────────────────────────────────────────────────────
# Global exception handlers
# ──────────────────────────────────────────────────────────────────────────────
@app.exception_handler(TokenExpiredError)
async def token_expired_handler(request: Request, exc: TokenExpiredError):
    return JSONResponse(status_code=401, content={"error": "token_expired", "message": str(exc)})


@app.exception_handler(TokenInvalidError)
async def token_invalid_handler(request: Request, exc: TokenInvalidError):
    return JSONResponse(status_code=401, content={"error": "token_invalid", "message": str(exc)})


@app.exception_handler(Exception)
async def generic_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception")
    return JSONResponse(status_code=500, content={"error": "internal_error", "message": "An unexpected error occurred."})


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok", "service": "stateless-otp"}
