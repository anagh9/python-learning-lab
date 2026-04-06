"""
API Routes
----------
All OTP endpoints wired to the OTPService.
"""

import logging
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Response
from fastapi.responses import JSONResponse

from core.service import OTPService
from core.token   import TokenExpiredError, TokenInvalidError
from api.schemas  import (
    OTPRequestBody, OTPVerifyBody, TokenIntrospectBody,
    OTPRequestResponse, OTPVerifyResponse, TokenIntrospectResponse,
    ErrorResponse,
)

logger = logging.getLogger(__name__)


def build_router(service: OTPService) -> APIRouter:
    router = APIRouter()

    # ──────────────────────────────────────────────────────────────────
    # POST /otp/request
    # ──────────────────────────────────────────────────────────────────
    @router.post(
        "/otp/request",
        response_model=OTPRequestResponse,
        responses={429: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
        tags=["OTP"],
        summary="Request a new OTP",
        description=(
            "Generates a stateless OTP for *identity* and dispatches it via the "
            "configured delivery backend (email, SMS, or webhook). "
            "No database writes occur — the OTP is derived deterministically "
            "from the server secret and the current time window."
        ),
    )
    async def request_otp(body: OTPRequestBody, response: Response):
        result = service.request_otp(body.identity)

        # Attach rate-limit headers
        response.headers["X-RateLimit-Remaining"] = str(result.rate_limit_remaining)

        if not result.success:
            status = 429 if result.error == "rate_limit_exceeded" else 502
            return JSONResponse(
                status_code=status,
                content={"error": result.error, "message": result.message},
            )

        return OTPRequestResponse(
            success=True,
            message=result.message,
            ttl_seconds=result.ttl_seconds,
            rate_limit_remaining=result.rate_limit_remaining,
        )

    # ──────────────────────────────────────────────────────────────────
    # POST /otp/verify
    # ──────────────────────────────────────────────────────────────────
    @router.post(
        "/otp/verify",
        response_model=OTPVerifyResponse,
        responses={
            400: {"model": ErrorResponse},
            429: {"model": ErrorResponse},
        },
        tags=["OTP"],
        summary="Verify an OTP and obtain an access token",
        description=(
            "Verifies the submitted OTP for *identity* using stateless HMAC "
            "comparison (no DB lookup). On success, issues a signed JWT access token. "
            "Accepts the current time window ± drift_tolerance to handle clock skew."
        ),
    )
    async def verify_otp(body: OTPVerifyBody, response: Response):
        result = service.verify_otp(body.identity, body.otp)

        response.headers["X-RateLimit-Remaining"] = str(result.rate_limit_remaining)

        if not result.success:
            status = 429 if result.error == "rate_limit_exceeded" else 400
            return JSONResponse(
                status_code=status,
                content={"error": result.error, "message": result.message},
            )

        return OTPVerifyResponse(
            success=True,
            message=result.message,
            access_token=result.access_token,
            token_type=result.token_type,
            expires_in=result.expires_in,
            rate_limit_remaining=result.rate_limit_remaining,
        )

    # ──────────────────────────────────────────────────────────────────
    # POST /token/introspect
    # ──────────────────────────────────────────────────────────────────
    @router.post(
        "/token/introspect",
        response_model=TokenIntrospectResponse,
        tags=["Token"],
        summary="Introspect a JWT access token",
        description=(
            "Validates the provided JWT and returns its claims. "
            "Returns active=false if the token is expired or invalid."
        ),
    )
    async def introspect_token(body: TokenIntrospectBody):
        try:
            claims = service.introspect_token(body.token)
            return TokenIntrospectResponse(active=True, claims=claims)
        except (TokenExpiredError, TokenInvalidError):
            return TokenIntrospectResponse(active=False, claims=None)

    # ──────────────────────────────────────────────────────────────────
    # GET /token/verify  (Bearer header convenience endpoint)
    # ──────────────────────────────────────────────────────────────────
    @router.get(
        "/token/verify",
        response_model=TokenIntrospectResponse,
        tags=["Token"],
        summary="Verify Bearer token from Authorization header",
        description=(
            "Pass `Authorization: Bearer <token>` to validate a JWT. "
            "Useful as a middleware check endpoint for reverse-proxy setups (e.g. nginx auth_request)."
        ),
    )
    async def verify_bearer(authorization: Annotated[str | None, Header()] = None):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
        token = authorization.removeprefix("Bearer ").strip()
        try:
            claims = service.introspect_token(token)
            return TokenIntrospectResponse(active=True, claims=claims)
        except TokenExpiredError:
            raise HTTPException(status_code=401, detail="Token expired")
        except TokenInvalidError:
            raise HTTPException(status_code=401, detail="Invalid token")

    return router
