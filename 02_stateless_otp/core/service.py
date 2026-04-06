"""
OTP Service
-----------
High-level orchestrator that wires together:
  - OTPEngine   (stateless generation / verification)
  - RateLimiter (abuse prevention)
  - DeliveryBackend (email / SMS / webhook)
  - TokenIssuer (JWT access token after verification)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from core.otp      import OTPEngine, OTPConfig
from core.token    import TokenIssuer, TokenConfig, TokenExpiredError, TokenInvalidError
from core.ratelimit import RateLimiter, RateLimitConfig
from core.delivery import DeliveryBackend, ConsoleBackend

logger = logging.getLogger(__name__)


@dataclass
class OTPServiceConfig:
    # Required
    otp_secret_key: bytes            # HMAC secret for OTP generation
    jwt_secret: str                  # HS256 signing secret for tokens

    # OTP behaviour
    otp_digits: int = 6
    otp_window_seconds: int = 300    # 5-minute windows
    otp_drift_tolerance: int = 1     # accept 1 adjacent window

    # JWT behaviour
    access_token_ttl: int = 900      # 15 minutes
    token_issuer: str = "stateless-otp"
    token_audience: str = "api"

    # Rate limiting
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)


@dataclass
class OTPRequestResult:
    success: bool
    message: str
    ttl_seconds: int = 0
    rate_limit_remaining: int = 0
    error: Optional[str] = None


@dataclass
class OTPVerifyResult:
    success: bool
    message: str
    access_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int = 0
    rate_limit_remaining: int = 0
    error: Optional[str] = None


class OTPService:
    """
    Main entry point for the stateless OTP system.

    Example:
        from core.service  import OTPService, OTPServiceConfig
        from core.delivery import ConsoleBackend

        svc = OTPService(
            config   = OTPServiceConfig(otp_secret_key=b"...", jwt_secret="..."),
            delivery = ConsoleBackend(),
        )
        svc.request_otp("user@example.com")
        result = svc.verify_otp("user@example.com", "123456")
        if result.success:
            print(result.access_token)
    """

    def __init__(
        self,
        config: OTPServiceConfig,
        delivery: Optional[DeliveryBackend] = None,
    ):
        self._engine = OTPEngine(OTPConfig(
            digits=config.otp_digits,
            window_seconds=config.otp_window_seconds,
            drift_tolerance=config.otp_drift_tolerance,
            secret_key=config.otp_secret_key,
        ))
        self._tokens = TokenIssuer(TokenConfig(
            jwt_secret=config.jwt_secret,
            access_token_ttl=config.access_token_ttl,
            issuer=config.token_issuer,
            audience=config.token_audience,
        ))
        self._limiter  = RateLimiter(config.rate_limit)
        self._delivery = delivery or ConsoleBackend()
        self._cfg      = config

    # ------------------------------------------------------------------ #
    #  Step 1 — Request an OTP                                            #
    # ------------------------------------------------------------------ #

    def request_otp(self, identity: str) -> OTPRequestResult:
        """
        Generate and deliver an OTP for *identity*.

        Args:
            identity: The user's unique identifier (email, phone, user_id, …).

        Returns:
            OTPRequestResult with success flag and metadata.
        """
        rl = self._limiter.check_request_otp(identity)
        if not rl.allowed:
            return OTPRequestResult(
                success=False,
                message="Too many OTP requests. Please wait before requesting again.",
                error="rate_limit_exceeded",
                rate_limit_remaining=0,
            )

        otp = self._engine.generate(identity)
        ttl = self._engine.remaining_seconds()

        delivered = self._delivery.send(identity, otp, ttl)
        if not delivered:
            logger.error("OTP delivery failed for identity=%s", identity)
            return OTPRequestResult(
                success=False,
                message="Failed to deliver OTP. Please try again.",
                error="delivery_failed",
            )

        logger.info("OTP requested for identity=%s ttl=%ds", identity, ttl)
        return OTPRequestResult(
            success=True,
            message="OTP sent successfully.",
            ttl_seconds=ttl,
            rate_limit_remaining=rl.remaining,
        )

    # ------------------------------------------------------------------ #
    #  Step 2 — Verify OTP and issue access token                         #
    # ------------------------------------------------------------------ #

    def verify_otp(
        self,
        identity: str,
        otp: str,
        **token_claims,
    ) -> OTPVerifyResult:
        """
        Verify *otp* for *identity* and issue a JWT on success.

        Args:
            identity:     Same identity used in request_otp().
            otp:          The OTP entered by the user.
            token_claims: Extra claims to embed in the issued JWT.

        Returns:
            OTPVerifyResult with access_token on success.
        """
        rl = self._limiter.check_verify_otp(identity)
        if not rl.allowed:
            return OTPVerifyResult(
                success=False,
                message="Too many verification attempts. Please request a new OTP.",
                error="rate_limit_exceeded",
            )

        valid = self._engine.verify(identity, otp)
        if not valid:
            logger.warning("OTP verification failed for identity=%s", identity)
            return OTPVerifyResult(
                success=False,
                message="Invalid or expired OTP.",
                error="otp_invalid",
                rate_limit_remaining=rl.remaining,
            )

        token = self._tokens.issue(identity, **token_claims)
        logger.info("OTP verified, token issued for identity=%s", identity)

        return OTPVerifyResult(
            success=True,
            message="OTP verified successfully.",
            access_token=token,
            token_type="bearer",
            expires_in=self._cfg.access_token_ttl,
            rate_limit_remaining=rl.remaining,
        )

    # ------------------------------------------------------------------ #
    #  Token introspection                                                 #
    # ------------------------------------------------------------------ #

    def introspect_token(self, token: str) -> dict:
        """
        Validate a JWT and return its claims.

        Raises TokenExpiredError or TokenInvalidError on failure.
        """
        return self._tokens.verify(token)
