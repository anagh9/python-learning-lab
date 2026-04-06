"""
JWT Token Issuer
-----------------
Issues short-lived access tokens once OTP is verified.
Entirely stateless — verification only needs the JWT_SECRET.
"""

import time
import hmac
import hashlib
import base64
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class TokenConfig:
    jwt_secret: str                          # HS256 signing secret
    access_token_ttl: int = 900             # 15 minutes
    issuer: str = "stateless-otp-service"
    audience: str = "api"
    extra_claims: Dict[str, Any] = field(default_factory=dict)


class TokenIssuer:
    """
    Minimal HS256 JWT issuer/verifier with no third-party dependencies.
    Drop-in replacement when you don't want PyJWT as a dependency.
    """

    def __init__(self, config: TokenConfig):
        self.cfg = config
        self._secret = config.jwt_secret.encode()

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def issue(self, identity: str, **extra_claims) -> str:
        """
        Issue a signed JWT for *identity*.

        Args:
            identity:     User identifier (sub claim).
            extra_claims: Any additional claims to embed.

        Returns:
            Compact JWT string: header.payload.signature
        """
        now = int(time.time())
        payload = {
            "sub": identity,
            "iss": self.cfg.issuer,
            "aud": self.cfg.audience,
            "iat": now,
            "exp": now + self.cfg.access_token_ttl,
            **self.cfg.extra_claims,
            **extra_claims,
        }
        return self._encode(payload)

    def verify(self, token: str) -> Dict[str, Any]:
        """
        Verify a JWT and return its claims.

        Raises:
            TokenExpiredError:   Token has expired.
            TokenInvalidError:   Signature mismatch or malformed token.
        """
        try:
            header_b64, payload_b64, sig_b64 = token.split(".")
        except ValueError:
            raise TokenInvalidError("Malformed token structure")

        # Verify signature first
        signing_input = f"{header_b64}.{payload_b64}".encode()
        expected_sig = self._sign(signing_input)
        actual_sig   = self._b64decode(sig_b64)

        if not hmac.compare_digest(expected_sig, actual_sig):
            raise TokenInvalidError("Invalid token signature")

        # Decode payload
        try:
            payload = json.loads(self._b64decode(payload_b64).decode())
        except Exception:
            raise TokenInvalidError("Cannot decode token payload")

        # Check expiry
        now = int(time.time())
        if payload.get("exp", 0) < now:
            raise TokenExpiredError("Token has expired")

        # Check audience
        if payload.get("aud") != self.cfg.audience:
            raise TokenInvalidError("Invalid token audience")

        return payload

    # ------------------------------------------------------------------ #
    #  Internals                                                           #
    # ------------------------------------------------------------------ #

    def _encode(self, payload: Dict[str, Any]) -> str:
        header  = {"alg": "HS256", "typ": "JWT"}
        h_b64   = self._b64encode(json.dumps(header, separators=(",", ":")).encode())
        p_b64   = self._b64encode(json.dumps(payload, separators=(",", ":")).encode())
        signing = f"{h_b64}.{p_b64}".encode()
        sig_b64 = self._b64encode(self._sign(signing))
        return f"{h_b64}.{p_b64}.{sig_b64}"

    def _sign(self, data: bytes) -> bytes:
        return hmac.new(self._secret, data, hashlib.sha256).digest()

    @staticmethod
    def _b64encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    @staticmethod
    def _b64decode(s: str) -> bytes:
        try:
            padding = 4 - len(s) % 4
            return base64.urlsafe_b64decode(s + "=" * padding)
        except Exception as exc:
            raise TokenInvalidError(f"Invalid base64 segment: {exc}") from exc


# ------------------------------------------------------------------ #
#  Custom exceptions                                                   #
# ------------------------------------------------------------------ #

class TokenError(Exception):
    """Base token error."""

class TokenExpiredError(TokenError):
    """Raised when a JWT has passed its expiry time."""

class TokenInvalidError(TokenError):
    """Raised when a JWT is malformed or has an invalid signature."""
