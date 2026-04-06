"""
Stateless OTP Core Engine
--------------------------
Generates and verifies time-based OTPs without any database or session storage.

How it works (stateless):
  OTP = HMAC-SHA256(SECRET_KEY + user_id + time_window) truncated to N digits

  Verification checks current window ± drift_tolerance windows, so no state is
  needed — just re-derive the same value and compare.
"""

import hmac
import hashlib
import struct
import time
import secrets
import base64
from dataclasses import dataclass
from typing import Optional


@dataclass
class OTPConfig:
    """Configuration for the OTP engine."""
    digits: int = 6                  # OTP length (4–10)
    window_seconds: int = 300        # Each time window width (default: 5 min)
    drift_tolerance: int = 1         # How many past/future windows to also accept
    algorithm: str = "sha256"        # hmac algorithm: sha1, sha256, sha512
    secret_key: bytes = b""          # Server-side HMAC secret (never expose)

    def __post_init__(self):
        if not (4 <= self.digits <= 10):
            raise ValueError("digits must be between 4 and 10")
        if self.window_seconds < 30:
            raise ValueError("window_seconds must be at least 30")
        if not self.secret_key:
            raise ValueError("secret_key must not be empty")
        if self.algorithm not in ("sha1", "sha256", "sha512"):
            raise ValueError("algorithm must be sha1, sha256, or sha512")


class OTPEngine:
    """
    Stateless OTP generator and verifier.

    Usage:
        engine = OTPEngine(config)
        otp    = engine.generate("user@example.com")
        valid  = engine.verify("user@example.com", otp)
    """

    _DIGEST_MAP = {
        "sha1":   hashlib.sha1,
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
    }

    def __init__(self, config: OTPConfig):
        self.cfg = config
        self._digest = self._DIGEST_MAP[config.algorithm]

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def generate(self, identity: str, *, at_time: Optional[float] = None) -> str:
        """
        Generate an OTP for *identity* (email, phone, user_id, …).

        Args:
            identity: Unique string that scopes this OTP to one user/action.
            at_time:  Unix timestamp override (useful for testing).

        Returns:
            Zero-padded numeric OTP string.
        """
        ts = at_time if at_time is not None else time.time()
        window = self._current_window(ts)
        return self._hotp(identity, window)

    def verify(
        self,
        identity: str,
        otp: str,
        *,
        at_time: Optional[float] = None,
    ) -> bool:
        """
        Verify *otp* for *identity* without any external storage.

        Checks current window plus ±drift_tolerance neighbouring windows to
        handle clock skew and network delay.

        Args:
            identity: Same identity used during generate().
            otp:      The OTP submitted by the user.
            at_time:  Unix timestamp override (useful for testing).

        Returns:
            True if valid, False otherwise.
        """
        if not otp or not otp.isdigit() or len(otp) != self.cfg.digits:
            return False

        ts = at_time if at_time is not None else time.time()
        window = self._current_window(ts)

        for delta in range(-self.cfg.drift_tolerance, self.cfg.drift_tolerance + 1):
            expected = self._hotp(identity, window + delta)
            # Constant-time comparison prevents timing attacks
            if hmac.compare_digest(expected, otp):
                return True
        return False

    def remaining_seconds(self, at_time: Optional[float] = None) -> int:
        """Seconds left before the current OTP window expires."""
        ts = at_time if at_time is not None else time.time()
        return self.cfg.window_seconds - int(ts % self.cfg.window_seconds)

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _current_window(self, ts: float) -> int:
        return int(ts) // self.cfg.window_seconds

    def _hotp(self, identity: str, window: int) -> str:
        """
        Derive an OTP from (identity, window) using HMAC.

        Message = identity bytes + big-endian 8-byte window counter.
        This binds the OTP to BOTH the user AND the time window.
        """
        counter_bytes = struct.pack(">Q", window)
        msg = identity.encode() + counter_bytes

        digest = hmac.new(self.cfg.secret_key, msg, self._digest).digest()

        # Dynamic truncation (same idea as RFC 4226 HOTP)
        offset = digest[-1] & 0x0F
        code = (
            ((digest[offset]     & 0x7F) << 24)
            | ((digest[offset + 1] & 0xFF) << 16)
            | ((digest[offset + 2] & 0xFF) << 8)
            | (digest[offset + 3]  & 0xFF)
        )
        return str(code % (10 ** self.cfg.digits)).zfill(self.cfg.digits)


# ------------------------------------------------------------------ #
#  Convenience: generate a cryptographically secure server secret    #
# ------------------------------------------------------------------ #

def generate_secret_key(length: int = 32) -> str:
    """
    Generate a random base64-encoded secret key for OTPConfig.

    Store this in your environment variables — never hard-code it.
    """
    return base64.urlsafe_b64encode(secrets.token_bytes(length)).decode()
