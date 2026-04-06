"""
Test Suite — Stateless OTP Service
------------------------------------
Run:  pytest tests/ -v
"""

import time
import base64
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.otp      import OTPEngine, OTPConfig, generate_secret_key
from core.token    import TokenIssuer, TokenConfig, TokenExpiredError, TokenInvalidError
from core.ratelimit import RateLimiter, RateLimitConfig
from core.delivery import ConsoleBackend
from core.service  import OTPService, OTPServiceConfig


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

SECRET = b"test-secret-key-32bytes-exactly!!"

@pytest.fixture
def engine():
    return OTPEngine(OTPConfig(
        digits=6,
        window_seconds=30,
        drift_tolerance=1,
        secret_key=SECRET,
    ))


@pytest.fixture
def token_issuer():
    return TokenIssuer(TokenConfig(
        jwt_secret="test-jwt-secret",
        access_token_ttl=60,
    ))


@pytest.fixture
def service():
    return OTPService(
        config=OTPServiceConfig(
            otp_secret_key=SECRET,
            jwt_secret="test-jwt-secret",
            otp_window_seconds=30,
            rate_limit=RateLimitConfig(
                request_otp_max=100,
                verify_otp_max=100,
            ),
        ),
        delivery=ConsoleBackend(),
    )


# ──────────────────────────────────────────────────────────────────────────────
# OTP Engine Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestOTPEngine:
    def test_generates_correct_digit_count(self, engine):
        otp = engine.generate("user@test.com")
        assert len(otp) == 6
        assert otp.isdigit()

    def test_same_identity_same_window_same_otp(self, engine):
        ts = time.time()
        otp1 = engine.generate("user@test.com", at_time=ts)
        otp2 = engine.generate("user@test.com", at_time=ts + 5)
        assert otp1 == otp2

    def test_different_identities_different_otps(self, engine):
        ts = time.time()
        otp1 = engine.generate("alice@test.com", at_time=ts)
        otp2 = engine.generate("bob@test.com",   at_time=ts)
        assert otp1 != otp2

    def test_different_windows_different_otps(self, engine):
        ts = time.time()
        window = engine.cfg.window_seconds
        otp1 = engine.generate("user@test.com", at_time=ts)
        otp2 = engine.generate("user@test.com", at_time=ts + window * 5)
        assert otp1 != otp2

    def test_verify_valid_otp(self, engine):
        ts  = time.time()
        otp = engine.generate("user@test.com", at_time=ts)
        assert engine.verify("user@test.com", otp, at_time=ts)

    def test_verify_wrong_otp_fails(self, engine):
        ts  = time.time()
        otp = engine.generate("user@test.com", at_time=ts)
        wrong = str((int(otp) + 1) % (10 ** engine.cfg.digits)).zfill(engine.cfg.digits)
        assert not engine.verify("user@test.com", wrong, at_time=ts)

    def test_verify_expired_otp_fails(self, engine):
        ts  = time.time()
        otp = engine.generate("user@test.com", at_time=ts)
        # Advance 3 windows beyond drift_tolerance
        future = ts + engine.cfg.window_seconds * (engine.cfg.drift_tolerance + 2)
        assert not engine.verify("user@test.com", otp, at_time=future)

    def test_verify_drift_tolerance(self, engine):
        """OTP generated near end of window should be valid at start of next."""
        window = engine.cfg.window_seconds
        # Generate near start of window N
        ts_gen = window * 100
        otp = engine.generate("user@test.com", at_time=ts_gen)
        # Verify in window N+1 (within drift_tolerance=1)
        ts_ver = ts_gen + window
        assert engine.verify("user@test.com", otp, at_time=ts_ver)

    def test_verify_rejects_non_numeric(self, engine):
        assert not engine.verify("user@test.com", "abcdef")

    def test_verify_rejects_wrong_length(self, engine):
        assert not engine.verify("user@test.com", "123")

    def test_remaining_seconds(self, engine):
        ts  = time.time()
        rem = engine.remaining_seconds(at_time=ts)
        assert 0 < rem <= engine.cfg.window_seconds

    def test_custom_digit_count(self):
        eng = OTPEngine(OTPConfig(digits=8, window_seconds=60, secret_key=SECRET))
        otp = eng.generate("user")
        assert len(otp) == 8

    def test_zero_pad(self):
        """OTP must be zero-padded to exact digit length."""
        # Run many iterations — a small OTP value will eventually be generated
        for i in range(200):
            ts  = time.time() + i * 0.001
            otp = engine_4().generate(f"pad-test-{i}", at_time=ts)
            assert len(otp) == 4


def engine_4():
    return OTPEngine(OTPConfig(digits=4, window_seconds=30, secret_key=SECRET))


# ──────────────────────────────────────────────────────────────────────────────
# Token Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestTokenIssuer:
    def test_issue_and_verify(self, token_issuer):
        token  = token_issuer.issue("alice@test.com")
        claims = token_issuer.verify(token)
        assert claims["sub"] == "alice@test.com"

    def test_extra_claims(self, token_issuer):
        token  = token_issuer.issue("alice", role="admin")
        claims = token_issuer.verify(token)
        assert claims["role"] == "admin"

    def test_expired_token_raises(self):
        issuer = TokenIssuer(TokenConfig(jwt_secret="s", access_token_ttl=-1))
        token  = issuer.issue("user")
        with pytest.raises(TokenExpiredError):
            issuer.verify(token)

    def test_tampered_token_raises(self, token_issuer):
        token = token_issuer.issue("user")
        parts = token.split(".")
        parts[1] = parts[1][:-1] + ("A" if parts[1][-1] != "A" else "B")
        with pytest.raises(TokenInvalidError):
            token_issuer.verify(".".join(parts))

    def test_wrong_secret_raises(self, token_issuer):
        token  = token_issuer.issue("user")
        other  = TokenIssuer(TokenConfig(jwt_secret="wrong-secret"))
        with pytest.raises(TokenInvalidError):
            other.verify(token)

    def test_token_structure(self, token_issuer):
        token = token_issuer.issue("user")
        assert token.count(".") == 2


# ──────────────────────────────────────────────────────────────────────────────
# Rate Limiter Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestRateLimiter:
    def test_allows_under_limit(self):
        rl = RateLimiter(RateLimitConfig(request_otp_max=3, request_otp_window=60))
        for _ in range(3):
            result = rl.check_request_otp("user")
            assert result.allowed

    def test_blocks_over_limit(self):
        rl = RateLimiter(RateLimitConfig(request_otp_max=2, request_otp_window=60))
        rl.check_request_otp("user")
        rl.check_request_otp("user")
        result = rl.check_request_otp("user")
        assert not result.allowed
        assert result.retry_after > 0

    def test_independent_keys(self):
        rl = RateLimiter(RateLimitConfig(request_otp_max=1, request_otp_window=60))
        rl.check_request_otp("alice")
        rl.check_request_otp("alice")   # over limit
        result = rl.check_request_otp("bob")   # different key
        assert result.allowed


# ──────────────────────────────────────────────────────────────────────────────
# OTPService Integration Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestOTPService:
    def test_request_and_verify_flow(self, service, capsys):
        req = service.request_otp("alice@test.com")
        assert req.success

        # Extract OTP printed by ConsoleBackend
        captured = capsys.readouterr()
        otp = [line.split(": ")[1].strip() for line in captured.out.split("\n") if ": " in line and line.strip()[0].isdigit() or (": " in line and line.split(": ")[1].strip().isdigit())]
        # Alternative: generate the expected OTP directly
        otp_val = service._engine.generate("alice@test.com")

        result = service.verify_otp("alice@test.com", otp_val)
        assert result.success
        assert result.access_token is not None

    def test_invalid_otp_fails(self, service):
        service.request_otp("bob@test.com")
        result = service.verify_otp("bob@test.com", "000000")
        assert not result.success
        assert result.access_token is None

    def test_issued_token_is_introspectable(self, service):
        otp = service._engine.generate("carol@test.com")
        result = service.verify_otp("carol@test.com", otp)
        assert result.success

        claims = service.introspect_token(result.access_token)
        assert claims["sub"] == "carol@test.com"

    def test_rate_limit_on_request(self):
        svc = OTPService(
            config=OTPServiceConfig(
                otp_secret_key=SECRET,
                jwt_secret="s",
                rate_limit=RateLimitConfig(request_otp_max=2, request_otp_window=600),
            ),
            delivery=ConsoleBackend(),
        )
        svc.request_otp("x@test.com")
        svc.request_otp("x@test.com")
        result = svc.request_otp("x@test.com")  # 3rd → blocked
        assert not result.success
        assert result.error == "rate_limit_exceeded"


# ──────────────────────────────────────────────────────────────────────────────
# generate_secret_key utility
# ──────────────────────────────────────────────────────────────────────────────

def test_generate_secret_key_is_base64():
    key = generate_secret_key()
    decoded = base64.urlsafe_b64decode(key + "==")
    assert len(decoded) == 32


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
