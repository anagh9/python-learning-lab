"""
FastAPI Integration Tests
--------------------------
Run: pytest tests/test_api.py -v
Requires: pip install httpx fastapi
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

os.environ.setdefault("OTP_SECRET_KEY", "dGVzdC1zZWNyZXQta2V5LTMyYnl0ZXMhISEhISEh")
os.environ.setdefault("JWT_SECRET", "test-jwt-secret-for-integration-tests")
os.environ.setdefault("DELIVERY_BACKEND", "console")

import pytest
from fastapi.testclient import TestClient
from api.main import app, _service


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def _get_valid_otp(identity: str) -> str:
    return _service._engine.generate(identity)


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestOTPRequestEndpoint:
    def test_valid_request(self, client):
        resp = client.post("/api/v1/otp/request", json={"identity": "user@test.com"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["ttl_seconds"] > 0

    def test_missing_identity(self, client):
        resp = client.post("/api/v1/otp/request", json={})
        assert resp.status_code == 422

    def test_empty_identity(self, client):
        resp = client.post("/api/v1/otp/request", json={"identity": ""})
        assert resp.status_code == 422


class TestOTPVerifyEndpoint:
    def test_valid_verify(self, client):
        identity = "verify@test.com"
        otp = _get_valid_otp(identity)
        resp = client.post("/api/v1/otp/verify", json={"identity": identity, "otp": otp})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["access_token"] is not None
        assert data["token_type"] == "bearer"

    def test_invalid_otp(self, client):
        resp = client.post("/api/v1/otp/verify", json={"identity": "x@test.com", "otp": "000000"})
        assert resp.status_code == 400

    def test_non_numeric_otp(self, client):
        resp = client.post("/api/v1/otp/verify", json={"identity": "x@test.com", "otp": "abcdef"})
        assert resp.status_code == 422

    def test_short_otp(self, client):
        resp = client.post("/api/v1/otp/verify", json={"identity": "x@test.com", "otp": "12"})
        assert resp.status_code == 422


class TestTokenIntrospectEndpoint:
    def test_introspect_valid_token(self, client):
        identity = "intro@test.com"
        otp  = _get_valid_otp(identity)
        resp = client.post("/api/v1/otp/verify", json={"identity": identity, "otp": otp})
        token = resp.json()["access_token"]

        resp2 = client.post("/api/v1/token/introspect", json={"token": token})
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["active"] is True
        assert data["claims"]["sub"] == identity

    def test_introspect_garbage_token(self, client):
        resp = client.post("/api/v1/token/introspect", json={"token": "not.a.token"})
        assert resp.status_code == 200
        assert resp.json()["active"] is False

    def test_bearer_header_verify(self, client):
        identity = "bearer@test.com"
        otp  = _get_valid_otp(identity)
        resp = client.post("/api/v1/otp/verify", json={"identity": identity, "otp": otp})
        token = resp.json()["access_token"]

        resp2 = client.get("/api/v1/token/verify", headers={"Authorization": f"Bearer {token}"})
        assert resp2.status_code == 200
        assert resp2.json()["active"] is True

    def test_bearer_header_missing(self, client):
        resp = client.get("/api/v1/token/verify")
        assert resp.status_code == 401


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
