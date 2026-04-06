"""
Stateless OTP — Python SDK Client
------------------------------------
A thin HTTP client for services that call the OTP API over the network
(e.g. a Django/Flask backend that does NOT run the FastAPI service in-process).

If you ARE running everything in Python, prefer importing core.service directly
(zero network hop, no serialization overhead).

Usage:
    from sdk.python.otp_client import OTPClient, OTPError

    client = OTPClient(base_url="http://otp-service:8000/api/v1")
    client.request_otp("user@example.com")
    result = client.verify_otp("user@example.com", "123456")
    print(result.access_token)
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, Optional


class OTPError(Exception):
    def __init__(self, message: str, code: str = "unknown", status_code: int = 0):
        super().__init__(message)
        self.code        = code
        self.status_code = status_code


@dataclass
class OTPRequestResult:
    success: bool
    message: str
    ttl_seconds: int
    rate_limit_remaining: int


@dataclass
class OTPVerifyResult:
    success: bool
    message: str
    access_token: Optional[str]
    token_type: str
    expires_in: int
    rate_limit_remaining: int


@dataclass
class IntrospectResult:
    active: bool
    claims: Optional[Dict[str, Any]]


class OTPClient:
    """
    HTTP client for the Stateless OTP Service REST API.

    Thread-safe (no shared mutable state beyond base_url and timeout).
    """

    def __init__(
        self,
        base_url: str,
        access_token: Optional[str] = None,
        timeout: int = 10,
    ):
        self.base_url     = base_url.rstrip("/")
        self.access_token = access_token
        self.timeout      = timeout

    # ── Public API ────────────────────────────────────────────────────────────

    def request_otp(self, identity: str) -> OTPRequestResult:
        data = self._post("/otp/request", {"identity": identity})
        return OTPRequestResult(
            success=data["success"],
            message=data["message"],
            ttl_seconds=data["ttl_seconds"],
            rate_limit_remaining=data["rate_limit_remaining"],
        )

    def verify_otp(self, identity: str, otp: str) -> OTPVerifyResult:
        data = self._post("/otp/verify", {"identity": identity, "otp": otp})
        if data.get("access_token"):
            self.access_token = data["access_token"]   # auto-store
        return OTPVerifyResult(
            success=data["success"],
            message=data["message"],
            access_token=data.get("access_token"),
            token_type=data.get("token_type", "bearer"),
            expires_in=data.get("expires_in", 0),
            rate_limit_remaining=data.get("rate_limit_remaining", 0),
        )

    def introspect_token(self, token: str) -> IntrospectResult:
        data = self._post("/token/introspect", {"token": token})
        return IntrospectResult(active=data["active"], claims=data.get("claims"))

    def verify_bearer(self) -> IntrospectResult:
        data = self._get("/token/verify")
        return IntrospectResult(active=data["active"], claims=data.get("claims"))

    # ── Internals ─────────────────────────────────────────────────────────────

    def _post(self, path: str, body: dict) -> dict:
        return self._request("POST", path, body)

    def _get(self, path: str) -> dict:
        return self._request("GET", path, None)

    def _request(self, method: str, path: str, body: Optional[dict]) -> dict:
        url     = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        data = json.dumps(body).encode() if body else None
        req  = urllib.request.Request(url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            try:
                err_body = json.loads(exc.read())
            except Exception:
                err_body = {}
            raise OTPError(
                err_body.get("message", str(exc)),
                err_body.get("error", "http_error"),
                exc.code,
            ) from exc
        except urllib.error.URLError as exc:
            raise OTPError(str(exc), "connection_error", 0) from exc
