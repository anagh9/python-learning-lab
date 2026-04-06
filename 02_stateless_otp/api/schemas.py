"""Pydantic schemas for request and response validation."""

from pydantic import BaseModel, Field, field_validator
import re


# ──────────────────────────────────────────────────────────────────────────────
# Request schemas
# ──────────────────────────────────────────────────────────────────────────────

class OTPRequestBody(BaseModel):
    """Body for POST /otp/request"""
    identity: str = Field(
        ...,
        min_length=1,
        max_length=320,
        description="User identifier — email address, phone number, or user ID.",
        examples=["user@example.com"],
    )

    @field_validator("identity")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class OTPVerifyBody(BaseModel):
    """Body for POST /otp/verify"""
    identity: str = Field(..., min_length=1, max_length=320)
    otp: str = Field(..., min_length=4, max_length=10, pattern=r"^\d+$")

    @field_validator("identity")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class TokenIntrospectBody(BaseModel):
    """Body for POST /token/introspect"""
    token: str = Field(..., min_length=1)


# ──────────────────────────────────────────────────────────────────────────────
# Response schemas
# ──────────────────────────────────────────────────────────────────────────────

class OTPRequestResponse(BaseModel):
    success: bool
    message: str
    ttl_seconds: int = 0
    rate_limit_remaining: int = 0


class OTPVerifyResponse(BaseModel):
    success: bool
    message: str
    access_token: str | None = None
    token_type: str = "bearer"
    expires_in: int = 0
    rate_limit_remaining: int = 0


class TokenIntrospectResponse(BaseModel):
    active: bool
    claims: dict | None = None


class ErrorResponse(BaseModel):
    error: str
    message: str
