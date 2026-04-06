"""
Rate Limiter
------------
Sliding-window in-memory rate limiter.

For single-process deployments this is ready to use.
For multi-process / distributed deployments swap the backend to Redis
by subclassing RateLimiter and overriding _get / _set / _incr.
"""

import time
from collections import defaultdict, deque
from threading import Lock
from dataclasses import dataclass
from typing import Dict, Deque


@dataclass
class RateLimitConfig:
    # OTP request limits
    request_otp_max: int = 5          # max OTP sends per window
    request_otp_window: int = 600     # 10 minutes

    # OTP verify attempt limits (prevents brute force)
    verify_otp_max: int = 10          # max verify attempts per window
    verify_otp_window: int = 600      # 10 minutes


class RateLimiter:
    """
    Sliding-window rate limiter backed by an in-memory dict.

    Thread-safe via a single lock. For production with multiple workers,
    replace with a Redis-backed implementation.
    """

    def __init__(self, config: RateLimitConfig):
        self.cfg = config
        self._windows: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check_request_otp(self, key: str) -> "RateLimitResult":
        return self._check(
            f"req:{key}",
            self.cfg.request_otp_max,
            self.cfg.request_otp_window,
        )

    def check_verify_otp(self, key: str) -> "RateLimitResult":
        return self._check(
            f"ver:{key}",
            self.cfg.verify_otp_max,
            self.cfg.verify_otp_window,
        )

    def _check(self, bucket: str, limit: int, window: int) -> "RateLimitResult":
        now = time.time()
        cutoff = now - window

        with self._lock:
            q = self._windows[bucket]

            # Drop timestamps outside the sliding window
            while q and q[0] < cutoff:
                q.popleft()

            remaining = limit - len(q)

            if remaining <= 0:
                retry_after = int(q[0] - cutoff) + 1
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    retry_after=retry_after,
                    limit=limit,
                    window=window,
                )

            # Record this request
            q.append(now)
            return RateLimitResult(
                allowed=True,
                remaining=remaining - 1,
                retry_after=0,
                limit=limit,
                window=window,
            )


class RateLimitResult:
    __slots__ = ("allowed", "remaining", "retry_after", "limit", "window")

    def __init__(self, allowed, remaining, retry_after, limit, window):
        self.allowed     = allowed
        self.remaining   = remaining
        self.retry_after = retry_after
        self.limit       = limit
        self.window      = window

    def headers(self) -> dict:
        """Standard X-RateLimit-* headers."""
        h = {
            "X-RateLimit-Limit":     str(self.limit),
            "X-RateLimit-Remaining": str(self.remaining),
            "X-RateLimit-Window":    str(self.window),
        }
        if not self.allowed:
            h["Retry-After"] = str(self.retry_after)
        return h
