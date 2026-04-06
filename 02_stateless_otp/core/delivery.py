"""
OTP Delivery Backends
----------------------
Pluggable delivery layer. Implement DeliveryBackend and pass it to the
OTPService. Ships with:
  - ConsoleBackend   (development / testing)
  - SMTPBackend      (email via SMTP)
  - TwilioBackend    (SMS via Twilio REST API)
  - WebhookBackend   (POST to any external endpoint)
"""

from __future__ import annotations

import abc
import smtplib
import urllib.request
import urllib.parse
import json
import ssl
import logging
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Abstract base
# ──────────────────────────────────────────────────────────────────────────────

class DeliveryBackend(abc.ABC):
    """Override send() to integrate any delivery channel."""

    @abc.abstractmethod
    def send(self, destination: str, otp: str, ttl_seconds: int) -> bool:
        """
        Deliver *otp* to *destination*.

        Args:
            destination: Email address, phone number, or channel-specific id.
            otp:         The OTP string to deliver.
            ttl_seconds: How many seconds before the OTP expires.

        Returns:
            True if delivery succeeded, False otherwise.
        """


# ──────────────────────────────────────────────────────────────────────────────
# Console (dev / testing)
# ──────────────────────────────────────────────────────────────────────────────

class ConsoleBackend(DeliveryBackend):
    """Prints OTP to stdout — perfect for local dev and CI."""

    def send(self, destination: str, otp: str, ttl_seconds: int) -> bool:
        minutes = ttl_seconds // 60
        print(
            f"\n{'='*50}\n"
            f"  OTP for {destination}: {otp}\n"
            f"  Valid for {minutes} minute(s) ({ttl_seconds}s)\n"
            f"{'='*50}\n"
        )
        return True


# ──────────────────────────────────────────────────────────────────────────────
# SMTP Email
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class SMTPConfig:
    host: str
    port: int = 587
    username: str = ""
    password: str = ""
    from_address: str = ""
    use_tls: bool = True
    subject_template: str = "Your verification code: {otp}"
    body_template: str = (
        "Your one-time verification code is:\n\n"
        "    {otp}\n\n"
        "This code expires in {minutes} minute(s).\n"
        "Do not share this code with anyone."
    )


class SMTPBackend(DeliveryBackend):
    """Send OTP via SMTP (works with Gmail, SendGrid SMTP, Mailgun, SES, etc.)."""

    def __init__(self, config: SMTPConfig):
        self.cfg = config

    def send(self, destination: str, otp: str, ttl_seconds: int) -> bool:
        minutes = max(1, ttl_seconds // 60)
        subject = self.cfg.subject_template.format(otp=otp)
        body    = self.cfg.body_template.format(otp=otp, minutes=minutes)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.cfg.from_address
        msg["To"]      = destination
        msg.attach(MIMEText(body, "plain"))

        # Simple HTML version
        html_body = f"""
        <html><body style="font-family:sans-serif;max-width:480px;margin:auto">
          <h2 style="color:#1a1a2e">Verification Code</h2>
          <p style="font-size:36px;font-weight:bold;letter-spacing:8px;
                    background:#f4f4f4;padding:16px;border-radius:8px;text-align:center">
            {otp}
          </p>
          <p style="color:#666">This code expires in <strong>{minutes} minute(s)</strong>.</p>
          <p style="color:#999;font-size:12px">
            If you didn't request this, please ignore this email.
          </p>
        </body></html>
        """
        msg.attach(MIMEText(html_body, "html"))

        try:
            ctx = ssl.create_default_context()
            with smtplib.SMTP(self.cfg.host, self.cfg.port) as server:
                if self.cfg.use_tls:
                    server.starttls(context=ctx)
                if self.cfg.username:
                    server.login(self.cfg.username, self.cfg.password)
                server.sendmail(self.cfg.from_address, destination, msg.as_string())
            logger.info("OTP email sent to %s", destination)
            return True
        except Exception as exc:
            logger.error("SMTP delivery failed: %s", exc)
            return False


# ──────────────────────────────────────────────────────────────────────────────
# Twilio SMS
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TwilioConfig:
    account_sid: str
    auth_token: str
    from_number: str
    message_template: str = "Your verification code is {otp}. Valid for {minutes} min."


class TwilioBackend(DeliveryBackend):
    """Send OTP via Twilio SMS (no twilio SDK dependency — pure stdlib)."""

    _API_URL = "https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"

    def __init__(self, config: TwilioConfig):
        self.cfg = config

    def send(self, destination: str, otp: str, ttl_seconds: int) -> bool:
        minutes = max(1, ttl_seconds // 60)
        body    = self.cfg.message_template.format(otp=otp, minutes=minutes)
        url     = self._API_URL.format(sid=self.cfg.account_sid)

        data = urllib.parse.urlencode({
            "To":   destination,
            "From": self.cfg.from_number,
            "Body": body,
        }).encode()

        pw_mgr   = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        pw_mgr.add_password(None, url, self.cfg.account_sid, self.cfg.auth_token)
        handler  = urllib.request.HTTPBasicAuthHandler(pw_mgr)
        opener   = urllib.request.build_opener(handler)

        try:
            req = urllib.request.Request(url, data=data, method="POST")
            with opener.open(req, timeout=10) as resp:
                result = json.loads(resp.read())
                logger.info("Twilio SMS sent: SID=%s", result.get("sid"))
                return True
        except Exception as exc:
            logger.error("Twilio delivery failed: %s", exc)
            return False


# ──────────────────────────────────────────────────────────────────────────────
# Generic Webhook (integrate with any service: Postmark, Vonage, etc.)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class WebhookConfig:
    url: str
    headers: dict = field(default_factory=dict)
    payload_template: Optional[dict] = None   # None → use default


class WebhookBackend(DeliveryBackend):
    """POST OTP data to any HTTP webhook endpoint."""

    def __init__(self, config: WebhookConfig):
        self.cfg = config

    def send(self, destination: str, otp: str, ttl_seconds: int) -> bool:
        payload = (self.cfg.payload_template or {}).copy()
        if not payload:
            payload = {
                "destination": destination,
                "otp": otp,
                "ttl_seconds": ttl_seconds,
            }
        else:
            # Simple placeholder substitution in custom templates
            raw = json.dumps(payload)
            raw = raw.replace("{destination}", destination)
            raw = raw.replace("{otp}", otp)
            raw = raw.replace("{ttl_seconds}", str(ttl_seconds))
            payload = json.loads(raw)

        body = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json", **self.cfg.headers}

        req = urllib.request.Request(self.cfg.url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("Webhook OTP delivery: status=%s", resp.status)
                return resp.status < 300
        except Exception as exc:
            logger.error("Webhook delivery failed: %s", exc)
            return False
