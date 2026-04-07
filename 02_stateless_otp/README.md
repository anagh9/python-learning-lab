# Stateless OTP Service

A **zero-database** OTP authentication service built in Python. Works as an embedded library or a standalone REST API that any tech stack can call.

## How it works (stateless)

```
OTP = HMAC-SHA256(server_secret + identity + time_window)[:6 digits]
```

- **No database writes** — the OTP is derived deterministically from the server secret and the current 5-minute time window.
- **Verification** re-derives the same value and does a constant-time comparison.
- **Clock skew** is handled by checking ±1 adjacent windows (`drift_tolerance`).
- A **signed JWT** is issued after successful verification — also stateless.

---

## Project Structure

```
stateless-otp/
├── core/
│   ├── otp.py          # OTPEngine — stateless HMAC-TOTP generation + verification
│   ├── token.py        # TokenIssuer — HS256 JWT issuer/verifier (no PyJWT dependency)
│   ├── ratelimit.py    # RateLimiter — sliding-window abuse prevention
│   ├── delivery.py     # Pluggable backends: Console, SMTP, Twilio, Webhook
│   └── service.py      # OTPService — high-level orchestrator
├── api/
│   ├── main.py         # FastAPI app + config from env vars
│   ├── routes.py       # REST endpoints
│   └── schemas.py      # Pydantic request/response models
├── sdk/
│   ├── javascript/     # Browser + Node.js SDK (zero dependencies)
│   └── python/         # Python HTTP client SDK (stdlib only)
├── tests/
│   ├── test_otp.py     # 27 unit tests (engine, token, rate limiter, service)
│   └── test_api.py     # 12 integration tests (FastAPI endpoints)
├── .env.example        # All configurable environment variables
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Quick Start

### 1. Install

```bash
pip install -r requirements.txt
```

### 2. Generate secrets

```bash
# OTP secret
python -c "from core.otp import generate_secret_key; print(generate_secret_key())"

# JWT secret
python -c "import secrets; print(secrets.token_hex(32))"
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env with your secrets and delivery backend
```

### 4. Run the API

```bash
uvicorn api.main:app --reload
# → http://localhost:8000/docs  (Swagger UI)
```

### 5. Run tests

```bash
pytest tests/ -v
```

---

## REST API

### POST `/api/v1/otp/request`
Request an OTP to be sent to the user.

```json
// Request
{ "identity": "user@example.com" }

// Response 200
{ "success": true, "message": "OTP sent successfully.", "ttl_seconds": 247 }
```

### POST `/api/v1/otp/verify`
Verify the OTP and receive a JWT access token.

```json
// Request
{ "identity": "user@example.com", "otp": "482931" }

// Response 200
{
  "success": true,
  "access_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 900
}
```

### POST `/api/v1/token/introspect`
Validate a JWT and return its claims.

```json
// Request
{ "token": "eyJ..." }

// Response 200
{ "active": true, "claims": { "sub": "user@example.com", "exp": 1234567890 } }
```

### GET `/api/v1/token/verify`
Verify via `Authorization: Bearer <token>` header (nginx `auth_request` compatible).

---

## Embed as a Python Library

No HTTP overhead — use `OTPService` directly in Django, Flask, FastAPI, etc.

```python
from core.service  import OTPService, OTPServiceConfig
from core.delivery import SMTPBackend, SMTPConfig

svc = OTPService(
    config=OTPServiceConfig(
        otp_secret_key=b"your-32-byte-secret",
        jwt_secret="your-jwt-secret",
    ),
    delivery=SMTPBackend(SMTPConfig(
        host="smtp.gmail.com",
        username="you@gmail.com",
        password="app-password",
        from_address="you@gmail.com",
    )),
)

# Step 1 — send OTP
svc.request_otp("user@example.com")

# Step 2 — verify
result = svc.verify_otp("user@example.com", "482931")
if result.success:
    print(result.access_token)   # use this JWT
```

---

## JavaScript SDK

```javascript
const { OTPClient } = require('./sdk/javascript/stateless-otp-sdk');

const client = new OTPClient({ baseUrl: 'http://localhost:8000/api/v1' });

// Step 1
await client.requestOTP('user@example.com');

// Step 2
const { accessToken } = await client.verifyOTP('user@example.com', userInputOTP);

// Token is auto-stored on the client for subsequent requests
const { active, claims } = await client.verifyBearer();
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OTP_SECRET_KEY` | (required) | Base64 HMAC secret — run `generate_secret_key()` |
| `JWT_SECRET` | (required) | HS256 JWT signing secret |
| `OTP_DIGITS` | `6` | OTP length (4–10) |
| `OTP_WINDOW_SECONDS` | `300` | Time window per OTP (seconds) |
| `OTP_DRIFT_TOLERANCE` | `1` | Adjacent windows to accept |
| `ACCESS_TOKEN_TTL` | `900` | JWT lifetime (seconds) |
| `DELIVERY_BACKEND` | `console` | `console` / `smtp` / `twilio` |
| `SMTP_HOST` | — | SMTP server hostname |
| `TWILIO_ACCOUNT_SID` | — | Twilio account SID |
| `RATE_REQUEST_OTP_MAX` | `5` | Max OTP sends per 10 min |
| `RATE_VERIFY_OTP_MAX` | `10` | Max verify attempts per 10 min |

---

## Docker

```bash
# Build and run
docker-compose up --build

# With your own secrets
OTP_SECRET_KEY=xxx JWT_SECRET=yyy DELIVERY_BACKEND=smtp \
  SMTP_HOST=smtp.gmail.com SMTP_USERNAME=you@gmail.com \
  SMTP_PASSWORD=pw SMTP_FROM=you@gmail.com \
  docker-compose up
```

---

## Security Notes

- `OTP_SECRET_KEY` must stay server-side only. Rotating it invalidates all current OTPs.
- Use HTTPS in production — OTPs are single-use but travel in plaintext over HTTP.
- Rate limiting defaults block 5 OTP requests and 10 verify attempts per 10-minute window per identity.
- For multi-process deployments, swap the in-memory `RateLimiter` with a Redis backend by subclassing `RateLimiter`.
- The JWT verifier uses constant-time comparison (`hmac.compare_digest`) to prevent timing attacks.
