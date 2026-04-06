/**
 * Stateless OTP Service — JavaScript SDK
 * ----------------------------------------
 * Works in both Node.js and the browser (fetch API required).
 *
 * Usage (Node.js):
 *   const { OTPClient } = require('./stateless-otp-sdk');
 *   const client = new OTPClient({ baseUrl: 'http://localhost:8000/api/v1' });
 *   await client.requestOTP('user@example.com');
 *   const { accessToken } = await client.verifyOTP('user@example.com', '123456');
 *
 * Usage (browser):
 *   <script src="stateless-otp-sdk.js"></script>
 *   <script>
 *     const client = new OTPClient({ baseUrl: '/api/v1' });
 *   </script>
 */

class OTPError extends Error {
  constructor(message, code, statusCode) {
    super(message);
    this.name = 'OTPError';
    this.code = code;
    this.statusCode = statusCode;
  }
}

class OTPClient {
  /**
   * @param {object} options
   * @param {string} options.baseUrl       - e.g. 'http://localhost:8000/api/v1'
   * @param {string} [options.accessToken] - Bearer token for authenticated requests
   * @param {number} [options.timeout]     - Fetch timeout in ms (default: 10000)
   */
  constructor({ baseUrl, accessToken = null, timeout = 10_000 }) {
    this.baseUrl     = baseUrl.replace(/\/$/, '');
    this.accessToken = accessToken;
    this.timeout     = timeout;
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Step 1: Request an OTP to be sent to the user.
   *
   * @param {string} identity - Email, phone, or user ID
   * @returns {{ success, message, ttlSeconds, rateLimitRemaining }}
   */
  async requestOTP(identity) {
    const data = await this._post('/otp/request', { identity });
    return {
      success:            data.success,
      message:            data.message,
      ttlSeconds:         data.ttl_seconds,
      rateLimitRemaining: data.rate_limit_remaining,
    };
  }

  /**
   * Step 2: Submit the OTP to verify identity and receive an access token.
   *
   * @param {string} identity - Same identity used in requestOTP()
   * @param {string} otp      - OTP entered by the user
   * @returns {{ success, accessToken, tokenType, expiresIn }}
   */
  async verifyOTP(identity, otp) {
    const data = await this._post('/otp/verify', { identity, otp });
    if (this.accessToken !== undefined && data.access_token) {
      this.accessToken = data.access_token;   // auto-store
    }
    return {
      success:     data.success,
      message:     data.message,
      accessToken: data.access_token,
      tokenType:   data.token_type,
      expiresIn:   data.expires_in,
    };
  }

  /**
   * Introspect a JWT access token.
   *
   * @param {string} token
   * @returns {{ active, claims }}
   */
  async introspectToken(token) {
    const data = await this._post('/token/introspect', { token });
    return { active: data.active, claims: data.claims };
  }

  /**
   * Verify the stored Bearer token via the Authorization header endpoint.
   *
   * @returns {{ active, claims }}
   */
  async verifyBearer() {
    const data = await this._get('/token/verify');
    return { active: data.active, claims: data.claims };
  }

  // ── Internals ─────────────────────────────────────────────────────────────

  async _post(path, body) {
    return this._request('POST', path, body);
  }

  async _get(path) {
    return this._request('GET', path, null);
  }

  async _request(method, path, body) {
    const url     = `${this.baseUrl}${path}`;
    const headers = { 'Content-Type': 'application/json' };

    if (this.accessToken) {
      headers['Authorization'] = `Bearer ${this.accessToken}`;
    }

    const controller = new AbortController();
    const timer      = setTimeout(() => controller.abort(), this.timeout);

    try {
      const resp = await fetch(url, {
        method,
        headers,
        body:   body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      const data = await resp.json();

      if (!resp.ok) {
        throw new OTPError(
          data.message || 'Request failed',
          data.error   || 'unknown_error',
          resp.status,
        );
      }

      return data;
    } catch (err) {
      if (err.name === 'AbortError') {
        throw new OTPError('Request timed out', 'timeout', 0);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }
}

// ── Module exports ────────────────────────────────────────────────────────────

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { OTPClient, OTPError };
} else {
  window.OTPClient = OTPClient;
  window.OTPError  = OTPError;
}
