FROM python:3.12-slim

# Security: run as non-root
RUN addgroup --system otp && adduser --system --ingroup otp otp

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY core/ ./core/
COPY api/  ./api/

# Ownership
RUN chown -R otp:otp /app
USER otp

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
