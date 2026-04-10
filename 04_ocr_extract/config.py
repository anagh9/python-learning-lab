import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


class Config:
    """Base configuration for the OCR extractor app."""

    SECRET_KEY = os.environ.get("SECRET_KEY", "development-secret-key")
    MAX_CONTENT_LENGTH = 1000 * 1024 * 1024  # Limit uploads to 100 MB
    UPLOAD_FOLDER = str(BASE_DIR / "tmp" / "uploads")
    ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "bmp", "tiff", "tif", "gif", "webp"}
    TESSERACT_COMMAND = os.environ.get("TESSERACT_COMMAND", "tesseract")
