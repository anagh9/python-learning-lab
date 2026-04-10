from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.utils.text import normalize_extracted_text


class OCRProcessingError(Exception):
    """Raised when uploaded files cannot be processed for OCR."""


@dataclass(frozen=True)
class OCRResult:
    """Structured OCR output for a single uploaded image."""

    filename: str
    extracted_text: str


def extract_text_from_uploads(
    uploaded_files: Iterable[FileStorage],
    upload_folder: str,
    allowed_extensions: set[str],
    tesseract_command: str,
) -> list[OCRResult]:
    """Extract text from each valid uploaded image file."""
    results: list[OCRResult] = []
    upload_path = Path(upload_folder)
    upload_path.mkdir(parents=True, exist_ok=True)

    for uploaded_file in uploaded_files:
        if not uploaded_file or not uploaded_file.filename:
            continue

        safe_filename = secure_filename(uploaded_file.filename)
        if not _is_allowed_file(safe_filename, allowed_extensions):
            raise OCRProcessingError(
                f"Unsupported file type for '{uploaded_file.filename}'. "
                "Please upload PNG, JPG, JPEG, BMP, TIFF, GIF, or WEBP images."
            )

        extracted_text = _run_ocr_on_upload(
            uploaded_file=uploaded_file,
            original_filename=safe_filename,
            upload_folder=upload_path,
            tesseract_command=tesseract_command,
        )
        results.append(
            OCRResult(
                filename=safe_filename,
                extracted_text=normalize_extracted_text(extracted_text),
            )
        )

    if not results:
        raise OCRProcessingError("No valid images were uploaded for OCR processing.")

    return results


def _is_allowed_file(filename: str, allowed_extensions: set[str]) -> bool:
    """Return whether the filename extension is supported."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in allowed_extensions


def _run_ocr_on_upload(
    uploaded_file: FileStorage,
    original_filename: str,
    upload_folder: Path,
    tesseract_command: str,
) -> str:
    """Persist an upload temporarily and invoke the Tesseract CLI."""
    suffix = Path(original_filename).suffix or ".png"

    with tempfile.NamedTemporaryFile(
        dir=upload_folder,
        suffix=suffix,
        delete=False,
    ) as temporary_file:
        temp_image_path = Path(temporary_file.name)

    try:
        uploaded_file.save(temp_image_path)
        completed_process = subprocess.run(
            [tesseract_command, str(temp_image_path), "stdout"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise OCRProcessingError("Failed to prepare the uploaded image for OCR.") from exc
    finally:
        temp_image_path.unlink(missing_ok=True)

    if completed_process.returncode != 0:
        error_output = completed_process.stderr.strip() or "Unknown OCR error."
        raise OCRProcessingError(
            f"OCR failed for '{original_filename}': {error_output}"
        )

    return completed_process.stdout
