from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

import database


ALLOWED_EXTENSIONS = {".png", ".jpg", ".jpeg"}
MAX_IMAGE_SIZE_BYTES = 100 * 1024
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
JPEG_SIGNATURE = b"\xff\xd8\xff"


class ImageServiceError(Exception):
    """Base error for image service operations."""


class ImageValidationError(ImageServiceError):
    """Raised when an uploaded image fails validation."""


class ImageNotFoundError(ImageServiceError):
    """Raised when the requested image does not exist."""


class ImageTooLargeError(ImageValidationError):
    """Raised when an uploaded image exceeds the allowed size."""


class UnsupportedImageError(ImageValidationError):
    """Raised when an uploaded image type is not supported."""


FORMAT_TO_MIMETYPE = {
    "png": "image/png",
    "jpeg": "image/jpeg",
}


def detect_image_format(image_bytes: bytes) -> str | None:
    if image_bytes.startswith(PNG_SIGNATURE):
        return "png"
    if image_bytes.startswith(JPEG_SIGNATURE):
        return "jpeg"
    return None


def extension_to_format(extension: str) -> str | None:
    normalized = extension.lower()
    if normalized == ".png":
        return "png"
    if normalized in {".jpg", ".jpeg"}:
        return "jpeg"
    return None


def validate_and_read_image(
    file_storage: FileStorage | None,
    max_size_bytes: int = MAX_IMAGE_SIZE_BYTES,
) -> tuple[str, bytes, str]:
    if file_storage is None:
        raise ImageValidationError("No image file was provided.")

    if not file_storage.filename:
        raise ImageValidationError("Please choose an image file to upload.")

    safe_name = secure_filename(file_storage.filename)
    extension = Path(safe_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise UnsupportedImageError("Only PNG, JPG, and JPEG files are allowed.")

    image_bytes = file_storage.read()
    if not image_bytes:
        raise ImageValidationError("The uploaded image is empty.")

    if len(image_bytes) > max_size_bytes:
        raise ImageTooLargeError(
            f"Image must be smaller than {max_size_bytes // 1024}KB."
        )

    detected_format = detect_image_format(image_bytes)
    expected_format = extension_to_format(extension)
    if detected_format is None or detected_format != expected_format:
        raise UnsupportedImageError(
            "File content does not match a supported PNG or JPEG image."
        )

    mimetype = FORMAT_TO_MIMETYPE[detected_format]
    return safe_name, image_bytes, mimetype


def store_uploaded_image(
    db_path: str | Path,
    file_storage: FileStorage | None,
    max_size_bytes: int = MAX_IMAGE_SIZE_BYTES,
) -> database.ImageRecord:
    name, image_bytes, _mimetype = validate_and_read_image(
        file_storage,
        max_size_bytes=max_size_bytes,
    )
    image_id = database.insert_image(db_path, name=name, image_data=image_bytes)
    record = database.get_image_by_id(db_path, image_id)
    if record is None:
        raise ImageServiceError("Image was stored but could not be reloaded.")
    return record


def fetch_image(
    db_path: str | Path,
    image_id: int | None = None,
    name: str | None = None,
) -> database.ImageRecord:
    if image_id is None and not name:
        raise ImageValidationError("Provide either an image ID or a name.")

    record = None
    if image_id is not None:
        record = database.get_image_by_id(db_path, image_id)
    elif name:
        record = database.get_image_by_name(db_path, name)

    if record is None:
        raise ImageNotFoundError("Image not found.")
    return record


def list_uploaded_images(db_path: str | Path, limit: int = 24) -> list[database.ImageSummary]:
    return database.list_images(db_path, limit=limit)


def infer_mimetype(filename: str, image_bytes: bytes | None = None) -> str:
    extension = Path(filename).suffix.lower()
    expected_format = extension_to_format(extension)
    if expected_format is not None:
        return FORMAT_TO_MIMETYPE[expected_format]

    if image_bytes:
        detected_format = detect_image_format(image_bytes)
        if detected_format is not None:
            return FORMAT_TO_MIMETYPE[detected_format]

    return "application/octet-stream"


def image_bytes_io(image_record: database.ImageRecord) -> BytesIO:
    stream = BytesIO(image_record.image_data)
    stream.seek(0)
    return stream


def save_image_to_disk(
    image_record: database.ImageRecord,
    output_path: str | Path,
) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(image_record.image_data)
    return output_path


def serialize_summary(summary: database.ImageSummary) -> dict[str, Any]:
    return {
        "id": summary.id,
        "name": summary.name,
        "created_at": summary.created_at,
    }
