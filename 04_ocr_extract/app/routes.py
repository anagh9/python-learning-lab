from http import HTTPStatus

from flask import Blueprint, current_app, render_template, request

from app.services.ocr import OCRProcessingError, extract_text_from_uploads
from app.utils.text import build_combined_output

main_bp = Blueprint("main", __name__)


@main_bp.get("/")
def index():
    """Render the main upload interface."""
    return render_template("index.html")


@main_bp.post("/extract")
def extract():
    """Handle multi-file image uploads and return extracted text."""
    uploaded_files = request.files.getlist("images")

    if not uploaded_files or all(not file.filename for file in uploaded_files):
        return (
            render_template(
                "index.html",
                error_message="Please choose at least one image to process.",
            ),
            HTTPStatus.BAD_REQUEST,
        )

    try:
        extraction_results = extract_text_from_uploads(
            uploaded_files,
            upload_folder=current_app.config["UPLOAD_FOLDER"],
            allowed_extensions=current_app.config["ALLOWED_EXTENSIONS"],
            tesseract_command=current_app.config["TESSERACT_COMMAND"],
        )
    except OCRProcessingError as exc:
        return (
            render_template("index.html", error_message=str(exc)),
            HTTPStatus.BAD_REQUEST,
        )

    return render_template(
        "index.html",
        extraction_results=extraction_results,
        combined_extraction_text=build_combined_output(extraction_results),
    )
