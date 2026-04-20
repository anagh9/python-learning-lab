from __future__ import annotations

import logging
from pathlib import Path

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.exceptions import RequestEntityTooLarge

import database
import service


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def create_app(config_overrides: dict | None = None) -> Flask:
    configure_logging()

    base_dir = Path(__file__).resolve().parent
    app = Flask(__name__)
    app.config.from_mapping(
        SECRET_KEY="image-store-dev-key",
        DATABASE=str(base_dir / "images.db"),
        MAX_IMAGE_SIZE_BYTES=service.MAX_IMAGE_SIZE_BYTES,
        MAX_CONTENT_LENGTH=250 * 1024,
        GALLERY_LIMIT=24,
    )

    if config_overrides:
        app.config.update(config_overrides)

    database.init_db(app.config["DATABASE"])
    logger = logging.getLogger("image_store")

    @app.route("/", methods=["GET"])
    def index():
        recent_images = service.list_uploaded_images(
            app.config["DATABASE"],
            limit=6,
        )
        return render_template(
            "index.html",
            recent_images=recent_images,
            max_size_kb=app.config["MAX_IMAGE_SIZE_BYTES"] // 1024,
        )

    @app.route("/upload", methods=["POST"])
    def upload_image():
        uploaded_file = request.files.get("image")
        try:
            image_record = service.store_uploaded_image(
                app.config["DATABASE"],
                uploaded_file,
                max_size_bytes=app.config["MAX_IMAGE_SIZE_BYTES"],
            )
            logger.info("Stored image id=%s name=%s", image_record.id, image_record.name)
            flash(
                f"Uploaded '{image_record.name}' successfully with ID {image_record.id}.",
                "success",
            )
            return redirect(url_for("view_images", image_id=image_record.id))
        except service.ImageValidationError as exc:
            logger.warning("Upload rejected: %s", exc)
            flash(str(exc), "error")
            return redirect(url_for("index"))
        except Exception:
            logger.exception("Unexpected error while uploading image")
            flash("Unexpected error while uploading image.", "error")
            return redirect(url_for("index"))

    @app.route("/image/<int:image_id>", methods=["GET"])
    def get_image(image_id: int):
        try:
            image_record = service.fetch_image(app.config["DATABASE"], image_id=image_id)
            return send_file(
                service.image_bytes_io(image_record),
                mimetype=service.infer_mimetype(
                    image_record.name,
                    image_record.image_data,
                ),
                download_name=image_record.name,
            )
        except service.ImageNotFoundError:
            abort(404)
        except Exception:
            logger.exception("Unexpected error while retrieving image id=%s", image_id)
            abort(500)

    @app.route("/view", methods=["GET"])
    def view_images():
        image_id = request.args.get("image_id", type=int)
        selected_image = None

        if image_id is not None:
            try:
                selected_image = service.fetch_image(app.config["DATABASE"], image_id=image_id)
            except service.ImageNotFoundError:
                flash(f"No image found with ID {image_id}.", "error")
            except Exception:
                logger.exception("Unexpected error while loading image view id=%s", image_id)
                flash("Unexpected error while loading the image.", "error")

        images = service.list_uploaded_images(
            app.config["DATABASE"],
            limit=app.config["GALLERY_LIMIT"],
        )
        return render_template(
            "view.html",
            images=images,
            selected_image=selected_image,
            selected_image_id=image_id,
        )

    @app.errorhandler(RequestEntityTooLarge)
    def handle_large_request(_error):
        flash("Upload request is too large. Keep images below 100KB.", "error")
        return redirect(url_for("index"))

    @app.errorhandler(404)
    def not_found(_error):
        return render_template("base.html", page_title="Not Found", inline_error="Resource not found."), 404

    @app.errorhandler(500)
    def internal_error(_error):
        return (
            render_template(
                "base.html",
                page_title="Server Error",
                inline_error="Something went wrong on the server.",
            ),
            500,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
