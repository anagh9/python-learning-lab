from http import HTTPStatus

from flask import Flask, render_template

from app.routes import main_bp


def create_app(test_config: dict | None = None) -> Flask:
    """Application factory for the OCR extractor web app."""
    app = Flask(__name__)
    app.config.from_object("config.Config")
    if test_config:
        app.config.update(test_config)
    app.register_blueprint(main_bp)

    @app.errorhandler(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    def handle_file_too_large(error):
        return (
            render_template(
                "index.html",
                error_message="Upload size exceeded the 16 MB limit. Please use smaller files.",
            ),
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
        )

    return app
