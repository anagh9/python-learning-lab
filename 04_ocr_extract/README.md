# OCR Extractor Web App

OCR Extractor is a Flask-based web application that accepts multiple image uploads, runs OCR on each image with Tesseract, and presents the extracted text in a clean, copyable interface. Each image result is displayed in its own section so text from different uploads stays clearly separated.

## Project Overview

- Upload multiple image files in a single request
- Extract text from each image using the `tesseract` CLI
- Normalize OCR output for cleaner spacing and readability
- Copy extracted text from per-image result panels
- Download a single `.txt` file containing text from all processed images
- Use a lightweight Tailwind CSS interface for a simple user experience

## Project Structure

```text
ocr_extract/
├── app/
│   ├── services/
│   │   └── ocr.py
│   ├── templates/
│   │   └── index.html
│   ├── utils/
│   │   └── text.py
│   ├── __init__.py
│   └── routes.py
├── tests/
│   └── test_app.py
├── config.py
├── implementation.md
├── requirements.txt
├── run.py
└── todo.md
```

## Setup Instructions

1. Create and activate a virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install Python dependencies.

```bash
pip install -r requirements.txt
```

3. Install Tesseract OCR if it is not already available on your system.

Ubuntu or Debian:

```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr
```

4. Optionally configure environment variables.

```bash
export SECRET_KEY="replace-this-in-production"
export TESSERACT_COMMAND="tesseract"
```

## Usage Guide

1. Start the Flask development server.

```bash
python3 run.py
```

2. Open `http://127.0.0.1:5000` in your browser.
3. Upload one or more image files.
4. Click `Extract Text`.
5. Review the combined output or the per-image result cards.
6. Use `Copy All Text`, `Download TXT`, or the per-image copy buttons as needed.

## Running Tests

```bash
python3 -m unittest discover -s tests -v
```

## Notes

- Supported image types: PNG, JPG, JPEG, BMP, TIFF, GIF, WEBP
- Maximum upload size is 16 MB per request
- Tailwind CSS is loaded through its CDN for a simple setup
