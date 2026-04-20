# 08 Store Image DB

A full-stack Flask application for uploading, storing, and retrieving small images directly from SQLite using BLOB storage.

## Folder Structure

```text
08_store_image_db/
├── app.py
├── database.py
├── service.py
├── requirements.txt
├── templates/
│   ├── base.html
│   ├── index.html
│   └── view.html
└── tests/
    └── test_app.py
```

## Features

- Flask web UI for upload and viewing
- SQLite database named `images.db`
- Images stored as binary BLOBs
- Allowed formats: PNG, JPG, JPEG
- Image size validation under 100KB
- Gallery page for browsing uploaded images
- Image preview before upload
- Timestamp support with `created_at`
- Logging for upload and retrieval flows
- Dynamic image reconstruction with `send_file`

## Database Schema

The app creates an `images` table automatically:

- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `name TEXT`
- `image_data BLOB`
- `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

## How to Run

From the repository root:

```bash
pip install -r 08_store_image_db/requirements.txt
python3 08_store_image_db/app.py
```

Then open:

- `http://127.0.0.1:5000/` for the upload page
- `http://127.0.0.1:5000/view` for the image gallery and fetch page

## Routes

- `GET /` → upload page
- `POST /upload` → upload an image and store it in SQLite
- `GET /image/<id>` → stream image from the database
- `GET /view` → fetch and browse uploaded images

## Sample Test Cases

Run the tests:

```bash
python3 -m unittest discover -s 08_store_image_db/tests
```

Manual checks:

1. Upload a small `.png` file under 100KB.
2. Confirm you are redirected to `/view?image_id=<id>`.
3. Open `/image/<id>` directly in the browser and verify the image renders.
4. Try uploading a `.gif` file and confirm it is rejected.
5. Try uploading a file larger than 100KB and confirm it is rejected.

## Notes

This project is intended for small image assets like QR codes, tags, icons, and thumbnails. Storing large media files in SQLite is usually not the best tradeoff.
