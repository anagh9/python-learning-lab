from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import app as image_app
import database


SMALL_PNG = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class ImageStoreAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_images.db"
        self.app = image_app.create_app(
            {
                "TESTING": True,
                "DATABASE": str(self.db_path),
                "SECRET_KEY": "test-secret",
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_home_page_renders(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Store an image as a SQLite BLOB", response.data)

    def test_upload_and_retrieve_image(self) -> None:
        response = self.client.post(
            "/upload",
            data={"image": (BytesIO(SMALL_PNG), "tiny.png")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Uploaded", response.data)

        image_record = database.get_image_by_id(self.db_path, 1)
        self.assertIsNotNone(image_record)
        self.assertEqual(image_record.name, "tiny.png")

        image_response = self.client.get("/image/1")
        self.assertEqual(image_response.status_code, 200)
        self.assertEqual(image_response.mimetype, "image/png")
        self.assertEqual(image_response.data, SMALL_PNG)

    def test_rejects_invalid_extension(self) -> None:
        response = self.client.post(
            "/upload",
            data={"image": (BytesIO(SMALL_PNG), "tiny.gif")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Only PNG, JPG, and JPEG files are allowed.", response.data)

    def test_rejects_large_image(self) -> None:
        large_payload = SMALL_PNG + (b"a" * (101 * 1024))
        response = self.client.post(
            "/upload",
            data={"image": (BytesIO(large_payload), "big.png")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Image must be smaller than 100KB.", response.data)

    def test_view_page_shows_uploaded_image(self) -> None:
        database.insert_image(self.db_path, "tiny.png", SMALL_PNG)
        response = self.client.get("/view?image_id=1")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"tiny.png", response.data)
        self.assertIn(b"Open Raw Image", response.data)

    def test_database_file_is_sqlite(self) -> None:
        database.init_db(self.db_path)
        connection = sqlite3.connect(self.db_path)
        try:
            row = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='images'"
            ).fetchone()
            self.assertEqual(row[0], "images")
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
