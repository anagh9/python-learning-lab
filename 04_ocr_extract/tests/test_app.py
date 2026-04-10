import io
import unittest

from app import create_app


class OCRExtractorAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_app({"TESTING": True})
        self.client = self.app.test_client()

    def test_index_page_loads(self) -> None:
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Extract text from multiple images in one pass", response.data)

    def test_extract_requires_at_least_one_file(self) -> None:
        response = self.client.post(
            "/extract",
            data={"images": (io.BytesIO(b""), "")},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Please choose at least one image to process.", response.data)


if __name__ == "__main__":
    unittest.main()
