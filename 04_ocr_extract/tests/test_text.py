import unittest

from app.services.ocr import OCRResult
from app.utils.text import build_combined_output, normalize_extracted_text


class TextUtilityTests(unittest.TestCase):
    def test_normalize_extracted_text_collapses_spacing(self) -> None:
        text = "Hello   world\r\n\r\n\r\nThis\t\tis OCR"

        normalized = normalize_extracted_text(text)

        self.assertEqual(normalized, "Hello world\n\nThis is OCR")

    def test_build_combined_output_separates_each_image(self) -> None:
        results = [
            OCRResult(filename="first.png", extracted_text="Alpha"),
            OCRResult(filename="second.jpg", extracted_text="Beta"),
        ]

        combined = build_combined_output(results)

        self.assertIn("===== Image 1: first.png =====\nAlpha", combined)
        self.assertIn("===== Image 2: second.jpg =====\nBeta", combined)


if __name__ == "__main__":
    unittest.main()
