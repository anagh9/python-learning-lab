import re
from typing import Protocol, Sequence


class ExtractedResult(Protocol):
    """Shape required to build the combined OCR export."""

    filename: str
    extracted_text: str


def normalize_extracted_text(text: str) -> str:
    """Clean OCR output into a readable, consistently spaced format."""
    stripped_text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    normalized_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in stripped_text.split("\n")]
    compact_lines = _collapse_blank_lines(normalized_lines)
    return "\n".join(compact_lines).strip() or "No text detected."


def build_combined_output(results: Sequence[ExtractedResult]) -> str:
    """Combine OCR results into a single downloadable text document."""
    sections = []

    for index, result in enumerate(results, start=1):
        sections.append(
            f"===== Image {index}: {result.filename} =====\n{result.extracted_text}"
        )

    return "\n\n".join(sections).strip()


def _collapse_blank_lines(lines: list[str]) -> list[str]:
    """Reduce consecutive blank lines to a single separator."""
    collapsed_lines: list[str] = []
    previous_blank = False

    for line in lines:
        is_blank = not line
        if is_blank and previous_blank:
            continue
        collapsed_lines.append(line)
        previous_blank = is_blank

    return collapsed_lines
