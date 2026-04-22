# Python Learning Lab

This repository is a hands-on collection of Python practice projects, mini apps, and data exercises. It starts with core Python basics and grows into web apps, authentication, OCR, data workflows, and storage internals.

## Repository Map

| Folder | Focus | Docs / Entry Point |
| --- | --- | --- |
| [`01_python_basics`](01_python_basics/) | Introductory Python scripts covering syntax, file handling, SQLite, web scraping, and a small thesaurus app | [README](01_python_basics/README.md) |
| [`02_stateless_otp`](02_stateless_otp/) | Stateless OTP service with FastAPI, delivery backends, SDKs, and tests | [README](02_stateless_otp/README.md) |
| [`03_calorie_calulator`](03_calorie_calulator/) | Flask calorie calculator app with templates, login flow, and dashboard views | [README](03_calorie_calulator/README.md) |
| [`04_ocr_extract`](04_ocr_extract/) | Flask OCR uploader that uses Tesseract to extract text from images | [README](04_ocr_extract/README.md) |
| [`05_problem_solving`](05_problem_solving/) | Algorithm and Python interview-style practice, including two pointers, decorators, and concurrency examples | Start with [`1.py`](05_problem_solving/1.py) or [`basics.py`](05_problem_solving/basics.py) |
| [`06_data_science`](06_data_science/) | Pandas and NumPy practice, data cleaning, ETL examples, joins, chunked processing, and reporting workflows | Start with [`basics.py`](06_data_science/basics.py) or [`5.py`](06_data_science/5.py) |
| [`07_database_storage`](07_database_storage/) | Custom append-only key-value storage engine with indexing, recovery, and compaction | [README](07_database_storage/README.md) |
| [`08_store_image_db`](08_store_image_db/) | Flask + SQLite demo for storing and serving small images as BLOBs | [README](08_store_image_db/README.md) |

## Getting Started

1. Create and activate a virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Choose a module and install its dependencies if it has a `requirements.txt` file.

```bash
pip install -r 04_ocr_extract/requirements.txt
```

3. Run the relevant script or app from the repository root.

```bash
python3 01_python_basics/1.Basics/main.py
python3 04_ocr_extract/run.py
python3 07_database_storage/storage_engine.py
```

## Notes

- Dependencies are managed per project, not at the repository root.
- `04_ocr_extract` requires the `tesseract` CLI to be installed on your system.
- Some folders are app-style projects with their own READMEs, while `05_problem_solving` and `06_data_science` are script collections you can run file by file.
- Folder names are documented exactly as they exist in the repository, including `03_calorie_calulator`.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
