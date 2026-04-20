from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Optional


@dataclass(frozen=True)
class ImageRecord:
    id: int
    name: str
    image_data: bytes
    created_at: str | None = None


@dataclass(frozen=True)
class ImageSummary:
    id: int
    name: str
    created_at: str | None = None


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: str | Path) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                image_data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_name ON images(name)"
        )
        connection.commit()


def insert_image(db_path: str | Path, name: str, image_data: bytes) -> int:
    with get_connection(db_path) as connection:
        cursor = connection.execute(
            "INSERT INTO images (name, image_data) VALUES (?, ?)",
            (name, sqlite3.Binary(image_data)),
        )
        connection.commit()
        return int(cursor.lastrowid)


def get_image_by_id(db_path: str | Path, image_id: int) -> Optional[ImageRecord]:
    with get_connection(db_path) as connection:
        row = connection.execute(
            "SELECT id, name, image_data, created_at FROM images WHERE id = ?",
            (image_id,),
        ).fetchone()

    if row is None:
        return None
    return ImageRecord(
        id=row["id"],
        name=row["name"],
        image_data=row["image_data"],
        created_at=row["created_at"],
    )


def get_image_by_name(db_path: str | Path, name: str) -> Optional[ImageRecord]:
    with get_connection(db_path) as connection:
        row = connection.execute(
            """
            SELECT id, name, image_data, created_at
            FROM images
            WHERE name = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (name,),
        ).fetchone()

    if row is None:
        return None
    return ImageRecord(
        id=row["id"],
        name=row["name"],
        image_data=row["image_data"],
        created_at=row["created_at"],
    )


def list_images(db_path: str | Path, limit: int = 24) -> list[ImageSummary]:
    with get_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT id, name, created_at
            FROM images
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        ImageSummary(id=row["id"], name=row["name"], created_at=row["created_at"])
        for row in rows
    ]
