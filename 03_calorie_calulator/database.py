import sqlite3
from datetime import date

DB_NAME = "calories.db"


def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            food_item TEXT NOT NULL,
            calories INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_entries_by_date(entry_date):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM entries WHERE entry_date = ? ORDER BY created_at DESC",
        (entry_date,)
    ).fetchall()
    conn.close()
    return rows


def add_entry(entry_date, food_item, calories):
    conn = get_connection()
    conn.execute(
        "INSERT INTO entries (entry_date, food_item, calories) VALUES (?, ?, ?)",
        (entry_date, food_item, calories)
    )
    conn.commit()
    conn.close()


def delete_entry(entry_id):
    conn = get_connection()
    conn.execute("DELETE FROM entries WHERE id = ?", (entry_id,))
    conn.commit()
    conn.close()


def get_all_dates():
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT entry_date FROM entries ORDER BY entry_date DESC"
    ).fetchall()
    conn.close()
    return [row["entry_date"] for row in rows]


def get_total_calories(entry_date):
    conn = get_connection()
    result = conn.execute(
        "SELECT SUM(calories) as total FROM entries WHERE entry_date = ?",
        (entry_date,)
    ).fetchone()
    conn.close()
    return result["total"] or 0
