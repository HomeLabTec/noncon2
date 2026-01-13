from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

DB_PATH = Path("noncon.db")

TAG_COLUMNS = [
    "tag_number",
    "part_description",
    "rejection_type",
    "rejection_class",
    "defect_description",
    "rejected_by",
    "containment_date",
    "total_rejected_qty_initial",
    "mfg_date",
    "mfg_shift",
    "supplier",
    "total_rejected_qty_final",
    "disposition",
    "authorized_by",
    "date_authorized",
    "date_sort_rework",
    "good_pcs",
    "reworked_pcs",
    "scrap_pcs",
    "complete",
    "closed_date",
    "closed_by",
    "qad_number",
    "qad_date",
    "completed_by",
    "is_closed",
    "created_at",
    "updated_at",
]


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag_number TEXT UNIQUE NOT NULL,
            part_description TEXT,
            rejection_type TEXT,
            rejection_class TEXT,
            defect_description TEXT,
            rejected_by TEXT,
            containment_date TEXT,
            total_rejected_qty_initial TEXT,
            mfg_date TEXT,
            mfg_shift TEXT,
            supplier TEXT,
            total_rejected_qty_final TEXT,
            disposition TEXT,
            authorized_by TEXT,
            date_authorized TEXT,
            date_sort_rework TEXT,
            good_pcs TEXT,
            reworked_pcs TEXT,
            scrap_pcs TEXT,
            complete TEXT,
            closed_date TEXT,
            closed_by TEXT,
            qad_number TEXT,
            qad_date TEXT,
            completed_by TEXT,
            is_closed INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dropdown_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            field TEXT NOT NULL,
            value TEXT NOT NULL,
            UNIQUE(field, value)
        )
        """
    )
    conn.commit()


def upsert_dropdowns(conn: sqlite3.Connection, dropdowns: Dict[str, List[str]]) -> None:
    for field, options in dropdowns.items():
        conn.execute("DELETE FROM dropdown_options WHERE field = ?", (field,))
        conn.executemany(
            "INSERT OR IGNORE INTO dropdown_options(field, value) VALUES (?, ?)",
            [(field, opt) for opt in options],
        )
    conn.commit()


def determine_is_closed(record: Dict[str, Optional[str]]) -> int:
    closed_date = (record.get("closed_date") or "").strip()
    complete = (record.get("complete") or "").strip().lower()
    if closed_date:
        return 1
    if complete in {"yes", "y", "true", "closed", "complete"}:
        return 1
    if complete in {"1", "closed", "done"}:
        return 1
    return 0


def clean_record(record: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    cleaned: Dict[str, Optional[str]] = {}
    for key in TAG_COLUMNS:
        if key not in record:
            continue
        value = record.get(key)
        if value is None:
            cleaned[key] = None
            continue
        if isinstance(value, str):
            text = value.strip()
            cleaned[key] = text or None
        else:
            cleaned[key] = value
    return cleaned


def seed_from_excel(conn: sqlite3.Connection, records: List[Dict[str, Optional[str]]]) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    for record in records:
        record = record.copy()
        record.setdefault("created_at", now)
        record.setdefault("updated_at", now)
        record["is_closed"] = determine_is_closed(record)
        cleaned = clean_record(record)
        columns = [key for key in TAG_COLUMNS if key in cleaned]
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)
        values = [cleaned[col] for col in columns]
        try:
            conn.execute(
                f"INSERT OR IGNORE INTO tags({column_names}) VALUES ({placeholders})",
                values,
            )
        except sqlite3.Error as exc:
            raise RuntimeError(f"Failed to insert record for tag {record.get('tag_number')}: {exc}")
    conn.commit()


def fetch_dropdowns(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    dropdowns: Dict[str, List[str]] = {}
    rows = conn.execute(
        "SELECT field, value FROM dropdown_options ORDER BY id ASC"
    ).fetchall()
    for row in rows:
        dropdowns.setdefault(row["field"], []).append(row["value"])
    return dropdowns


def generate_tag_number(conn: sqlite3.Connection) -> str:
    today = datetime.utcnow().strftime("%y%m%d")
    prefix = f"NC-{today}-"
    result = conn.execute(
        "SELECT COUNT(*) FROM tags WHERE tag_number LIKE ?",
        (f"{prefix}%",),
    ).fetchone()
    count = (result[0] or 0) + 1
    return f"{prefix}{count:03d}"


def list_tags(conn: sqlite3.Connection, is_closed: Optional[bool] = None) -> List[sqlite3.Row]:
    if is_closed is None:
        sql = "SELECT * FROM tags ORDER BY (created_at IS NULL), created_at DESC"
        return conn.execute(sql).fetchall()
    sql = "SELECT * FROM tags WHERE is_closed = ? ORDER BY (updated_at IS NULL), updated_at DESC"
    return conn.execute(sql, (1 if is_closed else 0,)).fetchall()


def get_tag(conn: sqlite3.Connection, tag_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM tags WHERE id = ?", (tag_id,)).fetchone()


def insert_tag(conn: sqlite3.Connection, data: Dict[str, Optional[str]]) -> int:
    now = datetime.utcnow().isoformat(timespec="seconds")
    data = data.copy()
    data.setdefault("created_at", now)
    data.setdefault("updated_at", now)
    data["is_closed"] = determine_is_closed(data)
    columns = [col for col in TAG_COLUMNS if col in data]
    placeholders = ", ".join(["?" for _ in columns])
    column_names = ", ".join(columns)
    values = [data[col] for col in columns]
    cur = conn.execute(
        f"INSERT INTO tags({column_names}) VALUES ({placeholders})",
        values,
    )
    conn.commit()
    return int(cur.lastrowid)


def update_tag(conn: sqlite3.Connection, tag_id: int, data: Dict[str, Optional[str]]) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    data = data.copy()
    data["updated_at"] = now
    data["is_closed"] = determine_is_closed(data)
    assignments = []
    values: List[Optional[str]] = []
    for col in TAG_COLUMNS:
        if col in data:
            assignments.append(f"{col} = ?")
            values.append(data[col])
    if not assignments:
        return
    values.append(tag_id)
    conn.execute(
        f"UPDATE tags SET {', '.join(assignments)} WHERE id = ?",
        values,
    )
    conn.commit()


def delete_tag(conn: sqlite3.Connection, tag_id: int) -> None:
    """Remove a tag permanently."""
    conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
    conn.commit()


def dashboard_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    total = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
    open_count = conn.execute(
        "SELECT COUNT(*) FROM tags WHERE is_closed = 0"
    ).fetchone()[0]
    closed_count = conn.execute(
        "SELECT COUNT(*) FROM tags WHERE is_closed = 1"
    ).fetchone()[0]
    return {
        "total": total,
        "open": open_count,
        "closed": closed_count,
    }


def report_rows(
    conn: sqlite3.Connection, start_date: str, end_date: str
) -> List[sqlite3.Row]:
    """
    Return tag records whose primary date falls within the given range.

    The primary date prefers containment, authorization, or closed dates,
    falling back to created_at when the others are missing.
    """
    report_date_expr = "date(COALESCE(containment_date, date_authorized, closed_date, created_at))"
    sql = f"""
        SELECT
            *,
            {report_date_expr} AS report_date
        FROM tags
        WHERE {report_date_expr} BETWEEN ? AND ?
        ORDER BY {report_date_expr} DESC, tag_number DESC
    """
    return conn.execute(sql, (start_date, end_date)).fetchall()


def report_summary(rows: Iterable[sqlite3.Row]) -> Dict[str, int]:
    rows_list = rows if isinstance(rows, list) else list(rows)
    total = len(rows_list)
    open_count = sum(1 for row in rows_list if not row["is_closed"])
    closed_count = total - open_count
    return {"total": total, "open": open_count, "closed": closed_count}
