from __future__ import annotations

import argparse
import os
import sqlite3
from contextlib import AbstractContextManager
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, cast


DEFAULT_DB_PATH = os.environ.get("SQLITE_DB_PATH", "data/analytics.sqlite3")


class SQLiteStore(AbstractContextManager["SQLiteStore"]):

    def __init__(self, db_path: str | Path = "data/analytics.sqlite3") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    def __enter__(self) -> "SQLiteStore":
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.db_path,
            timeout=30,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute("PRAGMA temp_store = MEMORY")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self.initialize_schema()
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        if self._conn is not None:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
            self._conn.close()
            self._conn = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SQLite connection is not open. Use the store as a context manager.")
        return self._conn

    def initialize_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                timestamp TEXT NOT NULL,
                title TEXT NOT NULL,
                category TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS window_metrics (
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                category TEXT NOT NULL,
                count INTEGER NOT NULL,
                trend_score REAL NOT NULL,
                UNIQUE (window_start, window_end, category)
            );

            CREATE INDEX IF NOT EXISTS idx_events_timestamp
                ON events (timestamp);

            CREATE INDEX IF NOT EXISTS idx_events_category_timestamp
                ON events (category, timestamp);

            CREATE INDEX IF NOT EXISTS idx_window_metrics_window_start
                ON window_metrics (window_start);

            CREATE INDEX IF NOT EXISTS idx_window_metrics_category_window_start
                ON window_metrics (category, window_start);

            CREATE INDEX IF NOT EXISTS idx_window_metrics_window_end
                ON window_metrics (window_end);

            CREATE INDEX IF NOT EXISTS idx_window_metrics_category_window_end
                ON window_metrics (category, window_end);
            """
        )

    def insert_event(self, event: Any) -> None:
        payload = self._normalize_record(event, required=("timestamp", "title", "category"))
        self.connection.execute(
            """
            INSERT INTO events (timestamp, title, category)
            VALUES (:timestamp, :title, :category)
            """,
            {
                "timestamp": self._to_iso_datetime(payload["timestamp"]),
                "title": str(payload["title"]),
                "category": str(payload["category"]),
            },
        )

    def insert_metric(self, metric: Any) -> None:
        payload = self._normalize_record(
            metric,
            required=("window_start", "window_end", "category", "count", "trend_score"),
        )
        self.connection.execute(
            """
            INSERT INTO window_metrics (
                window_start,
                window_end,
                category,
                count,
                trend_score
            )
            VALUES (
                :window_start,
                :window_end,
                :category,
                :count,
                :trend_score
            )
            ON CONFLICT(window_start, window_end, category)
            DO UPDATE SET
                count = excluded.count,
                trend_score = excluded.trend_score
            """,
            {
                "window_start": self._to_iso_datetime(payload["window_start"]),
                "window_end": self._to_iso_datetime(payload["window_end"]),
                "category": str(payload["category"]),
                "count": int(payload["count"]),
                "trend_score": float(payload["trend_score"]),
            },
        )

    def _normalize_record(self, value: Any, required: tuple[str, ...]) -> dict[str, Any]:
        if isinstance(value, Mapping):
            payload = dict(value)
        elif is_dataclass(value) and not isinstance(value, type):
            payload = asdict(cast(Any, value))
        elif hasattr(value, "__dict__"):
            payload = {
                key: item
                for key, item in vars(value).items()
                if not key.startswith("_") and not callable(item)
            }
        else:
            raise TypeError("Input must be a mapping, dataclass, or object with attributes.")

        missing = [field for field in required if field not in payload]
        if missing:
            raise KeyError(f"Missing required fields: {', '.join(missing)}")

        return payload

    def _to_iso_datetime(self, value: Any) -> str:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, date):
            dt = datetime.combine(value, datetime.min.time())
        elif isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
        elif isinstance(value, str):
            text = value.strip().replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError(f"Invalid datetime string: {value}") from exc
        else:
            raise TypeError("Datetime values must be datetime, date, timestamp, or ISO-8601 string.")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def create_store(db_path: str | Path = "data/analytics.sqlite3") -> SQLiteStore:
    return SQLiteStore(db_path=db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize or inspect the SQLite analytics store.")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to the SQLite database file.")
    parser.add_argument(
        "--init-only",
        action="store_true",
        help="Create the database file and schema, then exit.",
    )
    args = parser.parse_args()

    with SQLiteStore(args.db_path) as store:
        if args.init_only:
            print(f"SQLite schema initialized at {store.db_path}")
        else:
            print(f"SQLite store ready at {store.db_path}")


if __name__ == "__main__":
    main()
