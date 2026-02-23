"""
openclaw/rag_store.py
OpenClaw – RAG / Historical Pattern Store (stub)

Future: analyze historical run data to detect patterns in failures,
optimize retry parameters, and surface insights from past runs.

Currently: stores run history in a local SQLite DB for future RAG use.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    success     INTEGER NOT NULL,
    attempts    INTEGER NOT NULL,
    elapsed_s   REAL    NOT NULL,
    reason      TEXT    NOT NULL,
    errors_json TEXT    NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS error_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    source      TEXT    NOT NULL,
    severity    TEXT    NOT NULL,
    message     TEXT    NOT NULL,
    tags_json   TEXT    NOT NULL DEFAULT '[]'
);
"""


class RAGStore:
    """
    Lightweight historical store for run and error events.
    Designed to be extended into a vector-based RAG layer.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def record_run(
        self,
        success: bool,
        attempts: int,
        elapsed_s: float,
        reason: str,
        errors: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        errors_json = json.dumps(errors or [])
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run_history
                    (timestamp, success, attempts, elapsed_s, reason, errors_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (time.time(), int(success), attempts, elapsed_s, reason, errors_json),
            )

    def record_error(
        self,
        source: str,
        severity: str,
        message: str,
        tags: Optional[list[str]] = None,
    ) -> None:
        tags_json = json.dumps(tags or [])
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO error_events
                    (timestamp, source, severity, message, tags_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (time.time(), source, severity, message, tags_json),
            )

    # ------------------------------------------------------------------
    # Read / Analysis
    # ------------------------------------------------------------------

    def recent_runs(self, n: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM run_history ORDER BY timestamp DESC LIMIT ?", (n,)
            ).fetchall()
        return [dict(r) for r in rows]

    def failure_rate(self, last_n: int = 20) -> float:
        """Return failure rate over the last N runs (0.0 – 1.0)."""
        runs = self.recent_runs(last_n)
        if not runs:
            return 0.0
        failures = sum(1 for r in runs if not r["success"])
        return failures / len(runs)

    def most_common_errors(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT source, severity, message, COUNT(*) as count
                FROM error_events
                GROUP BY source, severity, message
                ORDER BY count DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def print_summary(self) -> None:
        runs       = self.recent_runs(20)
        fail_rate  = self.failure_rate(20)
        top_errors = self.most_common_errors(5)

        print(f"\n── RAG Store Summary ─────────────────────────────")
        print(f"  Recent runs : {len(runs)}")
        print(f"  Failure rate: {fail_rate * 100:.1f}%")
        if top_errors:
            print("  Top errors:")
            for e in top_errors:
                print(f"    [{e['source']}][{e['severity']}] ×{e['count']} – {e['message'][:80]}")
        print(f"──────────────────────────────────────────────────\n")
