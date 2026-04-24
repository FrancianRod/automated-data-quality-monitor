"""
=============================================================
Metadata Store — SQLite-based audit trail
Stores every run result for trend analysis and reporting.
=============================================================
"""

import sqlite3
import json
from datetime import datetime


class MetadataStore:
    """
    Persists every pipeline health check result.
    Enables: trend reports, SLA tracking, reliability score over time.
    """

    def __init__(self, db_path: str = "data/pipeline_metadata.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    run_at TEXT NOT NULL,
                    total_rows INTEGER,
                    checks_passed INTEGER,
                    checks_failed INTEGER,
                    overall_status TEXT,
                    issues_json TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS baselines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    baseline_mean REAL,
                    baseline_std REAL,
                    computed_at TEXT NOT NULL
                )
            """)

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def save_run(self, health_result) -> None:
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO pipeline_runs
                  (pipeline_name, table_name, run_at, total_rows,
                   checks_passed, checks_failed, overall_status, issues_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                health_result.pipeline_name,
                health_result.table_name,
                health_result.run_at,
                health_result.total_rows,
                health_result.checks_passed,
                health_result.checks_failed,
                health_result.overall_status,
                json.dumps(health_result.issues),
            ))

    def save_baseline(self, pipeline: str, table: str, column: str, mean: float, std: float) -> None:
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO baselines
                  (pipeline_name, table_name, column_name, baseline_mean, baseline_std, computed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (pipeline, table, column, mean, std, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    def get_recent_runs(self, pipeline_name: str, limit: int = 30) -> list:
        with self._conn() as conn:
            cursor = conn.execute("""
                SELECT pipeline_name, table_name, run_at, total_rows,
                       checks_passed, checks_failed, overall_status
                FROM pipeline_runs
                WHERE pipeline_name = ?
                ORDER BY run_at DESC
                LIMIT ?
            """, (pipeline_name, limit))
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_reliability_score(self, pipeline_name: str, days: int = 30) -> float:
        """Returns the % of runs that passed all checks in the last N days."""
        with self._conn() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN overall_status = 'OK' THEN 1 ELSE 0 END) as ok_runs
                FROM pipeline_runs
                WHERE pipeline_name = ?
                  AND run_at >= datetime('now', ?)
            """, (pipeline_name, f"-{days} days"))
            row = cursor.fetchone()
            total, ok_runs = row
            if not total:
                return 0.0
            return round((ok_runs / total) * 100, 1)
