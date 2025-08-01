import random
import sqlite3
import time
from pathlib import Path
from typing import Optional


class TaskTracker:
    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = Path.home() / ".mopper" / "db" / "cmor_tasks.db"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        # Enable WAL mode for better concurrent access
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cmor_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    variable TEXT NOT NULL,
                    experiment TEXT NOT NULL,
                    status TEXT CHECK(status IN ('pending', 'running', 'done', 'failed')) NOT NULL DEFAULT 'pending',
                    start_time TEXT,
                    end_time TEXT,
                    error_message TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_var_exp ON cmor_tasks(variable, experiment)"
            )

    def add_task(self, variable: str, experiment: str):
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO cmor_tasks (variable, experiment)
                VALUES (?, ?)
                """,
                (variable, experiment),
            )

    def mark_running(self, variable: str, experiment: str):
        with self.conn:
            self.conn.execute(
                """
                UPDATE cmor_tasks
                SET status='running', start_time=datetime('now')
                WHERE variable=? AND experiment=?
                """,
                (variable, experiment),
            )

    def mark_done(self, variable: str, experiment: str):
        with self.conn:
            self.conn.execute(
                """
                UPDATE cmor_tasks
                SET status='done', end_time=datetime('now'), error_message=NULL
                WHERE variable=? AND experiment=?
                """,
                (variable, experiment),
            )

    def mark_failed(self, variable: str, experiment: str, error_message: str):
        with self.conn:
            self.conn.execute(
                """
                UPDATE cmor_tasks
                SET status='failed', end_time=datetime('now'), error_message=?
                WHERE variable=? AND experiment=?
                """,
                (error_message, variable, experiment),
            )

    def is_done(self, variable: str, experiment: str) -> bool:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT status FROM cmor_tasks WHERE variable=? AND experiment=?
            """,
            (variable, experiment),
        )
        row = cur.fetchone()
        return row is not None and row[0] == "done"

    def _execute_with_retry(self, query, params=(), max_retries=5):
        for attempt in range(max_retries):
            try:
                with self.conn:
                    return self.conn.execute(query, params)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = (2**attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
                raise
