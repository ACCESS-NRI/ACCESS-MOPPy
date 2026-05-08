import random
import sqlite3
import time
from pathlib import Path
from typing import Optional


class TaskTracker:
    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = Path.home() / ".moppy" / "db" / "cmor_tasks.db"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self._init_db()

    def _init_db(self):
        # On Lustre (Gadi /scratch, /g/data), fsync() intermittently returns
        # EIO under concurrent PBS job access (SQLITE_IOERR: disk I/O error).
        # WAL mode also causes SIGBUS via its mmap'd .db-shm.
        # Fix: DELETE journal mode (journal file survives crashes for recovery)
        # + synchronous=OFF (no fsync() calls, eliminating the EIO source).
        # pwrite() to the journal file goes through the OS page cache and does
        # not trigger EIO; only fsync() does.
        self.conn.execute(
            "PRAGMA busy_timeout=30000"
        )  # set first so subsequent PRAGMAs wait on contention
        self.conn.execute(
            "PRAGMA wal_checkpoint(TRUNCATE)"
        )  # flush any pre-existing WAL before switching
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute(
            "PRAGMA synchronous=OFF"
        )  # no fsync(); journal file still written for crash recovery
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cmor_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    variable TEXT NOT NULL,
                    experiment_id TEXT NOT NULL,
                    status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed')) NOT NULL DEFAULT 'pending',
                    start_time TEXT,
                    end_time TEXT,
                    error_message TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_var_exp ON cmor_tasks(variable, experiment_id)"
            )

    def add_task(self, variable: str, experiment_id: str):
        self._execute_with_retry(
            """
            INSERT OR IGNORE INTO cmor_tasks (variable, experiment_id)
            VALUES (?, ?)
            """,
            (variable, experiment_id),
        )

    def mark_running(self, variable: str, experiment_id: str):
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='running', start_time=datetime('now')
            WHERE variable=? AND experiment_id=?
            """,
            (variable, experiment_id),
        )

    def mark_completed(self, variable: str, experiment_id: str):
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='completed', end_time=datetime('now'), error_message=NULL
            WHERE variable=? AND experiment_id=?
            """,
            (variable, experiment_id),
        )

    def mark_done(self, variable: str, experiment_id: str):
        """Alias for mark_completed for backward compatibility."""
        self.mark_completed(variable, experiment_id)

    def mark_failed(self, variable: str, experiment_id: str, error_message: str):
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='failed', end_time=datetime('now'), error_message=?
            WHERE variable=? AND experiment_id=?
            """,
            (error_message, variable, experiment_id),
        )

    def get_status(self, variable: str, experiment_id: str) -> Optional[str]:
        """Get the status of a task."""
        cur = self._execute_with_retry(
            "SELECT status FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            (variable, experiment_id),
        )
        row = cur.fetchone()
        return row[0] if row is not None else None

    def is_done(self, variable: str, experiment_id: str) -> bool:
        return self.get_status(variable, experiment_id) == "completed"

    def _execute_with_retry(self, query, params=(), max_retries=5):
        # Retries on two transient Lustre errors:
        # - "database is locked": another process holds the write lock
        # - "disk I/O error": Lustre metadata server EIO under high concurrency
        #   (open/unlink of the journal file can transiently fail; retrying succeeds)
        _TRANSIENT = ("database is locked", "disk I/O error")
        for attempt in range(max_retries):
            try:
                with self.conn:
                    return self.conn.execute(query, params)
            except sqlite3.OperationalError as e:
                if (
                    any(msg in str(e) for msg in _TRANSIENT)
                    and attempt < max_retries - 1
                ):
                    delay = (2**attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
                raise
