from __future__ import annotations

import json
import random
import sqlite3
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, cast

TaskStatus = Literal["pending", "running", "completed", "failed"]


class TaskTracker:
    """Track CMORisation task state in a small SQLite database.

    The tracker is used by the batch CMORiser, PBS monitor job, dashboard,
    and worker scripts to coordinate per-variable task state. It stores one
    row per ``(variable, experiment_id)`` pair and retries transient SQLite
    failures seen on Lustre-backed filesystems.

    Args:
        db_path: Optional path to the SQLite database. When omitted, the
            default database under ``~/.moppy/db/cmor_tasks.db`` is used.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        if db_path is None:
            db_path = Path.home() / ".moppy" / "db" / "cmor_tasks.db"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: sqlite3.Connection | None = sqlite3.connect(self.db_path, timeout=30)
        self._init_db()

    def _init_db(self) -> None:
        """Initialise the task table and Lustre-friendly SQLite settings."""
        # On Lustre (Gadi /scratch, /g/data), fsync() intermittently returns
        # EIO under concurrent PBS job access (SQLITE_IOERR: disk I/O error).
        # WAL mode also causes SIGBUS via its mmap'd .db-shm.
        # Fix: DELETE journal mode (journal file survives crashes for recovery)
        # + synchronous=OFF (no fsync() calls, eliminating the EIO source).
        # pwrite() to the journal file goes through the OS page cache and does
        # not trigger EIO; only fsync() does.
        #
        # The whole sequence is retried because PRAGMA wal_checkpoint and
        # journal_mode involve file I/O and can transiently fail with EIO on
        # Lustre. All operations are idempotent (IF NOT EXISTS), so retrying
        # from the top is safe.
        _TRANSIENT = ("database is locked", "disk I/O error")
        for attempt in range(5):
            try:
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
                            error_message TEXT,
                            pbs_job_id TEXT,
                            pbs_info_json TEXT,
                            worker_memory_json TEXT,
                            output_summary_json TEXT
                        )
                        """
                    )
                    self.conn.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_var_exp ON cmor_tasks(variable, experiment_id)"
                    )
                    self.conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS monitor_requests (
                            variable TEXT NOT NULL,
                            experiment_id TEXT NOT NULL,
                            requested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (variable, experiment_id)
                        )
                        """
                    )
                    # Migrate databases created before pbs_job_id existed.
                    # ALTER TABLE has no IF NOT EXISTS, so check first.
                    existing = {
                        row[1]
                        for row in self.conn.execute(
                            "PRAGMA table_info(cmor_tasks)"
                        ).fetchall()
                    }
                    if "pbs_job_id" not in existing:
                        self.conn.execute(
                            "ALTER TABLE cmor_tasks ADD COLUMN pbs_job_id TEXT"
                        )
                    if "pbs_info_json" not in existing:
                        self.conn.execute(
                            "ALTER TABLE cmor_tasks ADD COLUMN pbs_info_json TEXT"
                        )
                    if "worker_memory_json" not in existing:
                        self.conn.execute(
                            "ALTER TABLE cmor_tasks ADD COLUMN worker_memory_json TEXT"
                        )
                    if "output_summary_json" not in existing:
                        self.conn.execute(
                            "ALTER TABLE cmor_tasks ADD COLUMN output_summary_json TEXT"
                        )
                return
            except sqlite3.OperationalError as e:
                if any(msg in str(e) for msg in _TRANSIENT) and attempt < 4:
                    time.sleep((2**attempt) + random.uniform(0, 1))
                else:
                    raise

    def add_task(self, variable: str, experiment_id: str) -> None:
        """Insert a task row if it does not already exist.

        Args:
            variable: CMOR variable name or compound variable identifier.
            experiment_id: Experiment identifier associated with the task.
        """
        self._execute_with_retry(
            """
            INSERT OR IGNORE INTO cmor_tasks (variable, experiment_id)
            VALUES (?, ?)
            """,
            (variable, experiment_id),
        )

    def mark_running(self, variable: str, experiment_id: str) -> None:
        """Mark a task as running and set its start timestamp."""
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='running', start_time=datetime('now'), end_time=NULL,
                error_message=NULL, worker_memory_json=NULL, output_summary_json=NULL
            WHERE variable=? AND experiment_id=?
            """,
            (variable, experiment_id),
        )

    def mark_completed(self, variable: str, experiment_id: str) -> None:
        """Mark a task as completed and clear any previous error message."""
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='completed', end_time=datetime('now'), error_message=NULL
            WHERE variable=? AND experiment_id=?
            """,
            (variable, experiment_id),
        )

    def mark_done(self, variable: str, experiment_id: str) -> None:
        """Alias for mark_completed for backward compatibility."""
        self.mark_completed(variable, experiment_id)

    def mark_failed(
        self, variable: str, experiment_id: str, error_message: str
    ) -> None:
        """Mark a task as failed and store the supplied error message."""
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='failed', end_time=datetime('now'), error_message=?
            WHERE variable=? AND experiment_id=?
            """,
            (error_message, variable, experiment_id),
        )

    def get_status(self, variable: str, experiment_id: str) -> TaskStatus | None:
        """Get the current task status.

        Returns:
            One of ``"pending"``, ``"running"``, ``"completed"``, or
            ``"failed"`` if the task exists; otherwise ``None``.
        """
        cur = self._execute_with_retry(
            "SELECT status FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            (variable, experiment_id),
        )
        row = cur.fetchone()
        return row[0] if row is not None else None

    def is_done(self, variable: str, experiment_id: str) -> bool:
        """Return whether the task has reached the completed state."""
        return self.get_status(variable, experiment_id) == "completed"

    def reset_to_pending(self, variable: str, experiment_id: str) -> None:
        """Reset a task to pending so it will be resubmitted on the next run.

        Clears start/end timestamps, error message, and PBS metadata so the
        row looks identical to a freshly inserted task. Has no effect if the
        row does not exist.

        Args:
            variable: CMOR variable name or compound variable identifier.
            experiment_id: Experiment identifier associated with the task.
        """
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET status='pending', start_time=NULL, end_time=NULL,
                error_message=NULL, pbs_job_id=NULL, pbs_info_json=NULL
            WHERE variable=? AND experiment_id=?
            """,
            (variable, experiment_id),
        )

    def set_pbs_job_id(self, variable: str, experiment_id: str, job_id: str) -> None:
        """Record the PBS job id that the monitor submitted for this variable.

        Stored so the monitor (or a successor) can later query PBS for the
        outcome of a sub-job that died without writing its own terminal state.
        """
        self._execute_with_retry(
            """
            UPDATE cmor_tasks
            SET pbs_job_id=?, pbs_info_json=NULL
            WHERE variable=? AND experiment_id=?
            """,
            (job_id, variable, experiment_id),
        )

    def get_pbs_job_id(self, variable: str, experiment_id: str) -> str | None:
        """Return the PBS job id recorded for this variable, or None if unset."""
        cur = self._execute_with_retry(
            "SELECT pbs_job_id FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            (variable, experiment_id),
        )
        row = cur.fetchone()
        return row[0] if row is not None else None

    def set_pbs_info(
        self,
        variable: str,
        experiment_id: str,
        info: Mapping[str, Any] | None,
    ) -> None:
        """Store structured PBS metadata for a task.

        Args:
            variable: CMOR variable name or compound variable identifier.
            experiment_id: Experiment identifier associated with the task.
            info: Filtered PBS metadata from ``qstat``. ``None`` clears the
                stored metadata.
        """
        payload = (
            None
            if info is None
            else json.dumps(dict(info), sort_keys=True, separators=(",", ":"))
        )
        self._execute_with_retry(
            "UPDATE cmor_tasks SET pbs_info_json=? WHERE variable=? AND experiment_id=?",
            (payload, variable, experiment_id),
        )

    def get_pbs_info(self, variable: str, experiment_id: str) -> dict[str, Any] | None:
        """Return structured PBS metadata for a task, if present."""
        cur = self._execute_with_retry(
            "SELECT pbs_info_json FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            (variable, experiment_id),
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        try:
            loaded = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return loaded if isinstance(loaded, dict) else None

    def set_worker_memory(
        self,
        variable: str,
        experiment_id: str,
        info: Mapping[str, Any] | None,
    ) -> None:
        """Store per-worker peak memory usage recorded by the worker process.

        Args:
            variable: CMOR variable name or compound variable identifier.
            experiment_id: Experiment identifier associated with the task.
            info: Peak-RSS sizing data (worker count, per-worker memory budget,
                and observed peak RSS per worker). ``None`` clears it.
        """
        payload = (
            None
            if info is None
            else json.dumps(dict(info), sort_keys=True, separators=(",", ":"))
        )
        self._execute_with_retry(
            "UPDATE cmor_tasks SET worker_memory_json=? WHERE variable=? AND experiment_id=?",
            (payload, variable, experiment_id),
        )

    def get_worker_memory(
        self, variable: str, experiment_id: str
    ) -> dict[str, Any] | None:
        """Return recorded per-worker peak memory usage, if present."""
        cur = self._execute_with_retry(
            "SELECT worker_memory_json FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            (variable, experiment_id),
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        try:
            loaded = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return loaded if isinstance(loaded, dict) else None

    def set_output_summary(
        self,
        variable: str,
        experiment_id: str,
        summary: Mapping[str, Any] | None,
    ) -> None:
        """Store output file count and volume summary for a task."""
        payload = (
            None
            if summary is None
            else json.dumps(dict(summary), sort_keys=True, separators=(",", ":"))
        )
        self._execute_with_retry(
            "UPDATE cmor_tasks SET output_summary_json=? WHERE variable=? AND experiment_id=?",
            (payload, variable, experiment_id),
        )

    def get_output_summary(
        self, variable: str, experiment_id: str
    ) -> dict[str, Any] | None:
        """Return recorded output file count and volume summary, if present."""
        cur = self._execute_with_retry(
            "SELECT output_summary_json FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            (variable, experiment_id),
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        try:
            loaded = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return loaded if isinstance(loaded, dict) else None

    def list_unfinished(
        self, experiment_id: str
    ) -> list[tuple[str, TaskStatus, str | None]]:
        """Return rows for tasks that have not reached a terminal state.

        Used when a monitor restarts and needs to rebuild its watch set from
        the database. Each row is (variable, status, pbs_job_id).
        """
        cur = self._execute_with_retry(
            "SELECT variable, status, pbs_job_id FROM cmor_tasks "
            "WHERE experiment_id=? AND status NOT IN ('completed','failed')",
            (experiment_id,),
        )
        return cur.fetchall()

    def enqueue_monitor_request(self, variable: str, experiment_id: str) -> None:
        """Request that the active monitor submit a variable."""
        self._execute_with_retry(
            "INSERT OR IGNORE INTO monitor_requests (variable, experiment_id) "
            "VALUES (?, ?)",
            (variable, experiment_id),
        )

    def take_monitor_requests(self, experiment_id: str) -> list[str]:
        """Return and remove all queued requests for an experiment."""
        connection = cast(sqlite3.Connection, self.conn)
        with connection:
            rows = connection.execute(
                "SELECT variable FROM monitor_requests WHERE experiment_id=? "
                "ORDER BY requested_at, variable",
                (experiment_id,),
            ).fetchall()
            connection.execute(
                "DELETE FROM monitor_requests WHERE experiment_id=?",
                (experiment_id,),
            )
        return [row[0] for row in rows]

    def close(self) -> None:
        """Close the underlying sqlite connection. Idempotent.

        Use this (or the context-manager form `with TaskTracker(...) as t:`)
        to release the connection promptly. Leaving it open lets the OS hold
        on to journal files and file descriptors, which on Lustre causes
        `rmtree` of the parent directory to intermittently fail with
        ENOTEMPTY due to metadata-server eventual consistency.
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def __enter__(self) -> TaskTracker:
        """Return this tracker for context-manager use."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Close the SQLite connection when leaving a context-manager block."""
        self.close()
        return False

    def _db_execute(self, query: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        """Execute a SQL statement using the active connection."""
        return cast(sqlite3.Connection, self.conn).execute(query, params)

    def _execute_with_retry(
        self, query: str, params: Sequence[Any] = (), max_retries: int = 5
    ) -> sqlite3.Cursor:
        """Execute a statement, retrying transient SQLite/Lustre failures."""
        # Retries on two transient Lustre errors:
        # - "database is locked": another process holds the write lock
        # - "disk I/O error": Lustre metadata server EIO under high concurrency
        #   (open/unlink of the journal file can transiently fail; retrying succeeds)
        _TRANSIENT = ("database is locked", "disk I/O error")
        for attempt in range(max_retries):
            try:
                with cast(sqlite3.Connection, self.conn):
                    return self._db_execute(query, params)
            except sqlite3.OperationalError as e:
                if (
                    any(msg in str(e) for msg in _TRANSIENT)
                    and attempt < max_retries - 1
                ):
                    delay = (2**attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
                raise
