import random
import sqlite3
import time
from pathlib import Path
from typing import Optional


class TaskTracker:
    def __init__(self, db_path: Optional[Path] = None, db_url: Optional[str] = None):
        self._pg_conn = None
        self.conn = None

        if db_url is not None:
            try:
                import psycopg2
            except ImportError as e:
                raise ImportError(
                    "psycopg2 is required for PostgreSQL support. "
                    "Install it with: pip install psycopg2-binary"
                ) from e
            self._psycopg2 = psycopg2
            self._pg_conn = psycopg2.connect(db_url)
            self._pg_conn.autocommit = False
            self.db_path = None
        else:
            if db_path is None:
                db_path = Path.home() / ".moppy" / "db" / "cmor_tasks.db"
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.db_path, timeout=30)

        self._init_db()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _is_postgres(self) -> bool:
        return self._pg_conn is not None

    def _init_db(self):
        if self._is_postgres:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS cmor_tasks (
                            id SERIAL PRIMARY KEY,
                            variable TEXT NOT NULL,
                            experiment_id TEXT NOT NULL,
                            status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed')) NOT NULL DEFAULT 'pending',
                            start_time TIMESTAMPTZ,
                            end_time TIMESTAMPTZ,
                            error_message TEXT
                        )
                        """
                    )
                    cur.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_var_exp ON cmor_tasks(variable, experiment_id)"
                    )
                self._pg_conn.commit()
            except Exception:
                self._pg_conn.rollback()
                raise
        else:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
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

    def _execute_with_retry(self, query, params=(), max_retries=5):
        """Execute a SQLite query with exponential-backoff retry on lock errors."""
        for attempt in range(max_retries):
            try:
                with self.conn:
                    return self.conn.execute(query, params)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    delay = (2**attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
                raise

    def _pg_execute_with_retry(self, query, params=(), max_retries=5):
        """Execute a PostgreSQL query with retry on transient lock/serialisation errors."""
        for attempt in range(max_retries):
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute(query, params)
                    self._pg_conn.commit()
                    return cur
            except self._psycopg2.OperationalError as e:
                self._pg_conn.rollback()
                if attempt < max_retries - 1:
                    delay = (2**attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
                raise
            except Exception:
                self._pg_conn.rollback()
                raise

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_task(self, variable: str, experiment_id: str):
        if self._is_postgres:
            self._pg_execute_with_retry(
                """
                INSERT INTO cmor_tasks (variable, experiment_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (variable, experiment_id),
            )
        else:
            self._execute_with_retry(
                """
                INSERT OR IGNORE INTO cmor_tasks (variable, experiment_id)
                VALUES (?, ?)
                """,
                (variable, experiment_id),
            )

    def mark_running(self, variable: str, experiment_id: str):
        if self._is_postgres:
            self._pg_execute_with_retry(
                """
                UPDATE cmor_tasks
                SET status='running', start_time=NOW()
                WHERE variable=%s AND experiment_id=%s
                """,
                (variable, experiment_id),
            )
        else:
            self._execute_with_retry(
                """
                UPDATE cmor_tasks
                SET status='running', start_time=datetime('now')
                WHERE variable=? AND experiment_id=?
                """,
                (variable, experiment_id),
            )

    def mark_completed(self, variable: str, experiment_id: str):
        if self._is_postgres:
            self._pg_execute_with_retry(
                """
                UPDATE cmor_tasks
                SET status='completed', end_time=NOW(), error_message=NULL
                WHERE variable=%s AND experiment_id=%s
                """,
                (variable, experiment_id),
            )
        else:
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
        if self._is_postgres:
            self._pg_execute_with_retry(
                """
                UPDATE cmor_tasks
                SET status='failed', end_time=NOW(), error_message=%s
                WHERE variable=%s AND experiment_id=%s
                """,
                (error_message, variable, experiment_id),
            )
        else:
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
        if self._is_postgres:
            for attempt in range(5):
                try:
                    with self._pg_conn.cursor() as cur:
                        cur.execute(
                            "SELECT status FROM cmor_tasks WHERE variable=%s AND experiment_id=%s",
                            (variable, experiment_id),
                        )
                        row = cur.fetchone()
                    return row[0] if row is not None else None
                except self._psycopg2.OperationalError:
                    self._pg_conn.rollback()
                    if attempt < 4:
                        time.sleep((2**attempt) + random.uniform(0, 1))
                        continue
                    raise
                except Exception:
                    self._pg_conn.rollback()
                    raise
        else:
            for attempt in range(5):
                try:
                    cur = self.conn.cursor()
                    cur.execute(
                        "SELECT status FROM cmor_tasks WHERE variable=? AND experiment_id=?",
                        (variable, experiment_id),
                    )
                    row = cur.fetchone()
                    return row[0] if row is not None else None
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < 4:
                        time.sleep((2**attempt) + random.uniform(0, 1))
                        continue
                    raise

    def is_done(self, variable: str, experiment_id: str) -> bool:
        if self._is_postgres:
            for attempt in range(5):
                try:
                    with self._pg_conn.cursor() as cur:
                        cur.execute(
                            "SELECT status FROM cmor_tasks WHERE variable=%s AND experiment_id=%s",
                            (variable, experiment_id),
                        )
                        row = cur.fetchone()
                    return row is not None and row[0] == "completed"
                except self._psycopg2.OperationalError:
                    self._pg_conn.rollback()
                    if attempt < 4:
                        time.sleep((2**attempt) + random.uniform(0, 1))
                        continue
                    raise
                except Exception:
                    self._pg_conn.rollback()
                    raise
        else:
            for attempt in range(5):
                try:
                    cur = self.conn.cursor()
                    cur.execute(
                        "SELECT status FROM cmor_tasks WHERE variable=? AND experiment_id=?",
                        (variable, experiment_id),
                    )
                    row = cur.fetchone()
                    return row is not None and row[0] == "completed"
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < 4:
                        time.sleep((2**attempt) + random.uniform(0, 1))
                        continue
                    raise

    def claim_next_task(self, experiment_id: str) -> Optional[str]:
        """Atomically claim the next pending task for experiment_id.

        Returns the variable name of the claimed task, or None if no pending
        tasks exist for this experiment.
        """
        if self._is_postgres:
            for attempt in range(5):
                try:
                    with self._pg_conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE cmor_tasks SET status='running', start_time=NOW()
                            WHERE id = (
                                SELECT id FROM cmor_tasks
                                WHERE status='pending' AND experiment_id=%s
                                ORDER BY id
                                FOR UPDATE SKIP LOCKED
                                LIMIT 1
                            )
                            RETURNING variable
                            """,
                            (experiment_id,),
                        )
                        row = cur.fetchone()
                    self._pg_conn.commit()
                    return row[0] if row is not None else None
                except self._psycopg2.OperationalError:
                    self._pg_conn.rollback()
                    if attempt < 4:
                        time.sleep((2**attempt) + random.uniform(0, 1))
                        continue
                    raise
                except Exception:
                    self._pg_conn.rollback()
                    raise
        else:
            for attempt in range(5):
                try:
                    cur = self.conn.cursor()
                    cur.execute("BEGIN IMMEDIATE")
                    cur.execute(
                        """
                        SELECT id, variable FROM cmor_tasks
                        WHERE status='pending' AND experiment_id=?
                        ORDER BY id
                        LIMIT 1
                        """,
                        (experiment_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        self.conn.rollback()
                        return None
                    task_id, variable = row
                    cur.execute(
                        """
                        UPDATE cmor_tasks SET status='running', start_time=datetime('now')
                        WHERE id=? AND status='pending'
                        """,
                        (task_id,),
                    )
                    if cur.rowcount == 0:
                        self.conn.rollback()
                        continue
                    self.conn.commit()
                    return variable
                except sqlite3.OperationalError as e:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    if "database is locked" in str(e) and attempt < 4:
                        time.sleep((2**attempt) + random.uniform(0, 1))
                        continue
                    raise


def init_postgres_db(db_url: str) -> None:
    """Create the cmor_tasks schema in a PostgreSQL database.

    Args:
        db_url: PostgreSQL connection URL, e.g.
                ``postgresql://user:pass@host/dbname``
    """
    try:
        import psycopg2
    except ImportError as e:
        raise ImportError(
            "psycopg2 is required for PostgreSQL support. "
            "Install it with: pip install psycopg2-binary"
        ) from e

    conn = psycopg2.connect(db_url)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS cmor_tasks (
                    id SERIAL PRIMARY KEY,
                    variable TEXT NOT NULL,
                    experiment_id TEXT NOT NULL,
                    status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed')) NOT NULL DEFAULT 'pending',
                    start_time TIMESTAMPTZ,
                    end_time TIMESTAMPTZ,
                    error_message TEXT
                )
                """
            )
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_var_exp ON cmor_tasks(variable, experiment_id)"
            )
        conn.commit()
        print("PostgreSQL schema initialised successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
