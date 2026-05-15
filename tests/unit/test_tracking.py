import sqlite3
from unittest.mock import patch

import pytest

from access_moppy.tracking import TaskTracker


class TestTaskTracker:
    """Unit tests for TaskTracker class."""

    @pytest.mark.unit
    def test_init_creates_database(self, temp_dir):
        """Test that initialization creates database and tables."""
        db_path = temp_dir / "test_tracker.db"
        TaskTracker(db_path)

        assert db_path.exists()

        # Verify tables exist
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "cmor_tasks" in tables

    @pytest.mark.unit
    def test_add_task(self, temp_dir):
        """Test adding a new task."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        tracker.add_task("Amon.tas", "historical")

        # Verify task was added
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM cmor_tasks WHERE variable=? AND experiment_id=?",
            ("Amon.tas", "historical"),
        )
        result = cursor.fetchone()
        conn.close()

        assert result is not None
        assert result[1] == "Amon.tas"  # variable
        assert result[2] == "historical"  # experiment_id
        assert result[3] == "pending"  # status

    @pytest.mark.unit
    def test_mark_running(self, temp_dir):
        """Test marking task as running."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        status = tracker.get_status("Amon.tas", "historical")
        assert status == "running"

    @pytest.mark.unit
    def test_mark_completed(self, temp_dir):
        """Test marking task as completed."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

        status = tracker.get_status("Amon.tas", "historical")
        assert status == "completed"

    @pytest.mark.unit
    def test_is_done_functionality(self, temp_dir):
        """Test the is_done method used in templates."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        # Task not added yet
        assert not tracker.is_done("Amon.tas", "historical")

        # Task pending
        tracker.add_task("Amon.tas", "historical")
        assert not tracker.is_done("Amon.tas", "historical")

        # Task running
        tracker.mark_running("Amon.tas", "historical")
        assert not tracker.is_done("Amon.tas", "historical")

        # Task completed
        tracker.mark_completed("Amon.tas", "historical")
        assert tracker.is_done("Amon.tas", "historical")

    @pytest.mark.unit
    def test_no_shm_or_wal_files_on_disk(self, temp_dir):
        """Verify no WAL-mode files (.db-shm, .db-wal) are created.

        WAL mode creates .db-shm via mmap(), which causes SIGBUS on Lustre
        (Gadi) because cross-node mmap coherency is not guaranteed.
        DELETE+synchronous=OFF uses a journal file for crash recovery but
        no shared-memory structures.
        """
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.conn.close()

        assert not (temp_dir / "test_tracker.db-shm").exists()
        assert not (temp_dir / "test_tracker.db-wal").exists()

    @pytest.mark.unit
    def test_journal_mode_is_delete(self, temp_dir):
        """Verify DELETE journal mode: crash-safe journal file, no fsync()."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        row = tracker.conn.execute("PRAGMA journal_mode").fetchone()
        assert row[0] == "delete"

    @pytest.mark.unit
    def test_init_db_retries_on_eio(self, temp_dir):
        """_init_db retries when PRAGMA wal_checkpoint hits EIO on Lustre."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        real_conn = tracker.conn
        call_count = 0

        class FlakyConn:
            def execute(self, query, params=()):
                nonlocal call_count
                if "wal_checkpoint" in query:
                    call_count += 1
                    if call_count == 1:
                        raise sqlite3.OperationalError("disk I/O error")
                return real_conn.execute(query, params)

            def __enter__(self):
                return real_conn.__enter__()

            def __exit__(self, *args):
                return real_conn.__exit__(*args)

        tracker.conn = FlakyConn()

        with patch("time.sleep"):
            tracker._init_db()  # should succeed despite first EIO on wal_checkpoint

        assert call_count == 2  # wal_checkpoint retried once

    @pytest.mark.unit
    def test_init_db_no_retry_on_non_transient_error(self, temp_dir):
        """Non-transient errors in _init_db are raised immediately without retry."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        real_conn = tracker.conn
        call_count = 0

        class BadConn:
            def execute(self, query, params=()):
                nonlocal call_count
                if "wal_checkpoint" in query:
                    call_count += 1
                    raise sqlite3.OperationalError("no such table: nonexistent")
                return real_conn.execute(query, params)

            def __enter__(self):
                return real_conn.__enter__()

            def __exit__(self, *args):
                return real_conn.__exit__(*args)

        tracker.conn = BadConn()

        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            tracker._init_db()

        assert call_count == 1  # raised immediately, no retry

    @pytest.mark.unit
    def test_init_db_raises_after_max_retries(self, temp_dir):
        """Transient EIO in _init_db raises after all 5 attempts are exhausted."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        real_conn = tracker.conn
        call_count = 0

        class AlwaysBadConn:
            def execute(self, query, params=()):
                nonlocal call_count
                if "wal_checkpoint" in query:
                    call_count += 1
                    raise sqlite3.OperationalError("disk I/O error")
                return real_conn.execute(query, params)

            def __enter__(self):
                return real_conn.__enter__()

            def __exit__(self, *args):
                return real_conn.__exit__(*args)

        tracker.conn = AlwaysBadConn()

        with patch("time.sleep"):
            with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
                tracker._init_db()

        assert call_count == 5  # tried 5 times then gave up

    @pytest.mark.unit
    def test_retry_on_database_locked(self, temp_dir):
        """Transient 'database is locked' errors are retried with backoff."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")

        locked_error = sqlite3.OperationalError("database is locked")
        call_count = 0
        original = tracker._db_execute

        def flaky(query, params=()):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise locked_error
            return original(query, params)

        with patch.object(tracker, "_db_execute", side_effect=flaky):
            with patch("time.sleep"):
                tracker._execute_with_retry("SELECT 1", ())

        assert call_count == 3  # failed twice, succeeded on third attempt

    @pytest.mark.unit
    def test_retry_on_disk_io_error(self, temp_dir):
        """Transient 'disk I/O error' (Lustre EIO) errors are retried."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")

        io_error = sqlite3.OperationalError("disk I/O error")
        call_count = 0
        original = tracker._db_execute

        def flaky(query, params=()):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise io_error
            return original(query, params)

        with patch.object(tracker, "_db_execute", side_effect=flaky):
            with patch("time.sleep"):
                tracker._execute_with_retry("SELECT 1", ())

        assert call_count == 2  # failed once, succeeded on retry

    @pytest.mark.unit
    def test_no_retry_on_other_operational_error(self, temp_dir):
        """Non-transient OperationalError (e.g. syntax error) is not retried."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        call_count = 0

        def bad(query, params=()):
            nonlocal call_count
            call_count += 1
            raise sqlite3.OperationalError("no such table: nonexistent")

        with patch.object(tracker, "_db_execute", side_effect=bad):
            with pytest.raises(sqlite3.OperationalError, match="no such table"):
                tracker._execute_with_retry("SELECT 1", ())

        assert call_count == 1  # raised immediately, no retry

    @pytest.mark.unit
    def test_pbs_job_id_round_trip(self, temp_dir):
        """set_pbs_job_id stores a value that get_pbs_job_id reads back unchanged."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")

        # Before set, the column is NULL
        assert tracker.get_pbs_job_id("Amon.tas", "historical") is None

        tracker.set_pbs_job_id("Amon.tas", "historical", "12345.gadi-pbs")
        assert tracker.get_pbs_job_id("Amon.tas", "historical") == "12345.gadi-pbs"

        # Overwrite is allowed (e.g. monitor resubmits a failed job)
        tracker.set_pbs_job_id("Amon.tas", "historical", "67890.gadi-pbs")
        assert tracker.get_pbs_job_id("Amon.tas", "historical") == "67890.gadi-pbs"

    @pytest.mark.unit
    def test_get_pbs_job_id_for_unknown_task_returns_none(self, temp_dir):
        """get_pbs_job_id returns None when the (variable, experiment) row is absent."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        assert tracker.get_pbs_job_id("nonexistent.var", "historical") is None

    @pytest.mark.unit
    def test_list_unfinished_excludes_terminal_states(self, temp_dir):
        """list_unfinished returns only pending/running rows, not completed/failed."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        # pending (never touched after add)
        tracker.add_task("Amon.pending", "historical")
        # running
        tracker.add_task("Amon.running", "historical")
        tracker.mark_running("Amon.running", "historical")
        # completed
        tracker.add_task("Amon.done", "historical")
        tracker.mark_completed("Amon.done", "historical")
        # failed
        tracker.add_task("Amon.bad", "historical")
        tracker.mark_failed("Amon.bad", "historical", "test error")

        rows = tracker.list_unfinished("historical")
        returned = sorted(r[0] for r in rows)
        assert returned == ["Amon.pending", "Amon.running"]

    @pytest.mark.unit
    def test_list_unfinished_returns_status_and_pbs_job_id(self, temp_dir):
        """list_unfinished tuple format is (variable, status, pbs_job_id)."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")
        tracker.set_pbs_job_id("Amon.tas", "historical", "12345.gadi-pbs")

        rows = tracker.list_unfinished("historical")
        assert rows == [("Amon.tas", "running", "12345.gadi-pbs")]

    @pytest.mark.unit
    def test_list_unfinished_scoped_to_experiment(self, temp_dir):
        """list_unfinished filters by experiment_id, never bleeding across experiments."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        tracker.add_task("Amon.tas", "historical")  # stays pending
        tracker.add_task("Amon.tas", "piControl")
        tracker.mark_completed("Amon.tas", "piControl")  # terminal in piControl

        assert [r[0] for r in tracker.list_unfinished("historical")] == ["Amon.tas"]
        assert tracker.list_unfinished("piControl") == []

    @pytest.mark.unit
    def test_schema_migration_adds_pbs_job_id_column(self, temp_dir):
        """Opening an old-schema DB auto-adds pbs_job_id via ALTER TABLE."""
        db_path = temp_dir / "old.db"

        # Hand-build a pre-migration DB lacking the pbs_job_id column
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE cmor_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                variable TEXT NOT NULL,
                experiment_id TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                start_time TEXT, end_time TEXT, error_message TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO cmor_tasks (variable, experiment_id, status) "
            "VALUES ('Omon.zostoga', 'historical', 'running')"
        )
        conn.commit()
        conn.close()

        tracker = TaskTracker(db_path)

        columns = {
            row[1]
            for row in tracker.conn.execute("PRAGMA table_info(cmor_tasks)").fetchall()
        }
        assert "pbs_job_id" in columns

        # Existing rows are preserved by the migration
        assert tracker.get_status("Omon.zostoga", "historical") == "running"

        # The new column functions correctly on the migrated DB
        tracker.set_pbs_job_id("Omon.zostoga", "historical", "168282805.gadi-pbs")
        assert (
            tracker.get_pbs_job_id("Omon.zostoga", "historical") == "168282805.gadi-pbs"
        )

    @pytest.mark.unit
    def test_schema_migration_idempotent(self, temp_dir):
        """Re-opening a DB that already has pbs_job_id does not duplicate the column."""
        db_path = temp_dir / "test_tracker.db"

        # First open: creates schema with pbs_job_id
        t1 = TaskTracker(db_path)
        t1.add_task("Amon.tas", "historical")
        t1.set_pbs_job_id("Amon.tas", "historical", "12345.gadi-pbs")
        t1.conn.close()

        # Second open: migration path should be a no-op
        t2 = TaskTracker(db_path)
        columns = [
            row[1]
            for row in t2.conn.execute("PRAGMA table_info(cmor_tasks)").fetchall()
        ]
        assert columns.count("pbs_job_id") == 1
        # Data preserved across reopens
        assert t2.get_pbs_job_id("Amon.tas", "historical") == "12345.gadi-pbs"
