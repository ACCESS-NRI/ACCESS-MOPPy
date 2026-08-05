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
        with TaskTracker(db_path):
            pass

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
        with TaskTracker(db_path) as tracker:
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
    def test_monitor_requests_are_taken_once(self, temp_dir):
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.enqueue_monitor_request("Amon.tas", "historical")
            tracker.enqueue_monitor_request("Amon.tas", "historical")
            tracker.enqueue_monitor_request("Amon.pr", "historical")

            assert tracker.take_monitor_requests("historical") == [
                "Amon.pr",
                "Amon.tas",
            ]
            assert tracker.take_monitor_requests("historical") == []

    @pytest.mark.unit
    def test_mark_running(self, temp_dir):
        """Test marking task as running."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.mark_running("Amon.tas", "historical")

            status = tracker.get_status("Amon.tas", "historical")
            assert status == "running"

    @pytest.mark.unit
    def test_mark_completed(self, temp_dir):
        """Test marking task as completed."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.mark_running("Amon.tas", "historical")
            tracker.mark_completed("Amon.tas", "historical")

            status = tracker.get_status("Amon.tas", "historical")
            assert status == "completed"

    @pytest.mark.unit
    def test_is_done_functionality(self, temp_dir):
        """Test the is_done method used in templates."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
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
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")

        assert not (temp_dir / "test_tracker.db-shm").exists()
        assert not (temp_dir / "test_tracker.db-wal").exists()

    @pytest.mark.unit
    def test_journal_mode_is_delete(self, temp_dir):
        """Verify DELETE journal mode: crash-safe journal file, no fsync()."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            row = tracker.conn.execute("PRAGMA journal_mode").fetchone()
            assert row[0] == "delete"

    @pytest.mark.unit
    def test_init_db_retries_on_eio(self, temp_dir):
        """_init_db retries when PRAGMA wal_checkpoint hits EIO on Lustre."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        try:
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
                tracker._init_db()  # should succeed despite first EIO

            assert call_count == 2  # wal_checkpoint retried once
        finally:
            # Restore the real connection so close() reaches a real sqlite handle,
            # otherwise the journal file may linger and break temp_dir teardown.
            tracker.conn = real_conn
            tracker.close()

    @pytest.mark.unit
    def test_init_db_no_retry_on_non_transient_error(self, temp_dir):
        """Non-transient errors in _init_db are raised immediately without retry."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        try:
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
        finally:
            tracker.conn = real_conn
            tracker.close()

    @pytest.mark.unit
    def test_init_db_raises_after_max_retries(self, temp_dir):
        """Transient EIO in _init_db raises after all 5 attempts are exhausted."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        try:
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
        finally:
            tracker.conn = real_conn
            tracker.close()

    @pytest.mark.unit
    def test_retry_on_database_locked(self, temp_dir):
        """Transient 'database is locked' errors are retried with backoff."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
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
        with TaskTracker(db_path) as tracker:
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
        with TaskTracker(db_path) as tracker:
            call_count = 0

            def bad(query, params=()):
                nonlocal call_count
                call_count += 1
                raise sqlite3.OperationalError("no such table: nonexistent")

            with patch.object(tracker, "_db_execute", side_effect=bad):
                with pytest.raises(sqlite3.OperationalError, match="no such table"):
                    tracker._execute_with_retry("SELECT 1", ())

            assert call_count == 1  # raised immediately, no retry

    # ------------------------------------------------------------------
    # Context-manager / close() semantics
    # ------------------------------------------------------------------

    @pytest.mark.unit
    def test_close_releases_connection(self, temp_dir):
        """After close() the conn attribute is None and DB ops are rejected."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")

        tracker.close()

        assert tracker.conn is None
        # Subsequent DB ops now blow up because conn is None — by design;
        # callers must not reuse a closed tracker.
        with pytest.raises((AttributeError, TypeError)):
            tracker.get_status("Amon.tas", "historical")

    @pytest.mark.unit
    def test_close_is_idempotent(self, temp_dir):
        """Calling close() multiple times is safe."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.close()
        tracker.close()  # must not raise
        tracker.close()
        assert tracker.conn is None

    @pytest.mark.unit
    def test_close_tolerates_broken_conn(self, temp_dir):
        """close() swallows errors from an already-mangled conn (e.g. mock state)."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        class BrokenConn:
            def close(self):
                raise RuntimeError("conn already poisoned")

        # Close the real conn first so no sqlite handle leaks; then swap in
        # the broken conn to exercise close()'s error tolerance.
        tracker.conn.close()
        tracker.conn = BrokenConn()
        tracker.close()  # must not raise
        assert tracker.conn is None

    @pytest.mark.unit
    def test_context_manager_closes_on_normal_exit(self, temp_dir):
        """`with TaskTracker(...) as t:` closes the connection on block exit."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            assert tracker.conn is not None

        assert tracker.conn is None

    @pytest.mark.unit
    def test_context_manager_closes_on_exception(self, temp_dir):
        """An exception inside the with block still triggers cleanup, and the
        exception propagates (we must not swallow it)."""
        db_path = temp_dir / "test_tracker.db"
        tracker_ref = []

        with pytest.raises(RuntimeError, match="boom"):
            with TaskTracker(db_path) as tracker:
                tracker_ref.append(tracker)
                tracker.add_task("Amon.tas", "historical")
                raise RuntimeError("boom")

        # Connection was closed despite the exception
        assert tracker_ref[0].conn is None

    @pytest.mark.unit
    def test_context_manager_leaves_no_journal_file(self, temp_dir):
        """After `with` exits, the sqlite journal file must not linger.

        This is the actual fix for the temp_dir teardown bug on Lustre.
        """
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.mark_running("Amon.tas", "historical")
            tracker.mark_completed("Amon.tas", "historical")

        leftovers = [p.name for p in temp_dir.iterdir() if p.name != "test_tracker.db"]
        assert leftovers == [], f"unexpected files lingering: {leftovers}"

    @pytest.mark.unit
    def test_reusing_closed_tracker_raises(self, temp_dir):
        """A closed tracker is intentionally one-shot; reopening requires a new instance."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.close()

        with pytest.raises((AttributeError, TypeError)):
            tracker.add_task("Amon.pr", "historical")

    @pytest.mark.unit
    def test_pbs_job_id_round_trip(self, temp_dir):
        """set_pbs_job_id stores a value that get_pbs_job_id reads back unchanged."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
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
        with TaskTracker(db_path) as tracker:
            assert tracker.get_pbs_job_id("nonexistent.var", "historical") is None

    @pytest.mark.unit
    def test_pbs_info_round_trip(self, temp_dir):
        """set_pbs_info stores structured PBS metadata as JSON."""
        db_path = temp_dir / "test_tracker.db"
        pbs_info = {
            "scheduler": "pbs",
            "job_id": "12345.gadi-pbs",
            "Exit_status": "0",
            "resources_used.walltime": "00:10:00",
        }
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            assert tracker.get_pbs_info("Amon.tas", "historical") is None

            tracker.set_pbs_info("Amon.tas", "historical", pbs_info)
            assert tracker.get_pbs_info("Amon.tas", "historical") == pbs_info

            tracker.set_pbs_info("Amon.tas", "historical", None)
            assert tracker.get_pbs_info("Amon.tas", "historical") is None

    @pytest.mark.unit
    def test_get_pbs_info_returns_none_for_unknown_task(self, temp_dir):
        """get_pbs_info returns None when the task row is absent."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            assert tracker.get_pbs_info("Amon.tas", "historical") is None

    @pytest.mark.unit
    def test_get_pbs_info_returns_none_for_malformed_json(self, temp_dir):
        """Malformed legacy/corrupt PBS JSON is treated as missing metadata."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.conn.execute(
                "UPDATE cmor_tasks SET pbs_info_json='not json' WHERE variable=?",
                ("Amon.tas",),
            )

            assert tracker.get_pbs_info("Amon.tas", "historical") is None

    @pytest.mark.unit
    def test_worker_memory_round_trip(self, temp_dir):
        """set_worker_memory stores per-worker peak RSS metadata as JSON."""
        db_path = temp_dir / "test_tracker.db"
        worker_memory = {
            "n_workers": 4,
            "memory_limit_per_worker": "16.00GB",
            "peak_rss_mb": {
                "tcp://127.0.0.1:1234": 9821.4,
                "tcp://127.0.0.1:1235": 9650.2,
            },
        }
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            assert tracker.get_worker_memory("Amon.tas", "historical") is None

            tracker.set_worker_memory("Amon.tas", "historical", worker_memory)
            assert tracker.get_worker_memory("Amon.tas", "historical") == worker_memory

            tracker.set_worker_memory("Amon.tas", "historical", None)
            assert tracker.get_worker_memory("Amon.tas", "historical") is None

    @pytest.mark.unit
    def test_get_worker_memory_returns_none_for_unknown_task(self, temp_dir):
        """get_worker_memory returns None when the task row is absent."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            assert tracker.get_worker_memory("Amon.tas", "historical") is None

    @pytest.mark.unit
    def test_get_worker_memory_returns_none_for_malformed_json(self, temp_dir):
        """Malformed/corrupt worker-memory JSON is treated as missing metadata."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.conn.execute(
                "UPDATE cmor_tasks SET worker_memory_json='not json' WHERE variable=?",
                ("Amon.tas",),
            )

            assert tracker.get_worker_memory("Amon.tas", "historical") is None

    @pytest.mark.unit
    def test_list_unfinished_excludes_terminal_states(self, temp_dir):
        """list_unfinished returns only pending/running rows, not completed/failed."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
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
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.mark_running("Amon.tas", "historical")
            tracker.set_pbs_job_id("Amon.tas", "historical", "12345.gadi-pbs")

            rows = tracker.list_unfinished("historical")
            assert rows == [("Amon.tas", "running", "12345.gadi-pbs")]

    @pytest.mark.unit
    def test_list_unfinished_scoped_to_experiment(self, temp_dir):
        """list_unfinished filters by experiment_id, never bleeding across experiments."""
        db_path = temp_dir / "test_tracker.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")  # stays pending
            tracker.add_task("Amon.tas", "piControl")
            tracker.mark_completed("Amon.tas", "piControl")  # terminal in piControl

            assert [r[0] for r in tracker.list_unfinished("historical")] == ["Amon.tas"]
            assert tracker.list_unfinished("piControl") == []

    @pytest.mark.unit
    def test_schema_migration_adds_pbs_columns(self, temp_dir):
        """Opening an old-schema DB auto-adds PBS columns via ALTER TABLE."""
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

        with TaskTracker(db_path) as tracker:
            columns = {
                row[1]
                for row in tracker.conn.execute(
                    "PRAGMA table_info(cmor_tasks)"
                ).fetchall()
            }
            assert "pbs_job_id" in columns
            assert "pbs_info_json" in columns
            assert "worker_memory_json" in columns

            # Existing rows are preserved by the migration
            assert tracker.get_status("Omon.zostoga", "historical") == "running"

            # The new column functions correctly on the migrated DB
            tracker.set_pbs_job_id("Omon.zostoga", "historical", "168282805.gadi-pbs")
            assert (
                tracker.get_pbs_job_id("Omon.zostoga", "historical")
                == "168282805.gadi-pbs"
            )
            pbs_info = {"job_id": "168282805.gadi-pbs", "Exit_status": "0"}
            tracker.set_pbs_info("Omon.zostoga", "historical", pbs_info)
            assert tracker.get_pbs_info("Omon.zostoga", "historical") == pbs_info

    @pytest.mark.unit
    def test_schema_migration_idempotent(self, temp_dir):
        """Re-opening a DB that already has PBS columns does not duplicate them."""
        db_path = temp_dir / "test_tracker.db"

        # First open: creates schema with pbs_job_id
        with TaskTracker(db_path) as t1:
            t1.add_task("Amon.tas", "historical")
            t1.set_pbs_job_id("Amon.tas", "historical", "12345.gadi-pbs")

        # Second open: migration path should be a no-op
        with TaskTracker(db_path) as t2:
            columns = [
                row[1]
                for row in t2.conn.execute("PRAGMA table_info(cmor_tasks)").fetchall()
            ]
            assert columns.count("pbs_job_id") == 1
            assert columns.count("pbs_info_json") == 1
            assert columns.count("worker_memory_json") == 1
            # Data preserved across reopens
            assert t2.get_pbs_job_id("Amon.tas", "historical") == "12345.gadi-pbs"
