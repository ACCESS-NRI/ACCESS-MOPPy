import sqlite3
import threading

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
    def test_claim_next_task_returns_pending(self, temp_dir):
        """claim_next_task should return a pending variable and mark it running."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        tracker.add_task("Amon.pr", "historical")
        tracker.add_task("Amon.tas", "historical")

        variable = tracker.claim_next_task("historical")

        assert variable in {"Amon.pr", "Amon.tas"}
        assert tracker.get_status(variable, "historical") == "running"

    @pytest.mark.unit
    def test_claim_next_task_returns_none_when_empty(self, temp_dir):
        """claim_next_task should return None when no pending tasks exist."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        # No tasks at all
        assert tracker.claim_next_task("historical") is None

        # All tasks already completed
        tracker.add_task("Amon.pr", "historical")
        tracker.mark_running("Amon.pr", "historical")
        tracker.mark_completed("Amon.pr", "historical")

        assert tracker.claim_next_task("historical") is None

    @pytest.mark.unit
    def test_claim_next_task_processes_all_tasks(self, temp_dir):
        """Calling claim_next_task repeatedly should drain all pending tasks."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        variables = ["Amon.pr", "Amon.tas", "Omon.tos"]
        for var in variables:
            tracker.add_task(var, "historical")

        claimed = []
        while True:
            var = tracker.claim_next_task("historical")
            if var is None:
                break
            claimed.append(var)
            tracker.mark_completed(var, "historical")

        assert sorted(claimed) == sorted(variables)
        # All should now be completed
        for var in variables:
            assert tracker.is_done(var, "historical")

    @pytest.mark.unit
    def test_claim_next_task_is_safe_under_concurrent_access(self, temp_dir):
        """Multiple threads claiming tasks should each get a unique variable."""
        db_path = temp_dir / "test_tracker.db"
        # Seed the database from a single tracker
        seeder = TaskTracker(db_path)
        variables = [f"Amon.var{i}" for i in range(10)]
        for var in variables:
            seeder.add_task(var, "historical")

        claimed = []
        lock = threading.Lock()
        errors = []

        def worker():
            tracker = TaskTracker(db_path)
            while True:
                try:
                    var = tracker.claim_next_task("historical")
                    if var is None:
                        break
                    with lock:
                        claimed.append(var)
                    tracker.mark_completed(var, "historical")
                except Exception as e:
                    with lock:
                        errors.append(e)
                    break

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Concurrent access errors: {errors}"
        # Each variable should be claimed exactly once
        assert sorted(claimed) == sorted(variables)

    @pytest.mark.unit
    def test_sqlite_connection_has_busy_timeout(self, temp_dir):
        """SQLite connection should have WAL mode and busy_timeout configured."""
        db_path = temp_dir / "test_tracker.db"
        tracker = TaskTracker(db_path)

        journal_mode = tracker.conn.execute("PRAGMA journal_mode").fetchone()[0]
        busy_timeout = tracker.conn.execute("PRAGMA busy_timeout").fetchone()[0]

        assert journal_mode == "wal"
        assert busy_timeout > 0

    @pytest.mark.unit
    def test_postgres_import_error_is_helpful(self, temp_dir):
        """Attempting to use PostgreSQL without psycopg2 gives a clear error."""
        import sys
        from unittest.mock import patch

        with patch.dict(sys.modules, {"psycopg2": None}):
            with pytest.raises(ImportError, match="psycopg2"):
                TaskTracker(db_url="postgresql://localhost/test")

