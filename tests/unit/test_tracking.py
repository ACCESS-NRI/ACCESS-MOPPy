import sqlite3

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
