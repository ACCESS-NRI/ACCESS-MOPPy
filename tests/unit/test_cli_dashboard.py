"""Unit tests for the rich-based CLI dashboard."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from access_moppy.dashboard import cli_dashboard
from access_moppy.tracking import TaskTracker


def _seed_db(db_path: Path) -> None:
    """Populate a tracker DB with a representative mix of statuses."""
    with TaskTracker(db_path) as tracker:
        for var in ("Amon.tas", "Amon.pr", "Omon.tos", "SImon.siconc", "Omon.thetao"):
            tracker.add_task(var, "piControl")

    now = datetime(2026, 5, 13, 12, 0, 0)
    conn = sqlite3.connect(db_path)
    with conn:
        # completed with explicit start/end so avg duration is well-defined
        conn.execute(
            "UPDATE cmor_tasks SET status='completed', start_time=?, end_time=? "
            "WHERE variable='Amon.pr'",
            (
                now.isoformat(timespec="seconds"),
                (now + timedelta(seconds=600)).isoformat(timespec="seconds"),
            ),
        )
        conn.execute(
            "UPDATE cmor_tasks SET status='running', start_time=? "
            "WHERE variable='Amon.tas'",
            ((now + timedelta(minutes=10)).isoformat(timespec="seconds"),),
        )
        conn.execute(
            "UPDATE cmor_tasks SET status='running', start_time=? "
            "WHERE variable='Omon.tos'",
            ((now + timedelta(minutes=11)).isoformat(timespec="seconds"),),
        )
        conn.execute(
            "UPDATE cmor_tasks SET status='failed', start_time=?, end_time=?, "
            "error_message=? WHERE variable='SImon.siconc'",
            (
                now.isoformat(timespec="seconds"),
                (now + timedelta(seconds=120)).isoformat(timespec="seconds"),
                "KeyError: 'aice_m' not found in input files",
            ),
        )
        # Omon.thetao left as 'pending'
    conn.close()


@pytest.fixture()
def seeded_db(tmp_path: Path) -> Path:
    db = tmp_path / "cmor_tasks.db"
    _seed_db(db)
    return db


def _seed_many_failures_db(db_path: Path, n: int = 5) -> None:
    """Populate a tracker DB with `n` failed tasks, for pagination tests."""
    variables = [f"Amon.var{i}" for i in range(n)]
    with TaskTracker(db_path) as tracker:
        for var in variables:
            tracker.add_task(var, "piControl")

    now = datetime(2026, 5, 13, 12, 0, 0)
    conn = sqlite3.connect(db_path)
    with conn:
        for i, var in enumerate(variables):
            conn.execute(
                "UPDATE cmor_tasks SET status='failed', start_time=?, end_time=?, "
                "error_message=? WHERE variable=?",
                (
                    now.isoformat(timespec="seconds"),
                    (now + timedelta(seconds=i)).isoformat(timespec="seconds"),
                    f"error {i}",
                    var,
                ),
            )
    conn.close()


@pytest.fixture()
def many_failures_db(tmp_path: Path) -> Path:
    db = tmp_path / "cmor_tasks.db"
    _seed_many_failures_db(db, n=5)
    return db


class TestResolveDbPath:
    @pytest.mark.unit
    def test_cli_value_wins(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CMOR_TRACKER_DB", "/should/not/be/used.db")
        assert (
            cli_dashboard.resolve_db_path(str(tmp_path / "x.db")) == tmp_path / "x.db"
        )

    @pytest.mark.unit
    def test_env_used_when_no_cli(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CMOR_TRACKER_DB", str(tmp_path / "env.db"))
        assert cli_dashboard.resolve_db_path(None) == tmp_path / "env.db"

    @pytest.mark.unit
    def test_default_home_fallback(self, monkeypatch):
        monkeypatch.delenv("CMOR_TRACKER_DB", raising=False)
        result = cli_dashboard.resolve_db_path(None)
        assert result.name == "cmor_tasks.db"
        assert result.parent.name == "db"


class TestLoadSnapshot:
    @pytest.mark.unit
    def test_missing_db_returns_error_snapshot(self, tmp_path):
        snap = cli_dashboard.load_snapshot(tmp_path / "does_not_exist.db")
        assert snap.error is not None
        assert "Database not found" in snap.error
        assert snap.total == 0

    @pytest.mark.unit
    def test_summary_counts(self, seeded_db):
        snap = cli_dashboard.load_snapshot(seeded_db)
        assert snap.error is None
        assert snap.total == 5
        assert snap.summary == {
            "pending": 1,
            "running": 2,
            "completed": 1,
            "failed": 1,
        }
        assert snap.completed == 1

    @pytest.mark.unit
    def test_avg_completed_duration(self, seeded_db):
        snap = cli_dashboard.load_snapshot(seeded_db)
        # Only one completed task (600s).
        assert snap.avg_completed_seconds is not None
        assert abs(snap.avg_completed_seconds - 600.0) < 1.0

    @pytest.mark.unit
    def test_eta_uses_remaining_count(self, seeded_db):
        snap = cli_dashboard.load_snapshot(seeded_db)
        remaining = snap.total - snap.completed
        expected = snap.avg_completed_seconds * remaining
        assert abs(snap.eta_seconds - expected) < 1.0

    @pytest.mark.unit
    def test_status_filter(self, seeded_db):
        snap = cli_dashboard.load_snapshot(seeded_db, statuses=["failed"])
        assert len(snap.tasks) == 1
        assert snap.tasks[0]["variable"] == "SImon.siconc"

    @pytest.mark.unit
    def test_experiment_filter_no_match(self, seeded_db):
        snap = cli_dashboard.load_snapshot(seeded_db, experiment="historical")
        assert snap.tasks == []

    @pytest.mark.unit
    def test_failures_panel_data(self, seeded_db):
        snap = cli_dashboard.load_snapshot(seeded_db)
        assert len(snap.failures) == 1
        assert "aice_m" in snap.failures[0]["error_message"]


class TestRender:
    @pytest.mark.unit
    def test_render_contains_key_strings(self, seeded_db):
        from rich.console import Console

        snap = cli_dashboard.load_snapshot(seeded_db)
        console = Console(record=True, width=120, color_system=None)
        console.print(cli_dashboard.render(snap, page_size=20))
        out = console.export_text()

        assert "CMORisation Monitor" in out
        assert "Amon.tas" in out
        assert "SImon.siconc" in out
        assert "completed 1 / 5" in out
        assert "Recent failures" in out
        # New: title now uses range form
        assert "Tasks 1-5 of 5" in out

    @pytest.mark.unit
    def test_render_handles_error_snapshot(self, tmp_path):
        from rich.console import Console

        snap = cli_dashboard.load_snapshot(tmp_path / "missing.db")
        console = Console(record=True, width=120, color_system=None)
        console.print(cli_dashboard.render(snap))
        out = console.export_text()
        assert "Database not found" in out

    @pytest.mark.unit
    def test_render_pagination_slices_rows(self, seeded_db):
        from rich.console import Console

        snap = cli_dashboard.load_snapshot(seeded_db)
        console = Console(record=True, width=120, color_system=None)
        console.print(cli_dashboard.render(snap, offset=2, page_size=2))
        out = console.export_text()
        # 5 tasks total; offset=2, page_size=2 → tasks 3-4
        assert "Tasks 3-4 of 5" in out

    @pytest.mark.unit
    def test_render_failures_pagination_slices_rows(self, many_failures_db):
        from rich.console import Console

        snap = cli_dashboard.load_snapshot(many_failures_db)
        console = Console(record=True, width=120, color_system=None)
        console.print(cli_dashboard.render(snap, page_size=20, failures_offset=2))
        out = console.export_text()
        # 5 failures total, page_size=2 for the panel slice request below.
        console2 = Console(record=True, width=120, color_system=None)
        console2.print(
            cli_dashboard.render(snap, page_size=2, failures_offset=2, offset=0)
        )
        out2 = console2.export_text()
        assert "Recent failures 3-4 of 5" in out2
        assert "Recent failures 1-5 of 5" in out

    @pytest.mark.unit
    def test_render_focus_highlights_panel_title(self, many_failures_db):
        from rich.console import Console

        snap = cli_dashboard.load_snapshot(many_failures_db)
        console = Console(record=True, width=120, color_system=None)
        console.print(
            cli_dashboard.render(snap, page_size=20, show_footer=True, focus="failures")
        )
        out = console.export_text()
        assert "Recent failures" in out
        assert "(focused)" in out
        # The focus marker should be attached to the failures panel, not tasks.
        fail_line = next(line for line in out.splitlines() if "Recent failures" in line)
        assert "(focused)" in fail_line

    @pytest.mark.unit
    def test_render_filtered_title_shows_db_total(self, seeded_db):
        from rich.console import Console

        snap = cli_dashboard.load_snapshot(seeded_db, statuses=["failed"])
        console = Console(record=True, width=120, color_system=None)
        console.print(cli_dashboard.render(snap, page_size=20))
        out = console.export_text()
        # 1 visible after filter, 5 in DB
        assert "filtered" in out
        assert "DB total 5" in out


class TestPagingHelpers:
    @pytest.mark.unit
    def test_clamp_offset_bounds(self):
        assert cli_dashboard.clamp_offset(0, 20, 0) == 0
        assert cli_dashboard.clamp_offset(50, 20, 100) == 50
        assert cli_dashboard.clamp_offset(95, 20, 100) == 80
        assert cli_dashboard.clamp_offset(-5, 20, 100) == 0

    @pytest.mark.unit
    def test_apply_key_scroll(self):
        # j / arrow-down → +1
        assert cli_dashboard.apply_key("j", 0, 20, 100) == 1
        assert cli_dashboard.apply_key("\x1b[B", 5, 20, 100) == 6
        # k / arrow-up → -1
        assert cli_dashboard.apply_key("k", 5, 20, 100) == 4
        assert cli_dashboard.apply_key("\x1b[A", 0, 20, 100) == 0

    @pytest.mark.unit
    def test_apply_key_page(self):
        # n / Space / PgDn → +page_size, clamped
        assert cli_dashboard.apply_key("n", 0, 20, 100) == 20
        assert cli_dashboard.apply_key(" ", 70, 20, 100) == 80
        assert cli_dashboard.apply_key("\x1b[6~", 70, 20, 100) == 80
        # p / b / PgUp → -page_size
        assert cli_dashboard.apply_key("p", 40, 20, 100) == 20
        assert cli_dashboard.apply_key("\x1b[5~", 10, 20, 100) == 0

    @pytest.mark.unit
    def test_apply_key_jump(self):
        assert cli_dashboard.apply_key("g", 50, 20, 100) == 0
        assert cli_dashboard.apply_key("G", 0, 20, 100) == 80

    @pytest.mark.unit
    def test_apply_key_unknown_returns_offset(self):
        assert cli_dashboard.apply_key("x", 5, 20, 100) == 5

    @pytest.mark.unit
    def test_is_quit_key(self):
        for key in ("q", "Q", "\x03", "\x04"):
            assert cli_dashboard.is_quit_key(key)
        for key in ("j", "k", "n", "p"):
            assert not cli_dashboard.is_quit_key(key)


class TestMain:
    @pytest.mark.unit
    def test_json_mode_emits_valid_json(self, seeded_db, capsys, monkeypatch):
        monkeypatch.setenv("CMOR_TRACKER_DB", str(seeded_db))
        rc = cli_dashboard.main(["--json"])
        captured = capsys.readouterr().out
        payload = json.loads(captured)
        assert rc == 0
        assert payload["total"] == 5
        assert payload["summary"]["running"] == 2
        assert payload["failures"][0]["variable"] == "SImon.siconc"

    @pytest.mark.unit
    def test_once_mode_runs_and_returns_zero(self, seeded_db, capsys):
        rc = cli_dashboard.main(["--once", "--db", str(seeded_db), "--no-color"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "completed 1 / 5" in out

    @pytest.mark.unit
    def test_once_mode_respects_page_arg(self, seeded_db, capsys):
        rc = cli_dashboard.main(
            [
                "--once",
                "--db",
                str(seeded_db),
                "--page-size",
                "2",
                "--page",
                "2",
                "--no-color",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "Tasks 3-4 of 5" in out

    @pytest.mark.unit
    def test_invalid_status_value_rejected(self, seeded_db):
        with pytest.raises(SystemExit):
            cli_dashboard.main(["--once", "--db", str(seeded_db), "--status", "bogus"])
