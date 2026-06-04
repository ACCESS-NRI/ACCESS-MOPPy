"""Unit tests for durable batch JSON report generation."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from access_moppy import batch_report
from access_moppy.batch_cmoriser import SIDECAR_FILENAME, finalize_monitor
from access_moppy.tracking import TaskTracker


def _seed_mixed_db(db_path: Path, script_dir: Path) -> None:
    with TaskTracker(db_path) as tracker:
        for variable in ("Amon.tas", "Amon.pr", "Omon.tos"):
            tracker.add_task(variable, "historical")
        tracker.set_pbs_job_id("Amon.tas", "historical", "123.gadi-pbs")
        tracker.set_pbs_job_id("Amon.pr", "historical", "124.gadi-pbs")
        tracker.set_pbs_job_id("Omon.tos", "historical", "125.gadi-pbs")
        tracker.set_pbs_info(
            "Amon.tas",
            "historical",
            {
                "scheduler": "pbs",
                "qstat_format": "json",
                "job_id": "123.gadi-pbs",
                "job_state": "F",
                "Exit_status": 0,
                "queue": "normal",
                "project": "tm70",
                "Account_Name": "tm70",
                "pbs_server": "gadi-pbs",
                "pbs_version": "2022.1.3",
                "ctime": "Thu Jun 04 01:00:00 2026",
                "etime": "Thu Jun 04 01:00:01 2026",
                "qtime": "Thu Jun 04 01:00:02 2026",
                "stime": "Thu Jun 04 01:01:00 2026",
                "mtime": "Thu Jun 04 01:30:00 2026",
                "resources_used.cpupercent": 95,
                "resources_used.cput": "00:25:12",
                "resources_used.mem": "6gb",
                "resources_used.vmem": "7gb",
                "resources_used.walltime": "00:27:03",
                "Resource_List.ncpus": 4,
                "Resource_List.mem": "8gb",
                "Resource_List.walltime": "00:30:00",
                "Resource_List.storage": "gdata/tm70+scratch/tm70",
                "exec_host": "gadi-cpu-clx-0001/0*4",
                "comment": "Job run",
            },
        )

    start = datetime(2026, 6, 4, 1, 0, 0)
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "UPDATE cmor_tasks SET status='completed', start_time=?, end_time=? "
            "WHERE variable='Amon.tas'",
            (
                start.isoformat(timespec="seconds"),
                (start + timedelta(seconds=90)).isoformat(timespec="seconds"),
            ),
        )
        conn.execute(
            "UPDATE cmor_tasks SET status='failed', start_time=?, end_time=?, "
            "error_message=? WHERE variable='Amon.pr'",
            (
                start.isoformat(timespec="seconds"),
                (start + timedelta(seconds=30)).isoformat(timespec="seconds"),
                "input missing",
            ),
        )
        conn.execute(
            "UPDATE cmor_tasks SET status='running', start_time=? "
            "WHERE variable='Omon.tos'",
            ((start + timedelta(seconds=10)).isoformat(timespec="seconds"),),
        )
    conn.close()

    err_dir = script_dir / "Amon_pr"
    err_dir.mkdir(parents=True)
    (err_dir / "cmor_Amon_pr.err").write_text("line1\nline2\nline3\n")


def test_build_batch_report_mixed_statuses(tmp_path: Path) -> None:
    db_path = tmp_path / "cmor_tasks.db"
    script_dir = tmp_path / "cmor_job_scripts"
    _seed_mixed_db(db_path, script_dir)
    (tmp_path / SIDECAR_FILENAME).write_text("999.gadi-pbs\n2026-06-04T00:00:00\n")

    report = batch_report.build_batch_report(
        db_path,
        config={"experiment_id": "historical"},
        config_path=tmp_path / "batch_config.yml",
        script_dir=script_dir,
        created_at="2026-06-04T02:00:00+00:00",
        stderr_tail_lines=2,
    )

    assert report["schema_version"] == "access-moppy.batch-report.v1"
    assert report["status"] == "incomplete"
    assert report["success"] is False
    assert report["all_tasks_terminal"] is False
    assert report["completed_at"] is None
    assert report["summary"] == {
        "completed": 1,
        "failed": 1,
        "running": 1,
        "pending": 0,
        "retrying": 0,
        "total": 3,
    }
    assert report["monitor"]["pbs_job_id"] == "999.gadi-pbs"
    assert report["monitor"]["submitted_at"] == "2026-06-04T00:00:00"
    completed = next(t for t in report["tasks"] if t["variable"] == "Amon.tas")
    assert completed["duration_seconds"] == 90.0
    assert completed["pbs_job_id"] == "123.gadi-pbs"
    assert completed["pbs"]["scheduler"] == "pbs"
    assert completed["pbs"]["job_id"] == "123.gadi-pbs"
    assert completed["pbs"]["exit_status"] == 0
    assert completed["pbs"]["queue"] == "normal"
    assert completed["pbs"]["resources_used"]["mem"] == "6gb"
    assert completed["pbs"]["resources_requested"]["walltime"] == "00:30:00"
    assert completed["pbs"]["timestamps"]["started_at"] == "Thu Jun 04 01:01:00 2026"
    assert completed["pbs"]["raw_qstat"]["qstat_format"] == "json"
    running = next(t for t in report["tasks"] if t["variable"] == "Omon.tos")
    assert running["pbs"] is None
    failure = report["failures"][0]
    assert failure["variable"] == "Amon.pr"
    assert failure["stderr_tail"] == "line2\nline3"


@pytest.mark.parametrize(
    ("statuses", "expected_status", "success", "all_terminal"),
    [
        (["completed", "completed"], "completed", True, True),
        (["completed", "failed"], "failed", False, True),
        (["completed", "pending"], "incomplete", False, False),
    ],
)
def test_report_status_classification(
    tmp_path: Path,
    statuses: list[str],
    expected_status: str,
    success: bool,
    all_terminal: bool,
) -> None:
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        for idx, status in enumerate(statuses):
            variable = f"Amon.var{idx}"
            tracker.add_task(variable, "historical")
            if status == "completed":
                tracker.mark_completed(variable, "historical")
            elif status == "failed":
                tracker.mark_failed(variable, "historical", "boom")

    report = batch_report.build_batch_report(
        db_path, created_at="2026-06-04T02:00:00+00:00"
    )

    assert report["status"] == expected_status
    assert report["success"] is success
    assert report["all_tasks_terminal"] is all_terminal
    assert (report["completed_at"] is not None) is all_terminal


def test_write_batch_report_and_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

    report_path = tmp_path / "report.json"
    written = batch_report.write_batch_report(db_path, report_path)
    assert written == report_path
    payload = json.loads(report_path.read_text())
    assert payload["summary"]["completed"] == 1

    cli_path = tmp_path / "cli-report.json"
    rc = batch_report.main(["--db", str(db_path), "--output", str(cli_path)])
    assert rc == 0
    assert str(cli_path) in capsys.readouterr().out
    assert json.loads(cli_path.read_text())["tasks"][0]["variable"] == "Amon.tas"


def test_build_batch_report_handles_old_tracker_schema(tmp_path: Path) -> None:
    """Reports can still be generated from pre-PBS-metadata tracker DBs."""
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            """
            CREATE TABLE cmor_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                variable TEXT NOT NULL,
                experiment_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                start_time TEXT,
                end_time TEXT,
                error_message TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO cmor_tasks (variable, experiment_id, status) "
            "VALUES ('Amon.tas', 'historical', 'completed')"
        )
    conn.close()

    report = batch_report.build_batch_report(db_path)

    assert report["tasks"][0]["pbs_job_id"] is None
    assert report["tasks"][0]["pbs"] is None


def test_finalize_monitor_writes_report_before_removing_sidecar(tmp_path: Path) -> None:
    db_path = tmp_path / "cmor_tasks.db"
    script_dir = tmp_path / "cmor_job_scripts"
    script_dir.mkdir()
    (tmp_path / SIDECAR_FILENAME).write_text("monitor.123\n2026-06-04T00:00:00\n")

    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")
        finalize_monitor(
            tracker,
            {"variables": ["Amon.tas"], "experiment_id": "historical"},
            "historical",
            db_path,
            config_path=tmp_path / "batch_config.yml",
            script_dir=script_dir,
        )

    assert not (tmp_path / SIDECAR_FILENAME).exists()
    report = json.loads((tmp_path / "moppy_batch_report.json").read_text())
    assert report["status"] == "completed"
    assert report["success"] is True
    assert report["monitor"]["pbs_job_id"] == "monitor.123"
    assert report["script_dir"] == str(script_dir)
