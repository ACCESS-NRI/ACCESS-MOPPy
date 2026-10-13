"""Unit tests for durable batch JSON report generation."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

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
        config={
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-5",
            "variant_label": "r1i1p1f1",
        },
        config_path=tmp_path / "batch_config.yml",
        script_dir=script_dir,
        created_at="2026-06-04T02:00:00+00:00",
        stderr_tail_lines=2,
    )

    assert report["schema_version"] == "access-moppy.batch-report.v2"
    assert report["source_id"] == "ACCESS-ESM1-5"
    assert report["variant_label"] == "r1i1p1f1"
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
    assert failure["stderr_tail"] == ["line2", "line3"]


def test_build_batch_report_includes_worker_memory(tmp_path: Path) -> None:
    """A task's recorded per-worker peak RSS surfaces in the report."""
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")
        tracker.set_worker_memory(
            "Amon.tas",
            "historical",
            {
                "n_workers": 4,
                "memory_limit_per_worker": "16.00GB",
                "peak_rss_mb": {"tcp://w1": 9821.4, "tcp://w2": 9650.2},
            },
        )
        tracker.add_task("Amon.pr", "historical")
        tracker.mark_completed("Amon.pr", "historical")

    report = batch_report.build_batch_report(db_path)

    with_memory = next(t for t in report["tasks"] if t["variable"] == "Amon.tas")
    assert with_memory["worker_memory"] == {
        "n_workers": 4,
        "memory_limit_per_worker": "16.00GB",
        "peak_rss_mb": {"tcp://w1": 9821.4, "tcp://w2": 9650.2},
    }
    without_memory = next(t for t in report["tasks"] if t["variable"] == "Amon.pr")
    assert without_memory["worker_memory"] is None


def test_build_batch_report_includes_output_summary(tmp_path: Path) -> None:
    """A task's recorded output file count and volume surfaces in the report."""
    db_path = tmp_path / "cmor_tasks.db"
    output_summary = {
        "file_count": 1,
        "total_bytes": 2048,
        "total_size": "2.0 KiB",
        "files": [
            {
                "path": str(tmp_path / "tas.nc"),
                "size_bytes": 2048,
                "size": "2.0 KiB",
            }
        ],
    }
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")
        tracker.set_output_summary("Amon.tas", "historical", output_summary)
        tracker.add_task("Amon.pr", "historical")
        tracker.mark_completed("Amon.pr", "historical")

    report = batch_report.build_batch_report(db_path)

    with_output = next(t for t in report["tasks"] if t["variable"] == "Amon.tas")
    assert with_output["output_summary"] == output_summary
    without_output = next(t for t in report["tasks"] if t["variable"] == "Amon.pr")
    assert without_output["output_summary"] is None


def test_as_lines_keeps_single_line_but_splits_multiline() -> None:
    """Single-line messages stay strings; multi-line ones become line lists."""
    assert batch_report._as_lines(None) is None
    assert batch_report._as_lines("") is None
    assert batch_report._as_lines("input missing") == "input missing"
    assert batch_report._as_lines("job 1 | err_tail:\nline1\nline2") == [
        "job 1 | err_tail:",
        "line1",
        "line2",
    ]


def test_build_batch_report_splits_multiline_error_message(tmp_path: Path) -> None:
    """Multi-line failure error messages serialise as line lists for readability."""
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.pr", "historical")
        tracker.mark_failed(
            "Amon.pr", "historical", "job 1 | err_tail:\nTraceback\n  boom"
        )

    report = batch_report.build_batch_report(db_path)

    assert report["failures"][0]["error_message"] == [
        "job 1 | err_tail:",
        "Traceback",
        "  boom",
    ]


def test_build_batch_report_empty_stderr_tail_is_none(tmp_path: Path) -> None:
    """An existing but empty stderr file yields no stderr tail."""
    db_path = tmp_path / "cmor_tasks.db"
    script_dir = tmp_path / "cmor_job_scripts"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.pr", "historical")
        tracker.mark_failed("Amon.pr", "historical", "boom")

    err_dir = script_dir / "Amon_pr"
    err_dir.mkdir(parents=True)
    (err_dir / "cmor_Amon_pr.err").write_text("")

    report = batch_report.build_batch_report(db_path, script_dir=script_dir)

    assert report["failures"][0]["stderr_tail"] is None


def test_build_batch_report_handles_invalid_dates_and_stderr_read_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid timestamps and unreadable stderr tails are omitted safely."""
    db_path = tmp_path / "cmor_tasks.db"
    script_dir = tmp_path / "cmor_job_scripts"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_failed("Amon.tas", "historical", "boom")

    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "UPDATE cmor_tasks SET start_time='not-a-date', end_time='also-bad' "
            "WHERE variable='Amon.tas'"
        )
    conn.close()

    err_dir = script_dir / "Amon_tas"
    err_dir.mkdir(parents=True)
    err_path = err_dir / "cmor_Amon_tas.err"
    err_path.write_text("line1\n")

    original_read_text = Path.read_text

    def flaky_read_text(self: Path, *args, **kwargs) -> str:
        if self == err_path:
            raise OSError("cannot read")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", flaky_read_text)

    report = batch_report.build_batch_report(db_path, script_dir=script_dir)

    task = report["tasks"][0]
    assert task["duration_seconds"] is None
    assert report["failures"][0]["stderr_tail"] is None


def test_build_batch_report_ignores_invalid_pbs_json(tmp_path: Path) -> None:
    """Malformed or non-object PBS JSON is treated as absent metadata."""
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.badjson", "historical")
        tracker.add_task("Amon.listjson", "historical")

    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "UPDATE cmor_tasks SET pbs_info_json='not json' "
            "WHERE variable='Amon.badjson'"
        )
        conn.execute(
            "UPDATE cmor_tasks SET pbs_info_json='[]' " "WHERE variable='Amon.listjson'"
        )
    conn.close()

    report = batch_report.build_batch_report(db_path)

    assert [task["pbs"] for task in report["tasks"]] == [None, None]


def test_batch_report_pbs_object_omits_invalid_exit_status(tmp_path: Path) -> None:
    """Non-integer PBS exit statuses do not break report generation."""
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.set_pbs_job_id("Amon.tas", "historical", "123.gadi-pbs")
        tracker.set_pbs_info(
            "Amon.tas",
            "historical",
            {"job_id": "123.gadi-pbs", "Exit_status": "not-an-int"},
        )

    task = batch_report.build_batch_report(db_path)["tasks"][0]

    assert task["pbs"]["job_id"] == "123.gadi-pbs"
    assert "exit_status" not in task["pbs"]


def test_batch_report_pbs_object_handles_missing_exit_status(tmp_path: Path) -> None:
    """Missing PBS exit status is omitted, not represented as null."""
    db_path = tmp_path / "cmor_tasks.db"
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.set_pbs_info("Amon.tas", "historical", {"job_state": "F"})

    task = batch_report.build_batch_report(db_path)["tasks"][0]

    assert task["pbs"]["job_state"] == "F"
    assert "exit_status" not in task["pbs"]


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


def test_cli_loads_config_file(tmp_path: Path) -> None:
    """The report CLI can include experiment metadata from a config file."""
    db_path = tmp_path / "cmor_tasks.db"
    config_path = tmp_path / "batch_config.yml"
    config_path.write_text("experiment_id: historical\n")
    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")

    output = tmp_path / "report.json"
    rc = batch_report.main(
        ["--db", str(db_path), "--output", str(output), "--config", str(config_path)]
    )

    assert rc == 0
    assert json.loads(output.read_text())["experiment_id"] == "historical"


def test_cli_skip_qc_omits_qc_section(tmp_path: Path) -> None:
    """The report CLI forwards --skip-qc into report generation."""
    db_path = tmp_path / "cmor_tasks.db"
    _write_qc_test_file(
        tmp_path / "tas.nc",
        "tas",
        "historical",
        [285.0],
    )

    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

    output = tmp_path / "report.json"
    rc = batch_report.main(
        [
            "--db",
            str(db_path),
            "--output",
            str(output),
            "--skip-qc",
        ]
    )

    assert rc == 0
    assert "qc" not in json.loads(output.read_text())


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
    report_files = list(tmp_path.glob("moppy_batch_report_*.json"))
    assert len(report_files) == 1
    report = json.loads(report_files[0].read_text())
    assert report["status"] == "completed"
    assert report["success"] is True
    assert report["monitor"]["pbs_job_id"] == "monitor.123"
    assert report["script_dir"] == str(script_dir)


def _write_qc_test_file(
    path: Path,
    variable_id: str,
    experiment_id: str,
    values: list[float],
    units: str = "K",
) -> None:
    """Write a test NetCDF file for QC testing."""
    ds = xr.Dataset(
        {
            variable_id: xr.DataArray(
                np.asarray(values, dtype=float),
                dims=["time"],
                attrs={"units": units},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": variable_id,
            "experiment_id": experiment_id,
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)
    ds.close()


@pytest.mark.unit
def test_run_qc_on_output_folder_with_all_passing_files(tmp_path: Path) -> None:
    """QC reports success when all files pass validation."""
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    # Create a single passing file to avoid xarray caching issues
    _write_qc_test_file(
        output_folder / "tas_pass.nc",
        "tas",
        "historical",
        [285.0, 287.5, 289.0],
    )

    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results is not None
    assert results["total"] == 1
    assert results["passed"] == 1
    assert results["failed"] == 0
    assert results["failures"] == []


@pytest.mark.unit
def test_run_qc_on_output_folder_with_range_warnings(tmp_path: Path) -> None:
    """QC reports range violations as warnings rather than failures."""
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    # Create an out-of-range file (tas above the piControl range)
    _write_qc_test_file(
        output_folder / "tas_fail.nc",
        "tas",
        "piControl",
        [326.5],  # Above 325K limit for piControl
    )

    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results is not None
    assert results["passed"] == 1
    assert results["failed"] == 0
    assert results["warned"] == 1
    assert results["total"] == 1
    assert results["failures"] == []
    assert results["warnings"][0]["variable_id"] == "tas"
    assert results["warnings"][0]["experiment_id"] == "piControl"
    assert "warning" in results["warnings"][0]


@pytest.mark.unit
def test_run_qc_on_output_folder_with_mixed_pass_warn(tmp_path: Path) -> None:
    """QC correctly tallies clean and warned passing files."""
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    # Create passing file
    _write_qc_test_file(
        output_folder / "tas_pass.nc",
        "tas",
        "historical",
        [285.0],
    )

    # Create out-of-range file
    _write_qc_test_file(
        output_folder / "tas_fail.nc",
        "tas",
        "piControl",
        [326.5],
    )

    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results is not None
    assert results["passed"] == 2
    assert results["failed"] == 0
    assert results["warned"] == 1
    assert results["total"] == 2
    assert len(results["warnings"]) == 1


@pytest.mark.unit
def test_run_qc_on_output_folder_respects_moppy_skip_qc_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """QC returns None when MOPPY_SKIP_QC=1 environment variable is set."""
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    _write_qc_test_file(
        output_folder / "tas.nc",
        "tas",
        "historical",
        [285.0],
    )

    monkeypatch.setenv("MOPPY_SKIP_QC", "1")
    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results is None


@pytest.mark.unit
def test_run_qc_on_output_folder_with_no_nc_files(tmp_path: Path) -> None:
    """QC returns None when no .nc files are found."""
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results is None


@pytest.mark.unit
def test_run_qc_on_output_folder_with_nested_files(tmp_path: Path) -> None:
    """QC finds and validates files in nested subdirectories."""
    output_folder = tmp_path / "output"
    nested = output_folder / "deep" / "nested" / "path"
    nested.mkdir(parents=True)

    _write_qc_test_file(
        nested / "tas.nc",
        "tas",
        "historical",
        [285.0],
    )

    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results is not None
    assert results["total"] == 1
    assert results["passed"] == 1


@pytest.mark.unit
def test_build_batch_report_includes_qc_section_by_default(tmp_path: Path) -> None:
    """build_batch_report includes QC results by default."""
    db_path = tmp_path / "cmor_tasks.db"
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    # Create a valid test file
    _write_qc_test_file(
        output_folder / "tas.nc",
        "tas",
        "historical",
        [285.0],
    )

    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

    report = batch_report.build_batch_report(
        db_path,
        output_folder=output_folder,
        skip_qc=False,
    )

    assert "qc" in report
    assert report["qc"]["passed"] == 1
    assert report["qc"]["failed"] == 0
    assert report["qc"]["total"] == 1


@pytest.mark.unit
def test_build_batch_report_omits_qc_section_when_skip_qc_true(tmp_path: Path) -> None:
    """build_batch_report omits QC results when skip_qc=True."""
    db_path = tmp_path / "cmor_tasks.db"
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    _write_qc_test_file(
        output_folder / "tas.nc",
        "tas",
        "historical",
        [285.0],
    )

    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

    report = batch_report.build_batch_report(
        db_path,
        output_folder=output_folder,
        skip_qc=True,
    )

    assert "qc" not in report


@pytest.mark.unit
def test_write_batch_report_passes_skip_qc_to_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """write_batch_report correctly passes skip_qc parameter to build_batch_report."""
    db_path = tmp_path / "cmor_tasks.db"
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    _write_qc_test_file(
        output_folder / "tas.nc",
        "tas",
        "historical",
        [285.0],
    )

    with TaskTracker(db_path) as tracker:
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

    # Test with skip_qc=False (should include QC)
    report_path = batch_report.write_batch_report(
        db_path,
        output_folder=output_folder,
        skip_qc=False,
    )

    report = json.loads(report_path.read_text())
    assert "qc" in report

    # Clean up report
    report_path.unlink()

    # Test with skip_qc=True (should omit QC)
    report_path = batch_report.write_batch_report(
        db_path,
        output_folder=output_folder,
        skip_qc=True,
    )

    report = json.loads(report_path.read_text())
    assert "qc" not in report


@pytest.mark.unit
def test_run_qc_on_output_folder_includes_detailed_warning_info(tmp_path: Path) -> None:
    """QC warning details include ranges and units where available."""
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    _write_qc_test_file(
        output_folder / "tas_fail.nc",
        "tas",
        "piControl",
        [326.5],
    )

    results = batch_report._run_qc_on_output_folder(output_folder)

    assert results["failed"] == 0
    assert results["warned"] == 1
    warning = results["warnings"][0]
    assert "observed_range" in warning
    assert "allowed_range" in warning
    assert "units" in warning
    assert warning["units"] == "K"
    assert warning["observed_range"][0] == 326.5
    assert warning["observed_range"][1] == 326.5
    assert warning["allowed_range"][0] == 180.0
    assert warning["allowed_range"][1] == 325.0
