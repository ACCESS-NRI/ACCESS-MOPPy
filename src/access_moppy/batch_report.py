"""Durable JSON reports for ACCESS-MOPPy batch coordination."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "access-moppy.batch-report.v1"
REPORT_FILENAME = "moppy_batch_report.json"
SIDECAR_FILENAME = ".moppy_main.jobid"
TERMINAL_STATUSES = {"completed", "failed"}
KNOWN_STATUSES = ("completed", "failed", "running", "pending", "retrying")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _timestamped_report_filename() -> str:
    """Filename for the default report path, stamped with the current UTC time.

    Keeps repeated runs under the same output folder from overwriting each
    other's reports. Example: ``moppy_batch_report_20260707T143022Z.json``.
    """
    base = Path(REPORT_FILENAME)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{base.stem}_{stamp}{base.suffix}"


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _duration_seconds(start: str | None, end: str | None) -> float | None:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds()


def _read_monitor_sidecar(output_folder: Path) -> dict[str, str | None]:
    sidecar = output_folder / SIDECAR_FILENAME
    if not sidecar.exists():
        return {"pbs_job_id": None, "submitted_at": None}
    lines = sidecar.read_text(encoding="utf-8").splitlines()
    return {
        "pbs_job_id": lines[0].strip() if lines and lines[0].strip() else None,
        "submitted_at": lines[1].strip()
        if len(lines) > 1 and lines[1].strip()
        else None,
    }


def _log_path(path: Path) -> str | None:
    return str(path) if path.exists() else str(path)


def _task_log_paths(script_dir: Path | None, variable: str) -> dict[str, str | None]:
    if script_dir is None:
        return {"stdout": None, "stderr": None}
    safe_variable = variable.replace(".", "_")
    var_dir = script_dir / safe_variable
    return {
        "stdout": _log_path(var_dir / f"cmor_{safe_variable}.out"),
        "stderr": _log_path(var_dir / f"cmor_{safe_variable}.err"),
    }


def _monitor_log_paths(script_dir: Path | None) -> dict[str, str | None]:
    if script_dir is None:
        return {"stdout": None, "stderr": None}
    return {
        "stdout": _log_path(script_dir / "moppy_monitor.out"),
        "stderr": _log_path(script_dir / "moppy_monitor.err"),
    }


def _stderr_tail(stderr_path: str | None, max_lines: int) -> str | None:
    if not stderr_path or max_lines <= 0:
        return None
    path = Path(stderr_path)
    if not path.exists():
        return None
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    return "\n".join(lines[-max_lines:]) if lines else None


def _classify_status(summary: Mapping[str, int]) -> tuple[str, bool, bool]:
    total = summary.get(
        "total", sum(count for status, count in summary.items() if status != "total")
    )
    nonterminal = sum(
        count
        for status, count in summary.items()
        if status != "total" and status not in TERMINAL_STATUSES
    )
    failed = summary.get("failed", 0)
    all_terminal = total == 0 or nonterminal == 0
    if not all_terminal:
        return "incomplete", False, False
    if failed:
        return "failed", False, True
    return "completed", True, True


def _load_task_rows(db_path: Path) -> list[sqlite3.Row]:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(cmor_tasks)").fetchall()
        }
        pbs_job_id_expr = (
            "pbs_job_id" if "pbs_job_id" in columns else "NULL AS pbs_job_id"
        )
        pbs_info_expr = (
            "pbs_info_json" if "pbs_info_json" in columns else "NULL AS pbs_info_json"
        )
        return conn.execute(
            f"""
            SELECT id, variable, experiment_id, status, start_time, end_time,
                   error_message, {pbs_job_id_expr}, {pbs_info_expr}
            FROM cmor_tasks
            ORDER BY id
            """  # noqa: S608  # PBS expressions are chosen from constants above
        ).fetchall()
    finally:
        conn.close()


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _load_pbs_info(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return loaded if isinstance(loaded, dict) else None


def _compact_dict(data: Mapping[str, Any]) -> dict[str, Any]:
    """Return a dict with null/empty nested values removed."""
    compact: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, Mapping):
            nested = _compact_dict(value)
            if nested:
                compact[key] = nested
        elif value is not None:
            compact[key] = value
    return compact


def _prefixed_values(info: Mapping[str, Any], prefix: str) -> dict[str, Any]:
    prefix_dot = f"{prefix}."
    return {
        key.removeprefix(prefix_dot): value
        for key, value in info.items()
        if key.startswith(prefix_dot)
    }


def _pbs_report(
    info: Mapping[str, Any] | None, job_id: str | None
) -> dict[str, Any] | None:
    """Build the filtered PBS object written into the batch report.

    The report intentionally avoids unbounded PBS fields such as submit
    arguments. ``raw_qstat`` is a filtered subset persisted by the monitor, not
    the full scheduler response.
    """
    if info is None:
        return None

    pbs = {
        "scheduler": info.get("scheduler", "pbs"),
        "job_id": info.get("job_id", job_id),
        "job_state": info.get("job_state"),
        "exit_status": _parse_int(info.get("Exit_status")),
        "queue": info.get("queue"),
        "account": info.get("Account_Name") or info.get("account"),
        "project": info.get("project"),
        "pbs_server": info.get("pbs_server"),
        "pbs_version": info.get("pbs_version"),
        "timestamps": {
            "created_at": info.get("ctime"),
            "eligible_at": info.get("etime"),
            "queued_at": info.get("qtime"),
            "started_at": info.get("stime"),
            "modified_at": info.get("mtime"),
        },
        "resources_requested": _prefixed_values(info, "Resource_List"),
        "resources_used": _prefixed_values(info, "resources_used"),
        "exec_host": info.get("exec_host"),
        "exec_vnode": info.get("exec_vnode"),
        "comment": info.get("comment"),
        "qstat_format": info.get("qstat_format"),
        "raw_qstat": dict(info),
    }
    return _compact_dict(pbs)


def _run_qc_on_output_folder(output_folder: Path) -> dict[str, Any] | None:
    """Run QC on CMORised netCDF files found in output folder and return results.

    Returns None if QC cannot be run (import unavailable, no files found, or disabled).
    Set environment variable MOPPY_SKIP_QC=1 to disable QC collection.
    """
    import os

    if os.environ.get("MOPPY_SKIP_QC", "").lower() in ("1", "true", "yes"):
        return None

    try:
        from access_moppy.qc.cmip7 import validate_cmip7_output_detailed
    except ImportError:
        return None

    nc_files = sorted(output_folder.glob("**/*.nc"))
    if not nc_files:
        return None

    results = {
        "passed": 0,
        "failed": 0,
        "total": len(nc_files),
        "failures": [],
    }

    for nc_file in nc_files:
        result = validate_cmip7_output_detailed(nc_file)
        if result.passed:
            results["passed"] += 1
        else:
            results["failed"] += 1
            failure = {
                "file": str(nc_file),
                "variable_id": result.variable_id,
                "experiment_id": result.experiment_id,
                "error": result.error,
            }
            if result.observed_min is not None:
                failure["observed_range"] = [
                    float(result.observed_min),
                    float(result.observed_max),
                ]
            if result.allowed_min is not None:
                failure["allowed_range"] = [
                    float(result.allowed_min),
                    float(result.allowed_max),
                ]
            if result.units:
                failure["units"] = result.units
            results["failures"].append(failure)

    return results if results["total"] > 0 else None


def build_batch_report(
    db_path: str | Path,
    *,
    config: Mapping[str, Any] | None = None,
    config_path: str | Path | None = None,
    output_folder: str | Path | None = None,
    script_dir: str | Path | None = None,
    created_at: str | None = None,
    completed_at: str | None = None,
    stderr_tail_lines: int = 20,
    skip_qc: bool = False,
) -> dict[str, Any]:
    """Build a JSON-serialisable batch coordination report.

    SQLite remains the source of truth; this report is a derived snapshot for
    after-the-fact completion checks, provenance capture, and dashboard/database
    ingestion.

    Args:
        skip_qc: If True, skip QC data collection. Can also be set via MOPPY_SKIP_QC env var.
    """
    db_path = Path(db_path)
    output_path = Path(output_folder) if output_folder is not None else db_path.parent
    script_path = Path(script_dir) if script_dir is not None else None
    config_path_str = str(config_path) if config_path is not None else None
    now = created_at or _utc_now_iso()

    rows = _load_task_rows(db_path)
    summary = {status: 0 for status in KNOWN_STATUSES}
    for row in rows:
        summary[row["status"]] = summary.get(row["status"], 0) + 1
    summary["total"] = len(rows)

    status, success, all_terminal = _classify_status(summary)
    final_completed_at = completed_at if all_terminal else None
    if all_terminal and final_completed_at is None:
        final_completed_at = now

    tasks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for row in rows:
        logs = _task_log_paths(script_path, row["variable"])
        pbs = _pbs_report(_load_pbs_info(row["pbs_info_json"]), row["pbs_job_id"])
        task = {
            "id": row["id"],
            "variable": row["variable"],
            "experiment_id": row["experiment_id"],
            "status": row["status"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "duration_seconds": _duration_seconds(row["start_time"], row["end_time"]),
            "pbs_job_id": row["pbs_job_id"],
            "pbs": pbs,
            "stdout": logs["stdout"],
            "stderr": logs["stderr"],
            "error_message": row["error_message"],
        }
        tasks.append(task)
        if row["status"] == "failed":
            failure = {
                "variable": row["variable"],
                "experiment_id": row["experiment_id"],
                "pbs_job_id": row["pbs_job_id"],
                "error_message": row["error_message"],
                "stderr": logs["stderr"],
                "stderr_tail": _stderr_tail(logs["stderr"], stderr_tail_lines),
            }
            failures.append(failure)

    monitor = _read_monitor_sidecar(output_path)
    monitor.update(_monitor_log_paths(script_path))

    qc_results = None if skip_qc else _run_qc_on_output_folder(output_path)

    report_dict = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now,
        "completed_at": final_completed_at,
        "status": status,
        "success": success,
        "all_tasks_terminal": all_terminal,
        "db_path": str(db_path),
        "config_path": config_path_str,
        "output_folder": str(output_path),
        "script_dir": str(script_path) if script_path is not None else None,
        "experiment_id": config.get("experiment_id") if config else None,
        "summary": summary,
        "monitor": monitor,
        "tasks": tasks,
        "failures": failures,
    }
    if qc_results is not None:
        report_dict["qc"] = qc_results

    return report_dict


def write_batch_report(
    db_path: str | Path,
    output_path: str | Path | None = None,
    skip_qc: bool = False,
    **kwargs: Any,
) -> Path:
    """Write a durable batch report and return the report path.

    Args:
        skip_qc: If True, skip QC data collection. Can also be set via MOPPY_SKIP_QC env var.
    """
    db_path = Path(db_path)
    report_path = (
        Path(output_path)
        if output_path is not None
        else db_path.parent / _timestamped_report_filename()
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = build_batch_report(db_path, skip_qc=skip_qc, **kwargs)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-batch-report",
        description="Export an ACCESS-MOPPy batch tracker database to JSON.",
    )
    parser.add_argument("--db", required=True, help="Path to cmor_tasks.db.")
    parser.add_argument(
        "--output",
        help="Report path. Defaults to <db parent>/moppy_batch_report_<UTC>.json.",
    )
    parser.add_argument("--config", help="Original batch configuration path.")
    parser.add_argument(
        "--script-dir", help="Directory containing generated PBS scripts/logs."
    )
    parser.add_argument(
        "--stderr-tail-lines",
        type=int,
        default=20,
        help="Number of stderr tail lines to include for failed tasks (default: 20).",
    )
    parser.add_argument(
        "--skip-qc",
        action="store_true",
        help="Skip QC data collection (can also set MOPPY_SKIP_QC=1).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for exporting a batch report."""
    args = _build_parser().parse_args(argv)
    config = None
    if args.config:
        import yaml

        with Path(args.config).open() as f:
            config = yaml.safe_load(f)
    report_path = write_batch_report(
        args.db,
        args.output,
        skip_qc=args.skip_qc,
        config=config,
        config_path=args.config,
        script_dir=args.script_dir,
        stderr_tail_lines=args.stderr_tail_lines,
    )
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
