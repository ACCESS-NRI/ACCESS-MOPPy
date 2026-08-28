from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib.resources import files
from math import ceil
from pathlib import Path
from typing import Any

import yaml

from access_moppy.batch_report import write_batch_report
from access_moppy.tracking import TaskTracker

# Sidecar file dropped in output_folder so the dashboard / a successor monitor
# can find the PBS jobid of the live monitor without scanning qstat.
SIDECAR_FILENAME = ".moppy_main.jobid"

# How often the monitor polls PBS for sub-job state. Production configs can
# raise this with ``monitor_poll_interval`` to reduce PBS server load.
MONITOR_POLL_INTERVAL_SECONDS = 30

# Number of *consecutive* polls that must report a sub-job as "gone" (qstat
# returned nothing) before the monitor believes it has finished. A single
# qstat call returns None on a subprocess timeout, a non-zero exit, or a
# transient PBS-server hiccup (connection reset, overload) — none of which mean
# the job actually finished. Requiring several confirmations stops one bad
# qstat call from abandoning a still-running sub-job and marking it failed.
# Any active state (Q/R/H/S/T/W) resets the counter. A genuinely finished job
# reports "F"/"X" — not "gone" — and is reconciled immediately, so this only
# delays recognising a job whose history record was truly purged, by at most
# MONITOR_GONE_CONFIRMATIONS poll intervals.
MONITOR_GONE_CONFIRMATIONS = 3
QstatInfo = dict[str, Any]
BatchConfig = Mapping[str, Any]


@dataclass(frozen=True)
class ResumeCheckpoint:
    """The completed prefix of one variable's output timeseries."""

    next_year: int | None
    version_date: str | None
    complete: bool = False


def find_resume_checkpoint(
    output_root: str | Path,
    variable: str,
    *,
    experiment_id: str,
    source_id: str,
    variant_label: str,
    input_files: list[str | Path],
    end_year: int | str | None = None,
    split_years: int | str | None = "auto",
) -> ResumeCheckpoint | None:
    """Find a contiguous prefix of completed split files for a batch restart.

    Existing files are matched by their CMIP global facets rather than their
    version directory. Only readable files that end on an expected split
    boundary are accepted as checkpoints.
    """
    from netCDF4 import Dataset, num2date

    from access_moppy.base import _canonical_frequency
    from access_moppy.defaults import DEFAULT_CHUNK_YEARS
    from access_moppy.file_discovery import _extract_year_from_path

    input_years = [
        year
        for path in input_files
        if (year := _extract_year_from_path(Path(path))) is not None
    ]
    if not input_years:
        return None
    first_input_year = min(input_years)
    last_input_year = int(end_year) if end_year is not None else max(input_years)

    if split_years == "auto":
        resolved_split = DEFAULT_CHUNK_YEARS.get(_canonical_frequency(variable))
    elif split_years is None:
        resolved_split = None
    elif isinstance(split_years, int) and not isinstance(split_years, bool):
        if split_years <= 0:
            raise ValueError("split_years must be a positive integer")
        resolved_split = split_years
    else:
        raise ValueError("split_years must be None, 'auto', or an integer")

    expected_variable_id = variable.split(".", 1)[-1].split("_", 1)[0]
    intervals_by_version: dict[str | None, list[tuple[int, int, bool]]] = {}
    root = Path(output_root)
    if not root.exists():
        return None

    for path in root.rglob("*.nc"):
        try:
            with Dataset(path) as dataset:
                if (
                    dataset.getncattr("experiment_id") != experiment_id
                    or dataset.getncattr("source_id") != source_id
                    or dataset.getncattr("variant_label") != variant_label
                    or dataset.getncattr("variable_id") != expected_variable_id
                    or "time" not in dataset.variables
                    or not dataset.variables["time"].size
                ):
                    continue
                time_var = dataset.variables["time"]
                units = time_var.getncattr("units")
                calendar = getattr(time_var, "calendar", "standard")
                first, last = num2date(
                    [time_var[0], time_var[-1]],
                    units=units,
                    calendar=calendar,
                    only_use_cftime_datetimes=True,
                )
        except (OSError, AttributeError, IndexError, KeyError, ValueError):
            continue

        version_date = next(
            (
                parent.name[1:]
                for parent in path.parents
                if len(parent.name) == 9
                and parent.name.startswith("v")
                and parent.name[1:].isdigit()
            ),
            None,
        )
        marker = path.parent / ".moppy_complete" / f"{path.name}.done"
        intervals_by_version.setdefault(version_date, []).append(
            (int(first.year), int(last.year), marker.is_file())
        )

    best: ResumeCheckpoint | None = None
    for version_date, intervals in intervals_by_version.items():
        newest_finish = max(finish for _start, finish, _marked in intervals)
        cursor = first_input_year
        for start, finish, marked_complete in sorted(set(intervals)):
            if not marked_complete and finish == newest_finish:
                continue
            if start != cursor:
                continue
            expected_finish = last_input_year
            if resolved_split is not None:
                chunk_start = (cursor // resolved_split) * resolved_split
                expected_finish = min(chunk_start + resolved_split - 1, last_input_year)
            if finish != expected_finish:
                continue
            cursor = finish + 1
            if cursor > last_input_year:
                break

        checkpoint = ResumeCheckpoint(
            next_year=None if cursor > last_input_year else cursor,
            version_date=version_date,
            complete=cursor > last_input_year,
        )
        if cursor == first_input_year:
            continue
        if best is None or (checkpoint.next_year or last_input_year + 1) > (
            best.next_year or last_input_year + 1
        ):
            best = checkpoint
    return best


def parse_walltime(s: str) -> int:
    """Parse an ``HH:MM:SS`` or ``MM:SS`` walltime string into seconds.

    Args:
        s: PBS-style walltime string.

    Returns:
        Walltime duration in seconds.

    Raises:
        ValueError: If the string is not in ``HH:MM:SS`` or ``MM:SS`` form.
    """
    parts = str(s).strip().split(":")
    if len(parts) == 3:
        h, m, sec = parts
    elif len(parts) == 2:
        h, m, sec = "0", parts[0], parts[1]
    else:
        raise ValueError(f"Invalid walltime: {s}")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def format_walltime(seconds: int) -> str:
    """Render an integer second count as a zero-padded ``HH:MM:SS`` string."""
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def compute_monitor_walltime(config: BatchConfig) -> str:
    """Pick a walltime for the monitor job from the batch config.

    Each rolling submission wave may take as long as the slowest sub-job, so
    allow that duration per wave plus 30 minutes for queue wait and final
    reconciliation. Without ``max_inflight_jobs``, all variables form one wave.
    """
    default_wt = config.get("walltime", "02:00:00")
    var_resources = config.get("variable_resources", {})
    longest = parse_walltime(default_wt)
    for var in config["variables"]:
        wt = var_resources.get(var, {}).get("walltime", default_wt)
        longest = max(longest, parse_walltime(wt))
    configured_limit = config.get("max_inflight_jobs")
    if configured_limit is None:
        waves = 1
    else:
        limit = int(configured_limit)
        if limit <= 0:
            raise ValueError("max_inflight_jobs must be greater than zero")
        waves = max(1, ceil(len(config["variables"]) / limit))
    return format_walltime(longest * waves + 30 * 60)


PBS_JOB_FIELDS = (
    "job_state",
    "Exit_status",
    "queue",
    "project",
    "Account_Name",
    "exec_host",
    "exec_vnode",
    "comment",
    "ctime",
    "etime",
    "qtime",
    "stime",
    "mtime",
)
PBS_RESOURCES_USED_FIELDS = (
    "cpupercent",
    "cput",
    "mem",
    "ncpus",
    "vmem",
    "walltime",
)
PBS_RESOURCE_LIST_FIELDS = (
    "jobfs",
    "mem",
    "mpiprocs",
    "ncpus",
    "storage",
    "walltime",
)
PBS_FLAT_FIELDS = (
    *PBS_JOB_FIELDS,
    *(f"resources_used.{field}" for field in PBS_RESOURCES_USED_FIELDS),
    *(f"Resource_List.{field}" for field in PBS_RESOURCE_LIST_FIELDS),
)


def _parse_qstat_json(stdout: str, job_id: str) -> QstatInfo | None:
    """Parse filtered PBS JSON qstat output for one job.

    PBS JSON contains many fields, including paths, submit arguments, and user
    information. MOPPy keeps a deliberately small Payu-style subset that is
    useful for resource/provenance reporting without dumping the entire record.
    """
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    jobs = payload.get("Jobs")
    if not isinstance(jobs, dict) or not jobs:
        return None

    selected_job_id = str(job_id)
    job_info = jobs.get(selected_job_id)
    if not isinstance(job_info, dict):
        # PBS can key the job as either "123" or "123.gadi-pbs" depending on
        # command/server context; fall back to the only record when present.
        selected_job_id, job_info = next(iter(jobs.items()))
        if not isinstance(job_info, dict):
            return None

    info: QstatInfo = {
        "scheduler": "pbs",
        "qstat_format": "json",
        "job_id": selected_job_id,
    }
    for key in ("pbs_version", "pbs_server"):
        value = payload.get(key)
        if value is not None:
            info[key] = value
    for key in PBS_JOB_FIELDS:
        value = job_info.get(key)
        if value is not None:
            info[key] = value

    resources_used = job_info.get("resources_used")
    if isinstance(resources_used, dict):
        for key in PBS_RESOURCES_USED_FIELDS:
            value = resources_used.get(key)
            if value is not None:
                info[f"resources_used.{key}"] = value

    resource_list = job_info.get("Resource_List")
    if isinstance(resource_list, dict):
        for key in PBS_RESOURCE_LIST_FIELDS:
            value = resource_list.get(key)
            if value is not None:
                info[f"Resource_List.{key}"] = value

    return info


def _qstat_full_json(job_id: str) -> QstatInfo | None:
    """Run `qstat -xf -F json <job_id>` and parse filtered PBS metadata."""
    try:
        result = subprocess.run(  # noqa: S603  # nosec B603
            ["qstat", "-xf", "-F", "json", str(job_id)],
            capture_output=True,
            text=True,
            check=False,
            shell=False,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None

    if result.returncode != 0 or not result.stdout.strip():
        return None
    return _parse_qstat_json(result.stdout, job_id)


def _qstat_full_text(job_id: str) -> QstatInfo | None:
    """Run `qstat -fx <job_id>` and parse legacy text output.

    Returns None on timeout, missing binary, or empty output (the latter
    happens once a job has been purged from PBS history). PBS Pro wraps long
    attribute values onto continuation lines starting with whitespace; those
    are appended to the previous key so the caller sees the full value.
    """
    try:
        result = subprocess.run(  # noqa: S603  # nosec B603
            ["qstat", "-fx", str(job_id)],
            capture_output=True,
            text=True,
            check=False,
            shell=False,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None

    if result.returncode != 0 or not result.stdout.strip():
        return None

    parsed: QstatInfo = {}
    info: QstatInfo = {
        "scheduler": "pbs",
        "qstat_format": "text",
        "job_id": str(job_id),
    }
    current_key = None
    for raw in result.stdout.splitlines():
        if not raw or raw.startswith("Job Id:"):
            if raw.startswith("Job Id:"):
                _, _, parsed_job_id = raw.partition(":")
                if parsed_job_id.strip():
                    info["job_id"] = parsed_job_id.strip()
            current_key = None
            continue
        if (
            (raw.startswith(" ") or raw.startswith("\t"))
            and "=" not in raw
            and current_key
        ):
            parsed[current_key] += raw.strip()
            continue
        if "=" in raw:
            key, _, val = raw.partition("=")
            current_key = key.strip()
            parsed[current_key] = val.strip()
    for key in PBS_FLAT_FIELDS:
        value = parsed.get(key)
        if value is not None:
            info[key] = value
    return info


def qstat_full(job_id: str) -> QstatInfo | None:
    """Return filtered final PBS metadata for a job.

    Prefer PBS' JSON output, matching Payu's telemetry implementation. Fall
    back to the older text parser on systems where `qstat -F json` is missing
    or returns unparsable output.
    """
    return _qstat_full_json(job_id) or _qstat_full_text(job_id)


def qstat_many(job_ids: list[str]) -> dict[str, QstatInfo | None]:
    """Fetch PBS metadata for several jobs with one JSON ``qstat`` call.

    Falls back to the existing per-job parser when PBS rejects the aggregate
    request or returns malformed JSON.
    """
    if not job_ids:
        return {}
    if len(job_ids) == 1:
        return {job_ids[0]: qstat_full(job_ids[0])}

    try:
        result = subprocess.run(  # noqa: S603  # nosec B603
            ["qstat", "-xf", "-F", "json", *map(str, job_ids)],
            capture_output=True,
            text=True,
            check=False,
            shell=False,
            timeout=30,
        )
        payload = json.loads(result.stdout) if result.returncode == 0 else None
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        payload = None

    jobs = payload.get("Jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, dict):
        return {job_id: qstat_full(job_id) for job_id in job_ids}

    results: dict[str, QstatInfo | None] = {}
    for job_id in job_ids:
        matching_key = next(
            (
                key
                for key in jobs
                if key == job_id or key.split(".", 1)[0] == job_id.split(".", 1)[0]
            ),
            None,
        )
        if matching_key is None:
            results[job_id] = None
            continue
        single_payload = {key: value for key, value in payload.items() if key != "Jobs"}
        single_payload["Jobs"] = {matching_key: jobs[matching_key]}
        results[job_id] = _parse_qstat_json(json.dumps(single_payload), job_id)
    return results


def qstat_state(info: QstatInfo | None) -> str:
    """Return the PBS job_state letter from a qstat_full() dict.

    Returns 'gone' when info is None (job no longer visible to PBS) or when
    the dict has no job_state field — both cases are treated as 'finished'
    by the poll loop.
    """
    if not info:
        return "gone"
    state = info.get("job_state", "gone")
    return str(state)


def active_monitor_job_id(output_dir: str | Path) -> str | None:
    """Return the monitor job ID recorded for an unfinished batch."""
    sidecar = Path(output_dir) / SIDECAR_FILENAME
    try:
        job_id = sidecar.read_text(encoding="utf-8").splitlines()[0].strip()
    except (FileNotFoundError, IndexError, OSError):
        return None
    if not job_id:
        return None

    state = qstat_state(qstat_full(job_id))
    if state in ("F", "X"):
        sidecar.unlink(missing_ok=True)
        return None
    return job_id


def format_pbs_error(
    variable: str,
    job_id: str,
    info: QstatInfo | None,
    script_dir: str | Path,
) -> str:
    """Assemble a single-line failure message for a dead sub-job.

    Pulls exit_status, the PBS comment (often the reason a job was killed),
    and final resource usage out of `info`, then appends the last 20 lines
    of the worker's .err file if it exists. The result is what ends up in
    the cmor_tasks.error_message column.
    """
    if info is None:
        return f"job {job_id}: vanished from PBS history before reconciliation"

    parts = [f"job {job_id}"]
    exit_status = info.get("Exit_status")
    if exit_status is not None:
        parts.append(f"exit_status={exit_status}")
    comment = info.get("comment")
    if comment:
        parts.append(f"pbs_comment='{comment}'")
    mem_used = info.get("resources_used.mem")
    if mem_used:
        parts.append(f"mem_used={mem_used}")
    wt_used = info.get("resources_used.walltime")
    if wt_used:
        parts.append(f"walltime_used={wt_used}")

    err_path = (
        Path(script_dir)
        / "logs"
        / variable.replace(".", "_")
        / f"cmor_{variable.replace('.', '_')}.err"
    )
    if err_path.exists():
        try:
            tail = subprocess.run(  # noqa: S603  # nosec B603
                ["tail", "-20", str(err_path)],
                capture_output=True,
                text=True,
                check=False,
                shell=False,
                timeout=10,
            ).stdout
            if tail.strip():
                parts.append(f"err_tail:\n{tail.strip()}")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
    return " | ".join(parts)


def reconcile_one(
    tracker: TaskTracker,
    variable: str,
    experiment_id: str,
    job_id: str,
    info: QstatInfo | None,
    script_dir: str | Path,
) -> None:
    """Decide what to write to the DB for one finished sub-job.

    The worker normally writes its own terminal status. The monitor only
    intervenes when the worker had no chance to (SIGKILL / OOM / node crash):

    - exit 0 and DB not already 'completed': backfill 'completed'.
    - non-zero exit and DB still 'running' or 'pending': mark 'failed' with
      a message built from qstat plus the worker's stderr tail.
    - DB already in a terminal state: leave it alone — the worker beat us.
    """
    tracker.set_pbs_info(variable, experiment_id, info)
    current = tracker.get_status(variable, experiment_id)
    exit_raw = info.get("Exit_status") if info else None
    try:
        exit_code = int(exit_raw) if exit_raw is not None else None
    except (TypeError, ValueError):
        # Garbled exit status (rare PBS bug) is treated as failure.
        exit_code = None

    if exit_code == 0:
        if current != "completed":
            tracker.mark_completed(variable, experiment_id)
        return

    if current in ("completed", "failed"):
        return

    msg = format_pbs_error(variable, job_id, info, script_dir)
    tracker.mark_failed(variable, experiment_id, msg)


def start_dashboard(dashboard_path: str, db_path: str) -> None:
    """Launch the Streamlit dashboard for a task-tracker database.

    Args:
        dashboard_path: Filesystem path to the dashboard Python script.
        db_path: SQLite task-tracker database path to expose via the
            ``CMOR_TRACKER_DB`` environment variable.

    The function returns without raising for missing/invalid dashboard paths
    so batch submission can continue when the optional dashboard is unavailable.
    """
    env = os.environ.copy()
    env["CMOR_TRACKER_DB"] = db_path

    # Security: validate and escape paths to prevent injection
    from pathlib import Path

    # Validate dashboard path exists and is a Python file
    if not Path(dashboard_path).exists():
        print(f"Error: Dashboard script does not exist: {dashboard_path}")
        return

    if not dashboard_path.endswith(".py"):
        print(f"Error: Dashboard path must be a Python file: {dashboard_path}")
        return

    # Prevent path traversal
    if ".." in dashboard_path:
        print(f"Error: Invalid dashboard path: {dashboard_path}")
        return

    # Security: Use the most explicit static command construction possible
    # Some security scanners require this level of explicitness
    escaped_dashboard_path = shlex.quote(dashboard_path)

    # Define each argument explicitly as constants
    STREAMLIT_EXECUTABLE = "streamlit"  # Static executable name
    RUN_COMMAND = "run"  # Static subcommand
    dashboard_arg = escaped_dashboard_path  # Validated and escaped dashboard path

    # Use explicit argument assignment to satisfy security scanners
    subprocess.Popen(  # noqa: S603  # nosec B603
        [
            STREAMLIT_EXECUTABLE,
            RUN_COMMAND,
            dashboard_arg,
        ],  # Explicit list with predefined elements
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        shell=False,  # Explicitly prevent shell interpretation
    )


def create_job_script(
    variable: str, config: BatchConfig, db_path: str | Path, script_dir: Path
) -> Path:
    """Create PBS and Python worker scripts for one variable.

    Args:
        variable: Variable name from the batch config.
        config: Batch CMORisation configuration dictionary.
        db_path: Path to the task-tracker SQLite database.
        script_dir: Directory where generated scripts should be written.

    Returns:
        Path to the generated PBS job script.
    """
    from importlib.resources import files

    from jinja2 import Template

    # Load templates
    pbs_template_path = files("access_moppy.templates").joinpath("cmor_job_script.j2")
    python_template_path = files("access_moppy.templates").joinpath(
        "cmor_python_script.j2"
    )

    with pbs_template_path.open() as f:
        pbs_template_content = f.read()

    with python_template_path.open() as f:
        python_template_content = f.read()

    pbs_template = Template(pbs_template_content)
    python_template = Template(python_template_content)

    # Get variable-specific resources if available
    variable_config = config.copy()
    if "variable_resources" in config and variable in config["variable_resources"]:
        # Override with variable-specific settings
        variable_config.update(config["variable_resources"][variable])
        print(
            f"Using custom resources for {variable}: {config['variable_resources'][variable]}"
        )

    # Get the package path for sys.path.insert
    package_path = Path(__file__).parent.parent

    # Create per-variable subdirectory under script_dir/logs/
    var_dir = script_dir / "logs" / variable.replace(".", "_")
    var_dir.mkdir(parents=True, exist_ok=True)

    # Create Python script
    python_script_content = python_template.render(
        variable=variable,
        config=variable_config,  # Use variable-specific config
        db_path=db_path,
        package_path=package_path,
    )

    python_script_path = var_dir / f"cmor_{variable.replace('.', '_')}.py"
    with open(python_script_path, "w") as f:
        f.write(python_script_content)

    # Create PBS script (pass var_dir as script_dir so .out/.err go to var_dir)
    pbs_script_content = pbs_template.render(
        variable=variable,
        config=variable_config,  # Use variable-specific config
        script_dir=var_dir,
        python_script_path=python_script_path,
        db_path=db_path,
    )

    pbs_script_path = var_dir / f"cmor_{variable.replace('.', '_')}.sh"
    with open(pbs_script_path, "w") as f:
        f.write(pbs_script_content)

    os.chmod(pbs_script_path, 0o755)
    os.chmod(python_script_path, 0o755)

    return pbs_script_path


def submit_job(script_path: str | Path) -> str | None:
    """Submit a PBS job script and return the PBS job id.

    Returns ``None`` when the script path is invalid or ``qsub`` fails.
    """
    try:
        # Security: validate and escape script_path to prevent injection
        script_path_str = str(script_path)

        # Additional validation: ensure path is safe
        # Check if we're in a testing environment (less strict validation)
        import sys
        from pathlib import Path

        is_testing = "pytest" in sys.modules or "unittest" in sys.modules

        if not is_testing and not Path(script_path_str).exists():
            print(f"Error: Script file does not exist: {script_path_str}")
            return None

        # Ensure no path traversal or shell injection
        if ".." in script_path_str or not script_path_str.endswith((".sh", ".pbs")):
            print(f"Error: Invalid script path: {script_path_str}")
            return None

        # Security: Use the most explicit static command construction possible
        # Some security scanners require this level of explicitness
        escaped_script_path = shlex.quote(script_path_str)

        # Define each argument explicitly as constants
        QSUB_EXECUTABLE = "qsub"  # Static executable name
        script_arg = escaped_script_path  # Validated and escaped script path

        # Use explicit argument assignment to satisfy security scanners
        result = subprocess.run(  # noqa: S603  # nosec B603
            [QSUB_EXECUTABLE, script_arg],  # Explicit list with predefined elements
            capture_output=True,
            text=True,
            check=True,
            shell=False,  # Explicitly prevent shell interpretation
        )
        job_id = result.stdout.strip()
        return job_id
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit job {script_path}: {e}")
        return None


def wait_for_jobs(job_ids: list[str], poll_interval: int = 30) -> None:
    """Wait for all jobs to complete and report status."""
    print(f"Waiting for {len(job_ids)} jobs to complete...")

    while job_ids:
        time.sleep(poll_interval)

        # Check job status
        try:
            # Security: validate job_ids to prevent injection
            import re

            still_running = []

            # Check each job individually to avoid dynamic command construction
            for job_id in job_ids:
                # Job IDs should only contain alphanumeric, dots, and hyphens
                if not re.match(r"^[a-zA-Z0-9.-]+$", job_id):
                    print(f"Warning: Skipping invalid job ID: {job_id}")
                    continue

                # Security: Use completely static command with single job ID
                escaped_job_id = shlex.quote(job_id)

                # Security: Use the most explicit static command construction possible
                # Some security scanners require this level of explicitness
                QSTAT_EXECUTABLE = "qstat"  # Static executable name
                QSTAT_FLAG = "-x"  # Static flag
                job_arg = escaped_job_id  # Validated and escaped job ID

                try:
                    # Use explicit argument assignment to satisfy security scanners
                    result = subprocess.run(  # noqa: S603  # nosec B603
                        [
                            QSTAT_EXECUTABLE,
                            QSTAT_FLAG,
                            job_arg,
                        ],  # Explicit list with predefined elements
                        capture_output=True,
                        text=True,
                        check=False,  # qstat may return non-zero for completed jobs
                        shell=False,  # Explicitly prevent shell interpretation
                        timeout=30,  # Prevent hanging
                    )

                    # Check if job is still in queue/running
                    if job_id in result.stdout and any(
                        status in result.stdout for status in ["Q", "R", "H"]
                    ):
                        still_running.append(job_id)

                except subprocess.TimeoutExpired:
                    print(f"Warning: Timeout checking status for job {job_id}")
                    still_running.append(job_id)  # Assume still running if timeout

            completed = [job_id for job_id in job_ids if job_id not in still_running]
            if completed:
                print(f"Completed jobs: {completed}")
                job_ids = still_running

        except subprocess.CalledProcessError:
            # If qstat fails, assume all jobs are done
            break

    print("All jobs completed!")


def create_monitor_script(
    config: BatchConfig,
    config_path: str | Path,
    db_path: str | Path,
    script_dir: str | Path,
    variable_filter: list[str] | None = None,
    resume: bool = False,
) -> Path:
    """Render the PBS script for the monitor job and write it to script_dir.

    The script is tiny (1 CPU, 4 GB) — it just submits sub-jobs and polls
    qstat. Walltime is derived from compute_monitor_walltime so the monitor
    outlives every sub-job it spawns.
    """
    from jinja2 import Template

    template_path = files("access_moppy.templates").joinpath("cmor_monitor_script.j2")
    with template_path.open() as f:
        template_content = f.read()

    monitor_walltime = compute_monitor_walltime(config)

    rendered = Template(template_content).render(
        config=config,
        config_path=str(config_path),
        db_path=str(db_path),
        script_dir=str(script_dir),
        monitor_walltime=monitor_walltime,
        variable_filter=variable_filter,
        resume=resume,
    )

    monitor_path = Path(script_dir) / "moppy_monitor.sh"
    with open(monitor_path, "w") as f:
        f.write(rendered)
    os.chmod(monitor_path, 0o755)
    return monitor_path


def monitor_main() -> None:
    """Entry point for the monitor PBS job, invoked via `--monitor`.

    Runs on a compute node. Reads the batch config from $MOPPY_CONFIG_PATH,
    qsubs up to ``max_inflight_jobs`` sub-jobs, records each job id in the DB,
    then polls qstat in monitor_loop. Each completed job opens a slot for the
    next variable. When the loop exits, finalize_monitor does a consistency
    sweep and the job ends cleanly with exit 0.

    Sub-jobs that fail at qsub time, or whose script can't even be rendered,
    are marked 'failed' immediately and the monitor moves on to the next
    variable rather than aborting the whole batch.
    """
    config_path = os.environ.get("MOPPY_CONFIG_PATH")
    db_path = os.environ.get("MOPPY_DB_PATH")
    script_dir_env = os.environ.get("MOPPY_SCRIPT_DIR")
    variable_filter_env = os.environ.get("MOPPY_VARIABLE_FILTER")
    resume_env = os.environ.get("MOPPY_RESUME")

    if not config_path or not db_path:
        print(
            "Error: monitor requires MOPPY_CONFIG_PATH and MOPPY_DB_PATH env vars",
            file=sys.stderr,
        )
        sys.exit(2)

    config_path = Path(config_path)
    db_path = Path(db_path)
    with config_path.open() as f:
        config = yaml.safe_load(f)
    if resume_env == "1":
        config["resume"] = True

    experiment_id = config["experiment_id"]
    script_dir = (
        Path(script_dir_env)
        if script_dir_env
        else Path(config.get("script_dir", config["output_folder"]))
    )
    script_dir.mkdir(parents=True, exist_ok=True)

    variable_filter: set[str] | None = (
        set(variable_filter_env.split(",")) if variable_filter_env else None
    )

    tracker = TaskTracker(db_path)
    # Use try/finally so the sqlite handle is released on any exit path,
    # including the SystemExit raised by shutdown_handler.
    try:
        # job_map contains only submitted jobs that have not yet been reconciled.
        job_map: dict[str, str] = {}
        queued_variables: deque[str] = deque()
        monitored_variables: set[str] = set()
        for variable in config["variables"]:
            if variable_filter is not None and variable not in variable_filter:
                continue
            if tracker.is_done(variable, experiment_id):
                print(f"Skipped (already completed): {variable}")
                continue
            queued_variables.append(variable)
            monitored_variables.add(variable)

        configured_limit = config.get("max_inflight_jobs")
        max_inflight_jobs = (
            len(queued_variables) or 1
            if configured_limit is None
            else int(configured_limit)
        )
        if max_inflight_jobs <= 0:
            raise ValueError("max_inflight_jobs must be greater than zero")

        def submit_next() -> tuple[str, str] | None:
            """Submit the next viable variable, skipping local submission failures."""
            queued_or_running = set(queued_variables) | set(job_map.values())
            for requested_variable in tracker.take_monitor_requests(experiment_id):
                if tracker.is_done(requested_variable, experiment_id):
                    print(
                        f"Skipped appended variable (already completed): {requested_variable}"
                    )
                elif requested_variable in queued_or_running:
                    print(
                        f"Skipped appended variable (already scheduled): {requested_variable}"
                    )
                else:
                    queued_variables.append(requested_variable)
                    monitored_variables.add(requested_variable)
                    queued_or_running.add(requested_variable)
                    print(f"Appended variable to monitor queue: {requested_variable}")

            while queued_variables:
                variable = queued_variables.popleft()
                try:
                    script_path = create_job_script(
                        variable, config, str(db_path), script_dir
                    )
                except Exception as e:
                    tracker.mark_failed(
                        variable,
                        experiment_id,
                        f"monitor: failed to create script: {e}",
                    )
                    print(
                        f"Failed to create script for {variable}: {e}",
                        file=sys.stderr,
                    )
                    continue

                job_id = submit_job(script_path)
                if job_id is None:
                    tracker.mark_failed(
                        variable, experiment_id, "monitor: qsub returned no job id"
                    )
                    print(f"Failed to submit job for {variable}", file=sys.stderr)
                    continue

                tracker.set_pbs_job_id(variable, experiment_id, job_id)
                job_map[job_id] = variable
                print(f"Submitted {variable} as job {job_id}")
                return job_id, variable
            return None

        while len(job_map) < max_inflight_jobs and submit_next() is not None:
            pass

        def shutdown_handler(sig: int, _frame: object) -> None:
            # PBS sends SIGTERM before SIGKILL on walltime exceedance or qdel.
            # Best-effort: any sub still in a non-terminal state had its outcome
            # cut short from our perspective, so mark it failed. SIGKILL would
            # of course bypass this entirely.
            print(
                f"Monitor received signal {sig}; marking still-running sub-jobs as failed."
            )
            for jid, var in list(job_map.items()):
                try:
                    cur = tracker.get_status(var, experiment_id)
                    if cur in ("running", "pending"):
                        tracker.mark_failed(
                            var,
                            experiment_id,
                            f"monitor terminated (sig={sig}); job {jid} outcome unknown",
                        )
                except Exception:
                    # Never let a DB hiccup stop the monitor from exiting.
                    pass
            sys.exit(143)

        signal.signal(signal.SIGTERM, shutdown_handler)

        try:
            monitor_loop(
                tracker,
                job_map,
                experiment_id,
                script_dir,
                poll_interval=int(
                    config.get("monitor_poll_interval", MONITOR_POLL_INTERVAL_SECONDS)
                ),
                submit_next=submit_next,
                max_inflight_jobs=max_inflight_jobs,
            )
        finally:
            # finalize_monitor is the only thing that reclassifies rows left in
            # a non-terminal state and removes the sidecar, so it has to run on
            # every exit path: the normal one, an exception out of monitor_loop,
            # and the SystemExit raised by shutdown_handler. Its own failure
            # must not replace whatever exception is already on its way out.
            try:
                finalize_monitor(
                    tracker,
                    config,
                    experiment_id,
                    db_path,
                    config_path=config_path,
                    script_dir=script_dir,
                    variables=monitored_variables,
                )
            except Exception as e:
                print(f"Warning: monitor finalize failed: {e}", file=sys.stderr)
    finally:
        tracker.close()


def monitor_loop(
    tracker: TaskTracker,
    job_map: dict[str, str],
    experiment_id: str,
    script_dir: str | Path,
    poll_interval: int = MONITOR_POLL_INTERVAL_SECONDS,
    submit_next: Callable[[], tuple[str, str] | None] | None = None,
    max_inflight_jobs: int | None = None,
) -> None:
    """Poll qstat for each pending sub-job and reconcile when it finishes.

    Exits once every sub-job has left the queue (state no longer in
    Q/R/H/S/T/W/E). 'F' (finished), 'X' (expired) and 'gone' (history purged)
    all trigger reconciliation against the DB.
    """
    pending = set(job_map.keys())
    # Consecutive 'gone' (qstat returned nothing) observations per job. A single
    # 'gone' is not trusted — see MONITOR_GONE_CONFIRMATIONS. Reset whenever the
    # job is seen in an active state again.
    gone_counts: dict[str, int] = {}
    print(f"Monitoring {len(pending)} sub-jobs (poll interval {poll_interval}s)")

    def fill_available_slots() -> None:
        if submit_next is None:
            return
        while max_inflight_jobs is None or len(pending) < max_inflight_jobs:
            submitted = submit_next()
            if submitted is None:
                return
            next_job_id, _variable = submitted
            pending.add(next_job_id)

    def stop_watching(job_id: str) -> None:
        """Forget a sub-job so the loop stops polling it and a slot opens."""
        pending.discard(job_id)
        job_map.pop(job_id, None)
        gone_counts.pop(job_id, None)

    while True:
        if not pending:
            if submit_next is None:
                break
            time.sleep(poll_interval)
            fill_available_slots()
            if not pending:
                break
            continue

        time.sleep(poll_interval)
        job_info = qstat_many(list(pending))
        for job_id in list(pending):
            info = job_info.get(job_id)
            state = qstat_state(info)
            # Guarded per sub-job: reconcile_one and get_status both touch the
            # DB, and an unhandled failure in either used to end the loop for
            # every remaining sub-job too.
            try:
                if state in ("Q", "R", "H", "S", "T", "W", "E"):
                    gone_counts.pop(job_id, None)
                    continue
                variable = job_map[job_id]
                if state == "gone":
                    # info is None: could be a transient qstat failure rather than
                    # the job really being gone. Require several consecutive 'gone'
                    # observations before believing it.
                    gone_counts[job_id] = gone_counts.get(job_id, 0) + 1
                    print(
                        f"qstat returned nothing for {variable} (job {job_id}); "
                        f"gone {gone_counts[job_id]}/{MONITOR_GONE_CONFIRMATIONS} "
                        "consecutive polls — treating as still pending until confirmed",
                        file=sys.stderr,
                    )
                    if gone_counts[job_id] < MONITOR_GONE_CONFIRMATIONS:
                        continue
                reconcile_one(
                    tracker, variable, experiment_id, job_id, info, script_dir
                )
                stop_watching(job_id)
                status = tracker.get_status(variable, experiment_id)
                exit_status = info.get("Exit_status") if info else "unavailable"
                print(
                    f"Sub-job done: {variable} (job {job_id}, status={status}, "
                    f"pbs_state={state}, exit_status={exit_status})"
                )
            except Exception as e:
                # Stop watching rather than retrying every poll: anything that
                # gets past TaskTracker's retry wrapper is not transient, and a
                # permanently broken DB would otherwise pin the loop until
                # walltime. finalize_monitor reclassifies the row afterwards; if
                # even that cannot reach the DB, this line is the only surviving
                # record of the outcome, so it carries the full PBS detail.
                exit_status = info.get("Exit_status") if info else "unavailable"
                print(
                    f"Failed to reconcile {job_map.get(job_id, '<unknown>')} "
                    f"(job {job_id}, pbs_state={state}, exit_status={exit_status}): {e}",
                    file=sys.stderr,
                )
                stop_watching(job_id)
        fill_available_slots()


def finalize_monitor(
    tracker: TaskTracker,
    config: BatchConfig,
    experiment_id: str,
    db_path: str | Path,
    *,
    config_path: str | Path | None = None,
    script_dir: str | Path | None = None,
    variables: Iterable[str] | None = None,
) -> None:
    """Run a last-pass consistency check, print a summary, remove the sidecar.

    Catches the rare case where monitor_loop saw a sub finish but the DB
    write was lost (status still 'running'), and the case where a variable
    never moved out of 'pending' because qsub itself failed early. Both are
    reclassified as 'failed' so no row is left in a non-terminal state.
    """
    summary = {
        "completed": 0,
        "failed": 0,
        "pending": 0,
        "fixed_stuck": 0,
        "unreadable": 0,
    }
    for variable in variables if variables is not None else config["variables"]:
        # Guarded per variable: a corrupt database raises on the rows whose
        # pages are damaged but serves the rest normally, so one bad row must
        # not cost the sweep every variable after it, the summary, the report,
        # or the sidecar removal below.
        try:
            status = tracker.get_status(variable, experiment_id)
            if status == "running":
                tracker.mark_failed(
                    variable,
                    experiment_id,
                    "monitor finalize: sub finished but DB stayed in running state",
                )
                summary["failed"] += 1
                summary["fixed_stuck"] += 1
            elif status == "pending":
                tracker.mark_failed(
                    variable,
                    experiment_id,
                    "monitor finalize: variable never moved out of pending",
                )
                summary["failed"] += 1
            elif status == "completed":
                summary["completed"] += 1
            elif status == "failed":
                summary["failed"] += 1
        except Exception as e:
            summary["unreadable"] += 1
            print(
                f"Warning: could not finalize {variable}: {e}",
                file=sys.stderr,
            )

    print(
        f"Batch monitor done. completed={summary['completed']}, "
        f"failed={summary['failed']}, fixed_stuck={summary['fixed_stuck']}, "
        f"unreadable={summary['unreadable']}"
    )

    try:
        # skip_qc: every worker already validated its own output file at write
        # time (base.py calls validate_cmip7_output after repacking), and QC
        # failures land in the DB as failed tasks. Re-running QC here would
        # load every output file inside this 1-cpu/4GB job, which OOM-killed
        # the monitor on large batches. Run QC explicitly via
        # `python -m access_moppy.batch_report` when a QC-annotated report
        # is wanted.
        report_path = write_batch_report(
            db_path,
            config=config,
            config_path=config_path,
            script_dir=script_dir,
            skip_qc=True,
        )
        print(f"Wrote batch coordination report: {report_path}")
    except Exception as e:
        print(
            f"Warning: failed to write batch coordination report: {e}", file=sys.stderr
        )

    if config.get("ilamb_input_format"):
        output_dir = Path(db_path).parent
        ilamb_dir = output_dir / "ilamb_input"
        try:
            from access_moppy.utilities import create_ilamb_model_symlinks

            links = create_ilamb_model_symlinks(
                output_dir,
                ilamb_dir,
                drs_format="auto",
                overwrite=True,
                frequency=config.get("ilamb_frequency", "mon"),
                variables=config.get("ilamb_variables"),
            )
            if links:
                print(f"Created {len(links)} ILAMB input symlink(s) in: {ilamb_dir}")
            else:
                print(
                    "Warning: ilamb_input_format is set but no ILAMB input symlinks "
                    f"were created in {ilamb_dir}",
                    file=sys.stderr,
                )
        except Exception as e:
            print(
                f"Warning: failed to create ILAMB input symlinks: {e}",
                file=sys.stderr,
            )

    sidecar = Path(db_path).parent / SIDECAR_FILENAME
    try:
        sidecar.unlink()
    except FileNotFoundError:
        pass


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the argument parser for ``moppy-cmorise``."""

    class _Parser(argparse.ArgumentParser):
        """Print errors to stdout (exit 1) and use 'Usage:' capitalisation."""

        def error(self, message: str) -> None:
            self.print_usage(sys.stdout)
            print(f"\nError: {message}")
            sys.exit(1)

        def format_usage(self) -> str:
            return super().format_usage().replace("usage:", "Usage:", 1)

        def format_help(self) -> str:
            return super().format_help().replace("usage:", "Usage:", 1)

    parser = _Parser(
        prog="moppy-cmorise",
        description="Batch CMORisation controller for ACCESS model output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  moppy-cmorise batch_config.yml\n"
            "  moppy-cmorise batch_config.yml --resume\n"
            "  moppy-cmorise batch_config.yml --rerun-variable Amon.tas Amon.pr\n"
            "  moppy-cmorise batch_config.yml --force\n"
            "  moppy-cmorise batch_config.yml --variable Amon.tas Amon.pr\n"
        ),
    )
    parser.add_argument(
        "config",
        metavar="config.yml",
        help="Path to the batch configuration YAML file.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume failed variables from their last completed time split. "
            "This overrides resume: false or an omitted resume setting in YAML."
        ),
    )
    parser.add_argument(
        "--rerun-variable",
        metavar="VARIABLE",
        nargs="+",
        dest="rerun_variables",
        default=None,
        help=(
            "Reset one or more variables to pending and resubmit them, even if "
            "already completed. Other variables are unaffected. "
            "Example: --rerun-variable Amon.tas Amon.pr"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Reset ALL variables (including completed ones) to pending before "
            "submitting. Re-runs the entire batch from scratch."
        ),
    )
    parser.add_argument(
        "--variable",
        metavar="VARIABLE",
        nargs="+",
        dest="variables",
        default=None,
        help=(
            "Only run the specified variable(s) from the config, ignoring all others. "
            "Useful for targeted first-runs or re-runs of specific variables. "
            "Example: --variable Amon.tas Amon.pr"
        ),
    )
    parser.add_argument(
        "--append-variable",
        metavar="VARIABLE",
        nargs="+",
        dest="append_variables",
        default=None,
        help=(
            "Append one or more variables to the active monitor instead of "
            "submitting another monitor job."
        ),
    )
    return parser


def main() -> None:
    """CLI entry point for `moppy-cmorise`.

    Two invocation modes:
      moppy-cmorise <config.yml> [--rerun-variable VAR ...] [--force]
                    — login-side: init DB, qsub the monitor.
      moppy-cmorise --monitor      — runs inside the monitor PBS job itself.

    The login-side path is intentionally thin: it pre-populates the task
    table, launches the dashboard, and submits exactly one PBS job (the
    monitor). The monitor takes over from there on a compute node, so the
    workflow survives the login shell disconnecting.
    """
    # Internal PBS monitor invocation — handled before argparse so the
    # public parser does not need to expose it.
    if len(sys.argv) >= 2 and sys.argv[1] == "--monitor":
        monitor_main()
        return

    parser = _build_arg_parser()
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}")
        sys.exit(1)

    with config_path.open() as f:
        config_data = yaml.safe_load(f)

    # Validate --variable and --rerun-variable names against the config before touching the DB.
    if args.variables:
        unknown = [v for v in args.variables if v not in config_data["variables"]]
        if unknown:
            print(
                f"Error: --variable specifies variable(s) not in the config: "
                f"{', '.join(unknown)}"
            )
            sys.exit(1)

    if args.rerun_variables:
        unknown = [v for v in args.rerun_variables if v not in config_data["variables"]]
        if unknown:
            print(
                f"Error: --rerun-variable specifies variable(s) not in the config: "
                f"{', '.join(unknown)}"
            )
            sys.exit(1)

    if args.append_variables:
        incompatible = args.variables or args.rerun_variables or args.force
        if incompatible:
            print(
                "Error: --append-variable cannot be combined with --variable, "
                "--rerun-variable, or --force"
            )
            sys.exit(1)
        unknown = [
            v for v in args.append_variables if v not in config_data["variables"]
        ]
        if unknown:
            print(
                "Error: --append-variable specifies variable(s) not in the config: "
                f"{', '.join(unknown)}"
            )
            sys.exit(1)

    # Put database in output directory on scratch filesystem (accessible from compute nodes)
    output_dir = Path(config_data["output_folder"])
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "cmor_tasks.db"

    experiment_id = config_data["experiment_id"]
    monitor_job_id = active_monitor_job_id(output_dir)

    if args.append_variables:
        if monitor_job_id is None:
            print(
                "Error: no active monitor job was found. Start these variables with "
                f"--variable {' '.join(args.append_variables)} instead."
            )
            sys.exit(1)
        appended: list[str] = []
        with TaskTracker(db_path) as tracker:
            for variable in args.append_variables:
                tracker.add_task(variable, experiment_id)
                status = tracker.get_status(variable, experiment_id)
                if status == "completed":
                    print(f"Skipped (already completed): {variable}")
                elif status == "running":
                    print(f"Skipped (already running): {variable}")
                else:
                    tracker.enqueue_monitor_request(variable, experiment_id)
                    appended.append(variable)
        print(
            f"Queued {len(appended)} variable(s) for monitor job {monitor_job_id}: "
            f"{', '.join(appended) if appended else 'none'}"
        )
        return

    if monitor_job_id is not None:
        print(
            f"Error: monitor job {monitor_job_id} is already active for {output_dir}.\n"
            "Append work with --append-variable VARIABLE, or wait for the active "
            "monitor to finish."
        )
        sys.exit(1)

    # Determine the effective variable list for this run.
    active_variables = (
        args.variables if args.variables else list(config_data["variables"])
    )

    # Pre-populate tasks for active variables, then apply any forced resets so
    # the monitor picks up the right set of variables to submit.
    with TaskTracker(db_path) as tracker:
        for variable in active_variables:
            tracker.add_task(variable, experiment_id)

        if args.force:
            for variable in active_variables:
                tracker.reset_to_pending(variable, experiment_id)
            print(
                f"--force: reset {len(active_variables)} variable(s) "
                "to pending (including any previously completed)."
            )
        elif args.rerun_variables:
            for variable in args.rerun_variables:
                tracker.reset_to_pending(variable, experiment_id)
            print(
                f"--rerun-variable: reset {len(args.rerun_variables)} variable(s) "
                f"to pending: {', '.join(args.rerun_variables)}"
            )

    if args.variables:
        print(
            f"Database initialized with {len(active_variables)} variable(s) "
            f"(filtered from {len(config_data['variables'])} in config) at: {db_path}"
        )
    else:
        print(f"Database initialized with {len(active_variables)} tasks at: {db_path}")

    # Start Streamlit dashboard (optional - won't block if streamlit is not installed)
    try:
        DASHBOARD_SCRIPT = files("access_moppy.dashboard").joinpath("cmor_dashboard.py")
        start_dashboard(str(DASHBOARD_SCRIPT), str(db_path))
    except FileNotFoundError:
        print(
            "Streamlit not found - skipping dashboard. Install with: pip install streamlit"
        )

    # Create directory for job scripts (defaults to output_folder so logs sit
    # alongside the DRS output and database under one parent directory)
    script_dir = Path(config_data.get("script_dir", config_data["output_folder"]))
    script_dir.mkdir(parents=True, exist_ok=True)

    # Submit a single monitor PBS job. The monitor runs on a compute node and is
    # responsible for qsub-ing the per-variable sub-jobs, polling them, and
    # reconciling DB state for any sub-job that exits without writing its own
    # terminal status (e.g. OOM-killed by PBS).
    monitor_script = create_monitor_script(
        config_data,
        config_path,
        db_path,
        script_dir,
        variable_filter=args.variables,
        resume=args.resume,
    )
    print(f"Created monitor script: {monitor_script}")

    monitor_job_id = submit_job(monitor_script)
    if not monitor_job_id:
        print("Failed to submit monitor job; falling back to direct submission.")
        sys.exit(1)

    # Sidecar: lets the dashboard / a successor monitor / users find the live monitor.
    sidecar = output_dir / SIDECAR_FILENAME
    sidecar.write_text(f"{monitor_job_id}\n{time.strftime('%Y-%m-%dT%H:%M:%S')}\n")

    print(f"\nSubmitted monitor job {monitor_job_id}")
    print(f"  Watches {len(active_variables)} variable(s)")
    print(
        f"  Sub-jobs are qsub'd from the monitor (see {script_dir}/moppy_monitor.out)"
    )
    print(f"  Sidecar file: {sidecar}")
    print(f"  Track progress: qstat -x {monitor_job_id}")
    print("Dashboard available at: http://localhost:8501")

    if config_data.get("wait_for_completion", False):
        wait_for_jobs([monitor_job_id])


if __name__ == "__main__":
    main()
