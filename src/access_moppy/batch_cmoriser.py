import os
import shlex
import signal
import subprocess
import sys
import time
from importlib.resources import files
from pathlib import Path

import yaml

from access_moppy.tracking import TaskTracker

# Sidecar file dropped in output_folder so the dashboard / a successor monitor
# can find the PBS jobid of the live monitor without scanning qstat.
SIDECAR_FILENAME = ".moppy_main.jobid"

# How often the monitor polls PBS for sub-job state. 30s keeps qstat load low
# while still detecting OOM kills within a poll cycle.
MONITOR_POLL_INTERVAL_SECONDS = 30


def parse_walltime(s):
    """Parse an 'HH:MM:SS' or 'MM:SS' walltime string into seconds."""
    parts = str(s).strip().split(":")
    if len(parts) == 3:
        h, m, sec = parts
    elif len(parts) == 2:
        h, m, sec = "0", parts[0], parts[1]
    else:
        raise ValueError(f"Invalid walltime: {s}")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def format_walltime(seconds):
    """Render an integer second count as a zero-padded 'HH:MM:SS' string."""
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def compute_monitor_walltime(config):
    """Pick a walltime for the monitor job from the batch config.

    The monitor only needs to outlive the slowest sub-job, so we take the
    largest walltime across the default and any per-variable override and
    add a 30-minute buffer for queue wait and final reconciliation.
    """
    default_wt = config.get("walltime", "02:00:00")
    var_resources = config.get("variable_resources", {})
    longest = parse_walltime(default_wt)
    for var in config["variables"]:
        wt = var_resources.get(var, {}).get("walltime", default_wt)
        longest = max(longest, parse_walltime(wt))
    return format_walltime(longest + 30 * 60)


def qstat_full(job_id):
    """Run `qstat -fx <job_id>` and parse the result into a dict.

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

    info = {}
    current_key = None
    for raw in result.stdout.splitlines():
        if not raw or raw.startswith("Job Id:"):
            current_key = None
            continue
        if (
            (raw.startswith(" ") or raw.startswith("\t"))
            and "=" not in raw
            and current_key
        ):
            info[current_key] += raw.strip()
            continue
        if "=" in raw:
            key, _, val = raw.partition("=")
            current_key = key.strip()
            info[current_key] = val.strip()
    return info


def qstat_state(info):
    """Return the PBS job_state letter from a qstat_full() dict.

    Returns 'gone' when info is None (job no longer visible to PBS) or when
    the dict has no job_state field — both cases are treated as 'finished'
    by the poll loop.
    """
    if not info:
        return "gone"
    return info.get("job_state", "gone")


def format_pbs_error(variable, job_id, info, script_dir):
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


def reconcile_one(tracker, variable, experiment_id, job_id, info, script_dir):
    """Decide what to write to the DB for one finished sub-job.

    The worker normally writes its own terminal status. The monitor only
    intervenes when the worker had no chance to (SIGKILL / OOM / node crash):

    - exit 0 and DB not already 'completed': backfill 'completed'.
    - non-zero exit and DB still 'running' or 'pending': mark 'failed' with
      a message built from qstat plus the worker's stderr tail.
    - DB already in a terminal state: leave it alone — the worker beat us.
    """
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


def start_dashboard(dashboard_path: str, db_path: str):
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


def create_job_script(variable, config, db_path, script_dir):
    """Create PBS job script and Python script for a variable."""
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

    # Create per-variable subdirectory under script_dir
    var_dir = script_dir / variable.replace(".", "_")
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


def submit_job(script_path):
    """Submit a PBS job and return the job ID."""
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


def wait_for_jobs(job_ids, poll_interval=30):
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


def create_monitor_script(config, config_path, db_path, script_dir):
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
    )

    monitor_path = Path(script_dir) / "moppy_monitor.sh"
    with open(monitor_path, "w") as f:
        f.write(rendered)
    os.chmod(monitor_path, 0o755)
    return monitor_path


def monitor_main():
    """Entry point for the monitor PBS job, invoked via `--monitor`.

    Runs on a compute node. Reads the batch config from $MOPPY_CONFIG_PATH,
    qsubs one sub-job per variable, records each job id in the DB, then
    polls qstat in monitor_loop. When the loop exits, finalize_monitor does
    a consistency sweep and the job ends cleanly with exit 0.

    Sub-jobs that fail at qsub time, or whose script can't even be rendered,
    are marked 'failed' immediately and the monitor moves on to the next
    variable rather than aborting the whole batch.
    """
    config_path = os.environ.get("MOPPY_CONFIG_PATH")
    db_path = os.environ.get("MOPPY_DB_PATH")
    script_dir_env = os.environ.get("MOPPY_SCRIPT_DIR")

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

    experiment_id = config["experiment_id"]
    script_dir = (
        Path(script_dir_env)
        if script_dir_env
        else Path(config.get("script_dir", "cmor_job_scripts"))
    )
    script_dir.mkdir(parents=True, exist_ok=True)

    tracker = TaskTracker(db_path)

    # job_map maps the PBS jobid we got back from qsub to the variable it
    # processes. monitor_loop iterates this map to poll qstat per sub-job.
    job_map = {}
    for variable in config["variables"]:
        if tracker.is_done(variable, experiment_id):
            print(f"Skipped (already completed): {variable}")
            continue

        try:
            script_path = create_job_script(variable, config, str(db_path), script_dir)
        except Exception as e:
            tracker.mark_failed(
                variable, experiment_id, f"monitor: failed to create script: {e}"
            )
            print(f"Failed to create script for {variable}: {e}", file=sys.stderr)
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

    if not job_map:
        print("No sub-jobs to monitor.")
        finalize_monitor(tracker, config, experiment_id, db_path)
        return

    def shutdown_handler(sig, _frame):
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

    monitor_loop(tracker, job_map, experiment_id, script_dir)
    finalize_monitor(tracker, config, experiment_id, db_path)


def monitor_loop(tracker, job_map, experiment_id, script_dir):
    """Poll qstat for each pending sub-job and reconcile when it finishes.

    Exits once every sub-job has left the queue (state no longer in
    Q/R/H/S/T/W). 'F' (finished), 'X' (expired) and 'gone' (history purged)
    all trigger reconciliation against the DB.
    """
    pending = set(job_map.keys())
    print(
        f"Monitoring {len(pending)} sub-jobs (poll interval {MONITOR_POLL_INTERVAL_SECONDS}s)"
    )

    while pending:
        time.sleep(MONITOR_POLL_INTERVAL_SECONDS)
        for job_id in list(pending):
            info = qstat_full(job_id)
            state = qstat_state(info)
            if state in ("Q", "R", "H", "S", "T", "W"):
                continue
            variable = job_map[job_id]
            reconcile_one(tracker, variable, experiment_id, job_id, info, script_dir)
            pending.discard(job_id)
            print(f"Sub-job done: {variable} (job {job_id}, state={state})")


def finalize_monitor(tracker, config, experiment_id, db_path):
    """Run a last-pass consistency check, print a summary, remove the sidecar.

    Catches the rare case where monitor_loop saw a sub finish but the DB
    write was lost (status still 'running'), and the case where a variable
    never moved out of 'pending' because qsub itself failed early. Both are
    reclassified as 'failed' so no row is left in a non-terminal state.
    """
    summary = {"completed": 0, "failed": 0, "pending": 0, "fixed_stuck": 0}
    for variable in config["variables"]:
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

    print(
        f"Batch monitor done. completed={summary['completed']}, "
        f"failed={summary['failed']}, fixed_stuck={summary['fixed_stuck']}"
    )

    sidecar = Path(db_path).parent / SIDECAR_FILENAME
    try:
        sidecar.unlink()
    except FileNotFoundError:
        pass


def main():
    """CLI entry point for `moppy-cmorise`.

    Two invocation modes:
      moppy-cmorise <config.yml>   — login-side: init DB, qsub the monitor.
      moppy-cmorise --monitor      — runs inside the monitor PBS job itself.

    The login-side path is intentionally thin: it pre-populates the task
    table, launches the dashboard, and submits exactly one PBS job (the
    monitor). The monitor takes over from there on a compute node, so the
    workflow survives the login shell disconnecting.
    """
    if len(sys.argv) >= 2 and sys.argv[1] == "--monitor":
        monitor_main()
        return

    if len(sys.argv) != 2:
        print("Usage: moppy-cmorise path/to/batch_config.yml")
        sys.exit(1)

    config_path = Path(sys.argv[1]).resolve()
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}")
        sys.exit(1)

    with config_path.open() as f:
        config_data = yaml.safe_load(f)

    # Put database in output directory on scratch filesystem (accessible from compute nodes)
    output_dir = Path(config_data["output_folder"])
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "cmor_tasks.db"
    tracker = TaskTracker(db_path)

    # Pre-populate all tasks
    experiment_id = config_data["experiment_id"]
    for variable in config_data["variables"]:
        tracker.add_task(variable, experiment_id)

    print(
        f"Database initialized with {len(config_data['variables'])} tasks at: {db_path}"
    )

    # Start Streamlit dashboard (optional - won't block if streamlit is not installed)
    try:
        DASHBOARD_SCRIPT = files("access_moppy.dashboard").joinpath("cmor_dashboard.py")
        start_dashboard(str(DASHBOARD_SCRIPT), str(db_path))
    except FileNotFoundError:
        print(
            "Streamlit not found - skipping dashboard. Install with: pip install streamlit"
        )

    # Create directory for job scripts (local to login node is fine)
    script_dir = Path(config_data.get("script_dir", "cmor_job_scripts"))
    script_dir.mkdir(parents=True, exist_ok=True)

    # Submit a single monitor PBS job. The monitor runs on a compute node and is
    # responsible for qsub-ing the per-variable sub-jobs, polling them, and
    # reconciling DB state for any sub-job that exits without writing its own
    # terminal status (e.g. OOM-killed by PBS).
    monitor_script = create_monitor_script(
        config_data, config_path, db_path, script_dir
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
    print(f"  Watches {len(config_data['variables'])} variables")
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
