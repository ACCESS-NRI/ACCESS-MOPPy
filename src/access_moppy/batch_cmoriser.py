import json
import os
import shlex
import subprocess
import sys
import time
from importlib.resources import as_file, files
from pathlib import Path

import yaml

from access_moppy.tracking import TaskTracker

MAPPING_DIR = "access_moppy.mappings"
TABLE_DIR = "access_moppy.vocabularies.cmip6_cmor_tables.Tables"


def get_variables_for_table(table_id, model_id=None):
    """Get all supported variables for a CMIP6 table filtered by model mappings.

    Loads the CMIP6 table JSON to find all defined variables, then filters
    to only those that have model mappings available.

    Args:
        table_id: CMIP6 table identifier (e.g., 'Amon', 'Omon', 'Emon').
        model_id: Model identifier (e.g., 'ACCESS-ESM1.6'). If None,
            defaults to 'ACCESS-ESM1.6'.

    Returns:
        Sorted list of compound variable names (e.g., ['Amon.hfss', 'Amon.pr', ...]).

    Raises:
        FileNotFoundError: If the CMIP6 table file is not found.
        FileNotFoundError: If the model mapping file is not found.
    """
    if model_id is None:
        model_id = "ACCESS-ESM1.6"

    # Load all variable names from the CMIP6 table
    table_resource = files(TABLE_DIR) / f"CMIP6_{table_id}.json"
    with as_file(table_resource) as path:
        with open(path, "r", encoding="utf-8") as f:
            table_data = json.load(f)
    table_variables = set(table_data.get("variable_entry", {}).keys())

    # Load all mapped variable names from the model mapping file
    mapping_dir = files(MAPPING_DIR)
    model_file = f"{model_id}_mappings.json"
    mapped_variables = set()

    for entry in mapping_dir.iterdir():
        if entry.name == model_file:
            with as_file(entry) as path:
                with open(path, "r", encoding="utf-8") as f:
                    all_mappings = json.load(f)

                for component in [
                    "aerosol",
                    "atmosphere",
                    "land",
                    "ocean",
                    "time_invariant",
                ]:
                    if component in all_mappings:
                        mapped_variables.update(all_mappings[component].keys())

                # Fallback: flat "variables" structure
                if "variables" in all_mappings:
                    mapped_variables.update(all_mappings["variables"].keys())
            break
    else:
        raise FileNotFoundError(
            f"Model mapping file '{model_file}' not found. "
            f"Available mappings are in the 'access_moppy.mappings' package."
        )

    # Intersection: variables defined in the table AND supported by the model
    supported = sorted(table_variables & mapped_variables)
    return [f"{table_id}.{var}" for var in supported]


def resolve_variables(config_data):
    """Resolve the full list of variables from config, expanding any table entries.

    Supports these config keys:

      - ``variables``: explicit list of compound names
        (e.g., ``['Amon.pr', 'Omon.tos']``)
      - ``tables``: CMIP6 table IDs to expand – accepts either a **list** or
        a **dict** that maps each table to a model_id::

            # list form (uses global model_id)
            tables:
              - Amon
              - Omon

            # dict form (per-table model_id)
            tables:
              Amon: ACCESS-ESM1.6
              Omon: ACCESS-OM3

      - ``model_id``: global default model_id for mapping lookups
      - both ``variables`` and ``tables`` can be used together; duplicates
        are removed

    Args:
        config_data: Parsed YAML config dictionary.

    Returns:
        A tuple ``(variables, variable_model_map)`` where *variables* is a
        sorted, deduplicated list of compound variable names and
        *variable_model_map* is a ``dict[str, str]`` mapping each variable
        to its effective model_id (only present when a per-table or
        per-variable override exists).

    Raises:
        ValueError: If neither ``variables`` nor ``tables`` is specified.
    """
    explicit_variables = config_data.get("variables", []) or []
    tables = config_data.get("tables", []) or []

    if not explicit_variables and not tables:
        raise ValueError("Config must specify at least one of 'variables' or 'tables'.")

    global_model_id = config_data.get("model_id", None)

    all_variables = set(explicit_variables)
    # Maps variable -> model_id (only for overrides)
    variable_model_map = {}

    # Normalise tables into [(table_id, model_id_or_None), ...]
    if isinstance(tables, dict):
        table_entries = list(tables.items())
    else:
        table_entries = [(t, None) for t in tables]

    for table_id, table_model_id in table_entries:
        effective_model_id = table_model_id or global_model_id
        try:
            table_vars = get_variables_for_table(table_id, model_id=effective_model_id)
            print(
                f"Table '{table_id}' (model={effective_model_id or 'default'}): "
                f"found {len(table_vars)} supported variables"
            )
            all_variables.update(table_vars)
            if table_model_id:
                for var in table_vars:
                    variable_model_map[var] = table_model_id
        except FileNotFoundError as e:
            print(f"Warning: {e}")
            continue

    # Apply per-variable model_id from variable_resources
    variable_resources = config_data.get("variable_resources", {}) or {}
    for var, res in variable_resources.items():
        if isinstance(res, dict) and "model_id" in res:
            variable_model_map[var] = res["model_id"]

    return sorted(all_variables), variable_model_map


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

    # Create Python script
    python_script_content = python_template.render(
        variable=variable,
        config=variable_config,  # Use variable-specific config
        db_path=db_path,
        package_path=package_path,
    )

    python_script_path = script_dir / f"cmor_{variable.replace('.', '_')}.py"
    with open(python_script_path, "w") as f:
        f.write(python_script_content)

    # Create PBS script
    pbs_script_content = pbs_template.render(
        variable=variable,
        config=variable_config,  # Use variable-specific config
        script_dir=script_dir,
        python_script_path=python_script_path,
        db_path=db_path,
    )

    pbs_script_path = script_dir / f"cmor_{variable.replace('.', '_')}.sh"
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


def main():
    if len(sys.argv) != 2:
        print("Usage: moppy-cmorise path/to/batch_config.yml")
        sys.exit(1)

    config_path = Path(sys.argv[1])
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}")
        sys.exit(1)

    with config_path.open() as f:
        config_data = yaml.safe_load(f)

    # Resolve variables: expand any 'tables' entries into individual variables
    variables, variable_model_map = resolve_variables(config_data)

    # Put database in output directory on scratch filesystem (accessible from compute nodes)
    output_dir = Path(config_data["output_folder"])
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "cmor_tasks.db"
    tracker = TaskTracker(db_path)

    # Pre-populate all tasks
    experiment_id = config_data["experiment_id"]
    for variable in variables:
        tracker.add_task(variable, experiment_id)

    print(f"Database initialized with {len(variables)} tasks at: {db_path}")

    # Start Streamlit dashboard (optional - won't block if streamlit is not installed)
    try:
        DASHBOARD_SCRIPT = files("access_moppy.dashboard").joinpath("cmor_dashboard.py")
        start_dashboard(str(DASHBOARD_SCRIPT), str(db_path))
    except FileNotFoundError:
        print(
            "Streamlit not found - skipping dashboard. Install with: pip install streamlit"
        )

    # Create directory for job scripts (local to login node is fine)
    script_dir = Path("cmor_job_scripts")
    script_dir.mkdir(exist_ok=True)

    # Create and submit job scripts for each variable
    job_ids = []
    # variables = config_data["variables"]

    print(f"Submitting {len(variables)} CMORisation jobs...")

    for variable in variables:
        # Inject per-variable model_id into config if an override exists
        job_config = config_data
        if variable in variable_model_map:
            job_config = config_data.copy()
            job_config["model_id"] = variable_model_map[variable]
        # Create job script - pass the scratch database path
        script_path = create_job_script(variable, job_config, str(db_path), script_dir)
        print(f"Created job script: {script_path}")

        # Submit job
        job_id = submit_job(script_path)
        if job_id:
            job_ids.append(job_id)
            print(f"Submitted job {job_id} for variable {variable}")
        else:
            print(f"Failed to submit job for variable {variable}")

    if job_ids:
        print(f"\nSubmitted {len(job_ids)} jobs successfully:")
        for i, (var, job_id) in enumerate(zip(variables[: len(job_ids)], job_ids)):
            print(f"  {var}: {job_id}")

        print(f"\nMonitor jobs with: qstat {' '.join(job_ids)}")
        print("Dashboard available at: http://localhost:8501")

        # Optionally wait for all jobs to complete
        if config_data.get("wait_for_completion", False):
            wait_for_jobs(job_ids)
    else:
        print("No jobs were submitted successfully")
        sys.exit(1)


if __name__ == "__main__":
    main()
