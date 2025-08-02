import os
import subprocess
import sys
import time
from importlib.resources import files
from pathlib import Path

import yaml

from access_mopper.tracking import TaskTracker


def start_dashboard(dashboard_path: str, db_path: str):
    env = os.environ.copy()
    env["CMOR_TRACKER_DB"] = db_path
    subprocess.Popen(
        ["streamlit", "run", dashboard_path],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def create_job_script(variable, config, db_path, script_dir):
    """Create PBS job script and Python script for a variable."""
    from importlib.resources import files

    from jinja2 import Template

    # Load templates
    pbs_template_path = files("access_mopper.templates").joinpath("cmor_job_script.j2")
    python_template_path = files("access_mopper.templates").joinpath(
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
        result = subprocess.run(
            ["qsub", str(script_path)], capture_output=True, text=True, check=True
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
            result = subprocess.run(
                ["qstat", "-x"] + job_ids,
                capture_output=True,
                text=True,
                check=False,  # qstat returns non-zero when jobs complete
            )

            # Parse qstat output to see which jobs are still running
            still_running = []
            for line in result.stdout.split("\n"):
                for job_id in job_ids:
                    if job_id in line and any(
                        status in line for status in ["Q", "R", "H"]
                    ):
                        still_running.append(job_id)
                        break

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
        print("Usage: mopper-cmorise path/to/batch_config.yml")
        sys.exit(1)

    config_path = Path(sys.argv[1])
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

    # Start Streamlit dashboard
    DASHBOARD_SCRIPT = files("access_mopper.dashboard").joinpath("cmor_dashboard.py")
    start_dashboard(str(DASHBOARD_SCRIPT), str(db_path))

    # Create directory for job scripts (local to login node is fine)
    script_dir = Path("cmor_job_scripts")
    script_dir.mkdir(exist_ok=True)

    # Create and submit job scripts for each variable
    job_ids = []
    variables = config_data["variables"]

    print(f"Submitting {len(variables)} CMORisation jobs...")

    for variable in variables:
        # Create job script - pass the scratch database path
        script_path = create_job_script(variable, config_data, str(db_path), script_dir)
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
