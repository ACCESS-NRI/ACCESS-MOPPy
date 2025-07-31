import os
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import parsl
import yaml
from parsl import Config, HighThroughputExecutor, python_app
from parsl.addresses import address_by_hostname

from access_mopper.executors.pbs_scheduler import SmartPBSProvider
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


@python_app
def run_cmor(variable, config, db_path):
    import glob

    # Glob files for this variable
    from pathlib import Path

    import dask.distributed as dask

    from access_mopper import ACCESS_ESM_CMORiser
    from access_mopper.tracking import TaskTracker

    input_folder = config["input_folder"]
    pattern = config.get("file_patterns", {}).get(variable)
    full_pattern = str(input_folder + pattern)
    input_files = glob.glob(full_pattern)
    if not input_files:
        raise ValueError(f"No files found for pattern {pattern}")

    client = dask.Client(threads_per_worker=1)
    print(f"Dask dashboard for {variable}: {client.dashboard_link}")

    exp = config["experiment_id"]
    tracker = TaskTracker(Path(db_path))
    tracker.add_task(variable, exp)

    if tracker.is_done(variable, exp):
        return f"Skipped: {variable} (already done)"

    try:
        tracker.mark_running(variable, exp)
        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=input_files,
            compound_name=variable,
            experiment_id=config["experiment_id"],
            source_id=config["source_id"],
            variant_label=config["variant_label"],
            grid_label=config["grid_label"],
            activity_id=config.get("activity_id"),
            output_path=config["output_folder"],
            drs_root=config.get("drs_root"),
        )
        cmoriser.run()
        tracker.mark_done(variable, exp)
        client.close()
        return f"Completed: {variable}"
    except Exception as e:
        tracker.mark_failed(variable, exp, str(e))
        client.close()
        raise


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

    tracker = TaskTracker()
    DB_PATH = tracker.db_path

    # Start Streamlit dashboard
    DASHBOARD_SCRIPT = files("access_mopper.dashboard").joinpath("cmor_dashboard.py")
    start_dashboard(str(DASHBOARD_SCRIPT), str(DB_PATH))

    # Read resource settings from config_data, with defaults
    cpus_per_node = config_data.get("cpus_per_node", 4)
    mem = config_data.get("mem", "16GB")
    walltime = config_data.get("walltime", "01:00:00")
    storage = config_data.get("storage", None)
    nodes_per_block = config_data.get("nodes_per_block", 1)
    init_blocks = config_data.get("init_blocks", 1)
    max_blocks = config_data.get("max_blocks", 10)
    queue = config_data.get("queue", "normal")
    scheduler_options = config_data.get("scheduler_options", "#PBS -P your_project")
    worker_init = config_data.get("worker_init", "module load netcdf-python")

    # Configure Parsl
    parsl_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_pbs",
                address=address_by_hostname(),
                provider=SmartPBSProvider(
                    queue=queue,
                    scheduler_options=scheduler_options,
                    worker_init=worker_init,
                    nodes_per_block=nodes_per_block,
                    cpus_per_node=cpus_per_node,
                    mem=mem,
                    storage=storage,
                    walltime=walltime,
                    init_blocks=init_blocks,
                    max_blocks=max_blocks,
                ),
            )
        ],
        strategy="simple",
    )

    parsl.load(parsl_config)

    futures = [
        run_cmor(var, config_data, str(DB_PATH)) for var in config_data["variables"]
    ]
    results = [f.result() for f in futures]
    print("\n".join(results))


if __name__ == "__main__":
    main()
