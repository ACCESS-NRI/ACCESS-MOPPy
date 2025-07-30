import os
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import parsl
import yaml
from parsl import Config, HighThroughputExecutor, python_app
from parsl.addresses import address_by_hostname
from parsl.providers import PBSProProvider

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
    from pathlib import Path

    from access_mopper import ACCESS_ESM_CMORiser
    from access_mopper.tracking import TaskTracker

    exp = config["experiment_id"]
    tracker = TaskTracker(Path(db_path))
    tracker.add_task(variable, exp)

    if tracker.is_done(variable, exp):
        return f"Skipped: {variable} (already done)"

    try:
        tracker.mark_running(variable, exp)
        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=Path(config["input_folder"]),
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
        return f"Completed: {variable}"
    except Exception as e:
        tracker.mark_failed(variable, exp, str(e))
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

    # Configure Parsl
    parsl_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_pbs",
                address=address_by_hostname(),
                max_workers=1,
                provider=PBSProProvider(
                    queue="normal",
                    launcher=None,
                    walltime="01:00:00",
                    select_options="1:ncpus=4:mem=16GB",
                    scheduler_options="#PBS -P your_project",
                    worker_init="module load netcdf-python",
                    nodes_per_block=1,
                    init_blocks=1,
                    max_blocks=10,
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
