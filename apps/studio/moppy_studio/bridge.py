"""Runs inside analysis3; scientific implementation stays in ACCESS-MOPPy."""

import importlib.metadata
import json
import os
import re
import sys
from importlib.resources import files
from pathlib import Path


def execute(action, payload):
    from access_moppy import batch_cmoriser as batch
    from access_moppy.tracking import TaskTracker
    from jinja2 import Template

    version = importlib.metadata.version("access_moppy")
    if not version.startswith(("1.8.", "1.9.")):
        raise ValueError(
            f"MOPPy {version} needs adapter verification; this pilot supports 1.8.x and 1.9.x"
        )
    config = payload["config"]
    root = Path(config["output_folder"])
    if action == "initialize":
        # Called once before qsub. After this, only the native monitor writes SQLite.
        with TaskTracker(root / "cmor_tasks.db") as tracker:
            for variable in config["variables"]:
                tracker.add_task(variable, config["experiment_id"])
        return {"version": version}
    if action != "inspect":
        raise ValueError("Unknown bridge action")
    text = (
        files("access_moppy.templates").joinpath("cmor_monitor_script.j2").read_text()
    )
    script = Template(text).render(
        config=config,
        config_path=str(root / "batch_config.yml"),
        db_path=str(root / "cmor_tasks.db"),
        script_dir=str(root),
        monitor_walltime=batch.compute_monitor_walltime(config),
        variable_filter=None,
        resume=config.get("resume", False),
    )
    script = script.replace("#PBS -N moppy_monitor", "#PBS -N " + payload["job_name"])
    resolved = next(
        (
            m
            for m in os.environ.get("LOADEDMODULES", "").split(":")
            if re.fullmatch(r"conda/analysis3-\d{2}\.\d{2}", m)
        ),
        None,
    )
    if not resolved:
        raise ValueError("Could not resolve analysis3 to a dated module")
    return {
        "version": version,
        "module": resolved,
        "monitor_script": script,
        "monitor_walltime": batch.compute_monitor_walltime(config),
    }


if __name__ == "__main__":
    try:
        result = execute(sys.argv[1], json.load(sys.stdin))
    except Exception as exc:
        print("MOPPY_STUDIO_JSON=" + json.dumps({"error": str(exc)}))
        sys.exit(1)
    print("MOPPY_STUDIO_JSON=" + json.dumps(result))
