"""Lossless-at-the-data-level YAML editing and batch submission checks."""

import copy
import json
import re
from pathlib import Path

import yaml

MAX_YAML = 512_000
MODULE = re.compile(r"conda/analysis3(?:-\d{2}\.\d{2})?$")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*$")
PATH = re.compile(r"/[A-Za-z0-9_./+-]+$")


class UniqueLoader(yaml.SafeLoader):
    pass


def mapping(loader, node, deep=False):
    # Reject ambiguous explicit duplicates, but retain normal YAML merge keys.
    seen = set()
    for key, _ in node.value:
        if key.tag == "tag:yaml.org,2002:merge":
            continue
        value = loader.construct_object(key, deep=deep)
        if not isinstance(value, str) or value in seen:
            raise ValueError("YAML keys must be unique strings")
        seen.add(value)
    loader.flatten_mapping(node)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


UniqueLoader.add_constructor("tag:yaml.org,2002:map", mapping)


def parse_yaml(text):
    if not isinstance(text, str) or len(text.encode()) > MAX_YAML:
        raise ValueError("Batch YAML must be smaller than 512 KB")
    try:
        data = yaml.load(text, Loader=UniqueLoader)
        if not isinstance(data, dict):
            raise ValueError("Batch YAML must contain a mapping")
        encoded = json.dumps(data, allow_nan=False)
        if len(encoded) > MAX_YAML * 4:
            raise ValueError("Expanded YAML is too large")
    except (yaml.YAMLError, TypeError, RecursionError) as exc:
        raise ValueError(f"Invalid batch YAML: {exc}") from exc
    return data


class BatchDumper(yaml.SafeDumper):
    pass


BatchDumper.add_representer(
    str,
    lambda dumper, value: dumper.represent_scalar(
        "tag:yaml.org,2002:str", value, style="|" if "\n" in value else None
    ),
)


def dump(data):
    return yaml.dump(data, Dumper=BatchDumper, sort_keys=False, width=110)


def module_setup(module):
    if not isinstance(module, str) or not MODULE.fullmatch(module):
        raise ValueError("Choose conda/analysis3 or a dated analysis3 module")
    return (
        "set -e\nsource /etc/profile.d/modules.sh\n"
        "module purge\n"
        "module use /opt/Modules/modulefiles /apps/Modules/modulefiles /g/data/xp65/public/modules\n"
        f"module load {module}\n"
        "unset PYTHONHOME PYTHONPATH\nexport PYTHONNOUSERSITE=1\n"
    )


def template(user="YOUR_USER"):
    return dict(
        variables=["Amon.tas", "Amon.pr", "Omon.tos"],
        cmip_version="CMIP6",
        model_id="ACCESS-ESM1-6",
        source_id="ACCESS-ESM1-6",
        experiment_id="piControl",
        variant_label="r1i1p1f1",
        activity_id="CMIP",
        grid_label="gn",
        input_folder="/g/data/p73/archive/YOUR_EXPERIMENT",
        output_folder=f"/scratch/YOUR_PROJECT/{user}/moppy/new-batch",
        queue="normal",
        cpus_per_node=4,
        mem="16GB",
        jobfs="10GB",
        walltime="01:00:00",
        scheduler_options="#PBS -P YOUR_PROJECT",
        storage="gdata/xp65+gdata/p73",
        max_inflight_jobs=2,
        worker_init=module_setup("conda/analysis3"),
        wait_for_completion=False,
        qc_plots=False,
    )


def positive_int(value, label):
    if type(value) is not int or value < 1:
        raise ValueError(f"{label} must be a positive integer")


def resources(config, prefix=""):
    if "cpus_per_node" in config:
        positive_int(config["cpus_per_node"], prefix + "cpus_per_node")
    for key in ("mem", "jobfs"):
        if key in config and not re.fullmatch(r"[1-9]\d*GB", str(config[key])):
            raise ValueError(f"{prefix}{key} must use whole GB, e.g. 16GB")
    if "walltime" in config and not re.fullmatch(
        r"\d{2,3}:[0-5]\d:[0-5]\d", str(config["walltime"])
    ):
        raise ValueError(f"{prefix}walltime must use HH:MM:SS")
    if config.get("walltime") == "00:00:00":
        raise ValueError("walltime must be greater than zero")
    if "queue" in config and not IDENTIFIER.fullmatch(str(config["queue"])):
        raise ValueError(f"{prefix}queue is invalid")


def validate(data, filesystem=True):
    c = copy.deepcopy(data)
    c.setdefault("cmip_version", "CMIP6")
    c.setdefault("model_id", "ACCESS-ESM1-6")
    for key in (
        "experiment_id",
        "source_id",
        "model_id",
        "variant_label",
        "activity_id",
        "cmip_version",
    ):
        if not isinstance(c.get(key), str) or not IDENTIFIER.fullmatch(c[key]):
            raise ValueError(f"{key} is required and must be a simple identifier")
    if c["cmip_version"] not in ("CMIP6", "CMIP7", "CMIP6Plus", "TIPMIP"):
        raise ValueError("Unsupported cmip_version")
    variables = c.get("variables")
    if not isinstance(variables, list) or not variables or len(variables) > 2000:
        raise ValueError("Choose between 1 and 2000 variables")
    if any(
        not isinstance(v, str) or not IDENTIFIER.fullmatch(v) or "." not in v
        for v in variables
    ):
        raise ValueError(
            "Variables must be table.variable or CMIP7 compound identifiers"
        )
    if len(set(variables)) != len(variables):
        raise ValueError("Remove duplicate variables")
    for key in ("input_folder", "output_folder", "drs_root", "script_dir"):
        if key not in c:
            if key in ("input_folder", "output_folder"):
                raise ValueError(f"{key} is required")
            continue
        if (
            not isinstance(c[key], str)
            or not PATH.fullmatch(c[key])
            or ".." in Path(c[key]).parts
        ):
            raise ValueError(
                f"{key} must be an absolute path without spaces or shell metacharacters"
            )
        c[key] = str(Path(c[key]).resolve())
        if not PATH.fullmatch(c[key]):
            raise ValueError(
                f"{key} resolves to a path unsupported by the MOPPy shell template"
            )
    if c.get("script_dir", c["output_folder"]) != c["output_folder"]:
        raise ValueError(
            "This pilot keeps script_dir in output_folder; remove the custom script_dir"
        )
    c["script_dir"] = c["output_folder"]
    if "database_path" in c:
        raise ValueError("MOPPy uses output_folder/cmor_tasks.db; remove database_path")
    if c.get("drs_root") and not Path(c["drs_root"]).is_relative_to(c["output_folder"]):
        raise ValueError("drs_root must be inside this batch output_folder")
    # Native templates interpolate these fields into double-quoted shell assignments.
    for key in (
        "grid_label",
        "match_variable",
        "publication_lock_dir",
        "max_concurrent_publications",
        "publication_jitter_seconds",
        "publication_stale_seconds",
    ):
        if key in c and re.search(r'[\s$`"\\;\x00-\x1f]', str(c[key])):
            raise ValueError(
                f"{key} contains characters unsupported by the MOPPy shell template"
            )
    for key in (
        "queue",
        "cpus_per_node",
        "mem",
        "walltime",
        "storage",
        "scheduler_options",
        "worker_init",
    ):
        if key not in c:
            raise ValueError(f"{key} is required for submission from Studio")
    resources(c)
    if not re.fullmatch(
        r"#PBS -P [a-z]{1,4}\d{1,4}", str(c["scheduler_options"]).strip()
    ):
        raise ValueError(
            "scheduler_options must contain only #PBS -P <project> in this pilot"
        )
    c["scheduler_options"] = c["scheduler_options"].strip()
    if not isinstance(c["storage"], str) or not re.fullmatch(
        r"(?:gdata|scratch)/[a-z0-9]+(?:\+(?:gdata|scratch)/[a-z0-9]+)*", c["storage"]
    ):
        raise ValueError(
            "storage must be a + separated list such as gdata/xp65+scratch/iq82"
        )
    if (
        not isinstance(c["worker_init"], str)
        or not c["worker_init"].strip()
        or "\x00" in c["worker_init"]
    ):
        raise ValueError(
            "worker_init must contain the reviewed environment setup script"
        )
    overrides = c.get("variable_resources", {})
    if not isinstance(overrides, dict):
        raise ValueError("variable_resources must be a mapping")
    allowed = {"queue", "cpus_per_node", "mem", "jobfs", "walltime"}
    for variable, override in overrides.items():
        if not isinstance(override, dict) or set(override) - allowed:
            raise ValueError(
                f'{variable}: resource overrides support only {", ".join(sorted(allowed))}'
            )
        resources(override, variable + ".")
    if "max_inflight_jobs" in c:
        positive_int(c["max_inflight_jobs"], "max_inflight_jobs")
    for key in (
        "qc_plots",
        "wait_for_completion",
        "resume",
        "enable_chunking",
        "use_jobfs_staging",
    ):
        if key in c and type(c[key]) is not bool:
            raise ValueError(f"{key} must be true or false")
    for key in ("start_year", "end_year"):
        if key in c:
            positive_int(c[key], key)
    if c.get("start_year", 1) > c.get("end_year", 999999):
        raise ValueError("start_year must not exceed end_year")
    if filesystem:
        if not Path(c["input_folder"]).is_dir():
            raise ValueError(
                "Input folder is not accessible; check the path and ARE storage mounts"
            )
        if Path(c["output_folder"]).exists():
            raise ValueError(
                "Choose a new output folder. Use Runs → Attach existing batch to monitor an existing one."
            )
        parent = Path(c["output_folder"]).parent
        while not parent.exists():
            parent = parent.parent
        import os

        if not os.access(parent, os.W_OK | os.X_OK):
            raise ValueError(f"Output parent is not writable: {parent}")
    mounts = set(c["storage"].split("+"))
    for value in (c["input_folder"], c["output_folder"]):
        parts = Path(value).parts
        if len(parts) > 3 and parts[1:3] == ("g", "data"):
            mounts.add("gdata/" + parts[3])
        elif len(parts) > 2 and parts[1] == "scratch":
            mounts.add("scratch/" + parts[2])
    mounts.add("gdata/xp65")
    c["storage"] = "+".join(sorted(mounts))
    c["wait_for_completion"] = False
    return c
