"""Run the 117-variable piControl pilot with isolated per-variable processes.

Use the repository dev Python, with its bin directory on PATH. Outputs and
checker reports are retained; completed results are never overwritten.
"""

import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

BASE = Path("/home/romain/PROJECTS/ACCESS-MOPPy/reports/issue_767_rerun")
HERE = Path(os.environ["ISSUE767_RUN_ROOT"])
HERE.mkdir(parents=True, exist_ok=True)
NATIVE = Path(os.environ.get("ISSUE767_NATIVE", str(BASE / "native")))
for folder in ("results", "logs", "outputs"):
    (HERE / folder).mkdir(exist_ok=True)
SELECTION = json.loads((BASE / "selection.json").read_text())
SIZES = {
    f["path"]: f["bytes"]
    for f in json.loads((BASE / "archive_inventory.json").read_text())["files"]
}


def checker_env():
    env = os.environ.copy()
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "PYTHONPATH"):
        env.pop(name, None)
    env.update(
        ESGVOC_HOME=str(BASE / "checker_2_4/esgvoc"),
        ESGVOC_OFFLINE="true",
        XDG_CACHE_HOME="/tmp/issue767-checker-cache",
        PYTHONNOUSERSITE="1",
        HDF5_PLUGIN_PATH="/tmp/issue767-checker-2.4-venv/lib/python3.13/site-packages/hdf5plugin/plugins",
        OMP_NUM_THREADS="1",
        OPENBLAS_NUM_THREADS="1",
    )
    return env


def save(path, payload):
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def worker(row):
    from dataclasses import asdict

    import dask
    import netCDF4
    import numpy as np
    import yaml

    from access_moppy import ACCESS_ESM_CMORiser
    from access_moppy.qc.cmip7 import validate_cmip7_output_detailed

    dask.config.set(scheduler="synchronous")
    product = row["product"]
    destination = HERE / "outputs" / product
    destination.mkdir(parents=True, exist_ok=True)
    assert not list(destination.rglob("*.nc")), "Refusing to overwrite prior outputs"
    result = {k: row[k] for k in ["product", "legacy", "previous", "inputs"]}
    started = time.monotonic()
    run = None
    try:
        provenance = json.loads(
            (BASE.parent / "issue_798_sbl_validation/results.json").read_text()
        )["parent_provenance"]
        assert (
            yaml.safe_load((NATIVE / "metadata.yaml").read_text())["experiment_uuid"]
            == provenance["matching_experiment_uuid"]
        )
        with ACCESS_ESM_CMORiser(
            input_folder=NATIVE,
            compound_name=product,
            cmip_version="CMIP7",
            source_id="ACCESS-ESM1-6",
            experiment_id="piControl",
            variant_label="r1i1p1f1",
            publication_profile="access-consortium",
            parent_info=provenance["parent_info"],
            start_year=101,
            end_year=101,
            output_path=destination,
            drs_root=destination,
            split_years=1,
            enable_chunking=True,
            write_prefetch=1,
        ) as run:
            run.run(write_output=True)
            result["qc_gates"] = run.cmoriser.qc_gates
        result["processing"] = "passed"
    except Exception as exc:
        result["processing"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
        if run is not None and getattr(run, "cmoriser", None) is not None:
            result["qc_gates"] = run.cmoriser.qc_gates

    result["files"] = []
    for output in sorted(destination.rglob("*.nc")):
        info = {
            "path": str(output.relative_to(HERE)),
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        }
        try:
            info["range_qc"] = asdict(validate_cmip7_output_detailed(output))
            with netCDF4.Dataset(output) as ds:
                name = str(ds.variable_id)
                var = ds[name]
                info.update(
                    variable=name,
                    dimensions=list(var.dimensions),
                    shape=list(var.shape),
                    units=var.units,
                    cell_methods=getattr(var, "cell_methods", ""),
                    attributes={k: ds.getncattr(k) for k in ds.ncattrs()},
                )
                counts = {"valid": 0, "missing": 0, "nonfinite": 0}
                low, high = [], []
                for index in range(var.shape[0]):
                    data = np.ma.asarray(var[index]).astype("float64")
                    values = data.compressed()
                    counts["valid"] += values.size
                    counts["missing"] += data.size - values.size
                    counts["nonfinite"] += int((~np.isfinite(values)).sum())
                    finite = values[np.isfinite(values)]
                    if finite.size:
                        low.append(float(finite.min()))
                        high.append(float(finite.max()))
                info["values"] = {
                    **counts,
                    "min": min(low) if low else None,
                    "max": max(high) if high else None,
                }
                t = ds["time"]
                dates = netCDF4.num2date(t[:], t.units, t.calendar)
                bounds = ds[t.bounds][:]
                edges = netCDF4.num2date(
                    [bounds[0, 0], bounds[-1, 1]], t.units, t.calendar
                )
                info["time"] = {
                    "count": len(dates),
                    "first": str(dates[0]),
                    "last": str(dates[-1]),
                    "bounds": [str(x) for x in edges],
                    "calendar": t.calendar,
                }
                expected = {"mon": 12, "day": 365, "yr": 1}[product.split(".")[-2]]
                assert (
                    len(dates) == expected
                ), f"Expected {expected} time samples, found {len(dates)}"
                assert all(d.year == 101 for d in dates)
                assert (edges[0].year, edges[0].month, edges[0].day) == (101, 1, 1)
                assert (edges[-1].year, edges[-1].month, edges[-1].day) == (102, 1, 1)
                assert counts["valid"] > 0 and counts["nonfinite"] == 0
                info["structural"] = "passed"
        except Exception as exc:
            info["structural"] = "failed"
            info["structural_error"] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()
        report = destination / "wcrp.json"
        checker = subprocess.run(
            [
                "/tmp/issue767-checker-2.4-venv/bin/compliance-checker",
                "-c",
                "strict",
                "-t",
                "wcrp_cmip7:1.0",
                "-f",
                "json",
                "-o",
                str(report),
                str(output),
            ],
            capture_output=True,
            text=True,
            timeout=300,
            env=checker_env(),
        )
        (destination / "wcrp.log").write_text(checker.stdout + checker.stderr)
        info["wcrp"] = {"returncode": checker.returncode}
        if report.exists():
            payload = json.loads(report.read_text())["wcrp_cmip7:1.0"]
            info["wcrp"]["errors"] = payload.get("errors", {})
            info["wcrp"]["failed_checks"] = [
                c
                for level in ("high_priorities", "medium_priorities", "low_priorities")
                for c in payload.get(level, [])
                if c.get("value", [0, 0])[0] < c.get("value", [0, 0])[1]
            ]
        result["files"].append(info)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    result["passed"] = (
        result["processing"] == "passed"
        and len(result["files"]) == 1
        and all(
            f.get("structural") == "passed"
            and f.get("range_qc", {}).get("passed")
            and f.get("wcrp", {}).get("returncode") == 0
            and not f["wcrp"].get("failed_checks")
            for f in result["files"]
        )
    )
    save(HERE / "results" / (product + ".json"), result)


def coordinate():
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "logs").mkdir(exist_ok=True)
    pending = [
        r
        for r in SELECTION["variables"]
        if not (HERE / "results" / (r["product"] + ".json")).exists()
    ]
    pending.sort(key=lambda r: {"failed": 0, "added": 1, "succeeded": 2}[r["previous"]])
    while pending:
        ready = [
            r
            for r in pending
            if (NATIVE / "metadata.yaml").exists()
            and all(
                (NATIVE / p).exists() and (NATIVE / p).stat().st_size == SIZES[p]
                for p in r["inputs"]
            )
        ]
        if not ready:
            print(
                f"Waiting for input transfer: {len(pending)} variables remain",
                flush=True,
            )
            time.sleep(15)
            continue
        row = ready[0]
        with (HERE / "logs" / (row["product"] + ".log")).open("w") as log:
            call = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), row["product"]],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=900,
            )
        path = HERE / "results" / (row["product"] + ".json")
        if not path.exists():
            save(
                path,
                {
                    **{k: row[k] for k in ["product", "legacy", "previous"]},
                    "processing": "crashed",
                    "returncode": call.returncode,
                    "passed": False,
                },
            )
        result = json.loads(path.read_text())
        pending.remove(row)
        print(
            f"{117-len(pending)}/117 {row['legacy']}: {'PASS' if result['passed'] else 'FAIL'} {result.get('error', '')}",
            flush=True,
        )
    save(
        HERE / "results.json",
        [
            json.loads((HERE / "results" / (r["product"] + ".json")).read_text())
            for r in SELECTION["variables"]
        ],
    )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        worker(next(r for r in SELECTION["variables"] if r["product"] == sys.argv[1]))
    else:
        coordinate()
