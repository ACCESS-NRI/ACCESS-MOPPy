"""Run the production daily-mass mapping on the retained 1850 Gadi sample.

Input NetCDFs contain the actual 50 sampled strait contributions at their
original 300x360 indices; all unsampled cells are missing. This checks the
file-discovery/loading/derivation/writing path without transferring full grids.
Use the development environment on PATH and the PR's src on PYTHONPATH.
"""

import csv
import json
import subprocess
import tempfile
from pathlib import Path

import cftime
import netCDF4
import numpy as np
import xarray as xr

from access_moppy import ACCESS_ESM_CMORiser

HERE = Path(__file__).resolve().parent
sample = np.load("/tmp/issue772-daily-year-1850.npz")
metadata = json.loads(str(sample["metadata_json"]))
points = metadata["points"]
jj = np.array([p["j"] for p in points])
ii = np.array([p["i"] for p in points])
names = list(dict.fromkeys(p["line"] for p in points))
expected = np.empty((12, 4))
with (HERE / "monthly_comparison.csv").open() as handle:
    for row in csv.DictReader(handle):
        expected[int(row["month"]) - 1, names.index(row["line"])] = float(
            row["daily_input_monthly_mean_kg_s"]
        )
summaries = []
with tempfile.TemporaryDirectory(prefix="issue772-daily-inputs-") as temporary:
    root = Path(temporary)
    folder = root / "output000/ice"
    folder.mkdir(parents=True)
    for month in range(1, 13):
        select = sample["daily_month"] == month
        ds = xr.Dataset()
        for name in ("aice", "sithick", "siu", "siv"):
            data = np.full((int(select.sum()), 300, 360), np.nan, dtype="float32")
            data[:, jj, ii] = sample["daily_" + name][select]
            attrs = {
                k: v
                for k, v in metadata["attributes"]["daily_" + name].items()
                if k not in {"_FillValue", "missing_value"}
            }
            ds[name] = xr.DataArray(data, dims=("time", "nj", "ni"), attrs=attrs)
        for name in ("TLAT", "TLON", "ULAT", "ULON"):
            data = np.full((300, 360), np.nan, dtype="float32")
            data[jj, ii] = sample[name]
            ds[name] = xr.DataArray(
                data,
                dims=("nj", "ni"),
                attrs={
                    "units": "degrees_north" if name.endswith("LAT") else "degrees_east"
                },
            )
        ds["time"] = xr.DataArray(
            sample["daily_time"][select],
            dims="time",
            attrs=metadata["attributes"]["daily_time"],
        )
        ds["time_bounds"] = (("time", "d2"), sample["daily_time_bounds"][select])
        ds.to_netcdf(
            folder / f"iceh-1daily-mean_1850-{month:02d}.nc",
            encoding={
                "time": {"_FillValue": None},
                **{n: {"zlib": True, "complevel": 1} for n in ds.data_vars},
            },
        )
        ds.close()
    for version in ("CMIP6", "CMIP7"):
        variable = "simassacrossline"
        units = metadata["attributes"]["daily_time"]["units"]
        output_root = HERE.parent / "outputs/daily_reconstruction"
        with ACCESS_ESM_CMORiser(
            input_folder=root,
            compound_name=f"seaIce.{variable}.tavg-u-ht-u.mon.glb"
            if version == "CMIP7"
            else f"SImon.{variable}",
            cmip_version=version,
            source_id="ACCESS-ESM1-6",
            experiment_id="historical",
            variant_label="r1i1p1f1",
            output_path=output_root,
            drs_root=output_root,
            publication_profile="access-consortium"
            if version == "CMIP7"
            else "personal",
            parent_info={
                "parent_experiment_id": "piControl",
                "parent_activity_id": "CMIP",
                "parent_source_id": "ACCESS-ESM1-6",
                "parent_variant_label": "r1i1p1f1",
                "parent_mip_era": version,
                "parent_time_units": units,
                "branch_time_in_child": float(
                    cftime.date2num(
                        cftime.DatetimeProlepticGregorian(1850, 1, 1), units
                    )
                ),
                "branch_time_in_parent": float(
                    cftime.date2num(cftime.DatetimeProlepticGregorian(201, 1, 1), units)
                ),
                "branch_method": "standard",
            },
            enable_resampling=True,
        ) as driver:
            driver.run(write_output=True)
            output = driver.cmoriser.written_files[0]
            gates = driver.cmoriser.qc_gates
        with netCDF4.Dataset(output) as nc:
            actual = nc[variable][:]
            assert actual.shape == (12, 4)
            assert nc[variable].dimensions == ("time", "line")
            assert list(netCDF4.chartostring(nc["line"][:])) == names
            assert "daily" in nc[variable].comment and "0.001" in nc[variable].comment
            np.testing.assert_allclose(actual, expected, rtol=2e-6, atol=1.0)
            np.testing.assert_array_equal(
                nc["time_bnds"][:], sample["monthly_time_bounds"]
            )
            np.testing.assert_array_equal(
                nc["time"][:], sample["monthly_time_bounds"].mean(axis=1)
            )
            error = float(np.max(np.abs(actual - expected)))
        checks = {}
        for suite in (
            "wcrp_cmip7:1.0" if version == "CMIP7" else "wcrp_cmip6:1.0",
            "cf:1.11" if version == "CMIP7" else "cf:1.7",
        ):
            report = HERE / f"pipeline_{version}_{suite.split(':')[0]}.json"
            proc = subprocess.run(
                [
                    "compliance-checker",
                    "-t",
                    suite,
                    "-f",
                    "json",
                    "-o",
                    str(report),
                    str(output),
                ],
                capture_output=True,
                text=True,
            )
            payload = json.loads(report.read_text())[suite]
            checks[suite] = {
                "returncode": proc.returncode,
                "failed_checks": [
                    check
                    for level in (
                        "high_priorities",
                        "medium_priorities",
                        "low_priorities",
                    )
                    for check in payload.get(level, [])
                    if check["value"][0] < check["value"][1]
                ],
                "stderr": proc.stderr,
            }
        summaries.append(
            {
                "version": version,
                "output": str(output.relative_to(HERE.parent)),
                "max_abs_difference_kg_s": error,
                "qc_gates": gates,
                "checks": checks,
            }
        )
(HERE / "pipeline_validation.json").write_text(json.dumps(summaries, indent=2) + "\n")
print(
    json.dumps(
        [{k: v for k, v in row.items() if k != "checks"} for row in summaries], indent=2
    )
)
