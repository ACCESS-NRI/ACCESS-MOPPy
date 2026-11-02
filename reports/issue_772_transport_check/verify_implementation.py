"""Run both production mappings on the retained Gadi samples and check files.

Use the dev environment on PATH (for cmip7repack and compliance-checker).
Parent dates for the ESM1-6 sample come from its archived metadata. This is
a local validation output, not a publication to an external archive.
"""

import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import cftime
import netCDF4
import numpy as np
import xarray as xr

from access_moppy import ACCESS_ESM_CMORiser

HERE = Path(__file__).resolve().parent
source_path = Path("/tmp/issue772-app_functions.py")
source = source_path.read_text()
modern = np.load("/tmp/issue772_gadi_sample.npz")
legacy = np.load("/tmp/issue772_esm15_sample.npz")
modern_meta = json.loads(str(modern["metadata_json"]))
legacy_meta = json.loads(str(legacy["metadata_json"]))
grid = SimpleNamespace(
    variables={n: modern["app_grid_" + n] for n in ["hue", "hun"]}, close=lambda: None
)
namespace = {
    "np": np,
    "ancillary_path": "",
    "cdms2": SimpleNamespace(open=lambda *args: grid),
}
for name in [
    "geticeTransportLines",
    "transAcrossLine",
    "iceareaTransport",
    "icearealineTransports",
    "iceTransport",
    "icelineTransports",
]:
    body = source.split(f"def {name}(", 1)[1].split("\ndef ", 1)[0]
    exec(f"def {name}(" + body, namespace)


def modern_dataset():
    ds = xr.Dataset()
    for name in [
        "aice",
        "siu",
        "siv",
        "simass",
        "TLAT",
        "TLON",
        "ULAT",
        "ULON",
        "time",
        "time_bounds",
    ]:
        values = modern["p73_" + name]
        dims = (
            ("time", "d2")
            if name == "time_bounds"
            else ("time",)
            if name == "time"
            else ("time", "nj", "ni")
            if values.ndim == 3
            else ("nj", "ni")
        )
        attrs = modern_meta["attributes"].get(
            "p73_" + name,
            {"units": "degrees_north" if name.endswith("LAT") else "degrees_east"},
        )
        ds[name] = xr.DataArray(
            values,
            dims=dims,
            attrs={
                k: v
                for k, v in attrs.items()
                if k not in {"_FillValue", "missing_value"}
            },
        )
    return ds


def legacy_dataset():
    ds = xr.Dataset()
    for name in [
        "aice",
        "uvel",
        "vvel",
        "hi",
        "TLAT",
        "TLON",
        "ULAT",
        "ULON",
        "time",
        "time_bounds",
    ]:
        attrs = legacy_meta["attrs"][name]
        ds[name] = xr.DataArray(
            legacy[name],
            dims=attrs["dimensions"],
            attrs={
                k: v
                for k, v in attrs.items()
                if k not in {"_FillValue", "missing_value", "dimensions"}
            },
        )
    return ds


summaries = []
for model, ds in [
    ("ACCESS-ESM1-6", modern_dataset()),
    ("ACCESS-ESM1-5", legacy_dataset()),
]:
    path = Path("/tmp") / f"issue772-{model}-inputs.nc"
    # The raw CICE time coordinate has no fill value; prevent xarray's
    # default NaN fill encoding from inventing one in this reconstructed input.
    ds.time.encoding["_FillValue"] = None
    ds.to_netcdf(path)
    modern_model = model == "ACCESS-ESM1-6"
    u, v = (ds.siu, ds.siv) if modern_model else (ds.uvel, ds.vvel)
    quantity = ds.simass / 900 if modern_model else ds.hi
    references = {
        "siareaacrossline": namespace["icearealineTransports"](
            *[np.ma.masked_invalid(a.values) for a in (ds.aice, u, v)]
        ),
        "simassacrossline": namespace["icelineTransports"](
            *[np.ma.masked_invalid(a.values) for a in (quantity, u, v)]
        ),
    }
    for version in ["CMIP6", "CMIP7"] if modern_model else ["CMIP6"]:
        for variable, reference in references.items():
            compound = (
                f"seaIce.{variable}.tavg-u-ht-u.mon.glb"
                if version == "CMIP7"
                else f"SImon.{variable}"
            )
            # ESM1-5 parent branch metadata is illustrative: this test concerns
            # the calculation/encoding, not provenance approval of HI-05.
            parent = {
                "parent_experiment_id": "piControl",
                "parent_activity_id": "CMIP",
                "parent_source_id": model,
                "parent_variant_label": "r1i1p1f1",
                "parent_mip_era": version,
                "parent_time_units": ds.time.attrs["units"],
                "branch_time_in_child": float(
                    cftime.date2num(
                        cftime.DatetimeProlepticGregorian(1850, 1, 1),
                        ds.time.attrs["units"],
                    )
                ),
                "branch_time_in_parent": float(
                    cftime.date2num(
                        cftime.DatetimeProlepticGregorian(201, 1, 1),
                        ds.time.attrs["units"],
                    )
                ),
                "branch_method": "standard",
            }
            with ACCESS_ESM_CMORiser(
                input_data=path,
                compound_name=compound,
                cmip_version=version,
                source_id=model,
                experiment_id="historical",
                variant_label="r1i1p1f1",
                drs_root=HERE / "outputs",
                output_path=HERE / "outputs",
                parent_info=parent,
                publication_profile="access-consortium"
                if version == "CMIP7"
                else "personal",
                enable_resampling=False,
                enable_chunking=True,
            ) as driver:
                driver.run(write_output=True)
                output = driver.cmoriser.written_files[0]
                gates = driver.cmoriser.qc_gates
            with netCDF4.Dataset(output) as nc:
                value = nc[variable][:]
                assert nc[variable].dimensions == ("time", "line")
                assert nc[variable].coordinates == "line"
                assert (
                    list(netCDF4.chartostring(nc["line"][:]))
                    == namespace["geticeTransportLines"]()
                )
                np.testing.assert_allclose(
                    value,
                    reference,
                    rtol=2e-6,
                    atol=2.0 if variable == "simassacrossline" else 0.001,
                )
                max_abs = float(np.max(np.abs(value - reference)))
                units = nc[variable].units
            prefix = f"{model}_{version}_{variable}"
            checks = {}
            for suite in [
                "wcrp_cmip7:1.0" if version == "CMIP7" else "wcrp_cmip6:1.0",
                "cf:1.11" if version == "CMIP7" else "cf:1.7",
            ]:
                report = HERE / f"{prefix}_{suite.split(':')[0]}.json"
                result = subprocess.run(
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
                    text=True,
                    capture_output=True,
                )
                payload = json.loads(report.read_text())[suite]
                checks[suite] = {
                    "returncode": result.returncode,
                    "failed_checks": [
                        {k: c[k] for k in ["name", "weight", "value", "msgs"] if k in c}
                        for level in [
                            "high_priorities",
                            "medium_priorities",
                            "low_priorities",
                        ]
                        for c in payload.get(level, [])
                        if c.get("value", [0, 0])[0] < c.get("value", [0, 0])[1]
                    ],
                    "stderr": result.stderr,
                }
            summaries.append(
                {
                    "model": model,
                    "version": version,
                    "variable": variable,
                    "output": str(output.relative_to(HERE)),
                    "shape": list(value.shape),
                    "units": units,
                    "max_abs_difference_from_APP4": max_abs,
                    "max_abs_difference_divided_by_peak": max_abs
                    / float(np.max(abs(reference))),
                    "repack_gates": gates,
                    "checks": checks,
                }
            )
            print(
                prefix,
                "APP4 max abs",
                max_abs,
                "checks",
                json.dumps(checks),
                flush=True,
            )

(HERE / "implementation_validation.json").write_text(
    json.dumps(
        {
            "app4_source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
            "esm15_sample_source": legacy_meta["path"],
            "esm15_sample_sha256": hashlib.sha256(
                Path("/tmp/issue772_esm15_sample.npz").read_bytes()
            ).hexdigest(),
            "runs": summaries,
        },
        indent=2,
    )
    + "\n"
)
