"""Read-only Gadi sample extraction. Write a compressed NPZ archive to stdout.

Run under analysis3 with netCDF4/numpy/pyyaml, redirect stdout to a local file
over SSH. All progress goes to stderr; no source or remote file is modified.
"""

import hashlib
import io
import json
import sys
from pathlib import Path

import netCDF4 as nc
import numpy as np
import yaml

JQ = Path(
    "/g/data/jq44/access-nri/access-esm1p6/global/historical/r1i1p1f1/2026.06.12/output000"
)
P73 = Path(
    "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/ensemble-concentrations-historical/historical-historical-1.1-r1i1p1f1-01409bc3/historical-historical-1.1-r1i1p1f1-01409bc3/output000"
)
APP_GRID = Path("/g/data/p66/CMIP6/APP_ancils/cice_grid_20101208.nc")
NAMES = [
    "aice",
    "siu",
    "siv",
    "simass",
    "sisnmass",
    "sivol",
    "sidmasstranx",
    "sidmasstrany",
]
arrays = {}
metadata = {
    "sources": [],
    "attributes": {},
    "manifests": {},
    "daily_window": {"j": [244, 291], "i": [108, 304]},
}


def record(path):
    info = path.stat()
    metadata["sources"].append(
        {"path": str(path), "size_bytes": info.st_size, "mtime_ns": info.st_mtime_ns}
    )


def read(variable, selection=Ellipsis):
    value = variable[selection]
    return np.asarray(
        value.filled(np.nan)
        if np.ma.isMaskedArray(value) and value.dtype.kind == "f"
        else value
    )


def attrs(variable):
    return {k: variable.getncattr(k) for k in variable.ncattrs()}


for key, root in [("jq44", JQ), ("p73", P73)]:
    manifests = list(yaml.safe_load_all((root / "manifests/input.yaml").read_text()))
    records = manifests[-1]
    metadata["manifests"][key] = {
        k: v for k, v in records.items() if "/ice/INPUT/" in k
    }

for name in NAMES:
    path = next((JQ / "ice").glob(f"*.{name}.1mon.mean.1850.nc"))
    record(path)
    with nc.Dataset(path) as ds:
        arrays[f"jq44_{name}"] = read(ds[name])
        metadata["attributes"][f"jq44_{name}"] = attrs(ds[name])
        if name == "aice":
            for coord in [
                "TLAT",
                "TLON",
                "ULAT",
                "ULON",
                "tmask",
                "tarea",
                "uarea",
                "time",
                "time_bnds",
            ]:
                if coord in ds.variables:
                    arrays[f"jq44_{coord}"] = read(ds[coord])

monthly = {
    name: []
    for name in NAMES + ["siconc", "sithick", "sitimefrac", "time", "time_bounds"]
}
for month in range(1, 13):
    path = P73 / "ice" / f"iceh-1monthly-mean_1850-{month:02d}.nc"
    record(path)
    with nc.Dataset(path) as ds:
        for name in monthly:
            monthly[name].append(read(ds[name]))
            if month == 1:
                metadata["attributes"][f"p73_{name}"] = attrs(ds[name])
        if month == 1:
            for name in ["TLAT", "TLON", "ULAT", "ULON", "tmask", "tarea", "uarea"]:
                arrays[f"p73_{name}"] = read(ds[name])
    print(f"Read p73 monthly {month:02d}", file=sys.stderr, flush=True)
for name, values in monthly.items():
    arrays[f"p73_{name}"] = np.concatenate(values, axis=0)

# Two contrasting seasons. Only the Arctic window containing all four lines
# is transferred, with neighbouring cells for a staggering sensitivity check.
for month in [1, 9]:
    path = P73 / "ice" / f"iceh-1daily-mean_1850-{month:02d}.nc"
    record(path)
    with nc.Dataset(path) as ds:
        for name in ["aice", "siu", "siv", "siconc", "sithick", "sitimefrac"]:
            arrays[f"daily{month:02d}_{name}"] = read(
                ds[name], (slice(None), slice(244, 291), slice(108, 304))
            )
        arrays[f"daily{month:02d}_time_bounds"] = read(ds["time_bounds"])
        metadata["attributes"][f"daily{month:02d}_time"] = attrs(ds["time"])

for prefix, path in [
    ("app_grid", APP_GRID),
    (
        "model_grid",
        Path(metadata["manifests"]["p73"]["work/ice/INPUT/grid.nc"]["fullpath"]),
    ),
]:
    record(path)
    metadata[prefix + "_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with nc.Dataset(path) as ds:
        metadata[prefix + "_variables"] = list(ds.variables)
        for name in [
            "hue",
            "hun",
            "hte",
            "htn",
            "ulat",
            "ulon",
            "tlat",
            "tlon",
            "angle",
            "angleT",
            "tarea",
            "uarea",
        ]:
            if name in ds.variables:
                arrays[f"{prefix}_{name}"] = read(ds[name])
                metadata["attributes"][f"{prefix}_{name}"] = attrs(ds[name])


def default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value))


arrays["metadata_json"] = np.array(json.dumps(metadata, default=default))
buffer = io.BytesIO()
np.savez_compressed(buffer, **arrays)
print(f"Transferring {buffer.tell() / 1024**2:.1f} MiB", file=sys.stderr, flush=True)
sys.stdout.buffer.write(buffer.getvalue())
