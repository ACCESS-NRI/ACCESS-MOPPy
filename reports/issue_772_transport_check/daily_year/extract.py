"""Read 1850 daily/monthly CICE strait samples on Gadi; emit NPZ on stdout.

Only reads archive files. The extracted fields contain 50 strait contributions,
not the full grid. Run using analysis3, with stdout redirected on the SSH client.
"""

import io
import json
import sys
from pathlib import Path

import netCDF4
import numpy as np

ROOT = Path(
    "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/ensemble-concentrations-historical/"
    "historical-historical-1.1-r1i1p1f1-01409bc3/"
    "historical-historical-1.1-r1i1p1f1-01409bc3/output000/ice"
)
SEGMENTS = {
    "fram_strait": [("x", 267, 267, 279, 279), ("y", 268, 284, 278, 278)],
    "canadian_archipelago": [("y", 206, 212, 285, 285), ("x", 235, 235, 287, 288)],
    "barents_opening": [("y", 292, 300, 271, 271), ("x", 300, 300, 260, 271)],
    "bering_strait": [("y", 110, 111, 246, 246)],
}
points = [
    {"line": name, "component": component, "j": j, "i": i}
    for name, segments in SEGMENTS.items()
    for component, i0, i1, j0, j1 in segments
    for j in range(j0, j1 + 1)
    for i in range(i0, i1 + 1)
]
jj = np.array([p["j"] for p in points])
ii = np.array([p["i"] for p in points])
j0, j1 = int(jj.min()), int(jj.max()) + 1
i0, i1 = int(ii.min()), int(ii.max()) + 1
fields = ["aice", "siconc", "sithick", "siu", "siv", "sitimefrac"]
arrays = {}
metadata = {"points": points, "sources": [], "attributes": {}}


def read(var):
    if var.dimensions == ("time", "nj", "ni"):
        values = var[:, j0:j1, i0:i1][:, jj - j0, ii - i0]
    elif var.dimensions == ("nj", "ni"):
        values = var[j0:j1, i0:i1][jj - j0, ii - i0]
    else:
        values = var[:]
    return np.asarray(values.filled(np.nan) if np.ma.isMaskedArray(values) else values)


for frequency, prefix in (("1daily", "daily"), ("1monthly", "monthly")):
    names = fields + ["time", "time_bounds"]
    if prefix == "monthly":
        names += ["simass", "sivol"]
    collected = {name: [] for name in names}
    months = []
    for month in range(1, 13):
        path = ROOT / f"iceh-{frequency}-mean_1850-{month:02d}.nc"
        info = path.stat()
        metadata["sources"].append(
            {
                "path": str(path),
                "size_bytes": info.st_size,
                "mtime_ns": info.st_mtime_ns,
            }
        )
        with netCDF4.Dataset(path) as ds:
            assert (len(ds.dimensions["nj"]), len(ds.dimensions["ni"])) == (300, 360)
            months.extend([month] * len(ds.dimensions["time"]))
            for name in names:
                collected[name].append(read(ds[name]))
                attrs = {k: ds[name].getncattr(k) for k in ds[name].ncattrs()}
                if month == 1:
                    metadata["attributes"][prefix + "_" + name] = attrs
                if name == "time":
                    original = metadata["attributes"][prefix + "_" + name]
                    assert attrs["units"] == original["units"]
                    assert attrs["calendar"] == original["calendar"]
            for name in ("TLAT", "TLON", "ULAT", "ULON", "tmask"):
                coords = read(ds[name])
                if name in arrays:
                    np.testing.assert_array_equal(coords, arrays[name])
                else:
                    arrays[name] = coords
        print(f"Read {path.name}", file=sys.stderr, flush=True)
    for name, values in collected.items():
        arrays[prefix + "_" + name] = np.concatenate(values)
    arrays[prefix + "_month"] = np.asarray(months)


def json_default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value))


arrays["metadata_json"] = np.array(json.dumps(metadata, default=json_default))
buffer = io.BytesIO()
np.savez_compressed(buffer, **arrays)
print(f"Transferring {buffer.tell()} bytes", file=sys.stderr, flush=True)
sys.stdout.buffer.write(buffer.getvalue())
