"""Read the legacy Gadi sample and write a compressed NPZ to stdout only."""

import io
import json
import sys

import netCDF4
import numpy as np

SOURCE = "/g/data/p73/archive/CMIP6/ACCESS-ESM1-5/HI-05/history/ice/iceh.1860-12.nc"
arrays = {}
with netCDF4.Dataset(SOURCE) as ds:
    metadata = {"path": SOURCE, "variables": list(ds.variables), "attrs": {}}
    for name in (
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
    ):
        var = ds[name]
        values = var[:]
        arrays[name] = np.asarray(
            values.filled(np.nan) if np.ma.isMaskedArray(values) else values
        )
        metadata["attrs"][name] = {
            **{key: var.getncattr(key) for key in var.ncattrs()},
            "dimensions": list(var.dimensions),
        }


def json_default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value))


arrays["metadata_json"] = np.array(json.dumps(metadata, default=json_default))
buffer = io.BytesIO()
np.savez_compressed(buffer, **arrays)
sys.stdout.buffer.write(buffer.getvalue())
