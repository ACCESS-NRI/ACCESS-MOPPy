"""Manifest-backed native evaluation input for ESMValCore >= 2.15.

Configure this class as an external Core data source. It uses MOPPy's native
evaluation API and never creates intermediate CMOR files or publication IDs.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

IDENTITY_FACETS = {
    "project",
    "dataset",
    "exp",
    "ensemble",
    "mip",
    "short_name",
    "grid",
    "version",
    "frequency",
}


def _month(token, *, end=False):
    token = token.replace("-", "")
    if not re.fullmatch(r"\d{4}(?:\d{2}){0,2}", token):
        raise ValueError(
            f"Monthly timerange endpoint {token!r} must be YYYY, YYYYMM or YYYYMMDD"
        )
    year = int(token[:4])
    month = int(token[4:6]) if len(token) >= 6 else (12 if end else 1)
    if year < 1 or not 1 <= month <= 12:
        raise ValueError(f"Invalid monthly timerange endpoint {token!r}")
    return year * 12 + month


def _range(value):
    tokens = str(value).split("/")
    if len(tokens) != 2:
        raise ValueError("Monthly timerange must contain start/end")
    start, end = _month(tokens[0]), _month(tokens[1], end=True)
    if end < start:
        raise ValueError("Monthly timerange is reversed")
    return start, end


@dataclass(eq=False)
class MoppyDataElement:
    """One explicit native input selection, loaded as lazy Iris cubes."""

    name: str
    facets: dict[str, Any]
    spec: dict[str, Any] = field(repr=False)
    attributes: dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.name)

    def prepare(self):
        """Check the declared input files without modifying them."""
        paths = self.spec["files"] + [self.spec["grid_file"]]
        if self.spec.get("supergrid_file"):
            paths.append(self.spec["supergrid_file"])
        missing = [path for path in paths if not Path(path).is_file()]
        if missing:
            raise FileNotFoundError(f"Missing native evaluation inputs: {missing}")

    def to_iris(self):
        """Return the Core protocol's CubeList, preserving lazy field arrays."""
        import iris.cube
        import numpy as np
        import xarray as xr
        from xarray.backends import NetCDF4BackendEntrypoint

        from access_moppy.evaluation import normalize_native
        from access_moppy.iris_utils import dataset_to_iris

        self.prepare()
        owners = []
        try:
            # Use the explicit backend class to avoid unrelated backend discovery.
            native = xr.open_mfdataset(
                self.spec["files"],
                engine=NetCDF4BackendEntrypoint,
                combine="by_coords",
                join="exact",
                decode_coords="all",
                chunks=self.spec.get("chunks", {}),
            )
            owners.append(native)
            grid = xr.open_dataset(
                self.spec["grid_file"], engine=NetCDF4BackendEntrypoint, chunks={}
            )
            owners.append(grid)
            supergrid = None
            if self.spec.get("supergrid_file"):
                supergrid = xr.open_dataset(
                    self.spec["supergrid_file"],
                    engine=NetCDF4BackendEntrypoint,
                    chunks={},
                )
                owners.append(supergrid)
            result = normalize_native(
                native,
                short_name=self.facets["short_name"],
                component=self.spec["component"],
                grid=grid,
                supergrid=supergrid,
                run_id=self.spec["run_id"],
                model_id=self.facets["dataset"],
                options=self.spec,
            )
            if self.facets["frequency"] == "mon":
                first, last = _range(self.facets["timerange"])
                actual = result.time.dt.year.values * 12 + result.time.dt.month.values
                if not np.array_equal(actual, np.arange(first, last + 1)):
                    raise ValueError(
                        "Native months do not match the manifest timerange exactly"
                    )
            result.attrs.update(
                source_files=json.dumps(self.spec["files"]),
                source_grid_file=self.spec["grid_file"],
                native_identity=json.dumps(self.facets, sort_keys=True),
            )
            if self.spec.get("supergrid_file"):
                result.attrs["source_supergrid_file"] = self.spec["supergrid_file"]
            cube = dataset_to_iris(result, self.facets["short_name"])
            if (
                self.spec["component"] == "cice"
                and self.facets["short_name"] == "siconc"
            ):
                from iris.coords import AuxCoord

                # ncdata cannot represent scalar Unicode xarray coordinates.
                cube.add_aux_coord(
                    AuxCoord(
                        "sea_ice",
                        var_name="type",
                        standard_name="area_type",
                        long_name="Sea Ice area type",
                        units="no unit",
                    )
                )
            self.attributes = dict(result.attrs)
        finally:
            for owner in reversed(owners):
                owner.close()
        # File-backed Dask arrays reopen files through xarray's bounded file
        # cache when computed. Core has no DataElement.close() lifecycle, so
        # don't retain open datasets on this element between loads.
        return iris.cube.CubeList([cube])


class MoppyDataSource:
    """Find explicitly identified native fields from a versioned JSON manifest."""

    def __init__(self, *, name, project, manifest, priority=1):
        self.name, self.project, self.priority = name, project, priority
        self.path = Path(manifest).expanduser().resolve()
        document = json.loads(self.path.read_text())
        if document.get("schema_version") != 1 or not isinstance(
            document.get("entries"), list
        ):
            raise ValueError("Expected schema_version 1 and an entries list")
        self.entries = deepcopy(document["entries"])
        self.debug_info = f"MOPPy native evaluation manifest: {self.path}"
        seen = set()
        for entry in self.entries:
            facets = entry.get("facets", {})
            required = {
                "dataset",
                "exp",
                "ensemble",
                "mip",
                "short_name",
                "version",
                "grid",
                "frequency",
            }
            if not required <= facets.keys() or any(
                not isinstance(facets[k], str) or not facets[k].strip()
                for k in required
            ):
                raise ValueError(
                    f"Each entry requires nonempty identity facets {sorted(required)}"
                )
            if facets.get("project", self.project) != self.project:
                raise ValueError("Manifest project must match configured data source")
            if facets["dataset"] != "ACCESS-OM3":
                raise ValueError("Native evaluation currently supports ACCESS-OM3 only")
            if facets["frequency"] not in {"mon", "fx"}:
                raise ValueError(
                    "Native evaluation supports monthly or fixed fields only"
                )
            if (facets["short_name"] == "areacello") != (facets["frequency"] == "fx"):
                raise ValueError(
                    "Only areacello is a fixed field; all other fields require mon"
                )
            if facets["frequency"] == "mon":
                # A manifest describes complete monthly means. Day-level
                # requests are handled separately by Core's time extraction.
                if any(
                    len(token.replace("-", "")) not in {4, 6}
                    for token in facets.get("timerange", "").split("/")
                ):
                    raise ValueError(
                        "Manifest timerange must use whole years or months (YYYY or YYYYMM)"
                    )
                _range(facets.get("timerange", ""))
            identity = json.dumps(
                {k: facets[k] for k in sorted(required)}, sort_keys=True
            )
            if identity in seen:
                raise ValueError(
                    "Duplicate manifest identity; combine its files into one entry"
                )
            seen.add(identity)
            if entry.get("component") not in {"mom6", "cice"} or not entry.get(
                "native_grid"
            ):
                raise ValueError(
                    "Each entry requires component mom6/cice and native_grid"
                )
            if not isinstance(entry.get("run_id"), str) or not entry["run_id"].strip():
                raise ValueError("Each entry requires the actual run_id")
            if not isinstance(entry.get("files"), list) or not entry["files"]:
                raise ValueError("Each entry requires a nonempty explicit files list")
            paths = entry["files"] + [entry.get("grid_file")]
            if not all(isinstance(p, str) and p for p in paths):
                raise ValueError("Native files and grid_file must be nonempty paths")
            entry["files"] = [self._resolve(p) for p in entry["files"]]
            if len(set(entry["files"])) != len(entry["files"]):
                raise ValueError("Duplicate native input files")
            for key in ("grid_file", "supergrid_file"):
                if entry.get(key):
                    entry[key] = self._resolve(entry[key])

    def _resolve(self, value):
        path = Path(value).expanduser()
        return str(
            (self.path.parent / path).resolve()
            if not path.is_absolute()
            else path.resolve()
        )

    def find_data(self, **facets):
        """Select real identities; Core performs requested time extraction later."""
        matches = []
        for original in self.entries:
            actual = {"project": self.project, **original["facets"]}
            matched = True
            for key, query in facets.items():
                if query is None or key not in IDENTITY_FACETS:
                    continue
                queries = query if isinstance(query, (list, tuple, set)) else [query]
                if key not in actual or not any(
                    fnmatchcase(str(actual[key]), str(q)) for q in queries
                ):
                    matched = False
                    break
            query = facets.get("timerange")
            if (
                matched
                and query
                and "*" not in str(query)
                and actual["frequency"] == "mon"
            ):
                start, end = _range(query)
                first, last = _range(actual["timerange"])
                matched = start <= last and end >= first
            if matched:
                spec = deepcopy(original)
                digest = hashlib.sha256(
                    json.dumps(spec, sort_keys=True).encode()
                ).hexdigest()
                matches.append(MoppyDataElement(f"{self.name}:{digest}", actual, spec))
        return matches
