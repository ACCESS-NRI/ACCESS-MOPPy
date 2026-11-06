"""Manifest-backed CMORisation input for ESMValCore >= 2.15.

Configure this class as an external Core data source. Every load runs the
standard MOPPy CMORiser with its variable/axis tables before Iris conversion.
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
        """CMORise through the public driver and return the Core CubeList."""
        import iris.cube
        import xarray as xr
        from xarray.backends import NetCDF4BackendEntrypoint

        from access_moppy.driver import ACCESS_ESM_CMORiser

        self.prepare()
        owners = []
        try:
            native = xr.open_mfdataset(
                self.spec["files"],
                engine=NetCDF4BackendEntrypoint,
                combine="by_coords",
                join="exact",
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
            if self.facets["frequency"] == "mon":
                _validate_monthly_input(native, self.facets["timerange"])
            arguments = deepcopy(self.spec["cmoriser"])
            arguments.update(
                compound_name=f"{self.facets['mip']}.{self.facets['short_name']}",
                grid_data=grid,
                supergrid_data=supergrid,
                grid_identity_verified=self.spec.get("grid_identity_verified", False),
                grid_reference=self.spec.get("grid_reference"),
                enable_resampling=False,
            )
            with ACCESS_ESM_CMORiser(native, **arguments) as cmoriser:
                table_frequency = cmoriser.vocab.variable["frequency"]
                if table_frequency != self.facets["frequency"]:
                    raise ValueError(
                        f"Manifest frequency {self.facets['frequency']!r} does not "
                        f"match {cmoriser.compound_name} CMOR table frequency "
                        f"{table_frequency!r}"
                    )
                cmoriser.run()
                result = _select_cmorised(cmoriser.to_dataset(), self.spec)
                result.attrs.update(
                    source_files=json.dumps(self.spec["files"]),
                    source_grid_file=self.spec["grid_file"],
                    source_manifest_identity=json.dumps(self.facets, sort_keys=True),
                    access_moppy_processing_class=type(cmoriser.cmoriser).__name__,
                )
                cmoriser.cmoriser.ds = result
                cube = cmoriser.to_iris()
                self.attributes = dict(result.attrs)
        finally:
            for owner in reversed(owners):
                owner.close()
        return iris.cube.CubeList([cube])


def _validate_monthly_input(native, timerange):
    """Validate interval coverage before the CMORiser transforms time metadata."""
    import numpy as np

    if "time" not in native:
        raise ValueError("Monthly input needs a time coordinate")
    name = native.time.attrs.get("bounds") or native.time.encoding.get("bounds")
    if not name or name not in native:
        raise ValueError("Monthly input requires explicit time bounds")
    bounds = native[name]
    extra = [dim for dim in bounds.dims if dim != "time"]
    if len(extra) != 1 or bounds.ndim != 2 or bounds.sizes[extra[0]] != 2:
        raise ValueError("Monthly time bounds need two endpoints per interval")
    lower, upper = (bounds.isel({extra[0]: i}) for i in (0, 1))
    months = lower.dt.year.values * 12 + lower.dt.month.values
    end_months = upper.dt.year.values * 12 + upper.dt.month.values
    first, last = _range(timerange)
    if not np.array_equal(months, np.arange(first, last + 1)):
        raise ValueError("Native months do not match the manifest timerange exactly")
    if not np.all(end_months - months == 1) or any(
        np.any(getattr(endpoint.dt, part).values != expected)
        for endpoint in (lower, upper)
        for part, expected in (
            ("day", 1),
            ("hour", 0),
            ("minute", 0),
            ("second", 0),
            ("microsecond", 0),
        )
    ):
        raise ValueError("Monthly bounds must span complete calendar months")


def _select_cmorised(dataset, spec):
    """Apply diagnostic subsets only after full table-driven CMORisation."""
    import numpy as np

    selection = spec.get("index_selection", {})
    grid = spec["native_grid"]
    horizontal = {
        "mom6-t": ("yh", "xh"),
        "T": ("yh", "xh"),
        "mom6-u": ("yh", "xq"),
        "Cu": ("yh", "xq"),
        "mom6-v": ("yq", "xh"),
        "Cv": ("yq", "xh"),
        "cice-t": ("nj", "ni"),
    }[grid]
    for source, target in zip(horizontal, ("j", "i")):
        if source in selection:
            dataset = dataset.isel({target: slice(*selection[source])})
    depth = spec.get("depth_selection")
    if depth:
        if "lev" not in dataset.dims:
            raise ValueError("Depth selection requires a CMORised lev dimension")
        if set(depth) == {"index"}:
            dataset = dataset.isel(lev=int(depth["index"]))
        elif set(depth) == {"maximum"}:
            dataset = dataset.sel(lev=slice(None, float(depth["maximum"])))
        elif set(depth) == {"nearest_shallower"}:
            indices = np.flatnonzero(
                dataset.lev.values < float(depth["nearest_shallower"])
            )
            if not len(indices):
                raise ValueError("No CMORised level shallower than requested depth")
            dataset = dataset.isel(lev=slice(indices[-1], indices[-1] + 1))
        else:
            raise ValueError("Unsupported depth selection")
    return dataset


class MoppyDataSource:
    """Discover native inputs that will be CMORised by the standard driver."""

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
            arguments = entry.get("cmoriser")
            if (
                not isinstance(arguments, dict)
                or not {"experiment_id", "source_id", "variant_label"}
                <= arguments.keys()
            ):
                raise ValueError(
                    "Each entry requires explicit cmoriser identity arguments"
                )
            forbidden = {
                "input_data",
                "input_paths",
                "input_folder",
                "compound_name",
                "grid_data",
                "supergrid_data",
            }
            if forbidden & arguments.keys():
                raise ValueError(
                    "Input files, variable, and grids are defined by the manifest"
                )
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
            if not (facets["exp"] == entry["run_id"] == arguments["experiment_id"]):
                raise ValueError(
                    "Manifest exp, run_id, and cmoriser experiment_id must match"
                )
            if not (
                facets["dataset"]
                == arguments["source_id"]
                == arguments.get("model_id", arguments["source_id"])
            ):
                raise ValueError(
                    "Manifest dataset, cmoriser source_id, and model_id must match"
                )
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
