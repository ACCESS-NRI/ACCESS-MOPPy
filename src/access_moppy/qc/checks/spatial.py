"""
access_moppy.qc.checks.spatial
================================

Spatial coordinate checks derived from the ESM1.6 pre-publication QC document:

* Latitude values within ``[-90, 90]`` degrees.
* Longitude values within ``[-180, 360]`` degrees.
* Coordinate bounds (``lat_bnds``, ``lon_bnds``) present when required.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import xarray as xr

from ..base import QCCheck, register_check


def _find_coord(ds: xr.Dataset, candidates: list[str]) -> str | None:
    """Return the first name from *candidates* that appears in *ds*."""
    for name in candidates:
        if name in ds.coords or name in ds:
            return name
    return None


class LatRangeCheck(QCCheck):
    """Latitude values must be within ``[-90, 90]`` degrees."""

    name = "spatial.lat_range"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        lat_name = _find_coord(ds, ["lat", "latitude", "j"])
        if lat_name is None:
            return self._skip("No latitude coordinate found")

        lat = ds[lat_name].values.astype("float64")
        lat_min = float(np.nanmin(lat))
        lat_max = float(np.nanmax(lat))

        issues = []
        if lat_min < -90.0:
            issues.append(f"min={lat_min:.4f} < -90")
        if lat_max > 90.0:
            issues.append(f"max={lat_max:.4f} > 90")

        if issues:
            return self._fail(
                f"Latitude out of valid range [-90, 90]: {'; '.join(issues)}",
                lat_min=lat_min,
                lat_max=lat_max,
            )
        return self._pass(
            f"Latitude in [{lat_min:.2f}, {lat_max:.2f}]",
            lat_min=lat_min,
            lat_max=lat_max,
        )


class LonRangeCheck(QCCheck):
    """Longitude values must be within ``[-180, 360]`` degrees."""

    name = "spatial.lon_range"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        lon_name = _find_coord(ds, ["lon", "longitude", "i"])
        if lon_name is None:
            return self._skip("No longitude coordinate found")

        lon = ds[lon_name].values.astype("float64")
        lon_min = float(np.nanmin(lon))
        lon_max = float(np.nanmax(lon))

        if lon_min < -180.0 or lon_max > 360.0:
            return self._fail(
                f"Longitude out of valid range [-180, 360]: "
                f"min={lon_min:.4f}, max={lon_max:.4f}",
                lon_min=lon_min,
                lon_max=lon_max,
            )
        return self._pass(
            f"Longitude in [{lon_min:.2f}, {lon_max:.2f}]",
            lon_min=lon_min,
            lon_max=lon_max,
        )


class CoordinateBoundsCheck(QCCheck):
    """Coordinate bounds variables (``lat_bnds``, ``lon_bnds``) must be present.

    When a ``vocab`` object is available, only the bounds for dimensions
    listed in the CMOR table are checked.  Without ``vocab``, any lat/lon
    coordinate found in the dataset is expected to have bounds.
    """

    name = "spatial.coordinate_bounds"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        vocab = context.get("vocab")
        expected_bnds: set[str] = set()

        if vocab is not None and hasattr(vocab, "variable"):
            dims = vocab.variable.get("dimensions", "")
            if isinstance(dims, str):
                dims = dims.split()
            if "lat" in dims:
                expected_bnds.add("lat_bnds")
            if "lon" in dims:
                expected_bnds.add("lon_bnds")
        else:
            # Fallback: check for bounds whenever lat/lon coords exist
            if _find_coord(ds, ["lat", "latitude"]):
                expected_bnds.add("lat_bnds")
            if _find_coord(ds, ["lon", "longitude"]):
                expected_bnds.add("lon_bnds")

        if not expected_bnds:
            return self._skip("No lat/lon coordinates to check bounds for")

        missing_bnds = sorted(b for b in expected_bnds if b not in ds)
        if missing_bnds:
            return self._warn(
                f"Missing coordinate bounds variable(s): {missing_bnds}",
                missing_bounds=missing_bnds,
            )
        return self._pass(
            f"Coordinate bounds present: {sorted(expected_bnds)}"
        )


register_check(LatRangeCheck())
register_check(LonRangeCheck())
register_check(CoordinateBoundsCheck())
