"""
access_moppy.qc.checks.global_stats
=====================================

Science QC checks derived from the ESM1.6 pre-publication QC document.

Three checks are provided, all issuing WARN (not FAIL) because the goal is
to flag unexpected values for human review:

* :class:`GlobalMinMaxCheck` — actual min/max against per-variable physical thresholds.
* :class:`NonNegativeCheck`  — physically non-negative variables must not be < 0.
* :class:`GlobalMeanRangeCheck` — global spatial mean within climatological bounds.

To add thresholds for a new variable, extend ``_VARIABLE_THRESHOLDS`` with its
CMOR name as the key.  All values use CMIP6 standard units for that variable.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import xarray as xr

from ..base import QCCheck, register_check

# ---------------------------------------------------------------------------
# Per-variable thresholds
# ---------------------------------------------------------------------------
# Derived from:
#   - ESM1.6 pre-publication QC document (ts, pr observed ranges)
#   - General climatological knowledge
#
# Structure: {cmor_name: {actual_min, actual_max, mean_min, mean_max}}
# All values in CMIP6 standard units.

_VARIABLE_THRESHOLDS: dict[str, dict[str, float]] = {
    # ---- Temperature [K] ----
    "ts": {
        "actual_min": 180.0,  # doc shows ~194 K for ESM runs; allow margin
        "actual_max": 340.0,  # doc shows ~320 K; allow for extreme SST scenarios
        "mean_min": 270.0,
        "mean_max": 295.0,
    },
    "tas": {
        "actual_min": 180.0,
        "actual_max": 340.0,
        "mean_min": 270.0,
        "mean_max": 295.0,
    },
    "ta": {"actual_min": 140.0, "actual_max": 340.0},
    "tos": {
        "actual_min": 271.2,
        "actual_max": 313.0,
        "mean_min": 275.0,
        "mean_max": 293.0,
    },
    "thetao": {"actual_min": 271.0, "actual_max": 308.0},
    # ---- Precipitation [kg m-2 s-1] ----
    # doc: pr min=-0.0000, max=0.0013; allow small negative for floating-point noise
    "pr": {
        "actual_min": -1e-12,
        "actual_max": 2.0e-3,
        "mean_min": 1.5e-5,
        "mean_max": 5.0e-5,
    },
    "prsn": {"actual_min": -1e-12, "actual_max": 1.0e-3},
    "evspsbl": {"actual_min": -5.0e-5, "actual_max": 5.0e-4},
    # ---- Pressure [Pa] ----
    "psl": {"actual_min": 87_000.0, "actual_max": 108_000.0},
    "ps": {"actual_min": 47_000.0, "actual_max": 108_000.0},
    # ---- Radiation [W m-2] ----
    "rsdt": {"actual_min": 0.0, "actual_max": 620.0},
    "rsut": {"actual_min": 0.0, "actual_max": 620.0},
    "rlut": {"actual_min": 60.0, "actual_max": 400.0},
    "rsds": {"actual_min": 0.0, "actual_max": 600.0},
    "rsus": {"actual_min": 0.0, "actual_max": 400.0},
    "rlds": {"actual_min": 50.0, "actual_max": 560.0},
    "rlus": {"actual_min": 100.0, "actual_max": 700.0},
    # TOA net: N = rsdt - rsut - rlut; should be close to 0 for piControl
    # No per-element threshold here — handled by energy_balance module.
    # ---- Turbulent heat fluxes [W m-2] ----
    "hfss": {"actual_min": -300.0, "actual_max": 700.0},
    "hfls": {"actual_min": -100.0, "actual_max": 700.0},
    # ---- Wind [m s-1] ----
    "ua": {"actual_min": -250.0, "actual_max": 250.0},
    "va": {"actual_min": -120.0, "actual_max": 120.0},
    "uas": {"actual_min": -80.0, "actual_max": 80.0},
    "vas": {"actual_min": -80.0, "actual_max": 80.0},
    # ---- Humidity ----
    "hus": {"actual_min": 0.0, "actual_max": 0.06},    # kg/kg
    "hur": {"actual_min": 0.0, "actual_max": 110.0},   # %
    # ---- Ocean salinity [psu / 1e-3] ----
    "so": {"actual_min": 0.0, "actual_max": 45.0},
    # ---- Sea ice ----
    "siconc": {"actual_min": 0.0, "actual_max": 1.0},
    "sithick": {"actual_min": 0.0, "actual_max": 30.0},
    # ---- Sea level [m] ----
    "zos": {"actual_min": -10.0, "actual_max": 10.0},
    # ---- Carbon (land) ----
    # doc: GPP/NPP >= 0; pools cannot exceed 1e6 gC/m² per tile ≈ 10 kg m-2
    "gpp": {"actual_min": 0.0, "actual_max": 1.0e-4},   # kg m-2 s-1
    "npp": {"actual_min": -1.0e-6, "actual_max": 1.0e-4},
}

# Variables that are physically constrained to be >= 0.
# Small negative values from floating-point arithmetic are tolerated up to
# the noise floor defined in NonNegativeCheck._tolerance.
_NON_NEGATIVE_VARS: frozenset[str] = frozenset(
    {
        "pr",
        "prsn",
        "rsdt",
        "rsut",
        "rsds",
        "rsus",
        "siconc",
        "sithick",
        "gpp",
        "clt",
        "areacella",
        "areacello",
        "volcello",
    }
)


def _valid_data(da: xr.DataArray) -> xr.DataArray:
    """Return *da* with fill values masked to NaN."""
    fill = da.attrs.get("_FillValue", da.attrs.get("missing_value", None))
    if fill is not None:
        try:
            fill_f = float(fill)
            if fill_f != 0.0:
                da = da.where(np.abs(da.values) < 0.99 * abs(fill_f))
        except (TypeError, ValueError):
            pass
    return da


class GlobalMinMaxCheck(QCCheck):
    """Actual global min/max must lie within known per-variable physical bounds.

    Thresholds in ``_VARIABLE_THRESHOLDS`` were derived from the ESM1.6
    pre-publication QC document and general climatological knowledge.  A WARN
    (not FAIL) is issued so that the check acts as a flag for human review
    rather than a hard blocker.

    Only variables listed in ``_VARIABLE_THRESHOLDS`` are checked; all others
    receive a SKIP result.
    """

    name = "global_stats.min_max_range"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")

        thresholds = _VARIABLE_THRESHOLDS.get(cmor_name)
        if thresholds is None:
            return self._skip(f"No known thresholds for '{cmor_name}'")

        valid = _valid_data(ds[cmor_name])
        actual_min = float(valid.min().values)
        actual_max = float(valid.max().values)

        if np.isnan(actual_min):
            return self._skip(f"'{cmor_name}' has no valid (non-fill) values")

        issues = []
        details: dict[str, Any] = {
            "actual_min": actual_min,
            "actual_max": actual_max,
        }

        exp_min = thresholds.get("actual_min")
        exp_max = thresholds.get("actual_max")

        if exp_min is not None:
            details["expected_min"] = exp_min
            if actual_min < exp_min:
                issues.append(f"min={actual_min:.6g} < expected_min={exp_min:.6g}")

        if exp_max is not None:
            details["expected_max"] = exp_max
            if actual_max > exp_max:
                issues.append(f"max={actual_max:.6g} > expected_max={exp_max:.6g}")

        if issues:
            return self._warn(
                f"'{cmor_name}' global min/max outside expected range: "
                + "; ".join(issues),
                **details,
            )
        return self._pass(
            f"'{cmor_name}' min={actual_min:.6g}, max={actual_max:.6g} "
            "within expected range",
            **details,
        )


class NonNegativeCheck(QCCheck):
    """Variables that are physically non-negative must not contain negative values.

    Covers precipitation, radiation, sea-ice variables, GPP, and cell-area
    variables.  A tiny tolerance (``_tolerance``) is applied to allow for
    floating-point rounding noise.
    """

    name = "global_stats.non_negative"
    _tolerance: float = -1.0e-10

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in _NON_NEGATIVE_VARS:
            return self._skip(
                f"'{cmor_name}' is not in the non-negative-constrained variable set"
            )
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")

        valid = _valid_data(ds[cmor_name])
        actual_min = float(valid.min().values)

        if np.isnan(actual_min):
            return self._skip(f"'{cmor_name}' has no valid (non-fill) values")

        if actual_min < self._tolerance:
            return self._warn(
                f"'{cmor_name}' must be non-negative but has min={actual_min:.6g}",
                actual_min=actual_min,
                tolerance=self._tolerance,
            )
        return self._pass(
            f"'{cmor_name}' is non-negative (min={actual_min:.6g})"
        )


class GlobalMeanRangeCheck(QCCheck):
    """Global spatial mean must be within expected climatological bounds.

    A broad sanity check: if the global mean of e.g. surface temperature is
    outside a reasonable range, the variable has likely been mis-assigned or
    has a unit error.

    Only variables with ``mean_min`` / ``mean_max`` entries in
    ``_VARIABLE_THRESHOLDS`` are checked.
    """

    name = "global_stats.global_mean_range"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")

        thresholds = _VARIABLE_THRESHOLDS.get(cmor_name, {})
        mean_min = thresholds.get("mean_min")
        mean_max = thresholds.get("mean_max")

        if mean_min is None and mean_max is None:
            return self._skip(f"No global mean thresholds for '{cmor_name}'")

        valid = _valid_data(ds[cmor_name])
        global_mean = float(valid.mean().values)

        if np.isnan(global_mean):
            return self._skip(f"'{cmor_name}' has no valid (non-fill) values")

        issues = []
        details: dict[str, Any] = {"global_mean": global_mean}

        if mean_min is not None:
            details["expected_mean_min"] = mean_min
            if global_mean < mean_min:
                issues.append(
                    f"mean={global_mean:.6g} < expected_mean_min={mean_min:.6g}"
                )

        if mean_max is not None:
            details["expected_mean_max"] = mean_max
            if global_mean > mean_max:
                issues.append(
                    f"mean={global_mean:.6g} > expected_mean_max={mean_max:.6g}"
                )

        if issues:
            return self._warn(
                f"'{cmor_name}' global mean outside expected range: "
                + "; ".join(issues),
                **details,
            )
        return self._pass(
            f"'{cmor_name}' global mean={global_mean:.6g} within expected range",
            **details,
        )


register_check(GlobalMinMaxCheck())
register_check(NonNegativeCheck())
register_check(GlobalMeanRangeCheck())
