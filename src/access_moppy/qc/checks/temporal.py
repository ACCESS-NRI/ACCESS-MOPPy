"""
access_moppy.qc.checks.temporal
================================

Temporal-axis checks derived from the ESM1.6 pre-publication QC document:

* Time coordinate must be strictly monotonically increasing.
* No duplicate time steps.
* ``time_bnds`` must be present and internally consistent.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import xarray as xr

from ..base import QCCheck, register_check


class TimeMonotonicityCheck(QCCheck):
    """Time coordinate must be strictly monotonically increasing.

    Non-monotonic time axes cause silent data corruption in many downstream
    tools (xarray, CDO, NCO) and indicate problems in the source files or the
    CMORisation pipeline.
    """

    name = "temporal.time_monotonicity"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        if "time" not in ds.dims:
            return self._skip("No time dimension — not applicable")

        time_vals = ds["time"].values
        if len(time_vals) < 2:
            return self._skip("Time dimension has fewer than 2 steps")

        diffs = np.diff(time_vals.astype("float64"))
        non_monotonic = int((diffs <= 0).sum())

        if non_monotonic > 0:
            return self._fail(
                f"Time coordinate is not strictly monotonically increasing: "
                f"{non_monotonic} non-increasing step(s) found",
                non_monotonic_steps=non_monotonic,
                total_steps=len(time_vals),
            )
        return self._pass(
            f"Time coordinate is monotonically increasing ({len(time_vals)} steps)"
        )


class TimeDuplicatesCheck(QCCheck):
    """Time coordinate must not contain duplicate values."""

    name = "temporal.time_duplicates"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        if "time" not in ds.dims:
            return self._skip("No time dimension — not applicable")

        time_vals = ds["time"].values
        unique_count = len(np.unique(time_vals))
        total_count = len(time_vals)

        if unique_count < total_count:
            dupes = total_count - unique_count
            return self._fail(
                f"Time coordinate has {dupes} duplicate value(s)",
                duplicate_count=dupes,
                total_steps=total_count,
            )
        return self._pass(f"No duplicate time values ({total_count} steps)")


class TimeBoundsCheck(QCCheck):
    """``time_bnds`` must be present and internally consistent for time-dependent variables.

    Checks:

    1. The variable ``time_bnds`` (or ``time_bounds``) exists.
    2. Shape is ``(time, 2)``.
    3. For each step the lower bound does not exceed the upper bound.
    """

    name = "temporal.time_bounds_present"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        if "time" not in ds.dims:
            return self._skip("No time dimension — not applicable")

        bnds_var = next(
            (name for name in ("time_bnds", "time_bounds") if name in ds),
            None,
        )
        if bnds_var is None:
            return self._warn("time_bnds variable is absent")

        bnds = ds[bnds_var]

        if bnds.ndim != 2 or bnds.shape[1] != 2:
            return self._warn(
                f"'{bnds_var}' has unexpected shape {tuple(bnds.shape)}, expected (time, 2)",
                shape=list(bnds.shape),
            )

        lower = bnds[:, 0].values.astype("float64")
        upper = bnds[:, 1].values.astype("float64")
        bad = int((lower > upper).sum())
        if bad > 0:
            return self._fail(
                f"{bad} time bound(s) where lower > upper",
                bad_bounds=bad,
                total_steps=int(bnds.shape[0]),
            )
        return self._pass(
            f"time_bnds present and valid, shape={tuple(bnds.shape)}"
        )


register_check(TimeMonotonicityCheck())
register_check(TimeDuplicatesCheck())
register_check(TimeBoundsCheck())
