"""
access_moppy.qc.checks.data_integrity
======================================

Data-integrity checks derived from the ESM1.6 pre-publication QC document
and the APP4 quality_check reference:

* Values within ``valid_min`` / ``valid_max`` from the CMOR table.
* Fraction of missing / fill values must not be excessive.
* ``_FillValue`` and ``missing_value`` must be consistent with each other.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import xarray as xr

from ..base import QCCheck, register_check


def _mask_fill(da: xr.DataArray) -> xr.DataArray:
    """Return *da* with fill/missing values replaced by NaN.

    Reads ``_FillValue`` and ``missing_value`` from attrs.  Values within
    1 % of the fill magnitude are treated as fill.
    """
    fill = da.attrs.get("_FillValue", da.attrs.get("missing_value", None))
    if fill is not None:
        try:
            fill_f = float(fill)
            # Avoid masking 0 when fill is 0 (unusual but possible)
            if fill_f != 0.0:
                da = da.where(np.abs(da.values) < 0.99 * abs(fill_f))
            else:
                da = da.where(da.values != 0.0)
        except (TypeError, ValueError):
            pass
    return da


class ValidRangeCheck(QCCheck):
    """Values must lie within ``valid_min`` / ``valid_max`` from the CMOR table.

    Issues a WARN (not FAIL) because the QC document goal is to flag
    unexpected values for human review rather than to block output.
    Requires ``vocab`` and ``cmor_name`` in *context*.
    """

    name = "data_integrity.valid_range"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        vocab = context.get("vocab")

        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")
        if vocab is None:
            return self._skip("No vocab in context — cannot check valid range")

        variable = vocab.variable
        vmin_raw = variable.get("valid_min")
        vmax_raw = variable.get("valid_max")

        if vmin_raw is None and vmax_raw is None:
            return self._skip("No valid_min / valid_max in CMOR table")

        valid = _mask_fill(ds[cmor_name])
        actual_min = float(valid.min().values)
        actual_max = float(valid.max().values)

        if np.isnan(actual_min):
            return self._skip(f"'{cmor_name}' contains no valid (non-fill) values")

        issues = []
        details: dict[str, Any] = {
            "actual_min": actual_min,
            "actual_max": actual_max,
        }

        if vmin_raw is not None:
            vmin = float(vmin_raw)
            details["valid_min"] = vmin
            if actual_min < vmin:
                issues.append(f"min={actual_min:.6g} < valid_min={vmin:.6g}")

        if vmax_raw is not None:
            vmax = float(vmax_raw)
            details["valid_max"] = vmax
            if actual_max > vmax:
                issues.append(f"max={actual_max:.6g} > valid_max={vmax:.6g}")

        if issues:
            return self._warn(
                f"'{cmor_name}' outside CMOR valid range: " + "; ".join(issues),
                **details,
            )
        return self._pass(
            f"'{cmor_name}' within valid range (min={actual_min:.6g}, max={actual_max:.6g})",
            **details,
        )


class MissingValueFractionCheck(QCCheck):
    """Fraction of missing / fill values must not be excessive.

    Thresholds:

    * > 95 % missing → FAIL
    * > 50 % missing → WARN
    """

    name = "data_integrity.missing_fraction"
    _fail_threshold: float = 0.95
    _warn_threshold: float = 0.50

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")

        da = ds[cmor_name]
        total = int(da.size)
        if total == 0:
            return self._skip("Variable has no data")

        values = da.values
        # Count NaNs
        nan_count = int(np.isnan(values).sum())

        # Count explicit fill values
        fill = da.attrs.get("_FillValue", da.attrs.get("missing_value", None))
        fill_count = 0
        if fill is not None:
            try:
                fill_f = float(fill)
                if fill_f != 0.0:
                    fill_count = int((np.abs(values) >= 0.99 * abs(fill_f)).sum())
            except (TypeError, ValueError):
                pass

        missing = max(nan_count, fill_count)
        fraction = missing / total

        if fraction > self._fail_threshold:
            return self._fail(
                f"'{cmor_name}' has {fraction:.1%} missing/fill values "
                f"(threshold: {self._fail_threshold:.0%})",
                missing_fraction=round(fraction, 4),
                fail_threshold=self._fail_threshold,
            )
        if fraction > self._warn_threshold:
            return self._warn(
                f"'{cmor_name}' has {fraction:.1%} missing/fill values",
                missing_fraction=round(fraction, 4),
                warn_threshold=self._warn_threshold,
            )
        return self._pass(
            f"'{cmor_name}' missing fraction: {fraction:.2%}",
            missing_fraction=round(fraction, 4),
        )


class FillValueConsistencyCheck(QCCheck):
    """``_FillValue`` and ``missing_value`` attributes must be equal.

    CMIP6 requires both to be present and set to the same value (typically
    ``1e20``).
    """

    name = "data_integrity.fill_value_consistency"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")

        da = ds[cmor_name]
        fv = da.attrs.get("_FillValue")
        mv = da.attrs.get("missing_value")

        if fv is None and mv is None:
            return self._warn(
                f"'{cmor_name}' has neither _FillValue nor missing_value attribute"
            )
        if fv is None:
            return self._warn(f"'{cmor_name}' is missing _FillValue attribute")
        if mv is None:
            return self._warn(f"'{cmor_name}' is missing missing_value attribute")

        try:
            if float(fv) != float(mv):
                return self._fail(
                    f"_FillValue={fv} != missing_value={mv}",
                    fill_value=float(fv),
                    missing_value=float(mv),
                )
        except (TypeError, ValueError):
            return self._warn(
                f"Cannot compare _FillValue='{fv}' and missing_value='{mv}'"
            )

        return self._pass(f"_FillValue == missing_value == {fv}")


register_check(ValidRangeCheck())
register_check(MissingValueFractionCheck())
register_check(FillValueConsistencyCheck())
