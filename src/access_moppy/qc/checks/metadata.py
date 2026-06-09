"""
access_moppy.qc.checks.metadata
================================

Technical checks derived from the ESM1.6 pre-publication QC document:

* Required CMIP6 global attributes are present.
* ``variable_id`` attribute matches the expected CMOR variable name.
* ``units`` on the data variable match the CMOR table specification.
* ``cell_methods`` on the data variable match the CMOR table specification.
"""
from __future__ import annotations

from typing import Any

import xarray as xr

from ..base import QCCheck, register_check

# Minimal fallback attribute list used when no vocabulary object is available.
_CMIP6_REQUIRED_ATTRS = [
    "Conventions",
    "activity_id",
    "creation_date",
    "experiment_id",
    "frequency",
    "grid_label",
    "institution_id",
    "mip_era",
    "realm",
    "source_id",
    "table_id",
    "variable_id",
    "variant_label",
]


class RequiredGlobalAttributesCheck(QCCheck):
    """All required CMIP6 global attributes must be present in ``ds.attrs``.

    When a ``vocab`` object is available in *context* its
    :meth:`get_required_attribute_names` list is used; otherwise a minimal
    built-in fallback list is used.
    """

    name = "metadata.required_global_attributes"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        from ..base import QCResult  # local import to avoid circular at module level
        vocab = context.get("vocab")
        if vocab is not None and hasattr(vocab, "get_required_attribute_names"):
            required = vocab.get_required_attribute_names()
        else:
            required = _CMIP6_REQUIRED_ATTRS

        missing = [k for k in required if k not in ds.attrs]
        if missing:
            return self._fail(
                f"Missing {len(missing)} required global attribute(s): {missing}",
                missing_attributes=missing,
            )
        return self._pass("All required global attributes present")


class VariableNameCheck(QCCheck):
    """``variable_id`` global attribute must equal the expected CMOR variable name."""

    name = "metadata.variable_name"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        if cmor_name is None:
            return self._skip("No cmor_name in context")

        attr_var_id = ds.attrs.get("variable_id")
        if attr_var_id is None:
            return self._warn("variable_id attribute is missing from dataset")

        if attr_var_id != cmor_name:
            return self._fail(
                f"variable_id='{attr_var_id}' does not match expected '{cmor_name}'",
                variable_id=attr_var_id,
                expected=cmor_name,
            )
        return self._pass(f"variable_id='{cmor_name}' matches expected name")


class UnitsCheck(QCCheck):
    """Units on the data variable must match the CMOR table specification.

    Requires ``vocab`` and ``cmor_name`` in *context*.
    """

    name = "metadata.units"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        vocab = context.get("vocab")

        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")
        if vocab is None:
            return self._skip("No vocab in context — cannot verify units")

        expected = vocab.variable.get("units")
        actual = ds[cmor_name].attrs.get("units")

        if not expected:
            return self._skip("No expected units in CMOR table")
        if actual is None:
            return self._fail(f"Variable '{cmor_name}' has no 'units' attribute")
        if actual != expected:
            return self._fail(
                f"Units mismatch: got '{actual}', expected '{expected}'",
                actual=actual,
                expected=expected,
            )
        return self._pass(f"units='{actual}' matches CMOR table")


class CellMethodsCheck(QCCheck):
    """``cell_methods`` on the data variable must match the CMOR table.

    Issued as a warning rather than a failure because minor whitespace or
    ordering variations can occur in practice.  Requires ``vocab`` and
    ``cmor_name`` in *context*.
    """

    name = "metadata.cell_methods"

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:  # type: ignore[override]
        cmor_name = context.get("cmor_name")
        vocab = context.get("vocab")

        if cmor_name is None:
            return self._skip("No cmor_name in context")
        if cmor_name not in ds:
            return self._skip(f"Variable '{cmor_name}' not found in dataset")
        if vocab is None:
            return self._skip("No vocab in context — cannot verify cell_methods")

        expected = vocab.variable.get("cell_methods")
        actual = ds[cmor_name].attrs.get("cell_methods")

        if not expected:
            return self._skip("No cell_methods in CMOR table")
        if actual is None:
            return self._warn(f"Variable '{cmor_name}' has no 'cell_methods' attribute")
        if actual != expected:
            return self._warn(
                f"cell_methods mismatch: got '{actual}', expected '{expected}'",
                actual=actual,
                expected=expected,
            )
        return self._pass("cell_methods matches CMOR table")


register_check(RequiredGlobalAttributesCheck())
register_check(VariableNameCheck())
register_check(UnitsCheck())
register_check(CellMethodsCheck())
