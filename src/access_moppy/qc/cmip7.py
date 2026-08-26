from __future__ import annotations

import argparse
import json
import warnings
from dataclasses import dataclass
from fnmatch import fnmatchcase
from functools import lru_cache
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

import dask
import numpy as np
import xarray as xr
import yaml


@dataclass(frozen=True)
class RangeRule:
    variable_id: str
    experiment_id: str
    units: str | None
    minimum: float
    maximum: float
    rule_name: str


@dataclass
class ValidationResult:
    """Result of a single file validation."""

    file_path: str
    passed: bool
    variable_id: str | None = None
    experiment_id: str | None = None
    error: str | None = None
    warning: str | None = None
    observed_min: float | None = None
    observed_max: float | None = None
    allowed_min: float | None = None
    allowed_max: float | None = None
    units: str | None = None


@dataclass(frozen=True)
class DataSummary:
    """Scalar QC statistics computed together from a potentially lazy array."""

    non_missing: int
    minimum: float | None = None
    maximum: float | None = None


def _compute_data_summary(da: xr.DataArray) -> DataSummary:
    """Compute the QC reductions in one Dask graph execution."""

    reductions: list[xr.DataArray] = [da.count()]
    is_numeric = np.issubdtype(da.dtype, np.number)
    if is_numeric:
        reductions.extend([da.min(skipna=True), da.max(skipna=True)])

    computed = dask.compute(*reductions)
    non_missing = int(computed[0].item())
    if not is_numeric or non_missing == 0:
        return DataSummary(non_missing=non_missing)

    return DataSummary(
        non_missing=non_missing,
        minimum=float(computed[1].item()),
        maximum=float(computed[2].item()),
    )


def _iter_missing_sentinels(da: xr.DataArray) -> list[float]:
    """Collect numeric missing-value sentinels from attrs/encoding."""

    sentinels: list[float] = []
    for container in (da.attrs, da.encoding):
        for key in ("missing_value", "_FillValue"):
            value = container.get(key)
            if value is None:
                continue
            values = np.asarray(value).ravel()
            for item in values:
                try:
                    candidate = float(item)
                except (TypeError, ValueError):
                    continue
                if np.isfinite(candidate):
                    sentinels.append(candidate)
    return sentinels


def _mask_missing_sentinels_for_qc(da: xr.DataArray) -> xr.DataArray:
    """Mask numeric missing-value sentinels prior to QC reductions.

    Some files encode missing values as float32 and write to float64 arrays,
    so values can appear as 1.0000000200408773e+20 while metadata carries 1e20.
    Use tolerance-aware matching so these sentinels are not interpreted as data.
    """

    sentinels = _iter_missing_sentinels(da)
    if not sentinels:
        return da

    mask = xr.zeros_like(da, dtype=bool)
    for sentinel in sentinels:
        atol = np.finfo(np.float32).eps * max(abs(sentinel), 1.0)
        mask = mask | xr.apply_ufunc(
            np.isclose,
            da,
            sentinel,
            kwargs={"rtol": 0.0, "atol": atol, "equal_nan": False},
            dask="allowed",
        )

    return da.where(~mask)


def _is_outside_allowed_range(observed: float, minimum: float, maximum: float) -> bool:
    """Check whether an observed value is meaningfully outside a closed range.

    Small floating-point noise at the boundary is ignored so values like
    ``-6e-24`` are treated as zero for a lower bound of ``0``.
    """

    # Many CMIP outputs are derived from float32 fields where boundary values
    # (for example 100%) can round to slightly above/below the nominal limit.
    # Use a scale-aware absolute tolerance so closed-interval checks remain
    # robust without masking meaningfully out-of-range values.
    scale = max(abs(observed), abs(minimum), abs(maximum), 1.0)
    tolerance = max(1e-12, 8.0 * np.finfo(np.float32).eps * scale)
    lower_violation = observed < minimum and not np.isclose(
        observed, minimum, rtol=0.0, atol=tolerance
    )
    upper_violation = observed > maximum and not np.isclose(
        observed, maximum, rtol=0.0, atol=tolerance
    )
    return bool(lower_violation or upper_violation)


def _units_match(actual_units: Any, expected_units: Any) -> bool:
    """Return True when units are equivalent for QC purposes.

    Supports exact string matches and numeric-string equivalents such as
    ``"0.001"`` and ``"1E-03"``.
    """

    if actual_units == expected_units:
        return True

    if isinstance(actual_units, str) and isinstance(expected_units, str):
        if actual_units.strip() == expected_units.strip():
            return True
        try:
            actual_numeric = float(actual_units)
            expected_numeric = float(expected_units)
        except ValueError:
            return False
        if not (np.isfinite(actual_numeric) and np.isfinite(expected_numeric)):
            return False
        return bool(np.isclose(actual_numeric, expected_numeric, rtol=0.0, atol=1e-12))

    return False


@lru_cache(maxsize=1)
def _load_esm16_mapping_variables() -> dict[str, dict[str, Any]]:
    """Flatten ACCESS-ESM1-6 mapping entries keyed by CMIP variable id."""

    resource = files("access_moppy") / "mappings" / "ACCESS-ESM1-6_mappings.json"
    with as_file(resource) as path:
        with open(path, "r", encoding="utf-8") as handle:
            mapping = json.load(handle)

    flattened: dict[str, dict[str, Any]] = {}
    for section_name, section in mapping.items():
        if section_name == "model_info" or not isinstance(section, dict):
            continue
        for variable_id, metadata in section.items():
            if isinstance(metadata, dict):
                flattened[variable_id] = metadata
    return flattened


@lru_cache(maxsize=1)
def _load_qc_config() -> dict[str, Any]:
    resource = files("access_moppy.resources.qc") / "cmip7_ranges.yml"
    with as_file(resource) as path:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}


@lru_cache(maxsize=1)
def _load_rules() -> dict[str, Any]:
    return _load_qc_config().get("variables", {})


@lru_cache(maxsize=1)
def _load_unit_envelopes() -> dict[str, dict[str, Any]]:
    return _load_qc_config().get("unit_envelopes", {})


@lru_cache(maxsize=1)
def _load_mapping_variable_ranges() -> dict[str, dict[str, Any]]:
    """Optional per-variable overrides for mapping-derived physical ranges."""

    return _load_qc_config().get("mapping_variable_ranges", {})


def _select_experiment_rule(
    experiments: dict[str, dict[str, Any]], experiment_id: str
) -> tuple[str, dict[str, Any]] | None:
    if experiment_id in experiments:
        return experiment_id, experiments[experiment_id]

    matches: list[tuple[str, dict[str, Any]]] = [
        (pattern, rule)
        for pattern, rule in experiments.items()
        if fnmatchcase(experiment_id, pattern)
    ]
    if not matches:
        return None

    matches.sort(key=lambda item: (-len(item[0]), item[0]))
    return matches[0]


def _resolve_range_rule(variable_id: str, experiment_id: str) -> RangeRule | None:
    variables = _load_rules()
    variable_rules = variables.get(variable_id)
    if not variable_rules:
        return None

    base_rule = dict(variable_rules.get("default", {}))
    experiment_rules = variable_rules.get("experiments", {})
    selected = _select_experiment_rule(experiment_rules, experiment_id)
    if selected is not None:
        rule_name, experiment_rule = selected
        base_rule.update(experiment_rule)
    else:
        rule_name = "default"

    if "min" not in base_rule or "max" not in base_rule:
        return None

    return RangeRule(
        variable_id=variable_id,
        experiment_id=experiment_id,
        units=base_rule.get("units", variable_rules.get("units")),
        minimum=float(base_rule["min"]),
        maximum=float(base_rule["max"]),
        rule_name=rule_name,
    )


def _resolve_range_rule_from_mapping_definition(
    variable_id: str,
    experiment_id: str,
    mapping_entry: dict[str, Any],
) -> RangeRule | None:
    override = _load_mapping_variable_ranges().get(variable_id, {})

    units = mapping_entry.get("units")
    if not isinstance(units, str) or not units:
        return None

    if isinstance(override.get("units"), str) and override.get("units"):
        units = override["units"]

    envelope = _load_unit_envelopes().get(units)
    if "min" in override and "max" in override:
        minimum = float(override["min"])
        maximum = float(override["max"])
    else:
        if not envelope or "min" not in envelope or "max" not in envelope:
            return None
        minimum = float(envelope["min"])
        maximum = float(envelope["max"])

    return RangeRule(
        variable_id=variable_id,
        experiment_id=experiment_id,
        units=units,
        minimum=minimum,
        maximum=maximum,
        rule_name=f"mapping-variable:{variable_id}",
    )


def _is_auxiliary_data_variable(name: Any) -> bool:
    """Return True for non-primary data variables written alongside outputs."""

    text = str(name)
    return text.endswith("_bnds") or text.startswith("vertices_")


def _select_output_variable(ds: xr.Dataset, attrs: dict[str, Any]) -> str:
    candidate_names = [
        attrs.get("branded_variable"),
        attrs.get("variable_id"),
    ]
    for candidate in candidate_names:
        if isinstance(candidate, str) and candidate in ds.data_vars:
            return candidate

    primary_candidates = [v for v in ds.data_vars if not _is_auxiliary_data_variable(v)]
    if len(primary_candidates) == 1:
        return primary_candidates[0]

    available = ", ".join(sorted(ds.data_vars))
    raise ValueError(
        "CMIP7 QC could not determine the main output variable from the CMORised file. "
        f"Available data variables: {available}"
    )


def _validate_esm16_mapping_checks(
    da: xr.DataArray,
    *,
    variable_id: str,
    experiment_id: str,
    mapping_entry: dict[str, Any],
    summary: DataSummary | None = None,
) -> None:
    """Validate generic checks for ACCESS mapped variables."""

    summary = summary or _compute_data_summary(da)
    if summary.non_missing == 0:
        raise ValueError(
            "CMIP7 QC failed for "
            f"{variable_id} in experiment {experiment_id}: all values are missing."
        )

    if summary.minimum is not None and summary.maximum is not None:
        if np.isinf(summary.minimum) or np.isinf(summary.maximum):
            raise ValueError(
                "CMIP7 QC failed for "
                f"{variable_id} in experiment {experiment_id}: values contain infinity."
            )

    expected_units = mapping_entry.get("units")
    if isinstance(expected_units, str) and expected_units:
        actual_units = da.attrs.get("units")
        if not _units_match(actual_units, expected_units):
            raise ValueError(
                "CMIP7 QC failed for "
                f"{variable_id} in experiment {experiment_id}: expected units {expected_units!r} "
                f"from ACCESS-ESM1-6 mapping, found {actual_units!r}."
            )


def _validate_cmip7_output(output_path: str | Path) -> ValidationResult:
    """Validate a CMIP7 file against its physical-range rules.

    The single implementation behind both public entry points: it neither
    raises for a validation finding nor swallows unexpected errors, so the
    caller decides whether a finding aborts the variable
    (:func:`validate_cmip7_output`) or is only reported
    (:func:`validate_cmip7_output_detailed`).
    """

    path = Path(output_path)
    with xr.open_dataset(path, chunks="auto") as ds:
        attrs = dict(ds.attrs)
        variable_id = attrs.get("variable_id")
        experiment_id = attrs.get("experiment_id")
        source_id = attrs.get("source_id")

        if not isinstance(variable_id, str) or not variable_id:
            return ValidationResult(
                file_path=str(path),
                passed=False,
                error=(
                    "CMIP7 QC requires a 'variable_id' global attribute on the "
                    "output file."
                ),
            )
        if not isinstance(experiment_id, str) or not experiment_id:
            return ValidationResult(
                file_path=str(path),
                passed=False,
                variable_id=variable_id,
                error=(
                    "CMIP7 QC requires an 'experiment_id' global attribute on the "
                    "output file."
                ),
            )

        rule = _resolve_range_rule(variable_id, experiment_id)

        output_variable = _select_output_variable(ds, attrs)
        da = _mask_missing_sentinels_for_qc(ds[output_variable])
        mapping_entry = (
            _load_esm16_mapping_variables().get(variable_id)
            if source_id == "ACCESS-ESM1-6"
            else None
        )
        summary = (
            _compute_data_summary(da)
            if mapping_entry is not None or rule is not None
            else None
        )

        # Apply generic checks for variables present in the bundled ACCESS-ESM1-6
        # mapping so every mapped variable receives QC coverage.
        if mapping_entry is not None:
            try:
                _validate_esm16_mapping_checks(
                    da,
                    variable_id=variable_id,
                    experiment_id=experiment_id,
                    mapping_entry=mapping_entry,
                    summary=summary,
                )
            except ValueError as exc:
                return ValidationResult(
                    file_path=str(path),
                    passed=False,
                    variable_id=variable_id,
                    experiment_id=experiment_id,
                    error=str(exc),
                )
            if rule is None:
                rule = _resolve_range_rule_from_mapping_definition(
                    variable_id,
                    experiment_id,
                    mapping_entry,
                )

        if rule is None:
            return ValidationResult(
                file_path=str(path),
                passed=True,
                variable_id=variable_id,
                experiment_id=experiment_id,
            )

        units = da.attrs.get("units") or attrs.get("units")
        if rule.units is not None and not _units_match(units, rule.units):
            return ValidationResult(
                file_path=str(path),
                passed=False,
                variable_id=variable_id,
                experiment_id=experiment_id,
                units=units,
                error=(
                    "CMIP7 QC failed for "
                    f"{variable_id} in experiment {experiment_id}: expected units "
                    f"{rule.units!r}, found {units!r}."
                ),
            )

        if summary is None or summary.minimum is None or summary.maximum is None:
            return ValidationResult(
                file_path=str(path),
                passed=True,
                variable_id=variable_id,
                experiment_id=experiment_id,
                units=units,
            )

        observed_min = summary.minimum
        observed_max = summary.maximum

        warning = None
        if _is_outside_allowed_range(
            observed_min, rule.minimum, rule.maximum
        ) or _is_outside_allowed_range(observed_max, rule.minimum, rule.maximum):
            warning = (
                "CMIP7 QC range warning for "
                f"{variable_id} in experiment {experiment_id} using rule {rule.rule_name}: "
                f"observed range {observed_min:.3f}..{observed_max:.3f} {units or ''} "
                f"is outside allowed range {rule.minimum:.3f}..{rule.maximum:.3f} "
                f"{rule.units or units or ''}."
            )

        return ValidationResult(
            file_path=str(path),
            passed=True,
            variable_id=variable_id,
            experiment_id=experiment_id,
            units=units,
            observed_min=observed_min,
            observed_max=observed_max,
            allowed_min=rule.minimum,
            allowed_max=rule.maximum,
            warning=warning,
        )


def validate_cmip7_output(output_path: str | Path) -> ValidationResult:
    """Validate a CMIP7 CMORised file against output-time physical range rules.

    A failing check raises, so a batch worker stops the variable rather than
    publishing output that is known to be wrong. A value outside its allowed
    physical range is a warning, not a failure: the range rules are broad
    plausibility bounds, not hard limits.

    Returns:
        The :class:`ValidationResult`, so a caller that wants to record the
        outcome does not have to re-read the file to get it.

    Raises:
        ValueError: The file failed validation.
    """

    result = _validate_cmip7_output(output_path)
    if not result.passed:
        raise ValueError(result.error)
    if result.warning:
        warnings.warn(result.warning, stacklevel=2)
    return result


def validate_cmip7_output_detailed(output_path: str | Path) -> ValidationResult:
    """Validate a CMIP7 file and return detailed results (does not raise)."""

    try:
        return _validate_cmip7_output(output_path)
    except Exception as exc:  # noqa: BLE001
        return ValidationResult(
            file_path=str(Path(output_path)),
            passed=False,
            error=f"Unexpected error: {exc}",
        )


#: Location of the physical-range rules, as an import path for display.
RANGES_RESOURCE = "access_moppy/resources/qc/cmip7_ranges.yml"


def export_range_rules(
    variables: list[str] | None = None,
    experiment_id: str | None = None,
) -> dict[str, Any]:
    """Return the physical-range rules as a JSON-serialisable dictionary.

    This is the machine-readable view of
    ``access_moppy/resources/qc/cmip7_ranges.yml`` — the rules
    :func:`validate_cmip7_output` applies to a CMORised file.

    Args:
        variables: Restrict the export to these CMIP variable ids.  ``None``
            exports every variable carrying a rule.
        experiment_id: When given, each entry also carries a ``resolved`` block
            holding the bounds that apply to that experiment, after any
            experiment-specific override has been merged over the default.

    Returns:
        ``{"source": ..., "variable_count": ..., "variables": {...}}``, with an
        ``experiment_id`` key when one was requested.
    """

    rules = _load_rules()
    selected = sorted(rules) if variables is None else list(dict.fromkeys(variables))

    exported: dict[str, Any] = {}
    for variable_id in selected:
        variable_rules = rules.get(variable_id)
        if not variable_rules:
            continue

        entry: dict[str, Any] = json.loads(json.dumps(variable_rules))
        if experiment_id is not None:
            resolved = _resolve_range_rule(variable_id, experiment_id)
            if resolved is not None:
                entry["resolved"] = {
                    "units": resolved.units,
                    "min": resolved.minimum,
                    "max": resolved.maximum,
                    "rule": resolved.rule_name,
                }
        exported[variable_id] = entry

    payload: dict[str, Any] = {
        "source": RANGES_RESOURCE,
        "variable_count": len(exported),
    }
    if experiment_id is not None:
        payload["experiment_id"] = experiment_id
    payload["variables"] = exported
    return payload


def format_range_rules_table(payload: dict[str, Any]) -> str:
    """Render :func:`export_range_rules` output as a fixed-width table."""

    experiment_id = payload.get("experiment_id")
    rows: list[tuple[str, str, str, str, str]] = []
    for variable_id, entry in payload.get("variables", {}).items():
        resolved = entry.get("resolved")
        if resolved is None:
            resolved = dict(entry.get("default", {}))
            resolved.setdefault("units", entry.get("units"))
            resolved["rule"] = "default"
        if "min" not in resolved or "max" not in resolved:
            continue
        rows.append(
            (
                variable_id,
                str(resolved.get("units") or entry.get("units") or ""),
                f"{float(resolved['min']):g}",
                f"{float(resolved['max']):g}",
                str(resolved.get("rule", "default")),
            )
        )

    headers = ("variable", "units", "min", "max", "rule")
    widths = [
        max(len(headers[column]), *(len(row[column]) for row in rows))
        if rows
        else len(headers[column])
        for column in range(len(headers))
    ]

    def _line(values: tuple[str, ...]) -> str:
        return "  ".join(
            value.ljust(widths[column]) for column, value in enumerate(values)
        ).rstrip()

    lines = [_line(headers), _line(tuple("-" * width for width in widths))]
    lines.extend(_line(row) for row in rows)
    lines.append("")
    lines.append(
        f"{len(rows)} variable(s) from {payload.get('source', RANGES_RESOURCE)}"
    )
    if experiment_id:
        lines.append(f"resolved for experiment_id={experiment_id}")
    return "\n".join(lines)


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-qc",
        description=(
            "Run ACCESS-MOPPy CMIP7 output QC checks against one or more CMORised netCDF files."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="One or more CMORised output files to validate.",
    )
    parser.add_argument(
        "--show-ranges",
        action="store_true",
        help=(
            "Print the physical-range rules QC applies, instead of validating "
            f"files.  Rules are read from {RANGES_RESOURCE}."
        ),
    )
    parser.add_argument(
        "--variable",
        action="append",
        dest="variables",
        metavar="VARIABLE_ID",
        help=(
            "With --show-ranges, restrict the output to this CMIP variable id. "
            "Repeatable."
        ),
    )
    parser.add_argument(
        "--experiment",
        dest="experiment_id",
        metavar="EXPERIMENT_ID",
        help=(
            "With --show-ranges, resolve each rule for this experiment_id so "
            "experiment-specific overrides are applied."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="With --show-ranges, output format (default: table).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli_parser()
    args = parser.parse_args(argv)

    if args.show_ranges:
        payload = export_range_rules(
            variables=args.variables,
            experiment_id=args.experiment_id,
        )
        if args.format == "json":
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(format_range_rules_table(payload))
        return 0

    if not args.paths:
        parser.error("at least one file is required unless --show-ranges is given")

    failures: list[tuple[str, str]] = []
    for raw_path in args.paths:
        path = Path(raw_path)
        result = validate_cmip7_output_detailed(path)
        if result.warning:
            print(f"WARN {path}: {result.warning}")
        elif result.passed:
            print(f"PASS {path}")
        else:
            error = result.error or "Unknown validation error"
            failures.append((str(path), error))
            print(f"FAIL {path}: {error}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
