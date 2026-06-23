from __future__ import annotations

import argparse
from dataclasses import dataclass
from fnmatch import fnmatchcase
from functools import lru_cache
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

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


@lru_cache(maxsize=1)
def _load_rules() -> dict[str, Any]:
    resource = files("access_moppy.resources.qc") / "cmip7_ranges.yml"
    with as_file(resource) as path:
        with open(path, "r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    return payload.get("variables", {})


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


def _select_output_variable(ds: xr.Dataset, attrs: dict[str, Any]) -> str:
    candidate_names = [
        attrs.get("branded_variable"),
        attrs.get("variable_id"),
    ]
    for candidate in candidate_names:
        if isinstance(candidate, str) and candidate in ds.data_vars:
            return candidate

    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars))

    available = ", ".join(sorted(ds.data_vars))
    raise ValueError(
        "CMIP7 QC could not determine the main output variable from the CMORised file. "
        f"Available data variables: {available}"
    )


def validate_cmip7_output(output_path: str | Path) -> None:
    """Validate a CMIP7 CMORised file against output-time physical range rules."""

    path = Path(output_path)
    with xr.open_dataset(path) as ds:
        attrs = dict(ds.attrs)
        variable_id = attrs.get("variable_id")
        experiment_id = attrs.get("experiment_id")

        if not isinstance(variable_id, str) or not variable_id:
            raise ValueError(
                "CMIP7 QC requires a 'variable_id' global attribute on the output file."
            )
        if not isinstance(experiment_id, str) or not experiment_id:
            raise ValueError(
                "CMIP7 QC requires an 'experiment_id' global attribute on the output file."
            )

        rule = _resolve_range_rule(variable_id, experiment_id)
        if rule is None:
            return

        output_variable = _select_output_variable(ds, attrs)
        da = ds[output_variable]

        units = da.attrs.get("units") or attrs.get("units")
        if rule.units is not None and units != rule.units:
            raise ValueError(
                "CMIP7 QC failed for "
                f"{variable_id} in experiment {experiment_id}: expected units {rule.units!r}, "
                f"found {units!r}."
            )

        minimum = da.min(skipna=True).item()
        maximum = da.max(skipna=True).item()

        if np.isnan(minimum) or np.isnan(maximum):
            return

        observed_min = float(minimum)
        observed_max = float(maximum)

        if observed_min < rule.minimum or observed_max > rule.maximum:
            raise ValueError(
                "CMIP7 QC failed for "
                f"{variable_id} in experiment {experiment_id} using rule {rule.rule_name}: "
                f"observed range {observed_min:.3f}..{observed_max:.3f} {units or ''} "
                f"is outside allowed range {rule.minimum:.3f}..{rule.maximum:.3f} {rule.units or units or ''}."
            )


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-qc",
        description=(
            "Run ACCESS-MOPPy CMIP7 output QC checks against one or more CMORised netCDF files."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="One or more CMORised output files to validate.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli_parser()
    args = parser.parse_args(argv)

    failures: list[tuple[str, str]] = []
    for raw_path in args.paths:
        path = Path(raw_path)
        try:
            validate_cmip7_output(path)
            print(f"PASS {path}")
        except Exception as exc:  # noqa: BLE001
            failures.append((str(path), str(exc)))
            print(f"FAIL {path}: {exc}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
