"""Post-hoc compliance checking for runs made before ``compliance_check`` existed.

Batches run before that batch config option was added (see
:mod:`access_moppy.qc.compliance`) never validated the first file each
variable published. This module scans an already-completed run's output
tree, finds the earliest-published file for each variable, and runs the same
CF/WCRP checker suites against it.

Variables can be targeted explicitly (``variables=``/``--variable``), or
pulled straight from a tracker database's completed tasks
(``task_rows=``/``--db``, via :func:`list_completed_variables`); the latter
also lets :func:`write_results_to_db` write each result back onto its
``cmor_tasks`` row, under a ``compliance_json`` column that
``batch_report.py`` folds into the ``tasks[].compliance`` field of any batch
report built afterwards. Results can also be merged directly into an
existing ``moppy_batch_report.json`` via :func:`update_batch_report`, for
archived runs where the database is no longer available.

Unlike :func:`access_moppy.qc.compliance.enforce_compliance`, a completed
run's files are never renamed or removed here: a failure is reported, not
enforced, since there is no in-flight job left to stop.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .compliance import (
    WCRP_SUITES,
    check_output_file,
    classify_report,
    format_failures,
    resolve_suites,
)

#: Global attributes that identify one variable's output stream, common to
#: both the CMIP6 and CMIP7 required-global-attributes lists.
_IDENTITY_ATTRS = (
    "variable_id",
    "experiment_id",
    "source_id",
    "variant_label",
    "grid_label",
)

#: Read opportunistically, when present, to disambiguate a bare variable name
#: that exists in more than one table/realm (e.g. ``tas`` in both ``Amon``
#: and ``day``, or under two different CMIP7 realms). CMIP6/CMIP6Plus output
#: carries ``table_id``; CMIP7 output carries ``realm`` instead.
_TABLE_ATTRS = ("table_id", "realm")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _cmor_name(variable: str) -> str:
    """Return the bare CMOR variable name from a compound name.

    Compound names are either ``table.variable`` (CMIP6/CMIP6Plus — base.py's
    ``CMORiser`` requires exactly two dot-separated parts) or CMIP7's
    ``realm.physical_parameter[.processing_info][.frequency][.region]`` (see
    ``CMIP7Vocabulary._parse_compound_name``, up to five parts). Either way
    the bare variable name is the *second* component, not the last: for a
    branded CMIP7 name such as ``atmos.huss.tpt-h2m-hxy-u.3hr.glb``, the last
    component is the region (``glb``), not the variable. A name with no dot
    at all is passed through unchanged.
    """
    parts = variable.split(".")
    return parts[1] if len(parts) >= 2 else parts[0]


def _table_label(variable: str) -> str | None:
    """Return the table/realm component (the first part) of a compound name,
    or None for a bare name with no dot."""
    parts = variable.split(".", 1)
    return parts[0] if len(parts) >= 2 else None


def _attrs_table_label(attrs: dict[str, str]) -> str | None:
    """Return the table/realm label read from a file's global attributes,
    matching what :func:`_table_label` derives from a compound name."""
    for attr in _TABLE_ATTRS:
        if attr in attrs:
            return attrs[attr]
    return None


@dataclass(frozen=True)
class FirstFile:
    """The earliest-published output file found for one variable."""

    identity: str
    attrs: dict[str, str]
    directory: Path
    path: Path
    chunk_count: int


def _read_identity(path: Path) -> tuple[dict[str, str], float | None] | None:
    """Return (identity attrs, first time value) for *path*, or None if it is
    not a readable CMOR output file (missing an identity attribute, or a
    file the netCDF library cannot open)."""
    from netCDF4 import Dataset

    try:
        with Dataset(path) as dataset:
            attrs = {}
            for attr in _IDENTITY_ATTRS:
                try:
                    attrs[attr] = dataset.getncattr(attr)
                except AttributeError:
                    return None
            for attr in _TABLE_ATTRS:
                try:
                    attrs[attr] = dataset.getncattr(attr)
                except AttributeError:
                    pass
            time_value = None
            if "time" in dataset.variables and dataset.variables["time"].size:
                time_value = float(dataset.variables["time"][0])
            return attrs, time_value
    except OSError:
        return None


def find_first_files(
    output_folder: str | Path,
    *,
    variable_ids: set[str] | None = None,
    experiment_id: str | None = None,
) -> list[FirstFile]:
    """Group every ``*.nc`` file under *output_folder* by variable and return
    the earliest file in each group.

    Files are grouped by their CMIP identity attributes (``variable_id``,
    ``experiment_id``, ``source_id``, ``variant_label``, ``grid_label``) plus
    their parent directory, so the same variable published under two DRS
    versions, or sitting in two different folders, forms two separate groups
    rather than being conflated. Within a group, the file whose ``time``
    coordinate starts earliest is "first"; a fixed-field (``fx``) variable has
    no time coordinate and is expected to have a single file per group.

    Files that cannot be opened, or lack one of the identity attributes, are
    silently skipped — they are not CMOR output this check applies to.

    Args:
        variable_ids: When given, only files whose ``variable_id`` attribute
            is in this set are considered.
        experiment_id: When given, only files with this ``experiment_id``
            attribute are considered.
    """
    output_folder = Path(output_folder)
    groups: dict[tuple[Path, tuple[str, ...]], list[tuple[float, Path, dict]]] = {}
    for path in sorted(output_folder.rglob("*.nc")):
        read = _read_identity(path)
        if read is None:
            continue
        attrs, time_value = read
        if variable_ids is not None and attrs["variable_id"] not in variable_ids:
            continue
        if experiment_id is not None and attrs["experiment_id"] != experiment_id:
            continue
        key = (path.parent, tuple(attrs[name] for name in _IDENTITY_ATTRS))
        groups.setdefault(key, []).append((time_value or 0.0, path, attrs))

    first_files = []
    for (directory, identity_values), entries in groups.items():
        entries.sort(key=lambda entry: (entry[0], entry[1]))
        identity = ".".join(str(value) for value in identity_values)
        _, first_path, first_attrs = entries[0]
        first_files.append(
            FirstFile(
                identity=identity,
                attrs=first_attrs,
                directory=directory,
                path=first_path,
                chunk_count=len(entries),
            )
        )
    return sorted(first_files, key=lambda first_file: str(first_file.path))


def list_completed_variables(
    db_path: str | Path, experiment_id: str | None = None
) -> list[tuple[str, str]]:
    """Return ``(variable, experiment_id)`` pairs marked ``completed`` in a
    tracker database (``cmor_tasks.db``).

    Opens the database read-only, so this is safe to run against a database a
    live batch is still writing to.
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
    try:
        query = "SELECT DISTINCT variable, experiment_id FROM cmor_tasks WHERE status='completed'"
        params: tuple[str, ...] = ()
        if experiment_id is not None:
            query += " AND experiment_id=?"
            params = (experiment_id,)
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


@dataclass(frozen=True)
class ComplianceBackfillEntry:
    """The compliance result for one variable's first file."""

    identity: str
    file: Path
    passed: bool
    failed_checks: list[dict]
    environment_checks: list[dict]
    report_path: Path
    cv_version: str | None
    error: str | None = None
    #: The tracker's exact ``variable`` value, set only when this entry was
    #: matched against a database row (see *task_rows* on
    #: :func:`run_compliance_backfill`). ``write_results_to_db`` uses this to
    #: target the right row; entries found by a plain filesystem scan leave
    #: it unset, since there is no database row to write back to.
    variable: str | None = None
    experiment_id: str | None = None


def run_compliance_backfill(
    output_folder: str | Path,
    report_dir: str | Path,
    cmip_version: str = "CMIP6",
    min_weight: int = 3,
    suites: list[str] | None = None,
    skip_existing: bool = True,
    variables: list[str] | None = None,
    experiment_id: str | None = None,
    task_rows: list[tuple[str, str]] | None = None,
) -> list[ComplianceBackfillEntry]:
    """Run the CF/WCRP compliance suites against the first file of every
    variable found under *output_folder*.

    Each variable gets its own subdirectory of *report_dir*, named after its
    identity, holding a ``compliance_<file>.json`` report — mirroring the
    per-variable report layout ``compliance_check`` uses live.

    Args:
        skip_existing: Reuse an on-disk report instead of re-running the
            checker, when one already exists for a file and was produced
            with the same suites and *min_weight* requested here.
        variables: Restrict to these variables (``table.variable`` or a bare
            variable name). Ignored when *task_rows* is given.
        experiment_id: Restrict to this experiment.
        task_rows: ``(variable, experiment_id)`` pairs from a tracker
            database, as returned by :func:`list_completed_variables`. Drives
            both the variable filter and the ``variable``/``experiment_id``
            recorded on each entry, so :func:`write_results_to_db` can write
            the result back to the matching row. Matching includes each row's
            table/realm component, so e.g. ``Amon.tas`` and ``day.tas`` never
            collide onto the same entry.
    """
    #: (table/realm label, bare variable name, experiment_id) -> the tracker's
    #: exact compound name, so two variables sharing a bare name in different
    #: tables (``Amon.tas`` vs ``day.tas``) resolve to distinct rows.
    variable_lookup: dict[tuple[str | None, str, str], str] = {}
    variable_ids: set[str] | None = None
    if task_rows is not None:
        variable_ids = set()
        for variable, task_experiment_id in task_rows:
            cmor_name = _cmor_name(variable)
            variable_ids.add(cmor_name)
            variable_lookup[(_table_label(variable), cmor_name, task_experiment_id)] = (
                variable
            )
    elif variables is not None:
        variable_ids = {_cmor_name(variable) for variable in variables}

    report_dir = Path(report_dir)
    resolved_suites = resolve_suites(cmip_version, suites)
    entries: list[ComplianceBackfillEntry] = []

    for first_file in find_first_files(
        output_folder, variable_ids=variable_ids, experiment_id=experiment_id
    ):
        file_experiment_id = first_file.attrs["experiment_id"]
        db_variable = variable_lookup.get(
            (
                _attrs_table_label(first_file.attrs),
                first_file.attrs["variable_id"],
                file_experiment_id,
            )
        )
        var_report_dir = report_dir / first_file.identity
        report_path = var_report_dir / f"compliance_{first_file.path.stem}.json"

        cached_report = None
        if skip_existing and report_path.exists():
            try:
                candidate = json.loads(report_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                candidate = None
            meta = (candidate or {}).get("access_moppy", {})
            if (
                candidate is not None
                and meta.get("suites") == resolved_suites
                and meta.get("min_weight") == min_weight
            ):
                cached_report = candidate

        if cached_report is not None:
            failed_checks, environment_checks = classify_report(
                cached_report, resolved_suites, min_weight
            )
            entries.append(
                ComplianceBackfillEntry(
                    identity=first_file.identity,
                    file=first_file.path,
                    passed=not failed_checks,
                    failed_checks=failed_checks,
                    environment_checks=environment_checks,
                    report_path=report_path,
                    cv_version=meta.get("cv_version"),
                    variable=db_variable,
                    experiment_id=file_experiment_id,
                )
            )
            continue

        try:
            failed_checks, environment_checks, written_report_path, cv_version = (
                check_output_file(
                    first_file.path,
                    var_report_dir,
                    cmip_version=cmip_version,
                    min_weight=min_weight,
                    suites=suites,
                )
            )
        except RuntimeError as error:
            entries.append(
                ComplianceBackfillEntry(
                    identity=first_file.identity,
                    file=first_file.path,
                    passed=False,
                    failed_checks=[],
                    environment_checks=[],
                    report_path=report_path,
                    cv_version=None,
                    error=str(error),
                    variable=db_variable,
                    experiment_id=file_experiment_id,
                )
            )
            continue

        entries.append(
            ComplianceBackfillEntry(
                identity=first_file.identity,
                file=first_file.path,
                passed=not failed_checks,
                failed_checks=failed_checks,
                environment_checks=environment_checks,
                report_path=written_report_path,
                cv_version=cv_version,
                variable=db_variable,
                experiment_id=file_experiment_id,
            )
        )

    return entries


def write_results_to_db(
    db_path: str | Path, entries: list[ComplianceBackfillEntry]
) -> int:
    """Write each entry's compliance result back to its tracker row.

    Only entries matched against a database row (``entry.variable`` set, via
    *task_rows* on :func:`run_compliance_backfill`) are written; others are
    skipped, since there is no row to update.

    Returns:
        The number of rows updated.
    """
    from access_moppy.tracking import TaskTracker

    updated = 0
    with TaskTracker(db_path) as tracker:
        for entry in entries:
            if entry.variable is None or entry.experiment_id is None:
                continue
            tracker.set_compliance(
                entry.variable,
                entry.experiment_id,
                {
                    "checked_at": _utc_now_iso(),
                    "backfilled": True,
                    "passed": entry.passed,
                    "file": str(entry.file),
                    "report_path": str(entry.report_path),
                    "cv_version": entry.cv_version,
                    "error": entry.error,
                    "failed_checks": format_failures(entry.failed_checks)
                    if entry.failed_checks
                    else None,
                    "environment_warning": bool(entry.environment_checks),
                },
            )
            updated += 1
    return updated


def summarize(
    entries: list[ComplianceBackfillEntry],
    cmip_version: str,
    min_weight: int,
    suites: list[str] | None = None,
) -> dict[str, Any]:
    """Build the JSON-serialisable summary merged into a batch report."""
    failures = []
    errors = []
    environment_warnings = 0
    for entry in entries:
        if entry.error is not None:
            errors.append(
                {
                    "identity": entry.identity,
                    "variable": entry.variable,
                    "file": str(entry.file),
                    "error": entry.error,
                }
            )
            continue
        if entry.environment_checks:
            environment_warnings += 1
        if not entry.passed:
            failures.append(
                {
                    "identity": entry.identity,
                    "variable": entry.variable,
                    "file": str(entry.file),
                    "report": str(entry.report_path),
                    "checks": format_failures(entry.failed_checks),
                }
            )

    checked = [entry for entry in entries if entry.error is None]
    return {
        "checked_at": _utc_now_iso(),
        "backfilled": True,
        "cmip_version": cmip_version,
        "suites": resolve_suites(cmip_version, suites),
        "min_weight": min_weight,
        "total": len(entries),
        "passed": sum(1 for entry in checked if entry.passed),
        "failed": sum(1 for entry in checked if not entry.passed),
        "environment_warnings": environment_warnings,
        "errors": errors,
        "failures": failures,
    }


def update_batch_report(
    report_path: str | Path,
    summary: dict[str, Any],
    output_path: str | Path | None = None,
) -> Path:
    """Merge *summary* into an existing ``moppy_batch_report.json`` under a
    ``compliance`` key and write it back.

    Args:
        output_path: Where to write the updated report. Defaults to
            overwriting *report_path* in place.
    """
    report_path = Path(report_path)
    data = json.loads(report_path.read_text(encoding="utf-8"))
    data["compliance"] = summary
    out_path = Path(output_path) if output_path is not None else report_path
    out_path.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return out_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-compliance-backfill",
        description=(
            "Run the CF/WCRP compliance checkers against the first file of "
            "every variable in an already-completed run, for batches made "
            "before the compliance_check option existed."
        ),
    )
    parser.add_argument(
        "--output-folder",
        required=True,
        help="Root of the CMORised output tree to scan.",
    )
    parser.add_argument(
        "--report-dir",
        help=(
            "Directory the per-variable JSON reports are written under. "
            "Defaults to '<output-folder>/compliance_reports'."
        ),
    )
    parser.add_argument(
        "--cmip-version",
        default="CMIP6",
        choices=sorted(WCRP_SUITES),
        help="CMIP family, selecting the default WCRP suite (default: CMIP6).",
    )
    parser.add_argument(
        "--min-weight",
        type=int,
        default=3,
        help="Lowest checker weight treated as a failure (default: 3).",
    )
    parser.add_argument(
        "--suite",
        action="append",
        dest="suites",
        help="Explicit checker suite (repeatable). Overrides --cmip-version's default pair.",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_false",
        dest="skip_existing",
        default=True,
        help="Re-run the checker even if a matching report already exists for a file.",
    )
    parser.add_argument(
        "--variable",
        action="append",
        dest="variables",
        help=(
            "Restrict to this variable (repeatable). Accepts 'table.variable' "
            "or a bare variable name. With --db and no --variable, every "
            "variable marked 'completed' in the database is targeted."
        ),
    )
    parser.add_argument(
        "--db",
        help=(
            "Tracker database (cmor_tasks.db) to target variables from and "
            "write results back to, under a compliance_json column."
        ),
    )
    parser.add_argument(
        "--experiment-id",
        help="Restrict to this experiment (used with --db, and to disambiguate output files).",
    )
    parser.add_argument(
        "--batch-report",
        help="Existing moppy_batch_report.json to merge results into.",
    )
    parser.add_argument(
        "--report-output",
        help="Where to write the updated batch report. Defaults to overwriting --batch-report in place.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for backfilling compliance checks onto a completed run."""
    args = _build_parser().parse_args(argv)
    output_folder = Path(args.output_folder)
    report_dir = (
        Path(args.report_dir)
        if args.report_dir
        else output_folder / "compliance_reports"
    )

    task_rows = None
    if args.db:
        task_rows = list_completed_variables(args.db, experiment_id=args.experiment_id)
        if args.variables:
            wanted = {_cmor_name(variable) for variable in args.variables}
            task_rows = [
                (variable, row_experiment_id)
                for variable, row_experiment_id in task_rows
                if _cmor_name(variable) in wanted
            ]
        if not task_rows:
            print("No matching completed variables found in the tracker database.")
            return 0

    entries = run_compliance_backfill(
        output_folder,
        report_dir,
        cmip_version=args.cmip_version,
        min_weight=args.min_weight,
        suites=args.suites,
        skip_existing=args.skip_existing,
        variables=args.variables,
        experiment_id=args.experiment_id,
        task_rows=task_rows,
    )
    summary = summarize(entries, args.cmip_version, args.min_weight, args.suites)

    print(
        f"Checked {summary['total']} variable(s): "
        f"{summary['passed']} passed, {summary['failed']} failed, "
        f"{len(summary['errors'])} could not be checked."
    )
    for failure in summary["failures"]:
        print(f"FAIL {failure['identity']}: {failure['file']}\n{failure['checks']}")
    for error in summary["errors"]:
        print(f"ERROR {error['identity']}: {error['file']}: {error['error']}")

    if args.db:
        updated = write_results_to_db(args.db, entries)
        print(f"Updated {updated} row(s) in {args.db}")

    if args.batch_report:
        out_path = update_batch_report(args.batch_report, summary, args.report_output)
        print(f"Updated batch report: {out_path}")

    return 1 if summary["failed"] or summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
