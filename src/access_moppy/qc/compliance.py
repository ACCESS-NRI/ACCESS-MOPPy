"""CF and WCRP compliance validation for CMORised output files.

Batch CMORisation can validate the first file a variable publishes before
committing compute to the rest of it.  The check runs from
``CMORiser.first_write_hook``, so it fires as soon as that file lands and
before any further split of the same write, and a structurally broken file is
caught there instead of after the full time series has been CMORised.

Two checker suites run together, because they do not overlap: ``cf:1.11``
covers the CF conventions themselves, while the WCRP suite for the target
CMIP family covers required global attributes and their controlled-vocabulary
values, the DRS path and filename, the variable registry, and file chunking.

The entry point is :func:`enforce_compliance`, which is deliberately strict:
a failing file is renamed out of the way and a :class:`RuntimeError` is
raised, so the generated worker script aborts the variable.  This is opt-in
via the ``compliance_check`` batch config option.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

#: CF conventions suite, run for every CMIP family.
CF_SUITE = "cf:1.11"

#: WCRP project suite per CMIP family, run alongside :data:`CF_SUITE`.
WCRP_SUITES = {
    "CMIP6": "wcrp_cmip6:1.0",
    "CMIP6Plus": "wcrp_cmip6plus:1.0",
    "CMIP7": "wcrp_cmip7:1.0",
}

#: Messages that indicate a broken checker environment rather than bad output.
#: The WCRP suites need the esgvoc vocabulary database; without it their CV
#: checks fail for every file, so those results are reported but not enforced.
ENVIRONMENT_MSG_SUBSTRINGS = ("Universe database is not installed or active.",)

#: Appended to the name of an output file that failed validation.
FAILED_SUFFIX = ".compliance_failed"

#: esgvoc project holding the controlled vocabulary each WCRP suite applies.
ESGVOC_PROJECTS = {"CMIP6": "cmip6", "CMIP6Plus": "cmip6plus", "CMIP7": "cmip7"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve_suites(cmip_version: str, suites: list[str] | None = None) -> list[str]:
    """Return the checker suites to run.

    *suites* overrides the choice entirely.  Left unset, the CF suite is paired
    with the WCRP suite matching *cmip_version* — for CMIP7 that is
    ``["cf:1.11", "wcrp_cmip7:1.0"]``.  Deriving rather than hardcoding keeps a
    CMIP6 batch from being validated against the CMIP7 vocabulary.
    """
    if suites is not None:
        if not isinstance(suites, list) or not suites:
            raise ValueError("compliance_check_suite must be a non-empty list of names")
        if not all(isinstance(suite, str) for suite in suites):
            raise ValueError("compliance_check_suite entries must be strings")
        return suites

    if cmip_version not in WCRP_SUITES:
        raise ValueError(
            f"cmip_version must be one of {sorted(WCRP_SUITES)}, got '{cmip_version}'"
        )
    return [CF_SUITE, WCRP_SUITES[cmip_version]]


def resolve_cv_version(cmip_version: str) -> str | None:
    """Return the esgvoc vocabulary version the WCRP suite validates against.

    The suite name pins the checker plugin, not the vocabulary: ``wcrp_cmip7:1.0``
    applies whichever CMIP7 controlled vocabulary the local esgvoc database
    holds. Recording that version makes a report self-describing.

    Best-effort — this is provenance only, so any esgvoc problem yields ``None``
    rather than failing the check.
    """
    try:
        import esgvoc.api

        return esgvoc.api.get_project(ESGVOC_PROJECTS[cmip_version]).version
    except Exception:
        return None


def _is_failed(check: dict) -> bool:
    """Return True when a checker result scored below its maximum."""
    value = check.get("value", [0, 0])
    return len(value) >= 2 and value[0] != value[1]


def _is_environment_failure(check: dict) -> bool:
    """Return True when a failure comes from an unconfigured checker environment."""
    return any(
        substring in message
        for substring in ENVIRONMENT_MSG_SUBSTRINGS
        for message in check.get("msgs", [])
    )


def classify_report(
    report: dict, suites: list[str], min_weight: int
) -> tuple[list[dict], list[dict]]:
    """Split a compliance-checker JSON report into enforced and skipped failures.

    Shared between a live :func:`check_output_file` run and re-classifying an
    on-disk report without re-running the checker (see
    ``qc.backfill_compliance``).

    Returns:
        The enforced failures and the failures skipped as environment
        problems. Each returned check carries a ``suite`` key.

    Raises:
        RuntimeError: *report* has no section for one of *suites*.
    """
    failed_checks: list[dict] = []
    environment_checks: list[dict] = []
    for suite in suites:
        section = report.get(suite)
        if section is None:
            available = ", ".join(sorted(report)) or "<none>"
            raise RuntimeError(
                f"Compliance report has no '{suite}' section. "
                f"Available sections: {available}"
            )

        for check in section.get("all_priorities", []):
            if check.get("weight", 0) < min_weight or not _is_failed(check):
                continue
            check = dict(check, suite=suite)
            if _is_environment_failure(check):
                environment_checks.append(check)
            else:
                failed_checks.append(check)
    return failed_checks, environment_checks


def check_output_file(
    output_file: str | Path,
    report_dir: str | Path,
    cmip_version: str = "CMIP6",
    min_weight: int = 3,
    suites: list[str] | None = None,
) -> tuple[list[dict], list[dict], Path, str | None]:
    """Run the compliance checkers on *output_file*.

    All suites run in a single checker invocation and share one JSON report.
    An ``access_moppy`` block recording the suites, the esgvoc vocabulary
    version and the enforced weight is added to that report.

    Args:
        output_file: CMORised NetCDF file to validate.
        report_dir: Directory the JSON report is written to.  Batch jobs pass
            the per-variable script directory, so the report sits next to the
            generated scripts and PBS logs.
        cmip_version: CMIP family, selecting the WCRP suite.
        min_weight: Lowest checker weight treated as a failure.  ``3`` covers
            mandatory checks only; ``1`` treats every finding as a failure.
        suites: Explicit checker suites, overriding the *cmip_version* default.

    Returns:
        The enforced failures, the failures skipped as environment problems,
        the report path, and the esgvoc vocabulary version applied (``None``
        when it cannot be resolved).  Each returned check carries a ``suite``
        key.

    Raises:
        RuntimeError: The checker is unavailable, or produced no readable
            report for the requested suites.
    """
    if isinstance(min_weight, bool) or not isinstance(min_weight, int):
        raise ValueError("compliance_check_min_weight must be an integer")

    suites = resolve_suites(cmip_version, suites)

    output_file = Path(output_file)
    if not output_file.exists():
        raise RuntimeError(f"Compliance check input does not exist: {output_file}")

    checker_executable = shutil.which("compliance-checker")
    if checker_executable is None:
        raise RuntimeError(
            "compliance_check is enabled but the 'compliance-checker' executable "
            "is not on PATH; load an environment that provides it (for example "
            "conda/analysis3) from worker_init"
        )

    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"compliance_{output_file.stem}.json"

    command = [checker_executable]
    for suite in suites:
        command += ["--test", suite]
    command += ["--format", "json", "--output", str(report_path), str(output_file)]

    result = subprocess.run(  # noqa: S603  # nosec B603
        command,
        capture_output=True,
        text=True,
        check=False,
        shell=False,
    )

    if not report_path.exists():
        raise RuntimeError(
            f"Compliance checker produced no report for {output_file}\n"
            f"Exit code: {result.returncode}\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"Compliance checker wrote an unreadable report {report_path}: {error}\n"
            f"Exit code: {result.returncode}\n"
            f"stderr:\n{result.stderr}"
        ) from error

    try:
        failed_checks, environment_checks = classify_report(report, suites, min_weight)
    except RuntimeError as error:
        raise RuntimeError(f"Compliance report {report_path}: {error}") from error

    # Record what this report was produced with, so it stays self-describing:
    # the suite names pin the checker plugin, not the vocabulary it applied.
    cv_version = resolve_cv_version(cmip_version)
    report["access_moppy"] = {
        "suites": suites,
        "cmip_version": cmip_version,
        "cv_version": cv_version,
        "min_weight": min_weight,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return failed_checks, environment_checks, report_path, cv_version


def format_failures(failed_checks: list[dict]) -> str:
    """Render failing checks as an indented, human-readable summary."""
    lines = []
    for check in failed_checks:
        lines.append(
            f"- [{check.get('suite', '?')} | weight {check.get('weight', 0)}] "
            f"{check.get('name', '<unnamed check>')}"
        )
        for message in check.get("msgs", []):
            lines.append(f"    {message}")
    return "\n".join(lines)


def enforce_compliance(
    output_file: str | Path,
    report_dir: str | Path,
    cmip_version: str = "CMIP6",
    min_weight: int = 3,
    suites: list[str] | None = None,
    on_record: Callable[[dict], None] | None = None,
) -> Path:
    """Validate *output_file* and abort the variable when it does not comply.

    On failure the file is renamed with the :data:`FAILED_SUFFIX` suffix.  It
    is kept for inspection but no longer matches the published DRS layout, so
    a later ``resume`` run cannot mistake it for completed output.

    Failures caused by an unconfigured checker environment are reported but
    never abort the variable, so a missing esgvoc vocabulary database cannot
    take down a whole batch.

    Args:
        output_file: CMORised NetCDF file to validate.
        report_dir: Directory the JSON report is written to.
        cmip_version: CMIP family, selecting the WCRP suite.
        min_weight: Lowest checker weight treated as a failure.
        suites: Explicit checker suites, overriding the *cmip_version* default.
        on_record: Called with the outcome as a dictionary, whether the file
            passed or failed, before this function returns or raises. A batch
            worker passes a callback that writes the record to the task
            tracker, so the verdict survives in the batch report instead of
            only in the JSON report next to the job.

    Returns:
        Path to the JSON report, which is kept whether or not the file passed.

    Raises:
        RuntimeError: The file failed validation, or the checker could not run.
    """
    output_file = Path(output_file)
    failed_checks, environment_checks, report_path, cv_version = check_output_file(
        output_file,
        report_dir,
        cmip_version=cmip_version,
        min_weight=min_weight,
        suites=suites,
    )
    applied = " + ".join(resolve_suites(cmip_version, suites))
    vocabulary = (
        f"{cmip_version} CV {cv_version}"
        if cv_version
        else f"{cmip_version} CV unavailable"
    )
    print(f"Compliance report ({applied}, {vocabulary}) -> {report_path}")

    if environment_checks:
        print(
            f"Compliance check warning: {len(environment_checks)} checks could not "
            "run because the checker environment is incomplete, and were not "
            "enforced. Run 'esgvoc install' to enable the WCRP vocabulary checks.\n"
            f"{format_failures(environment_checks)}"
        )

    record: dict = {
        "checked_at": _utc_now_iso(),
        "backfilled": False,
        "passed": not failed_checks,
        "file": str(output_file),
        "report_path": str(report_path),
        "suites": resolve_suites(cmip_version, suites),
        "cv_version": cv_version,
        "error": None,
        "failed_checks": format_failures(failed_checks) if failed_checks else None,
        "environment_warning": bool(environment_checks),
    }

    if not failed_checks:
        print(f"Compliance check passed: {output_file.name}")
        if on_record is not None:
            on_record(record)
        return report_path

    failed_path = output_file.with_name(output_file.name + FAILED_SUFFIX)
    output_file.rename(failed_path)
    record["renamed_to"] = str(failed_path)
    record["error"] = f"Compliance check failed for {output_file.name}"
    if on_record is not None:
        on_record(record)
    raise RuntimeError(
        f"Compliance check failed for {output_file.name}; CMORisation of this "
        f"variable was stopped after its first output file.\n"
        f"The non-compliant file was renamed to: {failed_path}\n"
        f"Full report: {report_path}\n"
        f"{format_failures(failed_checks)}"
    )
