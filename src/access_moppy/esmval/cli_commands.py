"""
access_moppy.esmval.cli_commands
==================================

Command-line interface for the ACCESS-MOPPy ↔ ESMValTool integration.

Three entry points are exposed:

``moppy-esmval-prepare``
    CMORise all ACCESS-ESM1.6 data required by a recipe and write the
    ESMValCore config overlay.  Does **not** invoke ESMValTool.

``moppy-esmval-run``
    CMORise the required data and then invoke ``esmvaltool run`` in one
    step.

``esmvaltool cmorise``
    Registered via the ``esmvaltool_commands`` entry-point group.  This
    makes ``esmvaltool cmorise`` available once ACCESS-MOPPy is installed
    alongside ESMValCore.  It performs the same preparation step as
    ``moppy-esmval-prepare``.

All three accept the same core arguments:

* ``recipe``           — path to the ESMValTool YAML recipe  (required)
* ``--input-root``     — root directory of raw ACCESS-ESM1.6 data  (required)
* ``--cache-dir``      — where CMORised files will be written  (required)
* ``--config``         — path to an existing file in the ESMValCore config dir;
                       the MOPPy data-source file is placed in the same directory
* ``--output-config``  — where to write the generated ESMValCore data-source file
* ``--workers``        — number of parallel workers (default: 1)
* ``--dry-run``        — log what would be done, do not CMORise
* ``--pattern``        — ``compound_name:glob_pattern`` override (repeatable)
* ``--esmvaltool-args``— (run only) extra arguments forwarded to esmvaltool
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import yaml

logger = logging.getLogger(__name__)

_ACCESS_DATASETS: frozenset[str] = frozenset({"ACCESS-ESM1-5", "ACCESS-ESM1-6"})
_VERSION_DIR_RE = re.compile(r"^v\d{8}$")


@dataclass(frozen=True)
class PrepareResult:
    """Result bundle returned by :func:`_prepare`."""

    cfg_path: Path
    recipe_to_run: Path
    pinned_version: str | None = None


# ---------------------------------------------------------------------------
# Shared argument parser factory
# ---------------------------------------------------------------------------


def _build_parser(prog: str = "moppy-esmval-prepare") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "CMORise ACCESS-ESM1.6 raw output so that ESMValTool can use it "
            "directly from an unmodified recipe."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  # CMORise and write config overlay (no ESMValTool invocation):
  moppy-esmval-prepare my_recipe.yml \\
      --input-root /g/data/p73/archive/.../MyRun \\
      --cache-dir ~/.cache/moppy-esmval

  # CMORise and immediately run ESMValTool:
  moppy-esmval-run my_recipe.yml \\
      --input-root /g/data/p73/archive/.../MyRun \\
      --cache-dir ~/.cache/moppy-esmval \\
      --workers 4
""",
    )

    parser.add_argument(
        "recipe",
        metavar="RECIPE",
        help="Path to the ESMValTool YAML recipe file.",
    )
    parser.add_argument(
        "--input-root",
        required=True,
        metavar="PATH",
        help=(
            "Root directory of the raw ACCESS-ESM1.6 archive "
            "(e.g. /g/data/p73/archive/.../MyRun)."
        ),
    )
    parser.add_argument(
        "--cache-dir",
        required=True,
        metavar="PATH",
        help=(
            "Directory where CMORised files will be written in CMIP DRS "
            "structure (e.g. ~/.cache/moppy-esmval)."
        ),
    )
    parser.add_argument(
        "--model-id",
        default="ACCESS-ESM1-6",
        metavar="ID",
        help="ACCESS-MOPPy model identifier (default: 'ACCESS-ESM1-6').",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="FILE",
        help=(
            "Path to any existing file in the user's ESMValCore config directory. "
            "The MOPPy data-source config file is written into the same directory "
            "so ESMValCore loads both automatically."
        ),
    )
    parser.add_argument(
        "--output-config",
        default=None,
        metavar="FILE",
        help=(
            "Where to write the generated ESMValCore data-source config file "
            "(default: ~/.config/esmvaltool/moppy-esmval-data.yml)."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel CMORisation workers (default: 1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log what would be done without performing CMORisation.",
    )
    parser.add_argument(
        "--pattern",
        action="append",
        default=[],
        metavar="COMPOUND_NAME:GLOB",
        help=(
            "Override the raw-file glob pattern for a specific variable.  "
            "Format: 'Amon.tas:/output*/atmosphere/netCDF/*mon.nc'. "
            "Can be repeated for multiple variables."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Enable DEBUG-level logging.",
    )
    return parser


def _parse_pattern_overrides(pattern_args: list[str]) -> dict[str, str]:
    """Convert ``['Amon.tas:/path/pattern*.nc', ...]`` to a dict."""
    overrides: dict[str, str] = {}
    for item in pattern_args:
        if ":" not in item:
            raise ValueError(
                f"Invalid --pattern value '{item}'. Expected '<compound_name>:<glob>'."
            )
        compound_name, _, glob_pat = item.partition(":")
        overrides[compound_name.strip()] = glob_pat.strip()
    return overrides


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s  %(name)s  %(message)s",
        stream=sys.stderr,
    )
    # Quieten noisy libraries unless in verbose mode
    if not verbose:
        for noisy in ("distributed", "asyncio", "parsl", "iris", "cf_units"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def _extract_cmor_versions(results: Sequence[Any]) -> list[str]:
    """Return sorted unique CMOR version directory names found in task outputs."""
    versions: set[str] = set()
    for result in results:
        output_files = getattr(result, "output_files", []) or []
        for out in output_files:
            path = Path(out)
            for part in path.parts:
                if _VERSION_DIR_RE.match(part):
                    versions.add(part)
    return sorted(versions)


def _collect_dataset_entries(recipe: dict[str, Any]) -> list[dict[str, Any]]:
    """Collect all dataset mapping entries from recipe-level/diag/var scopes."""
    entries: list[dict[str, Any]] = []

    def _add_from(value: Any) -> None:
        if not isinstance(value, list):
            return
        for item in value:
            if isinstance(item, dict):
                entries.append(item)

    _add_from(recipe.get("datasets"))

    diagnostics = recipe.get("diagnostics")
    if isinstance(diagnostics, dict):
        for diag in diagnostics.values():
            if not isinstance(diag, dict):
                continue
            _add_from(diag.get("additional_datasets"))

            variables = diag.get("variables")
            if not isinstance(variables, dict):
                continue
            for var in variables.values():
                if isinstance(var, dict):
                    _add_from(var.get("additional_datasets"))

    return entries


def _pin_recipe_dataset_version(
    recipe_path: str | Path,
    version: str,
) -> tuple[Path | None, int]:
    """Write a recipe copy with ACCESS dataset entries pinned to *version*."""
    src = Path(recipe_path)
    with open(src, encoding="utf-8") as fh:
        recipe = yaml.safe_load(fh)

    if not isinstance(recipe, dict):
        return None, 0

    updated = 0
    for entry in _collect_dataset_entries(recipe):
        project = str(entry.get("project", "CMIP6"))
        dataset = str(entry.get("dataset", ""))
        if project == "CMIP6" and dataset in _ACCESS_DATASETS:
            if entry.get("version") != version:
                entry["version"] = version
                updated += 1

    if updated == 0:
        return None, 0

    dest = src.with_name(f"{src.stem}.moppy-pinned{src.suffix}")
    with open(dest, "w", encoding="utf-8") as fh:
        yaml.dump(recipe, fh, default_flow_style=False, sort_keys=False)
    return dest, updated


# ---------------------------------------------------------------------------
# Core prepare logic (shared between prepare and run)
# ---------------------------------------------------------------------------


def _prepare(
    recipe: str | Path,
    input_root: str | Path,
    cache_dir: str | Path,
    model_id: str = "ACCESS-ESM1-6",
    config: str | Path | None = None,
    output_config: str | Path | None = None,
    workers: int = 1,
    dry_run: bool = False,
    pattern_overrides: dict[str, str] | None = None,
) -> PrepareResult:
    """Run the CMORisation step and write the ESMValCore config overlay.

    Returns the written config path and the recipe path that should be run.
    """
    from access_moppy.esmval.config_gen import (
        write_esmval_config,
        write_esmval_config_alongside,
    )
    from access_moppy.esmval.orchestrator import CMORiseOrchestrator

    orch = CMORiseOrchestrator(
        input_root=input_root,
        cache_dir=cache_dir,
        model_id=model_id,
        pattern_overrides=pattern_overrides,
        max_workers=workers,
        dry_run=dry_run,
    )

    logger.info("Preparing recipe: %s", recipe)
    results = orch.prepare_recipe(recipe)

    if results:
        CMORiseOrchestrator.summarise(results)

    failed = [r for r in results if not r.succeeded]
    if failed:
        logger.error("%d task(s) failed:", len(failed))
        for r in failed:
            logger.error("  %s: %s", r.task.compound_name, r.error)
        # We still write the config so the user can run ESMValTool for the
        # tasks that succeeded.

    # Write ESMValCore config overlay
    if config:
        cfg_path = write_esmval_config_alongside(
            cache_dir=cache_dir,
            base_config_path=config,
            output_path=output_config,
        )
    else:
        cfg_path = write_esmval_config(
            cache_dir=cache_dir,
            output_path=output_config,
        )

    print(f"\nESMValCore config written to: {cfg_path}", flush=True)

    recipe_to_run = Path(recipe)
    pinned_version: str | None = None
    if not dry_run:
        versions = _extract_cmor_versions(results)
        if versions:
            pinned_version = max(versions)
            if len(versions) > 1:
                logger.warning(
                    "Multiple CMOR versions found in output files: %s; pinning recipe to latest %s.",
                    ", ".join(versions),
                    pinned_version,
                )

            pinned_recipe, updated_entries = _pin_recipe_dataset_version(
                recipe_path=recipe,
                version=pinned_version,
            )
            if pinned_recipe is not None:
                recipe_to_run = pinned_recipe
                logger.info(
                    "Pinned %d ACCESS dataset entry/entries to version '%s' in recipe copy '%s'.",
                    updated_entries,
                    pinned_version,
                    pinned_recipe,
                )

    # In ESMValCore 2.14+ there is no --config flag.  Config files are loaded
    # from the user config directory.  If we wrote to the default location
    # (~/.config/esmvaltool/) no extra env var is needed; otherwise the user
    # must point ESMVALTOOL_CONFIG_DIR at the parent directory.
    from access_moppy.esmval.config_gen import _default_user_config_dir

    config_dir = cfg_path.parent
    if config_dir.resolve() == _default_user_config_dir().resolve():
        run_cmd = f"pixi run -e esmval esmvaltool run {recipe_to_run}"
    else:
        run_cmd = f"ESMVALTOOL_CONFIG_DIR={config_dir} pixi run -e esmval esmvaltool run {recipe_to_run}"

    print(f"Run ESMValTool with:\n  {run_cmd}", flush=True)
    return PrepareResult(
        cfg_path=cfg_path,
        recipe_to_run=recipe_to_run,
        pinned_version=pinned_version,
    )


# ---------------------------------------------------------------------------
# ``moppy-esmval-prepare``
# ---------------------------------------------------------------------------


def main_prepare(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``moppy-esmval-prepare``.

    Parameters
    ----------
    argv:
        Optional argument list.  When omitted, arguments are read from
        ``sys.argv`` by :mod:`argparse`.

    Returns
    -------
    int
        Process-style exit code: ``0`` on success, ``1`` on parse or
        preparation failure.
    """
    parser = _build_parser(prog="moppy-esmval-prepare")
    args = parser.parse_args(list(argv) if argv is not None else None)
    _configure_logging(args.verbose)

    try:
        pattern_overrides = _parse_pattern_overrides(args.pattern)
    except ValueError as exc:
        parser.error(str(exc))
        return 1

    try:
        _prepare(
            recipe=args.recipe,
            input_root=args.input_root,
            cache_dir=args.cache_dir,
            model_id=args.model_id,
            config=args.config,
            output_config=args.output_config,
            workers=args.workers,
            dry_run=args.dry_run,
            pattern_overrides=pattern_overrides,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Prepare step failed: %s", exc, exc_info=True)
        return 1

    return 0


# ---------------------------------------------------------------------------
# ``moppy-esmval-run``
# ---------------------------------------------------------------------------


def main_run(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``moppy-esmval-run``.

    Runs CMORisation, writes config overlay, then calls
    ``esmvaltool run <recipe>`` with ``ESMVALTOOL_CONFIG_DIR`` set when the
    generated config was written outside the default user config directory.

    Parameters
    ----------
    argv:
        Optional argument list.  When omitted, arguments are read from
        ``sys.argv`` by :mod:`argparse`.

    Returns
    -------
    int
        Exit code from ESMValTool, or ``1`` when the preparation step fails.
    """
    parser = _build_parser(prog="moppy-esmval-run")
    parser.add_argument(
        "--esmvaltool-args",
        default="",
        metavar="ARGS",
        help="Extra arguments forwarded verbatim to 'esmvaltool run' (quoted string).",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    _configure_logging(args.verbose)

    try:
        pattern_overrides = _parse_pattern_overrides(args.pattern)
    except ValueError as exc:
        parser.error(str(exc))
        return 1

    try:
        prep_result = _prepare(
            recipe=args.recipe,
            input_root=args.input_root,
            cache_dir=args.cache_dir,
            model_id=args.model_id,
            config=args.config,
            output_config=args.output_config,
            workers=args.workers,
            dry_run=args.dry_run,
            pattern_overrides=pattern_overrides,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Prepare step failed: %s", exc, exc_info=True)
        return 1

    if args.dry_run:
        logger.info("[dry-run] would call: esmvaltool run %s", args.recipe)
        return 0

    if isinstance(prep_result, PrepareResult):
        cfg_path = prep_result.cfg_path
        recipe_to_run = prep_result.recipe_to_run
    else:
        # Backward-compatibility for tests/mocks that still patch _prepare to return Path
        cfg_path = Path(prep_result)
        recipe_to_run = Path(args.recipe)

    from access_moppy.esmval.config_gen import _default_user_config_dir

    extra = args.esmvaltool_args.split() if args.esmvaltool_args else []
    cmd = ["esmvaltool", "run", str(recipe_to_run)] + extra
    env = None
    config_dir = cfg_path.parent
    if config_dir.resolve() != _default_user_config_dir().resolve():
        import os

        env = {**os.environ, "ESMVALTOOL_CONFIG_DIR": str(config_dir)}
    logger.info("Invoking: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False, env=env)
    return result.returncode


# ---------------------------------------------------------------------------
# ``esmvaltool cmorise``  (esmvaltool_commands entry point)
# ---------------------------------------------------------------------------


class CMORiseCommand:
    """CMORise ACCESS-ESM1.6 data required for an ESMValTool recipe.

    This class is registered as an ``esmvaltool_commands`` entry point so
    that it becomes available as ``esmvaltool cmorise`` once
    ACCESS-MOPPy is installed alongside ESMValCore.

    ESMValCore's ``_main.py`` loads it via::

        entry_point.load()()   # instantiate
        instance(args)         # call with fire-dispatched arguments

    Because ESMValCore uses `fire <https://github.com/google/python-fire>`_
    to dispatch commands, each public method of this class becomes a
    sub-sub-command and the instance itself is callable for the default
    action.

    Usage::

        esmvaltool cmorise --recipe my_recipe.yml \\
            --input-root /g/data/... \\
            --cache-dir ~/.cache/moppy-esmval
    """

    def __call__(  # noqa: D102
        self,
        recipe: str,
        input_root: str,
        cache_dir: str,
        model_id: str = "ACCESS-ESM1-6",
        config: str | None = None,
        output_config: str | None = None,
        workers: int = 1,
        dry_run: bool = False,
        pattern: list[str] | None = None,
        verbose: bool = False,
    ) -> None:
        """CMORise data needed by *recipe* and write an ESMValCore config overlay.

        Parameters
        ----------
        recipe:
            Path to the ESMValTool YAML recipe.
        input_root:
            Root directory of the raw ACCESS-ESM1.6 archive.
        cache_dir:
            Directory where CMORised files will be written.
        model_id:
            ACCESS-MOPPy model identifier (default: ``"ACCESS-ESM1-6"``).
        config:
            Optional path to any file in the user's ESMValCore config
            directory.  The MOPPy data-source config is written into the
            same directory so ESMValCore picks up both.
        output_config:
            Where to write the generated config file
            (default: ``~/.config/esmvaltool/moppy-esmval-data.yml``).
        workers:
            Number of parallel workers (default: 1).
        dry_run:
            Log actions without running CMORisation.
        pattern:
            List of ``"compound_name:glob_pattern"`` overrides.
        verbose:
            Enable DEBUG-level logging.
        """
        _configure_logging(verbose)

        try:
            pattern_overrides = _parse_pattern_overrides(list(pattern or []))
        except ValueError as exc:
            logger.error("Invalid pattern override: %s", exc)
            sys.exit(1)

        try:
            _prepare(
                recipe=recipe,
                input_root=input_root,
                cache_dir=cache_dir,
                model_id=model_id,
                config=config,
                output_config=output_config,
                workers=workers,
                dry_run=dry_run,
                pattern_overrides=pattern_overrides,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("CMORise step failed: %s", exc, exc_info=True)
            sys.exit(1)
