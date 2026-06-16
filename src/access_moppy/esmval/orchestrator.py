"""
access_moppy.esmval.orchestrator
==================================

Drive CMORisation of all variables required by an ESMValTool recipe.

:class:`CMORiseOrchestrator` orchestrates the full preparation workflow:

1. Parse the recipe with :class:`~access_moppy.esmval.recipe_reader.RecipeReader`.
2. Build a :class:`~access_moppy.esmval.variable_mapper.VariableIndex` for the
   target model.
3. For each :class:`~access_moppy.esmval.recipe_reader.CMORTask`, locate raw
   input files with :class:`~access_moppy.esmval.file_finder.RawFileFinder`.
4. Skip tasks whose CMORised output already exists and is up-to-date.
5. Run :class:`~access_moppy.driver.ACCESS_ESM_CMORiser` for remaining tasks.

The output is written to a CMIP DRS directory tree under *cache_dir* so that
ESMValCore can find it using its standard ``drs: CMIP6`` configuration.
"""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from access_moppy.esmval.file_finder import RawFileFinder
from access_moppy.esmval.recipe_reader import CMORTask, RecipeReader
from access_moppy.esmval.variable_mapper import VariableIndex

logger = logging.getLogger(__name__)

_AREACELLA_COMPOUND_NAME = "fx.areacella"
_TIMERANGE_SUFFIX_RE = re.compile(r"_(\d{6,8})-(\d{6,8})$")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class TaskResult:
    """Outcome of processing one :class:`CMORTask`.

    Attributes
    ----------
    task:
        The originating :class:`CMORTask`.
    status:
        One of ``"done"``, ``"cached"``, ``"skipped"``, or ``"failed"``.
    output_files:
        Paths to the written CMORised NetCDF files (empty for cached/failed).
    error:
        Exception message when *status* is ``"failed"``; empty string otherwise.
    """

    task: CMORTask
    status: str  # "done" | "cached" | "skipped" | "failed"
    output_files: list[Path] = field(default_factory=list)
    error: str = ""

    @property
    def succeeded(self) -> bool:
        """Return whether the task produced or reused usable output."""
        return self.status in ("done", "cached")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class CMORiseOrchestrator:
    """Prepare all ACCESS-ESM1.6 data required by an ESMValTool recipe.

    Parameters
    ----------
    input_root:
        Root directory of the raw ACCESS-ESM1.6 archive.
    cache_dir:
        Directory where CMORised files will be written in CMIP DRS tree
        structure.  ESMValCore's ``rootpath`` should point here.
    model_id:
        ACCESS-MOPPy model identifier (default: ``"ACCESS-ESM1-6"``).
    pattern_overrides:
        Optional ``{compound_name: glob_pattern}`` mapping forwarded to
        :class:`~access_moppy.esmval.file_finder.RawFileFinder`.
    max_workers:
        Number of parallel worker processes (default: ``1``).  Set to
        ``>1`` to parallelise independent variables.
    dry_run:
        When ``True``, log what *would* be done without running
        CMORisation.

    Examples
    --------
    >>> orch = CMORiseOrchestrator(
    ...     input_root="/g/data/p73/archive/.../MyRun",
    ...     cache_dir="~/.cache/moppy-esmval",
    ... )
    >>> results = orch.prepare_recipe("my_recipe.yml")
    >>> for r in results:
    ...     print(r.task.compound_name, r.status)
    """

    def __init__(
        self,
        input_root: str | Path,
        cache_dir: str | Path,
        model_id: str,
        pattern_overrides: dict[str, str] | None = None,
        max_workers: int = 1,
        dry_run: bool = False,
    ) -> None:
        """Initialise the recipe preparation orchestrator.

        Parameters
        ----------
        input_root:
            Root directory containing raw ACCESS-ESM output.
        cache_dir:
            Directory where CMORised files will be written and later exposed
            to ESMValCore as a CMIP DRS root.
        model_id:
            ACCESS-MOPPy model mapping identifier.
        pattern_overrides:
            Optional per-compound-name glob overrides forwarded to
            :class:`RawFileFinder`.
        max_workers:
            Number of worker processes to use for independent tasks.  Values
            less than one are treated as one.
        dry_run:
            When ``True``, report the planned work without writing outputs.
        """
        self._input_root = Path(input_root)
        self._cache_dir = Path(cache_dir).expanduser().resolve()
        self._model_id = model_id
        self._dry_run = dry_run
        self._max_workers = max(1, int(max_workers))

        self._index = VariableIndex(model_id=model_id)
        self._finder = RawFileFinder(
            input_root=self._input_root,
            variable_index=self._index,
            pattern_overrides=pattern_overrides,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def cache_dir(self) -> Path:
        """Return the resolved CMORised-output cache directory."""
        return self._cache_dir

    def prepare_recipe(
        self,
        recipe_path: str | Path,
        allowed_datasets: frozenset[str] | None = None,
    ) -> list[TaskResult]:
        """Parse *recipe_path* and CMORise all required ACCESS-ESM1.6 data.

        Parameters
        ----------
        recipe_path:
            Path to the ESMValTool ``.yml`` recipe.
        allowed_datasets:
            Override the set of dataset names treated as ACCESS-ESM1.6 runs.

        Returns
        -------
        list[TaskResult]
            One result per unique task found in the recipe.
        """
        reader = RecipeReader(recipe_path, allowed_datasets=allowed_datasets)
        tasks = reader.tasks

        if not tasks:
            logger.info(
                "No supported ACCESS-ESM tasks found in recipe '%s'.", recipe_path
            )
            return []

        tasks = self._append_systematic_areacella_tasks(tasks)

        logger.info(
            "Found %d CMORisation task(s) in recipe '%s'.",
            len(tasks),
            recipe_path,
        )

        return self.prepare_tasks(tasks)

    def _append_systematic_areacella_tasks(
        self, tasks: list[CMORTask]
    ) -> list[CMORTask]:
        """Append ``fx.areacella`` once per ACCESS dataset run context.

        Many diagnostics rely on atmospheric grid-cell area as an external
        variable. Generating it during prepare avoids downstream dataset
        discovery mixing with stale local versions.
        """
        contexts: set[tuple[str, str, str, str, str, str]] = set()
        existing_keys = {
            (
                t.compound_name,
                t.experiment_id,
                t.variant_label,
                t.source_id,
                t.grid_label,
                t.cmip_version,
                t.activity_id,
            )
            for t in tasks
        }

        for task in tasks:
            contexts.add(
                (
                    task.experiment_id,
                    task.variant_label,
                    task.source_id,
                    task.grid_label,
                    task.cmip_version,
                    task.activity_id,
                )
            )

        augmented = list(tasks)
        added = 0
        for (
            exp,
            variant,
            source_id,
            grid,
            cmip_version,
            activity_id,
        ) in sorted(contexts):
            areacella_key = (
                _AREACELLA_COMPOUND_NAME,
                exp,
                variant,
                source_id,
                grid,
                cmip_version,
                activity_id,
            )
            if areacella_key in existing_keys:
                continue

            augmented.append(
                CMORTask(
                    compound_name=_AREACELLA_COMPOUND_NAME,
                    short_name="areacella",
                    mip="fx",
                    experiment_id=exp,
                    variant_label=variant,
                    source_id=source_id,
                    grid_label=grid,
                    cmip_version=cmip_version,
                    activity_id=activity_id,
                )
            )
            existing_keys.add(areacella_key)
            added += 1

        if added:
            logger.info("Added %d systematic fx.areacella task(s).", added)
        return augmented

    def prepare_tasks(self, tasks: Sequence[CMORTask]) -> list[TaskResult]:
        """CMORise the given list of :class:`CMORTask` objects.

        Parameters
        ----------
        tasks:
            Tasks to process.  Duplicate tasks (same ``(compound_name,
            experiment_id, variant_label)``) are deduplicated automatically.

        Returns
        -------
        list[TaskResult]
            Results in the same order as *tasks* (after deduplication).
        """
        unique_by_key: dict[tuple[str, str, str, str], CMORTask] = {}
        for t in tasks:
            key = (t.compound_name, t.experiment_id, t.variant_label, t.grid_label)
            existing = unique_by_key.get(key)
            if existing is None:
                unique_by_key[key] = t
                continue

            # Keep the broadest timerange when a variable appears in multiple
            # diagnostics with different temporal requests.
            if _is_timerange_broader(t.timerange, existing.timerange):
                unique_by_key[key] = t

        unique = list(unique_by_key.values())

        if self._max_workers == 1:
            return [self._process_task(t) for t in unique]

        results: list[TaskResult] = [None] * len(unique)  # type: ignore[list-item]
        with ProcessPoolExecutor(max_workers=self._max_workers) as pool:
            future_to_idx = {
                pool.submit(self._process_task, t): i for i, t in enumerate(unique)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # noqa: BLE001
                    results[idx] = TaskResult(
                        task=unique[idx],
                        status="failed",
                        error=str(exc),
                    )
        return results

    # ------------------------------------------------------------------
    # Individual task processing
    # ------------------------------------------------------------------

    def _process_task(self, task: CMORTask) -> TaskResult:
        """Process a single :class:`CMORTask`."""
        logger.info(
            "Processing task: %s  exp=%s  variant=%s",
            task.compound_name,
            task.experiment_id,
            task.variant_label,
        )

        # Check whether the entry is supported at all
        mip, short_name = task.mip, task.short_name
        if not self._index.is_supported(mip, short_name):
            logger.warning(
                "Variable '%s' (%s.%s) is not in the '%s' mapping — skipping.",
                task.compound_name,
                mip,
                short_name,
                self._model_id,
            )
            return TaskResult(
                task=task,
                status="skipped",
                error=f"No mapping for '{task.compound_name}' in model '{self._model_id}'",
            )

        # Locate raw input files
        raw_files = self._finder.find(task.compound_name, timerange=task.timerange)
        entry = self._index.get(mip, short_name)

        # For resource-backed or internal variables, raw_files == [] is fine
        needs_input = (
            entry is not None
            and entry.resource_file is None
            and entry.calculation_type != "internal"
        )
        if needs_input and not raw_files:
            return TaskResult(
                task=task,
                status="failed",
                error=(
                    f"No raw input files found for '{task.compound_name}' "
                    f"under '{self._input_root}'. "
                    "Add a pattern_override or check input_root."
                ),
            )

        # Check cache freshness
        expected_outputs = self._expected_output_paths(task)
        expected_outputs = self._prune_overlapping_outputs(task, expected_outputs)
        if self._cache_is_fresh(task, raw_files, expected_outputs):
            logger.info("  ↳ cache is up-to-date, skipping CMORisation.")
            return TaskResult(task=task, status="cached", output_files=expected_outputs)

        if self._dry_run:
            logger.info("  ↳ [dry-run] would CMORise %d file(s).", len(raw_files))
            return TaskResult(task=task, status="done")

        # Run CMORisation – force dask's synchronous scheduler so that
        # netCDF4 file handles are never shared across threads.
        return self._run_cmoriser(task, raw_files)

    def _run_cmoriser(self, task: CMORTask, raw_files: list[Path]) -> TaskResult:
        """Instantiate and run :class:`~access_moppy.driver.ACCESS_ESM_CMORiser`.

        Dask's synchronous scheduler is enforced to avoid thread-safety issues
        with the netCDF4 C library (concurrent file opens from multiple Dask
        worker threads cause segfaults).
        """
        # Import here to avoid making ESMValCore a hard dependency at import time
        from access_moppy.driver import ACCESS_ESM_CMORiser

        try:
            import dask

            _dask_ctx = dask.config.set(scheduler="synchronous")
        except Exception:  # dask not available or already synchronous
            from contextlib import nullcontext

            _dask_ctx = nullcontext()

        input_data = [str(p) for p in raw_files] if raw_files else None

        try:
            with (
                _dask_ctx,
                ACCESS_ESM_CMORiser(
                    input_data=input_data,
                    compound_name=task.compound_name,
                    experiment_id=task.experiment_id,
                    source_id=task.source_id,
                    variant_label=task.variant_label,
                    grid_label=task.grid_label,
                    cmip_version=task.cmip_version,
                    activity_id=task.activity_id or None,
                    output_path=str(self._cache_dir),
                    drs_root=str(self._cache_dir),
                    model_id=self._model_id,
                ) as cmoriser,
            ):
                cmoriser.run(write_output=True)

            # Discover written output files
            output_files = self._expected_output_paths(task)
            output_files = self._prune_overlapping_outputs(task, output_files)
            logger.info(
                "  ↳ CMORised successfully → %d output file(s).", len(output_files)
            )
            return TaskResult(task=task, status="done", output_files=output_files)

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "  ↳ CMORisation failed for '%s': %s",
                task.compound_name,
                exc,
                exc_info=True,
            )
            return TaskResult(task=task, status="failed", error=str(exc))

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _expected_output_paths(self, task: CMORTask) -> list[Path]:
        """Return a list of CMORised output files that exist on disk for
        *task*.  Uses a glob because the exact filename contains a time range
        stamp that we don't know in advance."""
        # CMIP6 DRS: <activity>/<institute>/<source_id>/<exp>/<variant>/<mip>/<short_name>/<grid>/v*/
        # We glob from <source_id> downward, ignoring activity/institute (not
        # stored in the CMORTask).
        pattern = (
            self._cache_dir
            / "**"
            / task.source_id
            / task.experiment_id
            / task.variant_label
            / task.mip
            / task.short_name
            / task.grid_label
            / "**"
            / f"{task.short_name}_{task.mip}_{task.source_id}_{task.experiment_id}_{task.variant_label}_{task.grid_label}*.nc"
        )
        import glob as _glob

        return sorted(Path(p) for p in _glob.glob(str(pattern), recursive=True))

    def _cache_is_fresh(
        self,
        task: CMORTask,
        raw_files: list[Path],
        expected_outputs: list[Path],
    ) -> bool:
        """Return ``True`` when at least one expected output file exists and is
        newer than all raw input files."""
        if not expected_outputs:
            return False

        if not _outputs_cover_timerange(task.timerange, expected_outputs):
            return False

        # If no raw files (resource-backed / internal), check output exists
        if not raw_files:
            return True

        try:
            output_mtime = min(os.path.getmtime(p) for p in expected_outputs)
            raw_mtime = max(os.path.getmtime(p) for p in raw_files)
            return output_mtime >= raw_mtime
        except OSError:
            return False

    def _prune_overlapping_outputs(
        self,
        task: CMORTask,
        output_files: Sequence[Path],
    ) -> list[Path]:
        """Remove older files that are fully covered by newer overlapping ones.

        This prevents stale, narrower timerange files from co-existing with
        newer broader products for the same task.
        """
        canonical_paths = _canonical_output_paths(output_files)
        candidates = [_output_file_info(path) for path in canonical_paths]
        candidates = [info for info in candidates if info is not None]
        if len(candidates) < 2:
            return sorted(p for p in canonical_paths if p.exists())

        # Newest files first; only remove older files.
        candidates.sort(key=lambda item: item.mtime, reverse=True)

        to_remove: set[Path] = set()
        for idx, newer in enumerate(candidates):
            for older in candidates[idx + 1 :]:
                if older.path in to_remove:
                    continue
                if _range_covers(newer, older) and _ranges_overlap(newer, older):
                    to_remove.add(older.path)

        removed_count = 0
        for path in sorted(to_remove):
            if _is_latest_alias_path(path):
                continue
            try:
                path.unlink()
                removed_count += 1
                logger.info(
                    "  ↳ removed overlapping stale output for %s: %s",
                    task.compound_name,
                    path,
                )
            except OSError:
                logger.warning(
                    "  ↳ failed to remove overlapping stale output for %s: %s",
                    task.compound_name,
                    path,
                )

        remaining = sorted(p for p in canonical_paths if p.exists())
        if removed_count:
            logger.info(
                "  ↳ pruned %d overlapping stale output file(s).",
                removed_count,
            )
        return remaining

    # ------------------------------------------------------------------
    # Summary helpers
    # ------------------------------------------------------------------

    @staticmethod
    def summarise(results: list[TaskResult]) -> None:
        """Print a concise summary table of *results* to stdout."""
        width = max((len(r.task.compound_name) for r in results), default=20) + 2
        header = f"{'Variable':<{width}} {'Status':<10} {'Files':>6}  Note"
        print(header)
        print("-" * (len(header) + 20))
        for r in sorted(results, key=lambda x: x.task.compound_name):
            note = r.error if r.error else ""
            print(
                f"{r.task.compound_name:<{width}} {r.status:<10} "
                f"{len(r.output_files):>6}  {note}"
            )


def _year_from_timerange_token(token: str) -> int | None:
    match = re.match(r"\s*(\d{4})", token)
    if match:
        return int(match.group(1))
    return None


def _parse_timerange_year_bounds(timerange: str) -> tuple[int | None, int | None]:
    if not timerange:
        return None, None

    parts = timerange.split("/", 1)
    if len(parts) != 2:
        return None, None

    start = _year_from_timerange_token(parts[0])
    end_token = parts[1].strip()

    # Duration-like right-hand side (e.g. "2000/P1M").
    if end_token.startswith("P"):
        return start, start

    end = _year_from_timerange_token(end_token)
    return start, end


def _timerange_priority(timerange: str) -> tuple[int, int]:
    # Empty timerange means unconstrained; treat as broadest possible span.
    if not timerange:
        return (-(10**9), 10**9)

    start, end = _parse_timerange_year_bounds(timerange)
    start_norm = start if start is not None else 10**9
    end_norm = end if end is not None else 10**9
    return start_norm, end_norm


def _is_timerange_broader(candidate: str, current: str) -> bool:
    c_start, c_end = _timerange_priority(candidate)
    x_start, x_end = _timerange_priority(current)

    if c_start < x_start:
        return True
    if c_start > x_start:
        return False
    return c_end > x_end


def _output_year_bounds(path: Path) -> tuple[int | None, int | None]:
    stem = path.stem
    match = _TIMERANGE_SUFFIX_RE.search(stem)
    if not match:
        return None, None

    start_token, end_token = match.group(1), match.group(2)
    start_year = int(start_token[:4])
    end_year = int(end_token[:4])
    return start_year, end_year


def _time_token_to_ordinal(token: str, *, end: bool) -> int | None:
    if not token.isdigit() or len(token) not in (4, 6, 8):
        return None

    year = int(token[:4])
    month = 12 if end else 1
    day = 31 if end else 1

    if len(token) >= 6:
        month = int(token[4:6])
    if len(token) == 8:
        day = int(token[6:8])

    # Comparable sortable ordinal, avoids calendar dependency.
    return (year * 10000) + (month * 100) + day


@dataclass(frozen=True)
class _OutputFileInfo:
    path: Path
    start_ord: int
    end_ord: int
    mtime: float


def _output_file_info(path: Path) -> _OutputFileInfo | None:
    match = _TIMERANGE_SUFFIX_RE.search(path.stem)
    if not match:
        return None

    start_ord = _time_token_to_ordinal(match.group(1), end=False)
    end_ord = _time_token_to_ordinal(match.group(2), end=True)
    if start_ord is None or end_ord is None:
        return None

    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0

    return _OutputFileInfo(
        path=path,
        start_ord=start_ord,
        end_ord=end_ord,
        mtime=mtime,
    )


def _is_latest_alias_path(path: Path) -> bool:
    return "latest" in path.parts


def _canonical_output_paths(paths: Sequence[Path]) -> list[Path]:
    """Deduplicate outputs that reference the same physical file.

    Preference is given to non-``latest`` paths to avoid deleting through
    a symlink alias.
    """
    by_real: dict[Path, Path] = {}
    for path in paths:
        try:
            real = path.resolve()
        except OSError:
            real = path

        chosen = by_real.get(real)
        if chosen is None:
            by_real[real] = path
            continue

        if _is_latest_alias_path(chosen) and not _is_latest_alias_path(path):
            by_real[real] = path

    return list(by_real.values())


def _ranges_overlap(a: _OutputFileInfo, b: _OutputFileInfo) -> bool:
    return not (a.end_ord < b.start_ord or b.end_ord < a.start_ord)


def _range_covers(container: _OutputFileInfo, containee: _OutputFileInfo) -> bool:
    return (
        container.start_ord <= containee.start_ord
        and container.end_ord >= containee.end_ord
    )


def _outputs_cover_timerange(timerange: str, outputs: Sequence[Path]) -> bool:
    req_start, req_end = _parse_timerange_year_bounds(timerange)

    # No recipe constraint: any output set is acceptable.
    if req_start is None and req_end is None:
        return True

    parsed_bounds = [_output_year_bounds(p) for p in outputs]
    known_bounds = [(s, e) for s, e in parsed_bounds if s is not None and e is not None]
    if not known_bounds:
        return False

    out_start = min(s for s, _ in known_bounds)
    out_end = max(e for _, e in known_bounds)

    if req_start is not None and out_start > req_start:
        return False
    if req_end is not None and out_end < req_end:
        return False
    return True
