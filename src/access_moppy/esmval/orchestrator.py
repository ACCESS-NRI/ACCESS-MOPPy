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
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from access_moppy.esmval.file_finder import RawFileFinder
from access_moppy.esmval.recipe_reader import CMORTask, RecipeReader
from access_moppy.esmval.variable_mapper import VariableIndex

logger = logging.getLogger(__name__)


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
        ACCESS-MOPPy model identifier (default: ``"ACCESS-ESM1.6"``).
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
        model_id: str = "ACCESS-ESM1.6",
        pattern_overrides: dict[str, str] | None = None,
        max_workers: int = 1,
        dry_run: bool = False,
    ) -> None:
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

        logger.info(
            "Found %d CMORisation task(s) in recipe '%s'.",
            len(tasks),
            recipe_path,
        )

        return self.prepare_tasks(tasks)

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
        seen: set[tuple] = set()
        unique: list[CMORTask] = []
        for t in tasks:
            key = (t.compound_name, t.experiment_id, t.variant_label, t.grid_label)
            if key not in seen:
                seen.add(key)
                unique.append(t)

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
        # If no raw files (resource-backed / internal), check output exists
        if not raw_files:
            return True

        try:
            output_mtime = min(os.path.getmtime(p) for p in expected_outputs)
            raw_mtime = max(os.path.getmtime(p) for p in raw_files)
            return output_mtime >= raw_mtime
        except OSError:
            return False

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
