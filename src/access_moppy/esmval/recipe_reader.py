"""
access_moppy.esmval.recipe_reader
==================================

Parse ESMValTool recipe YAML files and extract the set of
:class:`CMORTask` objects that need to be CMORised for the recipe to run.

A :class:`CMORTask` describes one variable × one dataset combination that
ACCESS-MOPPy needs to process.  Only datasets whose ``project`` facet is
one of the CMIP project strings handled by ACCESS-MOPPy (currently
``CMIP6``) *and* whose ``dataset`` identifies an ACCESS-ESM model run
(``ACCESS-ESM1-5`` or ``ACCESS-ESM1-6``) are kept; all other datasets are silently ignored.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Projects that ACCESS-MOPPy can produce
_SUPPORTED_PROJECTS: frozenset[str] = frozenset({"CMIP6"})

# dataset names that indicate ACCESS-ESM output supported by ACCESS-MOPPy
_ACCESS_ESM_DATASET_NAMES: frozenset[str] = frozenset(
    {"ACCESS-ESM1-5", "ACCESS-ESM1-6"}
)


@dataclass(frozen=True)
class CMORTask:
    """Describes a single CMORisation task extracted from a recipe.

    Attributes
    ----------
    compound_name:
        ACCESS-MOPPy compound name in ``"<table>.<short_name>"`` format,
        e.g. ``"Amon.tas"``.
    short_name:
        CMIP short name of the variable, e.g. ``"tas"``.
    mip:
        CMIP MIP (table) identifier, e.g. ``"Amon"``.
    experiment_id:
        CMIP experiment ID, e.g. ``"historical"``.
    variant_label:
        CMIP variant label, e.g. ``"r1i1p1f1"``.
    source_id:
        CMIP source identifier as used by ESMValTool, e.g. ``"ACCESS-ESM1-6"``.
    grid_label:
        CMIP grid label, e.g. ``"gn"``.
    timerange:
        ISO 8601 time range string as written in the recipe, e.g.
        ``"1850/2014"``.  Empty string when the recipe does not specify a
        time range.
    cmip_version:
        Which CMIP vocabulary to target, e.g. ``"CMIP6"``.
    activity_id:
        CMIP activity ID when present in the recipe facets, e.g. ``"CMIP"``.
    extra_facets:
        Any additional facets that were specified in the recipe dataset entry
        (passed through for informational purposes).
    """

    compound_name: str
    short_name: str
    mip: str
    experiment_id: str
    variant_label: str
    source_id: str
    grid_label: str
    timerange: str = ""
    cmip_version: str = "CMIP6"
    activity_id: str = ""
    extra_facets: dict[str, Any] = field(default_factory=dict, compare=False)


class RecipeReader:
    """Parse an ESMValTool recipe YAML and extract CMORisation tasks.

    Parameters
    ----------
    recipe_path:
        Path to the ``*.yml`` recipe file.
    allowed_datasets:
        Dataset names to treat as supported ACCESS-ESM runs.  Defaults to the
        built-in set :data:`_ACCESS_ESM_DATASET_NAMES`
        (``ACCESS-ESM1-5``, ``ACCESS-ESM1-6`` and dot-separated aliases).

    Examples
    --------
    >>> reader = RecipeReader("my_recipe.yml")
    >>> tasks = reader.tasks
    >>> for t in tasks:
    ...     print(t.compound_name, t.experiment_id, t.variant_label)
    """

    def __init__(
        self,
        recipe_path: str | Path,
        allowed_datasets: frozenset[str] | None = None,
    ) -> None:
        self._path = Path(recipe_path)
        self._allowed_datasets = allowed_datasets or _ACCESS_ESM_DATASET_NAMES
        self._recipe: dict[str, Any] = self._load()
        self._tasks: list[CMORTask] | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def tasks(self) -> list[CMORTask]:
        """Return the deduplicated list of :class:`CMORTask` objects."""
        if self._tasks is None:
            self._tasks = self._extract_tasks()
        return self._tasks

    @property
    def recipe(self) -> dict[str, Any]:
        """Return the raw parsed recipe dictionary."""
        return self._recipe

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, Any]:
        with open(self._path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, dict):
            raise ValueError(
                f"Recipe file {self._path} does not contain a YAML mapping at its top level."
            )
        return data

    def _extract_tasks(self) -> list[CMORTask]:
        """Walk the recipe and collect all CMORTask objects."""
        seen: set[CMORTask] = set()
        tasks: list[CMORTask] = []

        # Recipe-level global datasets
        global_datasets: list[dict] = self._recipe.get("datasets", []) or []

        diagnostics: dict = self._recipe.get("diagnostics", {}) or {}
        for diag_name, diag_body in diagnostics.items():
            if not isinstance(diag_body, dict):
                continue

            # Per-diagnostic datasets (may inherit globals)
            diag_datasets: list[dict] = (
                diag_body.get("additional_datasets", []) or []
            ) + global_datasets

            variables: dict = diag_body.get("variables", {}) or {}
            for short_name, var_body in variables.items():
                if var_body is None:
                    var_body = {}

                # Variable-level dataset overrides
                var_datasets: list[dict] = (
                    var_body.get("additional_datasets", []) or []
                ) + diag_datasets

                # Extract mip / table from variable body; may also appear as
                # a facet inside individual dataset entries.
                default_mip: str = var_body.get("mip", "")
                default_timerange: str = str(var_body.get("timerange", ""))
                default_grid: str = var_body.get("grid", "gn")
                default_exp: str = var_body.get("exp", "")

                for ds_entry in var_datasets:
                    task = self._make_task(
                        short_name=short_name,
                        ds_entry=ds_entry,
                        default_mip=default_mip,
                        default_timerange=default_timerange,
                        default_grid=default_grid,
                        default_exp=default_exp,
                    )
                    if task is not None and task not in seen:
                        seen.add(task)
                        tasks.append(task)

        if not tasks:
            logger.warning(
                "No supported ACCESS-ESM datasets found in recipe %s. "
                "Check that 'project: CMIP6' and 'dataset' is one of %s "
                "in the recipe.",
                self._path,
                sorted(_ACCESS_ESM_DATASET_NAMES),
            )
        return tasks

    def _make_task(
        self,
        *,
        short_name: str,
        ds_entry: dict,
        default_mip: str,
        default_timerange: str,
        default_grid: str,
        default_exp: str,
    ) -> CMORTask | None:
        """Convert a single dataset entry to a :class:`CMORTask`, or return
        ``None`` if the dataset is not a supported ACCESS-ESM run."""
        project: str = str(ds_entry.get("project", "CMIP6"))
        if project not in _SUPPORTED_PROJECTS:
            return None

        dataset: str = str(ds_entry.get("dataset", ""))
        if dataset not in self._allowed_datasets:
            return None

        mip: str = str(ds_entry.get("mip", default_mip))
        if not mip:
            logger.warning(
                "Cannot determine MIP for variable '%s' / dataset '%s' — skipping.",
                short_name,
                dataset,
            )
            return None

        exp: str = str(ds_entry.get("exp", default_exp))
        ensemble: str = str(ds_entry.get("ensemble", "r1i1p1f1"))
        grid: str = str(ds_entry.get("grid", default_grid)) or "gn"
        timerange: str = str(ds_entry.get("timerange", default_timerange))
        activity_id: str = str(ds_entry.get("activity", ""))

        compound_name = f"{mip}.{short_name}"

        # Collect remaining facets as extra_facets (exclude standard ones)
        _standard_keys = {
            "project",
            "dataset",
            "mip",
            "exp",
            "ensemble",
            "grid",
            "timerange",
            "activity",
        }
        extra: dict = {k: v for k, v in ds_entry.items() if k not in _standard_keys}

        return CMORTask(
            compound_name=compound_name,
            short_name=short_name,
            mip=mip,
            experiment_id=exp,
            variant_label=ensemble,
            source_id=dataset,
            grid_label=grid,
            timerange=timerange,
            cmip_version=project,
            activity_id=activity_id,
            extra_facets=extra,
        )
