"""
Unit tests for access_moppy.esmval.recipe_reader
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from access_moppy.esmval.recipe_reader import CMORTask, RecipeReader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_recipe(tmp_path: Path, content: str) -> Path:
    """Write a YAML string to a temp file and return the path."""
    recipe = tmp_path / "recipe.yml"
    recipe.write_text(textwrap.dedent(content))
    return recipe


# ---------------------------------------------------------------------------
# CMORTask dataclass
# ---------------------------------------------------------------------------


class TestCMORTask:
    def test_equality_ignores_extra_facets(self):
        t1 = CMORTask(
            "Amon.tas",
            "tas",
            "Amon",
            "historical",
            "r1i1p1f1",
            "ACCESS-ESM1-6",
            "gn",
            "2000/2005",
            extra_facets={"foo": 1},
        )
        t2 = CMORTask(
            "Amon.tas",
            "tas",
            "Amon",
            "historical",
            "r1i1p1f1",
            "ACCESS-ESM1-6",
            "gn",
            "2000/2005",
            extra_facets={"bar": 2},
        )
        assert t1 == t2

    def test_frozen(self):
        t = CMORTask(
            "Amon.tas", "tas", "Amon", "historical", "r1i1p1f1", "ACCESS-ESM1-6", "gn"
        )
        with pytest.raises((TypeError, AttributeError)):
            t.short_name = "pr"  # type: ignore[misc]

    def test_default_timerange_empty(self):
        t = CMORTask(
            "Amon.tas", "tas", "Amon", "historical", "r1i1p1f1", "ACCESS-ESM1-6", "gn"
        )
        assert t.timerange == ""

    def test_hashable(self):
        t = CMORTask(
            "Amon.tas", "tas", "Amon", "historical", "r1i1p1f1", "ACCESS-ESM1-6", "gn"
        )
        s = {t}
        assert t in s


# ---------------------------------------------------------------------------
# RecipeReader
# ---------------------------------------------------------------------------

_MINIMAL_RECIPE = """\
    datasets:
      - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,
         ensemble: r1i1p1f1, grid: gn, timerange: '2000/2005'}

    diagnostics:
      my_diag:
        variables:
          tas:
            mip: Amon
        scripts:
          null:
            script: null
"""


class TestRecipeReaderBasic:
    def test_single_variable(self, tmp_path):
        recipe = _write_recipe(tmp_path, _MINIMAL_RECIPE)
        reader = RecipeReader(recipe)
        tasks = reader.tasks
        assert len(tasks) == 1
        t = tasks[0]
        assert t.short_name == "tas"
        assert t.mip == "Amon"
        assert t.compound_name == "Amon.tas"
        assert t.experiment_id == "historical"
        assert t.variant_label == "r1i1p1f1"
        assert t.grid_label == "gn"
        assert t.timerange == "2000/2005"

    def test_multiple_variables_same_dataset(self, tmp_path):
        recipe = _write_recipe(
            tmp_path,
            """\
            datasets:
              - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,
                 ensemble: r1i1p1f1, grid: gn}

            diagnostics:
              d1:
                variables:
                  tas:
                    mip: Amon
                  pr:
                    mip: Amon
                scripts:
                  null: {script: null}
        """,
        )
        reader = RecipeReader(recipe)
        names = {t.short_name for t in reader.tasks}
        assert names == {"tas", "pr"}

    def test_deduplication(self, tmp_path):
        """Same (mip, short_name, exp, ensemble) appearing twice → one task."""
        recipe = _write_recipe(
            tmp_path,
            """\
            datasets:
              - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,
                 ensemble: r1i1p1f1, grid: gn}

            diagnostics:
              d1:
                variables:
                  tas:
                    mip: Amon
                scripts:
                  null: {script: null}
              d2:
                variables:
                  tas:
                    mip: Amon
                scripts:
                  null: {script: null}
        """,
        )
        reader = RecipeReader(recipe)
        assert len(reader.tasks) == 1

    def test_filters_out_non_access_dataset(self, tmp_path):
        recipe = _write_recipe(
            tmp_path,
            """\
            datasets:
              - {dataset: HadGEM3-GC31-LL, project: CMIP6, exp: historical,
                 ensemble: r1i1p1f1, grid: gn}

            diagnostics:
              d1:
                variables:
                  tas:
                    mip: Amon
                scripts:
                  null: {script: null}
        """,
        )
        reader = RecipeReader(recipe)
        assert reader.tasks == []

    def test_filters_out_non_cmip6_project(self, tmp_path):
        recipe = _write_recipe(
            tmp_path,
            """\
            datasets:
              - {dataset: ACCESS-ESM1-6, project: CMIP5, exp: historical,
                 ensemble: r1i1p1f1, grid: gr}

            diagnostics:
              d1:
                variables:
                  tas:
                    mip: Amon
                scripts:
                  null: {script: null}
        """,
        )
        reader = RecipeReader(recipe)
        assert reader.tasks == []

    def test_dot_alias_not_accepted(self, tmp_path):
        """ACCESS-ESM1.6 (dot alias) is not a recognised dataset name."""
        recipe = _write_recipe(
            tmp_path,
            """\
            datasets:
              - {dataset: ACCESS-ESM1.6, project: CMIP6, exp: piControl,
                 ensemble: r1i1p1f1, grid: gn}

            diagnostics:
              d1:
                variables:
                  tas:
                    mip: Amon
                scripts:
                  null: {script: null}
        """,
        )
        reader = RecipeReader(recipe)
        assert reader.tasks == []

    def test_missing_diagnostics_key(self, tmp_path):
        recipe = _write_recipe(
            tmp_path,
            """\
            datasets:
              - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,
                 ensemble: r1i1p1f1, grid: gn}
        """,
        )
        reader = RecipeReader(recipe)
        assert reader.tasks == []

    def test_invalid_yaml_raises(self, tmp_path):
        recipe = tmp_path / "bad.yml"
        recipe.write_text("this: [is: not valid yaml\n")
        with pytest.raises(Exception):  # yaml.YAMLError or ValueError
            RecipeReader(recipe)

    def test_non_dict_yaml_raises(self, tmp_path):
        recipe = tmp_path / "bad.yml"
        recipe.write_text("- item1\n- item2\n")
        with pytest.raises(ValueError, match="YAML mapping"):
            RecipeReader(recipe)

    def test_tasks_cached(self, tmp_path):
        """Calling .tasks twice should return the same list object."""
        recipe = _write_recipe(tmp_path, _MINIMAL_RECIPE)
        reader = RecipeReader(recipe)
        assert reader.tasks is reader.tasks

    def test_additional_datasets_in_diagnostic(self, tmp_path):
        recipe = _write_recipe(
            tmp_path,
            """\
            diagnostics:
              d1:
                additional_datasets:
                  - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,
                     ensemble: r1i1p1f1, grid: gn}
                variables:
                  pr:
                    mip: Amon
                scripts:
                  null: {script: null}
        """,
        )
        reader = RecipeReader(recipe)
        assert len(reader.tasks) == 1
        assert reader.tasks[0].short_name == "pr"
