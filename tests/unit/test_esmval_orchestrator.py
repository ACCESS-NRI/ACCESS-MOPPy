"""
Unit tests for access_moppy.esmval.orchestrator
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from access_moppy.esmval.orchestrator import CMORiseOrchestrator, TaskResult
from access_moppy.esmval.recipe_reader import CMORTask

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task(
    compound: str = "Amon.tas",
    exp: str = "historical",
    variant: str = "r1i1p1f1",
    grid: str = "gn",
    source_id: str = "ACCESS-ESM1-6",
    timerange: str = "",
) -> CMORTask:
    mip, short_name = compound.split(".", 1)
    return CMORTask(
        compound_name=compound,
        short_name=short_name,
        mip=mip,
        experiment_id=exp,
        variant_label=variant,
        source_id=source_id,
        grid_label=grid,
        timerange=timerange,
    )


# ---------------------------------------------------------------------------
# TaskResult
# ---------------------------------------------------------------------------


class TestTaskResult:
    def test_succeeded_when_done(self):
        r = TaskResult(task=_make_task(), status="done")
        assert r.succeeded is True

    def test_succeeded_when_cached(self):
        r = TaskResult(task=_make_task(), status="cached")
        assert r.succeeded is True

    def test_not_succeeded_when_failed(self):
        r = TaskResult(task=_make_task(), status="failed", error="oops")
        assert r.succeeded is False

    def test_not_succeeded_when_skipped(self):
        r = TaskResult(task=_make_task(), status="skipped")
        assert r.succeeded is False

    def test_default_output_files_empty(self):
        r = TaskResult(task=_make_task(), status="done")
        assert r.output_files == []

    def test_default_error_empty_string(self):
        r = TaskResult(task=_make_task(), status="done")
        assert r.error == ""


# ---------------------------------------------------------------------------
# CMORiseOrchestrator — construction
# ---------------------------------------------------------------------------


class TestCMORiseOrchestratorInit:
    def test_tilde_in_cache_dir_is_expanded(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir="~/some/cache")
        assert "~" not in str(orch.cache_dir)

    def test_max_workers_clamped_to_minimum_one(self, tmp_path):
        orch = CMORiseOrchestrator(
            input_root=tmp_path, cache_dir=tmp_path, max_workers=0
        )
        assert orch._max_workers == 1

    def test_cache_dir_property_returns_resolved_path(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        assert orch.cache_dir == tmp_path.resolve()

    def test_dry_run_attribute(self, tmp_path):
        orch = CMORiseOrchestrator(
            input_root=tmp_path, cache_dir=tmp_path, dry_run=True
        )
        assert orch._dry_run is True

    def test_pattern_overrides_stored(self, tmp_path):
        overrides = {"Amon.tas": "output*/atm/*.nc"}
        orch = CMORiseOrchestrator(
            input_root=tmp_path, cache_dir=tmp_path, pattern_overrides=overrides
        )
        assert orch._finder._overrides == overrides


# ---------------------------------------------------------------------------
# prepare_recipe
# ---------------------------------------------------------------------------


class TestPrepareRecipe:
    def test_returns_empty_list_when_no_access_tasks(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: HadGEM3, project: CMIP6, exp: historical,"
            " ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        assert orch.prepare_recipe(recipe) == []

    def test_delegates_to_prepare_tasks(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,"
            " ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        with patch.object(orch, "prepare_tasks", return_value=[]) as mock_pt:
            orch.prepare_recipe(recipe)
        mock_pt.assert_called_once()

    def test_prepare_recipe_returns_results(self, tmp_path):
        """Results from prepare_tasks are propagated through prepare_recipe."""
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,"
            " ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        fake_result = TaskResult(task=_make_task(), status="skipped")
        with patch.object(orch, "prepare_tasks", return_value=[fake_result]):
            results = orch.prepare_recipe(recipe)
        assert len(results) == 1
        assert results[0].status == "skipped"

    def test_prepare_recipe_adds_systematic_areacella_task(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,"
            " ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)

        with patch.object(orch, "prepare_tasks", return_value=[]) as mock_pt:
            orch.prepare_recipe(recipe)

        submitted_tasks = mock_pt.call_args.args[0]
        compounds = [task.compound_name for task in submitted_tasks]
        assert "Amon.tas" in compounds
        assert "fx.areacella" in compounds

    def test_prepare_recipe_does_not_duplicate_existing_areacella_task(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,"
            " ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "      areacella:\n"
            "        mip: fx\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)

        with patch.object(orch, "prepare_tasks", return_value=[]) as mock_pt:
            orch.prepare_recipe(recipe)

        submitted_tasks = mock_pt.call_args.args[0]
        areacella_count = sum(
            task.compound_name == "fx.areacella" for task in submitted_tasks
        )
        assert areacella_count == 1


# ---------------------------------------------------------------------------
# prepare_tasks
# ---------------------------------------------------------------------------


class TestPrepareTasks:
    def test_deduplicates_identical_tasks(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        t = _make_task()
        with patch.object(
            orch,
            "_process_task",
            return_value=TaskResult(task=t, status="done"),
        ) as mock_proc:
            orch.prepare_tasks([t, t])
        assert mock_proc.call_count == 1

    def test_different_tasks_both_processed(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        t1 = _make_task("Amon.tas")
        t2 = _make_task("Amon.pr")
        with patch.object(
            orch,
            "_process_task",
            side_effect=lambda t: TaskResult(task=t, status="done"),
        ) as mock_proc:
            orch.prepare_tasks([t1, t2])
        assert mock_proc.call_count == 2

    def test_returns_list_of_task_results(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        t = _make_task()
        with patch.object(
            orch,
            "_process_task",
            return_value=TaskResult(task=t, status="done"),
        ):
            results = orch.prepare_tasks([t])
        assert len(results) == 1
        assert isinstance(results[0], TaskResult)

    def test_empty_input_returns_empty_list(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        assert orch.prepare_tasks([]) == []

    def test_duplicate_variable_keeps_broadest_timerange(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        narrow = _make_task("Amon.tas", timerange="2000/P1M")
        broad = _make_task("Amon.tas", timerange="1850/2021")

        with patch.object(
            orch,
            "_process_task",
            side_effect=lambda t: TaskResult(task=t, status="done"),
        ) as mock_proc:
            orch.prepare_tasks([narrow, broad])

        assert mock_proc.call_count == 1
        processed = mock_proc.call_args.args[0]
        assert processed.timerange == "1850/2021"


# ---------------------------------------------------------------------------
# _process_task
# ---------------------------------------------------------------------------


class TestProcessTask:
    def test_unsupported_variable_returns_skipped(self, tmp_path):
        """A variable not in the index must produce a 'skipped' result."""
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.totally_nonexistent_xyz_abc")
        result = orch._process_task(task)
        assert result.status == "skipped"

    def test_no_raw_files_returns_failed(self, tmp_path):
        """A supported variable with no matching raw files must fail."""
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        # Amon.tas is a real supported variable; with empty input_root no files found
        task = _make_task("Amon.tas")
        result = orch._process_task(task)
        assert result.status == "failed"
        assert "No raw input files" in result.error

    def test_cached_result_returned(self, tmp_path):
        """When the cache is up-to-date the task must report 'cached'."""
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.tas")
        fake_raw = tmp_path / "raw.nc"
        fake_raw.touch()
        fake_output = tmp_path / "output.nc"
        fake_output.touch()
        with (
            patch.object(orch._index, "is_supported", return_value=True),
            patch.object(
                orch._index,
                "get",
                return_value=MagicMock(resource_file=None, calculation_type="direct"),
            ),
            patch.object(orch._finder, "find", return_value=[fake_raw]),
            patch.object(orch, "_cache_is_fresh", return_value=True),
            patch.object(orch, "_expected_output_paths", return_value=[fake_output]),
        ):
            result = orch._process_task(task)
        assert result.status == "cached"
        assert fake_output in result.output_files

    def test_cached_path_prunes_overlapping_outputs(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.tas")
        fake_raw = tmp_path / "raw.nc"
        fake_raw.touch()

        out_dir = tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        old_output = (
            out_dir / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_200001-202112.nc"
        )
        new_output = (
            out_dir / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-202112.nc"
        )
        old_output.touch()
        new_output.touch()
        old_stat = old_output.stat().st_mtime
        os.utime(new_output, (old_stat + 10, old_stat + 10))

        with (
            patch.object(orch._index, "is_supported", return_value=True),
            patch.object(
                orch._index,
                "get",
                return_value=MagicMock(resource_file=None, calculation_type="direct"),
            ),
            patch.object(orch._finder, "find", return_value=[fake_raw]),
            patch.object(
                orch, "_expected_output_paths", return_value=[old_output, new_output]
            ),
        ):
            result = orch._process_task(task)

        assert result.status == "cached"
        assert new_output in result.output_files
        assert old_output not in result.output_files
        assert new_output.exists()
        assert not old_output.exists()

    def test_cache_not_fresh_if_output_does_not_cover_requested_timerange(
        self, tmp_path
    ):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.tas", timerange="1850/2021")

        raw = tmp_path / "raw.nc"
        raw.touch()
        out_dir = tmp_path / "CMIP6" / "CMIP" / "CSIRO" / "ACCESS-ESM1-6"
        out_dir.mkdir(parents=True, exist_ok=True)
        output = (
            out_dir / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_200001-202112.nc"
        )
        output.touch()

        with (
            patch.object(orch._index, "is_supported", return_value=True),
            patch.object(
                orch._index,
                "get",
                return_value=MagicMock(resource_file=None, calculation_type="direct"),
            ),
            patch.object(orch._finder, "find", return_value=[raw]),
            patch.object(orch, "_expected_output_paths", return_value=[output]),
            patch.object(
                orch,
                "_run_cmoriser",
                return_value=TaskResult(
                    task=task, status="done", output_files=[output]
                ),
            ) as mock_run,
        ):
            result = orch._process_task(task)

        assert result.status == "done"
        mock_run.assert_called_once()


class TestOutputPruning:
    def test_prunes_older_fully_overlapped_file(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.tas")

        out_dir = tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        old_file = (
            out_dir / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_200001-202112.nc"
        )
        new_file = (
            out_dir / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-202112.nc"
        )

        old_file.touch()
        new_file.touch()

        # Ensure deterministic ordering by mtime: new_file newer than old_file.
        old_stat = old_file.stat().st_mtime
        os.utime(new_file, (old_stat + 10, old_stat + 10))

        kept = orch._prune_overlapping_outputs(task, [old_file, new_file])

        assert new_file in kept
        assert old_file not in kept
        assert new_file.exists()
        assert not old_file.exists()

    def test_latest_alias_is_not_deleted_as_separate_overlap(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.tas")

        base = tmp_path / "Amon" / "tas" / "gn"
        ver = base / "v20260515"
        ver.mkdir(parents=True, exist_ok=True)
        (base / "latest").symlink_to(ver, target_is_directory=True)

        real_file = (
            ver / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-202112.nc"
        )
        alias_file = (
            base
            / "latest"
            / "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-202112.nc"
        )
        real_file.touch()

        kept = orch._prune_overlapping_outputs(task, [alias_file, real_file])

        assert real_file in kept
        assert len(kept) == 1
        assert real_file.exists()

    def test_dry_run_returns_done_without_cmorising(self, tmp_path):
        orch = CMORiseOrchestrator(
            input_root=tmp_path, cache_dir=tmp_path, dry_run=True
        )
        task = _make_task("Amon.tas")
        fake_raw = tmp_path / "raw.nc"
        fake_raw.touch()
        with (
            patch.object(orch._index, "is_supported", return_value=True),
            patch.object(
                orch._index,
                "get",
                return_value=MagicMock(resource_file=None, calculation_type="direct"),
            ),
            patch.object(orch._finder, "find", return_value=[fake_raw]),
            patch.object(orch, "_cache_is_fresh", return_value=False),
            patch.object(orch, "_expected_output_paths", return_value=[]),
        ):
            result = orch._process_task(task)
        assert result.status == "done"

    def test_resource_file_variable_with_fresh_cache_returns_cached(self, tmp_path):
        """Variables backed by a resource file need no raw input."""
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Ofx.areacello")
        fake_out = tmp_path / "areacello.nc"
        fake_out.touch()
        with (
            patch.object(orch._index, "is_supported", return_value=True),
            patch.object(
                orch._index,
                "get",
                return_value=MagicMock(
                    resource_file="areacello.nc", calculation_type="dataset_function"
                ),
            ),
            patch.object(orch._finder, "find", return_value=[]),
            patch.object(orch, "_cache_is_fresh", return_value=True),
            patch.object(orch, "_expected_output_paths", return_value=[fake_out]),
        ):
            result = orch._process_task(task)
        assert result.status == "cached"

    def test_run_cmoriser_exception_returns_failed(self, tmp_path):
        """A CMORiser exception must be caught and reported as 'failed'."""
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        task = _make_task("Amon.tas")
        fake_raw = tmp_path / "raw.nc"
        fake_raw.touch()
        with (
            patch.object(orch._index, "is_supported", return_value=True),
            patch.object(
                orch._index,
                "get",
                return_value=MagicMock(resource_file=None, calculation_type="direct"),
            ),
            patch.object(orch._finder, "find", return_value=[fake_raw]),
            patch.object(orch, "_cache_is_fresh", return_value=False),
            patch.object(orch, "_expected_output_paths", return_value=[]),
            patch.object(
                orch,
                "_run_cmoriser",
                return_value=TaskResult(
                    task=task, status="failed", error="CMORiser boom"
                ),
            ),
        ):
            result = orch._process_task(task)
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# _cache_is_fresh
# ---------------------------------------------------------------------------


class TestCacheIsFresh:
    def test_no_outputs_returns_false(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        assert orch._cache_is_fresh(_make_task(), [], []) is False

    def test_no_raw_files_with_outputs_returns_true(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        out = tmp_path / "out.nc"
        out.touch()
        assert orch._cache_is_fresh(_make_task(), [], [out]) is True

    def test_output_newer_than_raw_returns_true(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        raw = tmp_path / "raw.nc"
        raw.touch()
        out = tmp_path / "out.nc"
        out.touch()
        os.utime(raw, (0, 1000.0))
        os.utime(out, (0, 2000.0))
        assert orch._cache_is_fresh(_make_task(), [raw], [out]) is True

    def test_output_older_than_raw_returns_false(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        raw = tmp_path / "raw.nc"
        raw.touch()
        out = tmp_path / "out.nc"
        out.touch()
        os.utime(raw, (0, 2000.0))
        os.utime(out, (0, 1000.0))
        assert orch._cache_is_fresh(_make_task(), [raw], [out]) is False

    def test_missing_output_oserror_returns_false(self, tmp_path):
        orch = CMORiseOrchestrator(input_root=tmp_path, cache_dir=tmp_path)
        raw = tmp_path / "raw.nc"
        raw.touch()
        missing = tmp_path / "does_not_exist.nc"
        assert orch._cache_is_fresh(_make_task(), [raw], [missing]) is False


# ---------------------------------------------------------------------------
# summarise
# ---------------------------------------------------------------------------


class TestSummarise:
    def test_header_and_rows_printed(self, capsys):
        t1 = _make_task("Amon.tas")
        t2 = _make_task("Amon.pr")
        results = [
            TaskResult(task=t1, status="done"),
            TaskResult(task=t2, status="cached"),
        ]
        CMORiseOrchestrator.summarise(results)
        out = capsys.readouterr().out
        assert "Amon.tas" in out
        assert "done" in out
        assert "Amon.pr" in out
        assert "cached" in out

    def test_empty_results_prints_header_only(self, capsys):
        CMORiseOrchestrator.summarise([])
        out = capsys.readouterr().out
        # Header line must still appear
        assert "Variable" in out

    def test_failed_task_shows_error_note(self, capsys):
        t = _make_task("Amon.tas")
        results = [TaskResult(task=t, status="failed", error="No mapping found")]
        CMORiseOrchestrator.summarise(results)
        out = capsys.readouterr().out
        assert "No mapping found" in out

    def test_output_files_count_shown(self, capsys):
        t = _make_task("Amon.tas")
        fake_file = Path("/fake/output.nc")
        results = [TaskResult(task=t, status="done", output_files=[fake_file])]
        CMORiseOrchestrator.summarise(results)
        out = capsys.readouterr().out
        assert "1" in out
