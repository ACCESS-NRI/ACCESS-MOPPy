"""
Unit tests for access_moppy.esmval.cli_commands
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from access_moppy.esmval.cli_commands import (
    CMORiseCommand,
    PrepareResult,
    _build_parser,
    _configure_logging,
    _extract_cmor_versions,
    _parse_pattern_overrides,
    _pin_recipe_dataset_version,
    main_prepare,
    main_run,
)

# ---------------------------------------------------------------------------
# _build_parser
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_missing_required_args_raises_systemexit(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_parses_positional_and_required_options(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["my_recipe.yml", "--input-root", "/data/in", "--cache-dir", "/data/cache"]
        )
        assert args.recipe == "my_recipe.yml"
        assert args.input_root == "/data/in"
        assert args.cache_dir == "/data/cache"

    def test_default_model_id(self):
        parser = _build_parser()
        args = parser.parse_args(["r.yml", "--input-root", "/in", "--cache-dir", "/c"])
        assert args.model_id == "ACCESS-ESM1.6"

    def test_workers_default_one(self):
        parser = _build_parser()
        args = parser.parse_args(["r.yml", "--input-root", "/in", "--cache-dir", "/c"])
        assert args.workers == 1

    def test_dry_run_default_false(self):
        parser = _build_parser()
        args = parser.parse_args(["r.yml", "--input-root", "/in", "--cache-dir", "/c"])
        assert args.dry_run is False

    def test_dry_run_flag(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["r.yml", "--input-root", "/in", "--cache-dir", "/c", "--dry-run"]
        )
        assert args.dry_run is True

    def test_pattern_is_repeatable(self):
        parser = _build_parser()
        args = parser.parse_args(
            [
                "r.yml",
                "--input-root",
                "/in",
                "--cache-dir",
                "/c",
                "--pattern",
                "Amon.tas:output*/atm/*.nc",
                "--pattern",
                "Omon.tos:output*/ocean/*.nc",
            ]
        )
        assert len(args.pattern) == 2

    def test_verbose_short_flag(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["r.yml", "--input-root", "/in", "--cache-dir", "/c", "-v"]
        )
        assert args.verbose is True

    def test_verbose_long_flag(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["r.yml", "--input-root", "/in", "--cache-dir", "/c", "--verbose"]
        )
        assert args.verbose is True

    def test_config_option(self):
        parser = _build_parser()
        args = parser.parse_args(
            [
                "r.yml",
                "--input-root",
                "/in",
                "--cache-dir",
                "/c",
                "--config",
                "/cfg/user.yml",
            ]
        )
        assert args.config == "/cfg/user.yml"

    def test_output_config_option(self):
        parser = _build_parser()
        args = parser.parse_args(
            [
                "r.yml",
                "--input-root",
                "/in",
                "--cache-dir",
                "/c",
                "--output-config",
                "/out/cfg.yml",
            ]
        )
        assert args.output_config == "/out/cfg.yml"

    def test_pattern_default_empty_list(self):
        parser = _build_parser()
        args = parser.parse_args(["r.yml", "--input-root", "/in", "--cache-dir", "/c"])
        assert args.pattern == []


# ---------------------------------------------------------------------------
# _parse_pattern_overrides
# ---------------------------------------------------------------------------


class TestParsePatternOverrides:
    def test_empty_list_returns_empty_dict(self):
        assert _parse_pattern_overrides([]) == {}

    def test_single_valid_entry(self):
        result = _parse_pattern_overrides(["Amon.tas:output*/atm/*.nc"])
        assert result == {"Amon.tas": "output*/atm/*.nc"}

    def test_multiple_entries(self):
        result = _parse_pattern_overrides(
            [
                "Amon.tas:output*/atm/*.nc",
                "Omon.tos:output*/ocean/*.nc",
            ]
        )
        assert result == {
            "Amon.tas": "output*/atm/*.nc",
            "Omon.tos": "output*/ocean/*.nc",
        }

    def test_colon_in_glob_preserved(self):
        # Only the first colon separates compound_name from glob
        result = _parse_pattern_overrides(["Amon.tas:output*/a:b/*.nc"])
        assert result["Amon.tas"] == "output*/a:b/*.nc"

    def test_missing_colon_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid --pattern"):
            _parse_pattern_overrides(["AmonTasNoColon"])

    def test_whitespace_stripped(self):
        result = _parse_pattern_overrides([" Amon.tas : output*/atm/*.nc "])
        assert "Amon.tas" in result
        assert result["Amon.tas"] == "output*/atm/*.nc"


# ---------------------------------------------------------------------------
# _configure_logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    def test_does_not_raise_non_verbose(self):
        _configure_logging(verbose=False)  # must not raise

    def test_does_not_raise_verbose(self):
        _configure_logging(verbose=True)  # must not raise

    def test_noisy_libs_set_to_warning_when_not_verbose(self):
        _configure_logging(verbose=False)
        for name in ("distributed", "asyncio", "parsl", "iris", "cf_units"):
            assert logging.getLogger(name).level == logging.WARNING


# ---------------------------------------------------------------------------
# Recipe version pinning helpers
# ---------------------------------------------------------------------------


class TestRecipeVersionPinningHelpers:
    def test_extract_cmor_versions_collects_unique_versions(self, tmp_path):
        out_a = (
            tmp_path
            / "CMIP6/CMIP/CSIRO/ACCESS-ESM1-6/historical/r1i1p1f1/Amon/tas/gn/v20260514/tas_1.nc"
        )
        out_b = (
            tmp_path
            / "CMIP6/CMIP/CSIRO/ACCESS-ESM1-6/historical/r1i1p1f1/Amon/pr/gn/v20260515/pr_1.nc"
        )
        out_c = (
            tmp_path
            / "CMIP6/CMIP/CSIRO/ACCESS-ESM1-6/historical/r1i1p1f1/Amon/pr/gn/v20260515/pr_2.nc"
        )
        versions = _extract_cmor_versions(
            [
                MagicMock(output_files=[out_a]),
                MagicMock(output_files=[out_b, out_c]),
            ]
        )
        assert versions == ["v20260514", "v20260515"]

    def test_pin_recipe_dataset_version_writes_pinned_copy(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical, ensemble: r1i1p1f1, grid: gn}\n"
            "  - {dataset: MPI-ESM1-2-HR, project: CMIP6, exp: historical, ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )

        pinned_path, updated = _pin_recipe_dataset_version(recipe, "v20260514")

        assert pinned_path is not None
        assert updated == 1
        assert pinned_path.exists()
        text = pinned_path.read_text()
        assert "version: v20260514" in text

    def test_pin_recipe_dataset_version_returns_none_when_no_access_dataset(
        self, tmp_path
    ):
        recipe = tmp_path / "recipe.yml"
        recipe.write_text(
            "datasets:\n"
            "  - {dataset: MPI-ESM1-2-HR, project: CMIP6, exp: historical, ensemble: r1i1p1f1, grid: gn}\n"
            "diagnostics:\n"
            "  d1:\n"
            "    variables:\n"
            "      tas:\n"
            "        mip: Amon\n"
            "    scripts:\n"
            "      null: {script: null}\n"
        )

        pinned_path, updated = _pin_recipe_dataset_version(recipe, "v20260514")
        assert pinned_path is None
        assert updated == 0


# ---------------------------------------------------------------------------
# main_prepare
# ---------------------------------------------------------------------------


class TestMainPrepare:
    def test_returns_zero_on_success(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/out/cfg.yml"),
        ):
            rc = main_prepare(
                [str(recipe), "--input-root", "/in", "--cache-dir", "/cache"]
            )
        assert rc == 0

    def test_returns_one_on_prepare_exception(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            side_effect=RuntimeError("boom"),
        ):
            rc = main_prepare(
                [str(recipe), "--input-root", "/in", "--cache-dir", "/cache"]
            )
        assert rc == 1

    def test_invalid_pattern_triggers_systemexit(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with pytest.raises(SystemExit):
            main_prepare(
                [
                    str(recipe),
                    "--input-root",
                    "/in",
                    "--cache-dir",
                    "/cache",
                    "--pattern",
                    "NoColonHere",
                ]
            )

    def test_missing_required_arg_triggers_systemexit(self):
        with pytest.raises(SystemExit):
            main_prepare(["recipe.yml"])

    def test_dry_run_passed_to_prepare(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/out/cfg.yml"),
        ) as mock_prep:
            main_prepare(
                [
                    str(recipe),
                    "--input-root",
                    "/in",
                    "--cache-dir",
                    "/cache",
                    "--dry-run",
                ]
            )
        assert mock_prep.call_args.kwargs.get("dry_run") is True

    def test_workers_passed_to_prepare(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/out/cfg.yml"),
        ) as mock_prep:
            main_prepare(
                [
                    str(recipe),
                    "--input-root",
                    "/in",
                    "--cache-dir",
                    "/cache",
                    "--workers",
                    "4",
                ]
            )
        assert mock_prep.call_args.kwargs.get("workers") == 4


# ---------------------------------------------------------------------------
# main_run
# ---------------------------------------------------------------------------


class TestMainRun:
    def test_returns_zero_on_dry_run(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/fake/cfg.yml"),
        ):
            rc = main_run(
                [
                    str(recipe),
                    "--input-root",
                    "/in",
                    "--cache-dir",
                    "/cache",
                    "--dry-run",
                ]
            )
        assert rc == 0

    def test_returns_one_on_prepare_exception(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            side_effect=RuntimeError("fail"),
        ):
            rc = main_run([str(recipe), "--input-root", "/in", "--cache-dir", "/cache"])
        assert rc == 1

    def test_calls_esmvaltool_subprocess(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        fake_cfg = tmp_path / "cfg.yml"
        fake_cfg.touch()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                return_value=fake_cfg,
            ),
            patch(
                "access_moppy.esmval.config_gen._default_user_config_dir",
                return_value=tmp_path,
            ),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            rc = main_run([str(recipe), "--input-root", "/in", "--cache-dir", "/cache"])
        assert rc == 0
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "esmvaltool" in cmd
        assert "run" in cmd

    def test_uses_pinned_recipe_from_prepare_result(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        pinned_recipe = tmp_path / "recipe.moppy-pinned.yml"
        pinned_recipe.touch()
        fake_cfg = tmp_path / "cfg.yml"
        fake_cfg.touch()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                return_value=PrepareResult(
                    cfg_path=fake_cfg,
                    recipe_to_run=pinned_recipe,
                    pinned_version="v20260514",
                ),
            ),
            patch(
                "access_moppy.esmval.config_gen._default_user_config_dir",
                return_value=tmp_path,
            ),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            rc = main_run([str(recipe), "--input-root", "/in", "--cache-dir", "/cache"])
        assert rc == 0
        cmd = mock_run.call_args[0][0]
        assert str(pinned_recipe) in cmd

    def test_no_env_when_config_in_default_dir(self, tmp_path):
        """When config is already in the default dir, env must be None."""
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        fake_cfg = tmp_path / "moppy-esmval-data.yml"
        fake_cfg.touch()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                return_value=fake_cfg,
            ),
            patch(
                "access_moppy.esmval.config_gen._default_user_config_dir",
                return_value=tmp_path,
            ),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            main_run([str(recipe), "--input-root", "/in", "--cache-dir", "/cache"])
        assert mock_run.call_args.kwargs.get("env") is None

    def test_sets_env_when_config_not_in_default_dir(self, tmp_path):
        """When config is in a non-default dir, ESMVALTOOL_CONFIG_DIR must be set."""
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cfg_subdir = tmp_path / "custom"
        cfg_subdir.mkdir()
        fake_cfg = cfg_subdir / "moppy-esmval-data.yml"
        fake_cfg.touch()
        default_dir = tmp_path / "default"
        default_dir.mkdir()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                return_value=fake_cfg,
            ),
            patch(
                "access_moppy.esmval.config_gen._default_user_config_dir",
                return_value=default_dir,
            ),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            main_run([str(recipe), "--input-root", "/in", "--cache-dir", "/cache"])
        env = mock_run.call_args.kwargs.get("env")
        assert env is not None
        assert "ESMVALTOOL_CONFIG_DIR" in env
        assert env["ESMVALTOOL_CONFIG_DIR"] == str(cfg_subdir)

    def test_returns_subprocess_returncode(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        fake_cfg = tmp_path / "cfg.yml"
        fake_cfg.touch()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                return_value=fake_cfg,
            ),
            patch(
                "access_moppy.esmval.config_gen._default_user_config_dir",
                return_value=tmp_path,
            ),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=42)
            rc = main_run([str(recipe), "--input-root", "/in", "--cache-dir", "/cache"])
        assert rc == 42

    def test_esmvaltool_args_forwarded(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        fake_cfg = tmp_path / "cfg.yml"
        fake_cfg.touch()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                return_value=fake_cfg,
            ),
            patch(
                "access_moppy.esmval.config_gen._default_user_config_dir",
                return_value=tmp_path,
            ),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            # Pass the extra args as a single quoted string (as the user would on a shell)
            main_run(
                [
                    str(recipe),
                    "--input-root",
                    "/in",
                    "--cache-dir",
                    "/cache",
                    "--esmvaltool-args=--skip-nonexistent",
                ]
            )
        cmd = mock_run.call_args[0][0]
        assert "--skip-nonexistent" in cmd


# ---------------------------------------------------------------------------
# CMORiseCommand
# ---------------------------------------------------------------------------


class TestCMORiseCommand:
    def test_callable_invokes_prepare(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cmd = CMORiseCommand()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/cfg.yml"),
        ) as mock_prep:
            cmd(recipe=str(recipe), input_root="/in", cache_dir="/cache")
        mock_prep.assert_called_once()

    def test_invalid_pattern_triggers_systemexit(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cmd = CMORiseCommand()
        with pytest.raises(SystemExit):
            cmd(
                recipe=str(recipe),
                input_root="/in",
                cache_dir="/cache",
                pattern=["NoColon"],
            )

    def test_prepare_exception_triggers_systemexit(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cmd = CMORiseCommand()
        with (
            patch(
                "access_moppy.esmval.cli_commands._prepare",
                side_effect=RuntimeError("fail"),
            ),
            pytest.raises(SystemExit),
        ):
            cmd(recipe=str(recipe), input_root="/in", cache_dir="/cache")

    def test_dry_run_passed_to_prepare(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cmd = CMORiseCommand()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/cfg.yml"),
        ) as mock_prep:
            cmd(
                recipe=str(recipe),
                input_root="/in",
                cache_dir="/cache",
                dry_run=True,
            )
        assert mock_prep.call_args.kwargs.get("dry_run") is True

    def test_pattern_list_converted_to_overrides(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cmd = CMORiseCommand()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/cfg.yml"),
        ) as mock_prep:
            cmd(
                recipe=str(recipe),
                input_root="/in",
                cache_dir="/cache",
                pattern=["Amon.tas:output*/atm/*.nc"],
            )
        assert mock_prep.call_args.kwargs["pattern_overrides"] == {
            "Amon.tas": "output*/atm/*.nc"
        }

    def test_none_pattern_treated_as_empty(self, tmp_path):
        recipe = tmp_path / "recipe.yml"
        recipe.touch()
        cmd = CMORiseCommand()
        with patch(
            "access_moppy.esmval.cli_commands._prepare",
            return_value=Path("/cfg.yml"),
        ) as mock_prep:
            cmd(
                recipe=str(recipe),
                input_root="/in",
                cache_dir="/cache",
                pattern=None,
            )
        assert mock_prep.call_args.kwargs["pattern_overrides"] == {}
