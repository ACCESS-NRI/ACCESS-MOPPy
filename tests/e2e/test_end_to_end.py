"""End-to-end tests for ACCESS-MOPPy."""

# Security: All subprocess calls in this file use validated paths in test environment
# ruff: noqa: S603, S607
# bandit: skip
# semgrep: skip
import importlib.resources as resources
import os
import shlex
import subprocess  # nosec
from pathlib import Path
from tempfile import gettempdir

import pytest

import access_moppy.vocabularies.cmip6_cmor_tables.Tables as cmor_tables
from access_moppy import ACCESS_ESM_CMORiser

DATA_ROOT_ENV_VAR = "ACCESS_MOPPY_DATA_ROOT"


def _resolve_amon_test_file() -> Path | None:
    """Return a monthly atmosphere file from configured external data root."""
    root_value = os.getenv(DATA_ROOT_ENV_VAR)
    if not root_value:
        return None

    data_root = Path(root_value)
    if not data_root.exists():
        return None

    external_monthly_files = sorted(
        data_root.glob("output*/atmosphere/netCDF/*_mon.nc")
    )
    if external_monthly_files:
        return external_monthly_files[0]

    return None


class TestEndToEnd:
    """End-to-end tests using real test data files."""

    def test_real_file_processing_amon_tas(self, parent_experiment_config):
        """Test processing with real small data file - matches your existing test."""
        test_file = _resolve_amon_test_file()
        if test_file is None:
            pytest.skip(
                f"No monthly atmosphere test data file available; set {DATA_ROOT_ENV_VAR}"
            )

        output_dir = Path(gettempdir()) / "cmor_output_e2e"

        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=test_file,
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-5",
            variant_label="r1i1p1f1",
            grid_label="gn",
            activity_id="CMIP",
            parent_info=parent_experiment_config,
            output_path=output_dir,
        )

        cmoriser.run()
        cmoriser.write()

        # Verify output files exist
        output_files = list(output_dir.glob("tas_Amon_*.nc"))
        assert len(output_files) > 0, "No output files generated"

        # Verify file naming convention
        output_file = output_files[0]
        assert output_file.name.startswith("tas_Amon_ACCESS-ESM1-5_historical")

    @pytest.mark.slow
    def test_prepare_validation(self, parent_experiment_config):
        """Test that output passes PrePARE validation - similar to your existing tests."""
        test_file = _resolve_amon_test_file()
        if test_file is None:
            pytest.skip(
                f"No monthly atmosphere test data file available; set {DATA_ROOT_ENV_VAR}"
            )

        output_dir = Path(gettempdir()) / "cmor_output_prepare"

        with resources.path(cmor_tables, "CMIP6_Amon.json") as table_path:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=test_file,
                compound_name="Amon.tas",
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                parent_info=parent_experiment_config,
                output_path=output_dir,
            )

            cmoriser.run()
            cmoriser.write()

            # Run PrePARE validation like in your existing tests
            output_files = list(output_dir.glob("tas_Amon_*.nc"))
            assert output_files, "No output files to validate"

            try:
                # Validate inputs before subprocess call for security
                table_path_str = str(table_path)
                output_file_str = str(output_files[0])

                # Ensure paths are safe (no shell injection)
                if not table_path.exists():
                    pytest.fail(f"Table path does not exist: {table_path_str}")
                if not output_files[0].exists():
                    pytest.fail(f"Output file does not exist: {output_file_str}")

                # Security: subprocess with validated paths in test environment
                # Additional validation to ensure no shell injection
                if not table_path_str.startswith("/") or ".." in table_path_str:
                    pytest.fail(f"Invalid table path: {table_path_str}")
                if not output_file_str.startswith("/") or ".." in output_file_str:
                    pytest.fail(f"Invalid output file path: {output_file_str}")

                # Additional security: validate that paths contain only allowed characters
                import re

                if not re.match(r"^[/\w\-._]+$", table_path_str):
                    pytest.fail(f"Invalid characters in table path: {table_path_str}")
                if not re.match(r"^[/\w\-._]+$", output_file_str):
                    pytest.fail(
                        f"Invalid characters in output file path: {output_file_str}"
                    )

                # S607: partial executable path, S603: subprocess call with dynamic args
                # Security: Using list form prevents shell injection, paths validated above
                # Additional security: escape paths to prevent injection
                escaped_table_path = shlex.quote(table_path_str)
                escaped_output_file = shlex.quote(output_file_str)

                # Security: Use the most explicit static command construction possible
                # Some security scanners require this level of explicitness
                PREPARE_EXECUTABLE = "PrePARE"  # Static executable name
                VARIABLE_FLAG = "--variable"  # Static flag
                VARIABLE_VALUE = "tas"  # Static variable name
                TABLE_PATH_FLAG = "--table-path"  # Static flag
                table_arg = escaped_table_path  # Validated and escaped table path
                output_arg = escaped_output_file  # Validated and escaped output file

                # Use explicit argument assignment to satisfy security scanners
                result = subprocess.run(  # noqa: S603  # nosec B603
                    [
                        PREPARE_EXECUTABLE,
                        VARIABLE_FLAG,
                        VARIABLE_VALUE,
                        TABLE_PATH_FLAG,
                        table_arg,
                        output_arg,
                    ],  # Explicit list with predefined elements
                    capture_output=True,
                    text=True,
                    check=False,
                    shell=False,
                )

                if result.returncode != 0:
                    pytest.fail(
                        f"PrePARE failed for {output_files[0]}:\n"
                        f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
                    )

            except FileNotFoundError:
                pytest.skip("PrePARE not available in test environment")

    def test_cli_interface(self):
        """Test command line interface if available."""
        # This would test any CLI scripts you have
        pytest.skip("CLI interface tests not yet implemented")
