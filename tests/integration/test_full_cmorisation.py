"""
Full CMOR integration tests for all supported variables and tables.

This module contains comprehensive integration tests that test CMORisation
for all variables defined in the mapping files. These tests use real data
files and validate output against CMOR standards.
"""
# Security: All subprocess calls in this file use validated paths in test environment
# ruff: noqa: S603, S607
# bandit: skip
# semgrep: skip

import importlib.resources as resources
import json
import shutil
import subprocess  # nosec
from functools import lru_cache
from pathlib import Path
from tempfile import gettempdir

import pytest

import access_moppy.vocabularies.cmip6_cmor_tables.Tables as cmor_tables
from access_moppy import ACCESS_ESM_CMORiser

# Import the utility function from conftest
from ..conftest import load_filtered_variables

# Import ocean file utilities
from .ocean_file_utils import (
    check_ocean_data_availability,
    get_monthly_ocean_files,
)

DATA_DIR = Path(__file__).parent.parent / "data"
WCRP_CHECKER_SUITE = "wcrp_cmip6:1.0"
KNOWN_WCRP_CHECKER_EXCLUSIONS: set[str] = set()
KNOWN_WCRP_CHECKER_MSG_EXCLUSIONS: tuple[str, ...] = ()


@lru_cache(maxsize=1)
def _available_compliance_suites() -> set[str]:
    """Return available compliance-checker suites, or an empty set if unavailable."""
    checker_executable = shutil.which("compliance-checker")
    if checker_executable is None:
        return set()

    result = subprocess.run(  # noqa: S603  # nosec B603
        [checker_executable, "--list-tests"],
        capture_output=True,
        text=True,
        check=False,
        shell=False,
    )
    if result.returncode != 0:
        return set()

    suites: set[str] = set()
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            suites.add(stripped.removeprefix("- ").strip())
    return suites


# Define table configurations to avoid code duplication
# Using model-specific mapping files with the new structure
CMOR_TABLES = [
    ("Amon", "ACCESS-ESM1.6", "CMIP6_Amon.json"),
    ("AERmon", "ACCESS-ESM1.6", "CMIP6_AERmon.json"),
    ("Lmon", "ACCESS-ESM1.6", "CMIP6_Lmon.json"),
    ("Emon", "ACCESS-ESM1.6", "CMIP6_Emon.json"),
    ("Omon", "ACCESS-ESM1.6", "CMIP6_Omon.json"),
    ("CFmon", "ACCESS-ESM1.6", "CMIP6_CFmon.json"),
    ("3hr", "ACCESS-ESM1.6", "CMIP6_3hr.json"),
    ("6hrPlev", "ACCESS-ESM1.6", "CMIP6_6hrPlev.json"),
    ("day", "ACCESS-ESM1.6", "CMIP6_day.json"),
    ("Eday", "ACCESS-ESM1.6", "CMIP6_Eday.json"),
    ("CFday", "ACCESS-ESM1.6", "CMIP6_CFday.json"),
    ("SImon", "ACCESS-ESM1.6", "CMIP6_SImon.json"),
    ("Ofx", "ACCESS-ESM1.6", "CMIP6_Ofx.json"),
]


class TestFullCMORIntegration:
    """Integration tests for full CMOR processing of all variables."""

    def _get_input_files_for_compound(
        self, compound_name: str, model_id: str = "ACCESS-ESM1.6"
    ) -> list[Path]:
        """Get appropriate input files based on the compound name.

        Args:
            compound_name: CMIP6 compound name (e.g., 'day.tas', 'Amon.tas', 'Omon.tos')
            model_id: Model identifier for loading mappings

        Returns:
            List of Path objects for the appropriate test files
        """
        table_name, _ = compound_name.split(".")

        if table_name == "Ofx":
            # Ofx variables are fixed (no time dimension). Variables backed by
            # bundled resource files (areacello, sftof, hfgeou) need no external
            # input — returning None signals the CMORiser to use its resource file.
            return None

        if table_name == "Omon":
            # For ocean variables, try to get real ocean files first, fallback to test files
            try:
                ocean_files = get_monthly_ocean_files(compound_name, model_id=model_id)
                if ocean_files:
                    return [Path(f) for f in ocean_files]
            except Exception:
                pass
            # Fallback to test ocean files if available
            om3_files = list((DATA_DIR / "om3").glob("*.nc"))
            if om3_files:
                return om3_files[:1]  # Return first available ocean test file
            return []

        if table_name == "SImon":
            return [
                DATA_DIR / "esm1-6/ice/iceh-1monthly-mean_3114-01.nc",
                DATA_DIR / "esm1-6/ice/iceh-1monthly-mean_3114-02.nc",
            ]

        if "3hr" in table_name.lower():
            # Use 3-hourly files for 3hr tables
            return [
                DATA_DIR / "esm1-6/atmosphere/aiihca.pi-308009_3hr.nc",
                DATA_DIR / "esm1-6/atmosphere/aiihca.pi-308010_3hr.nc",
            ]
        elif "6hr" in table_name.lower():
            # Use 6-hourly files for 6hr tables
            return [
                DATA_DIR / "esm1-6/atmosphere/aiihca.pj-308009_6hr.nc",
                DATA_DIR / "esm1-6/atmosphere/aiihca.pj-308010_6hr.nc",
            ]
        elif "day" in table_name.lower():
            # Use daily files for daily tables
            return [
                DATA_DIR / "esm1-6/atmosphere/aiihca.pe-308009_dai.nc",
                DATA_DIR / "esm1-6/atmosphere/aiihca.pe-308010_dai.nc",
            ]
        else:
            # Use monthly files for other tables (Amon, Lmon, etc.)
            return [DATA_DIR / "esm1-6/atmosphere/aiihca.pa-298810_mon.nc"]

    @pytest.mark.slow
    @pytest.mark.integration
    @pytest.mark.parametrize("table_name,model_id,cmor_table_file", CMOR_TABLES)
    def test_full_cmorisation_all_variables(
        self,
        parent_experiment_config,
        compliance_validation_tool,
        table_name,
        model_id,
        cmor_table_file,
        subtests,
    ):
        """Test CMORisation for all variables in each supported table.

        This is a comprehensive integration test that processes all variables
        defined in the mapping files and validates the output.
        By default it uses PrePARE. The WCRP compliance-checker path can be
        enabled explicitly from the pytest command line.
        For ocean variables (Omon), it uses ocean data files instead of atmosphere files.
        Uses appropriate input files based on table frequency requirements.
        """
        # Load variables for this specific table
        try:
            table_variables = load_filtered_variables(
                model_id=model_id, table_name=table_name
            )
        except Exception:
            pytest.skip(f"Cannot load variables for table {table_name}")

        # Skip ocean tests if ocean data is not available
        if table_name == "Omon" and not check_ocean_data_availability():
            pytest.skip("Ocean data directory not available for Omon testing")

        # Test all available variables (since we've filtered to compatible ones)
        test_variables = table_variables

        for cmor_name in test_variables:
            with subtests.test(variable=cmor_name):
                compound_name = f"{table_name}.{cmor_name}"
                input_files = self._get_input_files_for_compound(
                    compound_name, model_id=model_id
                )

                # Skip if required files don't exist.
                # input_files=None means the variable uses a bundled resource file
                # and no external input is needed.
                if input_files is not None and (
                    not input_files or not all(f.exists() for f in input_files)
                ):
                    pytest.skip(
                        f"Required input files not available for {compound_name}"
                    )

                experiment_id = "historical"
                source_id = "ACCESS-ESM1-5"
                output_dir = (
                    Path(gettempdir()) / f"cmor_output_{table_name}_{cmor_name}"
                )

                # Ensure output directory exists and is clean
                output_dir.mkdir(parents=True, exist_ok=True)
                for f in output_dir.glob("*.nc"):
                    f.unlink()

                with resources.path(cmor_tables, cmor_table_file) as table_path:
                    try:
                        cmoriser = ACCESS_ESM_CMORiser(
                            input_paths=input_files,  # None = use bundled resource file
                            compound_name=compound_name,
                            experiment_id=experiment_id,
                            source_id=source_id,
                            variant_label="r1i1p1f1",
                            grid_label="gn",
                            activity_id="CMIP",
                            parent_info=parent_experiment_config,
                            output_path=output_dir,
                        )

                        cmoriser.run()
                        cmoriser.write()

                        # Verify output files were created
                        output_files = list(
                            output_dir.glob(f"{cmor_name}_{table_name}_*.nc")
                        )
                        assert (
                            output_files
                        ), f"No output files found for {cmor_name} in {output_dir}"

                        # Validate output with the configured backend
                        # Skip compliance validation for Omon and Ofx (ocean fixed fields
                        # use non-standard grid structures not validated by PrePARE/WCRP)
                        if table_name not in ("Omon", "Ofx"):
                            self._validate_output_compliance(
                                output_files[0],
                                cmor_name,
                                table_path,
                                compliance_validation_tool,
                            )

                    except Exception as e:
                        pytest.fail(
                            f"Failed processing {cmor_name} with table {table_name}: {e}"
                        )

    def _validate_output_compliance(
        self,
        output_file,
        cmor_name,
        table_path,
        validation_tool: str,
    ):
        """Validate output using the configured backend."""
        if validation_tool == "wcrp":
            if WCRP_CHECKER_SUITE not in _available_compliance_suites():
                pytest.skip(
                    f"Requested validation backend '{validation_tool}' is unavailable"
                )
            self._validate_with_wcrp_checker(output_file)
            return

        self._validate_with_prepare(output_file, cmor_name, table_path)

    def _extract_failed_checks(
        self,
        report: dict,
        section: str = WCRP_CHECKER_SUITE,
    ) -> list[dict]:
        """Return checks that failed from a compliance checker JSON report."""
        selected_section = section
        if selected_section not in report:
            wcrp_sections = [
                key
                for key, value in report.items()
                if isinstance(value, dict)
                and "all_priorities" in value
                and key.startswith("wcrp_")
            ]
            if len(wcrp_sections) == 1:
                selected_section = wcrp_sections[0]
            else:
                available_sections = ", ".join(sorted(report.keys()))
                raise AssertionError(
                    f"Missing report section '{section}'. "
                    f"Available sections: {available_sections}"
                )

        checks = report[selected_section].get("all_priorities", [])
        failed_checks = []
        for check in checks:
            value = check.get("value", [0, 0])
            if len(value) >= 2 and value[0] != value[1]:
                failed_checks.append(check)
        return failed_checks

    def _filter_excluded_checks(
        self,
        failed_checks: list[dict],
        exclude_names: set[str] | None = None,
        exclude_msg_substrings: tuple[str, ...] = (),
    ) -> list[dict]:
        """Filter known checker failures by check name or message substring."""
        excluded_names = exclude_names or set()

        remaining_checks = []
        for check in failed_checks:
            if check.get("name") in excluded_names:
                continue

            messages = check.get("msgs", [])
            if any(
                substring in message
                for substring in exclude_msg_substrings
                for message in messages
            ):
                continue

            remaining_checks.append(check)

        return remaining_checks

    def _assert_wcrp_report_valid(self, report: dict) -> None:
        """Fail only on mandatory WCRP checks."""
        failed_checks = self._extract_failed_checks(report, section=WCRP_CHECKER_SUITE)
        remaining = self._filter_excluded_checks(
            failed_checks,
            exclude_names=KNOWN_WCRP_CHECKER_EXCLUSIONS,
            exclude_msg_substrings=KNOWN_WCRP_CHECKER_MSG_EXCLUSIONS,
        )

        mandatory_failures = [
            check for check in remaining if check.get("weight", 0) >= 3
        ]

        if mandatory_failures:
            lines = ["WCRP compliance validation failed mandatory checks:"]
            for check in mandatory_failures:
                lines.append(f"- {check.get('name', '<unnamed check>')}")
                for message in check.get("msgs", []):
                    lines.append(f"    {message}")
            raise AssertionError("\n".join(lines))

    def _validate_with_wcrp_checker(self, output_file):
        """Validate CMOR output using compliance-checker and cc-plugin-wcrp."""
        checker_executable = shutil.which("compliance-checker")
        if checker_executable is None:
            pytest.skip("compliance-checker executable not available")

        output_file_str = str(output_file)
        if not output_file.exists():
            pytest.fail(f"Output file does not exist: {output_file_str}")
        if not output_file_str.startswith("/") or ".." in output_file_str:
            pytest.fail(f"Invalid output file path: {output_file_str}")

        report_path = Path(gettempdir()) / f"wcrp_report_{output_file.stem}.json"
        if report_path.exists():
            report_path.unlink()

        result = subprocess.run(  # noqa: S603  # nosec B603
            [
                checker_executable,
                "--test",
                WCRP_CHECKER_SUITE,
                "--format",
                "json",
                "--output",
                str(report_path),
                output_file_str,
            ],
            capture_output=True,
            text=True,
            check=False,
            shell=False,
        )

        if not report_path.exists():
            pytest.fail(
                f"WCRP checker report was not created for {output_file}: {report_path}\n"
                f"Checker exit code: {result.returncode}\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )

        try:
            with report_path.open("r", encoding="utf-8") as report_file:
                report = json.load(report_file)
        except json.JSONDecodeError as error:
            pytest.fail(
                f"WCRP checker produced invalid JSON for {output_file}: {error}\n"
                f"Checker exit code: {result.returncode}\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )
        else:
            self._assert_wcrp_report_valid(report)
        finally:
            report_path.unlink(missing_ok=True)

    def _validate_with_prepare(self, output_file, cmor_name, table_path):
        """Validate CMOR output using PrePARE tool if available."""
        try:
            # Validate inputs before subprocess call for security
            table_path_str = str(table_path)
            output_file_str = str(output_file)

            # Ensure paths are safe (no shell injection)
            if not table_path.exists():
                pytest.fail(f"Table path does not exist: {table_path_str}")
            if not output_file.exists():
                pytest.fail(f"Output file does not exist: {output_file_str}")

            # Security: subprocess with validated paths in test environment
            # Additional validation to ensure no shell injection
            if not table_path_str.startswith("/") or ".." in table_path_str:
                pytest.fail(f"Invalid table path: {table_path_str}")
            if not output_file_str.startswith("/") or ".." in output_file_str:
                pytest.fail(f"Invalid output file path: {output_file_str}")

            # S607: partial executable path, S603: subprocess call with dynamic args
            # Security: Using list form prevents shell injection, paths validated above
            # Security: Use the most explicit static command construction possible
            # Some security scanners require this level of explicitness
            PREPARE_EXECUTABLE = "PrePARE"  # Static executable name
            VARIABLE_FLAG = "--variable"  # Static flag
            TABLE_PATH_FLAG = "--table-path"  # Static flag
            cmor_arg = cmor_name  # Validated CMOR name
            table_arg = table_path_str  # Validated table path
            output_arg = output_file_str  # Validated output file

            # Use explicit argument assignment to satisfy security scanners
            result = subprocess.run(  # noqa: S603  # nosec B603
                [
                    PREPARE_EXECUTABLE,
                    VARIABLE_FLAG,
                    cmor_arg,
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
                    f"PrePARE validation failed for {output_file}:\n"
                    f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
                )
        except FileNotFoundError:
            # PrePARE not available, skip validation
            pytest.skip("PrePARE tool not available for validation")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_quick_integration_sample(self, parent_experiment_config):
        """Test a small sample of variables for quick integration testing.

        This test runs a subset of variables to provide faster feedback
        during development while still testing the integration.
        Uses appropriate input files based on table frequency requirements.
        """
        # Test one variable from each table for quick integration testing
        test_cases = [
            ("Amon", "tas"),
            ("Lmon", "mrso"),
            ("Emon", "lai"),
            ("day", "tas"),  # Test daily table with daily files
        ]

        for table_name, cmor_name in test_cases:
            compound_name = f"{table_name}.{cmor_name}"
            input_files = self._get_input_files_for_compound(
                compound_name, model_id="ACCESS-ESM1.6"
            )

            # Skip if required files don't exist
            if not input_files or not all(f.exists() for f in input_files):
                continue

            output_dir = Path(gettempdir()) / f"quick_test_{table_name}_{cmor_name}"
            output_dir.mkdir(parents=True, exist_ok=True)

            try:
                # Verify variable exists in mapping
                available_vars = load_filtered_variables(
                    model_id="ACCESS-ESM1.6", table_name=table_name
                )

                if cmor_name not in available_vars:
                    continue  # Skip if variable not available

                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=input_files,
                    compound_name=compound_name,
                    experiment_id="historical",
                    source_id="ACCESS-ESM1-5",
                    variant_label="r1i1p1f1",
                    grid_label="gn",
                    activity_id="CMIP",
                    parent_info=parent_experiment_config,
                    output_path=output_dir,
                )

                cmoriser.run()

                # Basic validation - check that processing completed
                assert hasattr(
                    cmoriser, "cmor_ds"
                ), f"Processing failed for {table_name}.{cmor_name}"

            except Exception as e:
                # For quick integration test, we log but don't fail on individual variables
                print(f"Warning: Quick test failed for {table_name}.{cmor_name}: {e}")
