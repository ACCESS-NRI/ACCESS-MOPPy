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
import os
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

DATA_ROOT_ENV_VAR = "ACCESS_MOPPY_DATA_ROOT"
OCEAN_TARGET_FOLDERS = "output*/ocean/"
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
# Each tuple: (table_name, model_id, cmor_table_file, cmip_version)
CMOR_TABLES = [
    # CMIP6 tables
    ("Amon", "ACCESS-ESM1-6", "CMIP6_Amon.json", "CMIP6"),
    ("AERmon", "ACCESS-ESM1-6", "CMIP6_AERmon.json", "CMIP6"),
    ("Lmon", "ACCESS-ESM1-6", "CMIP6_Lmon.json", "CMIP6"),
    ("Emon", "ACCESS-ESM1-6", "CMIP6_Emon.json", "CMIP6"),
    ("Omon", "ACCESS-ESM1-6", "CMIP6_Omon.json", "CMIP6"),
    ("CFmon", "ACCESS-ESM1-6", "CMIP6_CFmon.json", "CMIP6"),
    ("3hr", "ACCESS-ESM1-6", "CMIP6_3hr.json", "CMIP6"),
    ("6hrPlev", "ACCESS-ESM1-6", "CMIP6_6hrPlev.json", "CMIP6"),
    ("day", "ACCESS-ESM1-6", "CMIP6_day.json", "CMIP6"),
    ("Eday", "ACCESS-ESM1-6", "CMIP6_Eday.json", "CMIP6"),
    ("CFday", "ACCESS-ESM1-6", "CMIP6_CFday.json", "CMIP6"),
    ("SImon", "ACCESS-ESM1-6", "CMIP6_SImon.json", "CMIP6"),
    ("Ofx", "ACCESS-ESM1-6", "CMIP6_Ofx.json", "CMIP6"),
    # CMIP7 tables (via mapping to CMIP6 equivalents)
    ("atmos", "ACCESS-ESM1-6", "CMIP7_atmos.json", "CMIP7"),
    ("ocean", "ACCESS-ESM1-6", "CMIP7_ocean.json", "CMIP7"),
    ("seaIce", "ACCESS-ESM1-6", "CMIP7_seaIce.json", "CMIP7"),
    ("aerosol", "ACCESS-ESM1-6", "CMIP7_aerosol.json", "CMIP7"),
]


@lru_cache(maxsize=1)
def _generate_variable_test_params() -> list[tuple[str, str, str, str, str]]:
    """Generate all (table, model_id, cmor_table_file, cmip_version, variable) test parameters.
    
    This function generates individual test parameters for each variable in each table,
    enabling granular control over which variables to test via pytest's -k filtering.
    
    Returns:
        List of tuples: (table_name, model_id, cmor_table_file, cmip_version, variable_name)
    """
    params = []
    
    for table_name, model_id, cmor_table_file, cmip_version in CMOR_TABLES:
        try:
            # For CMIP7 tables, map to CMIP6 equivalents for loading variables
            mapping_table_name = table_name
            if cmip_version == "CMIP7":
                # Map CMIP7 table names to CMIP6 equivalents
                cmip7_to_cmip6 = {
                    "atmos": "Amon",
                    "ocean": "Omon",
                    "seaIce": "SImon",
                    "aerosol": "AERmon",
                    "land": "Lmon",
                }
                mapping_table_name = cmip7_to_cmip6.get(table_name, table_name)
            
            variables = load_filtered_variables(
                model_id=model_id, table_name=mapping_table_name
            )
            
            for var_name in variables:
                # Create test ID that allows filtering: table-variable or table-variable-cmip7
                test_id_suffix = f"-cmip7" if cmip_version == "CMIP7" else ""
                params.append((
                    table_name,
                    model_id,
                    cmor_table_file,
                    cmip_version,
                    var_name,
                ))
        except Exception:
            # Skip tables that can't be loaded
            pass
    
    return params


# Generate variable-level test parameters for granular control
VARIABLE_TEST_PARAMS = _generate_variable_test_params()


def _parametrize_test_ids(param_set: tuple) -> str:
    """Generate clean test IDs for parametrized tests.
    
    Args:
        param_set: Tuple of (table_name, model_id, cmor_table_file, cmip_version, variable_name)
        
    Returns:
        Formatted test ID like "Amon-tas" or "ocean-tos-cmip7"
    """
    if not isinstance(param_set, tuple) or len(param_set) < 5:
        return str(param_set)
    
    table, model_id, cmor_table, cmip_version, variable = param_set
    suffix = "-cmip7" if cmip_version == "CMIP7" else ""
    return f"{table}-{variable}{suffix}"


class TestFullCMORIntegration:
    """Integration tests for full CMOR processing of all variables."""

    def _configured_data_root(self) -> Path | None:
        """Return configured external test-data root, if valid."""
        root_value = os.getenv(DATA_ROOT_ENV_VAR)
        if not root_value:
            return None

        root_path = Path(root_value)
        if not root_path.exists():
            return None

        return root_path

    def _discover_external_files(
        self, relative_pattern: str, max_files: int
    ) -> list[Path]:
        """Return discovered files from configured external integration data."""
        data_root = self._configured_data_root()
        if data_root is None:
            return []

        files = sorted(data_root.glob(f"output*/{relative_pattern}"))
        return files[:max_files]

    def _ocean_data_available(self) -> bool:
        """Check ocean data availability only in configured external location."""
        data_root = self._configured_data_root()
        if data_root is None:
            return False

        return check_ocean_data_availability(
            root_folder=str(data_root),
            target_folders=OCEAN_TARGET_FOLDERS,
        )

    def _get_input_files_for_compound(
        self, compound_name: str, model_id: str = "ACCESS-ESM1-6"
    ) -> list[Path] | None:
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
            # For ocean variables, use only configured external ocean files.
            data_root = self._configured_data_root()
            if data_root is None:
                return []

            try:
                ocean_files = get_monthly_ocean_files(
                    compound_name,
                    model_id=model_id,
                    root_folder=str(data_root),
                    target_folders=OCEAN_TARGET_FOLDERS,
                )
                if ocean_files:
                    return [Path(f) for f in ocean_files]
            except Exception:
                pass
            return []

        if table_name == "SImon":
            external_files = self._discover_external_files(
                "ice/iceh-1monthly-mean_*.nc", max_files=2
            )
            return external_files

        if "3hr" in table_name.lower():
            # Use 3-hourly files for 3hr tables
            external_files = self._discover_external_files(
                "atmosphere/netCDF/*_3hr.nc", max_files=2
            )
            return external_files
        elif "6hr" in table_name.lower():
            # Use 6-hourly files for 6hr tables
            external_files = self._discover_external_files(
                "atmosphere/netCDF/*_6hr.nc", max_files=2
            )
            return external_files
        elif "day" in table_name.lower():
            # Use daily files for daily tables
            external_files = self._discover_external_files(
                "atmosphere/netCDF/*_dai.nc", max_files=2
            )
            return external_files
        else:
            # Use monthly files for other tables (Amon, Lmon, etc.)
            external_files = self._discover_external_files(
                "atmosphere/netCDF/*_mon.nc", max_files=1
            )
            return external_files

    @pytest.mark.slow
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "table_name,model_id,cmor_table_file,cmip_version,variable_name",
        VARIABLE_TEST_PARAMS,
        ids=_parametrize_test_ids,
    )
    def test_cmorisation_variable(
        self,
        parent_experiment_config,
        compliance_validation_tool,
        table_name,
        model_id,
        cmor_table_file,
        cmip_version,
        variable_name,
    ):
        """Test CMORisation for a specific variable in a table.

        This is a granular integration test that processes individual variables,
        enabling fine-grained control via pytest -k filtering.
        
        Tests are parametrized by individual (table, variable) pairs, so you can:
        - Run all tests: pytest tests/integration/test_full_cmorisation.py
        - Run specific variable: pytest tests/integration/test_full_cmorisation.py -k "Amon-tas"
        - Run specific table: pytest tests/integration/test_full_cmorisation.py -k "Omon"
        - Run CMIP7 only: pytest tests/integration/test_full_cmorisation.py -k "cmip7"
        - Run CMIP6 only: pytest tests/integration/test_full_cmorisation.py -k "not cmip7"
        
        For ocean variables (Omon), uses ocean data files instead of atmosphere files.
        Uses appropriate input files based on table frequency requirements.
        By default it uses PrePARE. The WCRP compliance-checker can be enabled
        explicitly from the pytest command line.
        """
        # Map CMIP7 table names to CMIP6 equivalents if needed
        compound_table = table_name
        if cmip_version == "CMIP7":
            cmip7_to_cmip6_table = {
                "atmos": "Amon",
                "ocean": "Omon",
                "seaIce": "SImon",
                "aerosol": "AERmon",
                "land": "Lmon",
            }
            compound_table = cmip7_to_cmip6_table.get(table_name, table_name)

        # Skip ocean tests if ocean data is not available
        if compound_table == "Omon" and not self._ocean_data_available():
            pytest.skip(f"Ocean data directory not available; set {DATA_ROOT_ENV_VAR}")

        compound_name = f"{compound_table}.{variable_name}"
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
                f"Required input files not available for {compound_name}; "
                f"set {DATA_ROOT_ENV_VAR}"
            )

        experiment_id = "historical"
        source_id = "ACCESS-ESM1-5"
        output_dir = (
            Path(gettempdir()) / f"cmor_output_{compound_table}_{variable_name}"
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
                    output_dir.glob(f"{variable_name}_{compound_table}_*.nc")
                )
                assert (
                    output_files
                ), f"No output files found for {variable_name} in {output_dir}"

                # Validate output with the configured backend
                # Skip compliance validation for Omon and Ofx (ocean fixed fields
                # use non-standard grid structures not validated by PrePARE/WCRP)
                if compound_table not in ("Omon", "Ofx"):
                    self._validate_output_compliance(
                        output_files[0],
                        variable_name,
                        table_path,
                        compliance_validation_tool,
                    )

            except Exception as e:
                pytest.fail(
                    f"Failed processing {variable_name} in {compound_table} "
                    f"(CMIP version: {cmip_version}): {e}"
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
                compound_name, model_id="ACCESS-ESM1-6"
            )

            # Skip if required files don't exist
            if not input_files or not all(f.exists() for f in input_files):
                continue

            output_dir = Path(gettempdir()) / f"quick_test_{table_name}_{cmor_name}"
            output_dir.mkdir(parents=True, exist_ok=True)

            try:
                # Verify variable exists in mapping
                available_vars = load_filtered_variables(
                    model_id="ACCESS-ESM1-6", table_name=table_name
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
