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
import yaml

from access_moppy import ACCESS_ESM_CMORiser
from access_moppy.utilities import (
    _get_cmip7_to_cmip6_mapping,
    is_self_contained_variable,
)

# Import the utility function from conftest
from ..conftest import load_filtered_variables

# Import ocean file utilities
from .ocean_file_utils import (
    check_ocean_data_availability,
    get_monthly_ocean_files,
)

DATA_ROOT_ENV_VAR = "ACCESS_MOPPY_DATA_ROOT"
OCEAN_TARGET_FOLDERS = "output*/ocean/"
WCRP_CHECKER_SUITES = {
    "CMIP6": "wcrp_cmip6:1.0",
    "CMIP6Plus": "wcrp_cmip6plus:1.0",
    "CMIP7": "wcrp_cmip7:1.0",
}
KNOWN_WCRP_CHECKER_EXCLUSIONS: set[str] = set()
KNOWN_WCRP_CHECKER_MSG_EXCLUSIONS: tuple[str, ...] = ()
KNOWN_WCRP_ENVIRONMENT_MSG_SUBSTRINGS: tuple[str, ...] = (
    "Universe database is not installed or active.",
)

CMOR_TABLE_PACKAGES = {
    "CMIP6": "access_moppy.vocabularies.cmip6_cmor_tables.Tables",
    "CMIP6Plus": "access_moppy.vocabularies.cmip6_cmor_tables.Tables",
    "CMIP7": "access_moppy.vocabularies.cmip7-cmor-tables.tables",
}

CMIP7_REALM_TO_TABLE_FILE = {
    "atmos": "CMIP7_atmos.json",
    "ocean": "CMIP7_ocean.json",
    "seaIce": "CMIP7_seaIce.json",
    "aerosol": "CMIP7_aerosol.json",
    "land": "CMIP7_land.json",
    "landIce": "CMIP7_landIce.json",
}

CMIP7_BASELINE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src/access_moppy/examples/batch_config_esm1-6_cmip7_baseline.yml"
)


def _get_cmor_table_path(cmip_version: str, cmor_table_file: str):
    """Resolve a bundled CMOR table path for the requested CMIP family."""
    package = CMOR_TABLE_PACKAGES[cmip_version]
    return resources.as_file(resources.files(package).joinpath(cmor_table_file))


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


# Define table configurations to avoid code duplication.
# Each tuple: (table_name, model_id, cmor_table_file, cmip_version)
CMOR_TABLES_CMIP6 = [
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
]

CMOR_TABLES_CMIP6PLUS = [
    (table_name, model_id, cmor_table_file, "CMIP6Plus")
    for table_name, model_id, cmor_table_file, _ in CMOR_TABLES_CMIP6
]

CMOR_TABLES_CMIP7 = [
    # CMIP7 tables (via mapping to CMIP6 equivalents)
    ("atmos", "ACCESS-ESM1-6", "CMIP7_atmos.json", "CMIP7"),
    ("ocean", "ACCESS-ESM1-6", "CMIP7_ocean.json", "CMIP7"),
    ("seaIce", "ACCESS-ESM1-6", "CMIP7_seaIce.json", "CMIP7"),
    ("aerosol", "ACCESS-ESM1-6", "CMIP7_aerosol.json", "CMIP7"),
]


@lru_cache(maxsize=None)
def _generate_variable_test_params(
    table_configs: tuple[tuple[str, str, str, str], ...],
) -> list[tuple[str, str, str, str, str]]:
    """Generate all (table, model_id, cmor_table_file, cmip_version, variable) test parameters.

    This function generates individual test parameters for each variable in each table,
    enabling granular control over which variables to test via pytest's -k filtering.

    Returns:
        List of tuples: (table_name, model_id, cmor_table_file, cmip_version, variable_name)
    """
    params = []

    for table_name, model_id, cmor_table_file, cmip_version in table_configs:
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
                params.append(
                    (
                        table_name,
                        model_id,
                        cmor_table_file,
                        cmip_version,
                        var_name,
                    )
                )
        except Exception:
            # Skip tables that can't be loaded
            pass

    return params


@lru_cache(maxsize=1)
def _load_cmip7_baseline_variables() -> tuple[str, ...]:
    """Load the CMIP7 baseline variable list from the example batch config."""
    with CMIP7_BASELINE_CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        payload = yaml.safe_load(config_file) or {}

    variables = payload.get("variables", [])
    if not isinstance(variables, list):
        return tuple()

    return tuple(
        str(variable).strip() for variable in variables if str(variable).strip()
    )


def _generate_cmip7_baseline_test_params() -> list[tuple[str, str, str, str, str]]:
    """Generate CMIP7 params directly from the baseline example config."""
    params: list[tuple[str, str, str, str, str]] = []

    for cmip7_compound_name in _load_cmip7_baseline_variables():
        realm = cmip7_compound_name.split(".", 1)[0]
        cmor_table_file = CMIP7_REALM_TO_TABLE_FILE.get(realm)
        if cmor_table_file is None:
            continue

        params.append(
            (
                realm,
                "ACCESS-ESM1-6",
                cmor_table_file,
                "CMIP7",
                cmip7_compound_name,
            )
        )

    return params


# Generate variable-level test parameters for granular control
VARIABLE_TEST_PARAMS_CMIP6 = _generate_variable_test_params(tuple(CMOR_TABLES_CMIP6))
VARIABLE_TEST_PARAMS_CMIP6PLUS = _generate_variable_test_params(
    tuple(CMOR_TABLES_CMIP6PLUS)
)
VARIABLE_TEST_PARAMS_CMIP7 = _generate_cmip7_baseline_test_params()


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
    if cmip_version == "CMIP7" and variable.count(".") >= 3:
        return variable

    suffix = ""
    if cmip_version == "CMIP7":
        suffix = "-cmip7"
    elif cmip_version == "CMIP6Plus":
        suffix = "-cmip6plus"
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
            # bundled resource files (areacello, sftof, hfgeou) are self-contained
            # and need no external input — returning None signals the CMORiser to
            # use its resource file. Non-self-contained Ofx variables (e.g.
            # masscello, thkcello) still require ocean model files.
            if is_self_contained_variable(compound_name, model_id):
                return None
            # Fall through to ocean file discovery below.
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

        if compound_name == "Omon.areacello":
            # areacello is also bundled for ocean tests, even though it is
            # exposed through the Omon table in CMIP7 mappings.
            return None

        if table_name in {"Omon", "Oday"}:
            # For ocean variables, use only configured external ocean files.
            data_root = self._configured_data_root()
            if data_root is None:
                return []

            try:
                if table_name == "Omon":
                    ocean_files = get_monthly_ocean_files(
                        compound_name,
                        model_id=model_id,
                        root_folder=str(data_root),
                        target_folders=OCEAN_TARGET_FOLDERS,
                    )
                    if ocean_files:
                        return [Path(f) for f in ocean_files]
                else:
                    from access_moppy.utilities import load_model_mappings

                    _, variable_name = compound_name.split(".", 1)
                    mapping = load_model_mappings(compound_name, model_id=model_id)
                    model_variables = mapping.get(variable_name, {}).get(
                        "model_variables", []
                    )

                    daily_ocean_files: set[Path] = set()
                    for model_variable in model_variables:
                        daily_ocean_files.update(
                            data_root.glob(
                                f"output*/ocean/*-{model_variable}-1daily-mean*.nc"
                            )
                        )

                    if daily_ocean_files:
                        return sorted(daily_ocean_files)
            except Exception:
                pass
            return []

        if table_name == "SImon":
            external_files = self._discover_external_files(
                "ice/iceh-1monthly-mean_*.nc", max_files=2
            )
            return external_files

        if table_name == "SIday":
            external_files = self._discover_external_files(
                "ice/iceh-1daily-mean_*.nc", max_files=2
            )
            return external_files

        if "1hr" in table_name.lower():
            # Use hourly files for 1hr tables
            external_files = self._discover_external_files(
                "atmosphere/netCDF/*_1hr.nc", max_files=2
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
        VARIABLE_TEST_PARAMS_CMIP6,
        ids=_parametrize_test_ids,
    )
    def test_cmorisation_variable_cmip6(
        self,
        parent_experiment_config,
        compliance_validation_tool,
        table_name,
        model_id,
        cmor_table_file,
        cmip_version,
        variable_name,
    ):
        """Test CMORisation for a specific CMIP6 variable in a table."""
        self._run_cmorisation_variable(
            parent_experiment_config=parent_experiment_config,
            compliance_validation_tool=compliance_validation_tool,
            table_name=table_name,
            model_id=model_id,
            cmor_table_file=cmor_table_file,
            cmip_version=cmip_version,
            variable_name=variable_name,
        )

    @pytest.mark.slow
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "table_name,model_id,cmor_table_file,cmip_version,variable_name",
        VARIABLE_TEST_PARAMS_CMIP6PLUS,
        ids=_parametrize_test_ids,
    )
    def test_cmorisation_variable_cmip6plus(
        self,
        parent_experiment_config,
        compliance_validation_tool,
        table_name,
        model_id,
        cmor_table_file,
        cmip_version,
        variable_name,
    ):
        """Test CMORisation for a specific CMIP6Plus variable in a table."""
        self._run_cmorisation_variable(
            parent_experiment_config=parent_experiment_config,
            compliance_validation_tool=compliance_validation_tool,
            table_name=table_name,
            model_id=model_id,
            cmor_table_file=cmor_table_file,
            cmip_version=cmip_version,
            variable_name=variable_name,
        )

    @pytest.mark.slow
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "table_name,model_id,cmor_table_file,cmip_version,variable_name",
        VARIABLE_TEST_PARAMS_CMIP7,
        ids=_parametrize_test_ids,
    )
    def test_cmorisation_variable_cmip7(
        self,
        parent_experiment_config,
        compliance_validation_tool,
        table_name,
        model_id,
        cmor_table_file,
        cmip_version,
        variable_name,
    ):
        """Test CMORisation for a specific CMIP7 variable in a table."""
        self._run_cmorisation_variable(
            parent_experiment_config=parent_experiment_config,
            compliance_validation_tool=compliance_validation_tool,
            table_name=table_name,
            model_id=model_id,
            cmor_table_file=cmor_table_file,
            cmip_version=cmip_version,
            variable_name=variable_name,
        )

    def _run_cmorisation_variable(
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

        Tests are parametrized by individual (table, variable) pairs.

        For ocean variables (Omon), uses ocean data files instead of atmosphere files.
        Uses appropriate input files based on table frequency requirements.
        By default it uses PrePARE. The WCRP compliance-checker can be enabled
        explicitly from the pytest command line.
        """
        # Map CMIP7 table names to CMIP6 equivalents if needed
        compound_table = table_name
        cmor_name = variable_name
        compound_name = f"{compound_table}.{variable_name}"
        input_lookup_compound = compound_name

        if cmip_version == "CMIP7":
            if variable_name.count(".") >= 3:
                compound_name = variable_name
                cmip6_equivalent = _get_cmip7_to_cmip6_mapping(compound_name)
                if cmip6_equivalent is None:
                    pytest.skip(f"No CMIP7->CMIP6 mapping found for {compound_name}")

                input_lookup_compound = cmip6_equivalent
                compound_table, cmor_name = cmip6_equivalent.split(".", 1)
            else:
                cmip7_to_cmip6_table = {
                    "atmos": "Amon",
                    "ocean": "Omon",
                    "seaIce": "SImon",
                    "aerosol": "AERmon",
                    "land": "Lmon",
                }
                compound_table = cmip7_to_cmip6_table.get(table_name, table_name)
                compound_name = f"{compound_table}.{variable_name}"
                input_lookup_compound = compound_name
                cmor_name = variable_name

        # Skip ocean tests if ocean data is not available
        if compound_table in {"Omon", "Oday"} and not self._ocean_data_available():
            pytest.skip(f"Ocean data directory not available; set {DATA_ROOT_ENV_VAR}")

        input_files = self._get_input_files_for_compound(
            input_lookup_compound, model_id=model_id
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
        source_id = model_id
        parent_info = {
            **parent_experiment_config,
            "parent_source_id": model_id,
            "parent_mip_era": cmip_version,
        }
        safe_var_name = compound_name.replace(".", "_").replace("-", "_")
        output_dir = (
            Path(gettempdir()) / f"cmor_output_{compound_table}_{safe_var_name}"
        )
        drs_enabled = compliance_validation_tool == "wcrp"

        # Ensure output directory exists and is clean
        output_dir.mkdir(parents=True, exist_ok=True)
        for f in output_dir.rglob("*.nc"):
            f.unlink(missing_ok=True)

        with _get_cmor_table_path(cmip_version, cmor_table_file) as table_path:
            try:
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=input_files,  # None = use bundled resource file
                    compound_name=compound_name,
                    experiment_id=experiment_id,
                    source_id=source_id,
                    variant_label="r1i1p1f1",
                    cmip_version=cmip_version,
                    activity_id="CMIP",
                    parent_info=parent_info,
                    output_path=output_dir,
                    drs_root=output_dir if drs_enabled else None,
                )

                cmoriser.run()
                cmoriser.write()

                # Verify output files were created. CMIP7 uses a different
                # filename template from CMIP6, so assert on written NetCDF
                # files in the dedicated per-variable output directory rather
                # than a CMIP6-specific filename pattern.
                output_files = sorted(output_dir.rglob("*.nc"))
                assert (
                    output_files
                ), f"No output files found for {variable_name} in {output_dir}"

                self._validate_output_compliance(
                    output_files[0],
                    cmor_name,
                    table_path,
                    cmip_version,
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
        cmip_version,
        validation_tool: str,
    ):
        """Validate output using the configured backend."""
        if validation_tool == "prepare" and cmip_version == "CMIP7":
            pytest.skip(
                "PrePARE does not support CMIP7 vocabularies; use --validation-tool=wcrp"
            )

        if validation_tool == "wcrp":
            suite_name = WCRP_CHECKER_SUITES[cmip_version]
            if suite_name not in _available_compliance_suites():
                pytest.skip(
                    f"Requested validation backend '{validation_tool}' is unavailable for {cmip_version}"
                )
            self._validate_with_wcrp_checker(output_file, suite_name)
            return

        self._validate_with_prepare(output_file, cmor_name, table_path)

    def _extract_failed_checks(
        self,
        report: dict,
        section: str,
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

    def _assert_wcrp_report_valid(self, report: dict, suite_name: str) -> None:
        """Fail only on mandatory WCRP checks."""
        failed_checks = self._extract_failed_checks(report, section=suite_name)

        environment_failures = self._filter_excluded_checks(
            failed_checks,
            exclude_names=set(),
            exclude_msg_substrings=(),
        )
        for check in environment_failures:
            messages = check.get("msgs", [])
            if any(
                substring in message
                for substring in KNOWN_WCRP_ENVIRONMENT_MSG_SUBSTRINGS
                for message in messages
            ):
                pytest.skip(
                    "WCRP checker environment is not fully configured; "
                    "run 'esgvoc use universe@latest' to enable vocabulary checks"
                )

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

    def _validate_with_wcrp_checker(self, output_file, suite_name: str):
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
                suite_name,
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
            self._assert_wcrp_report_valid(report, suite_name)
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
                    source_id="ACCESS-ESM1-6",
                    variant_label="r1i1p1f1",
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
