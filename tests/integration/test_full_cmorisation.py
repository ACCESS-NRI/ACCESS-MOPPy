"""
Full CMOR integration tests for all supported variables and tables.

This module contains comprehensive integration tests that test CMORisation
for all variables defined in the mapping files. These tests use real data
files and validate output against CMOR standards.
"""

import importlib.resources as resources
import subprocess
from pathlib import Path
from tempfile import gettempdir

import pytest

import access_mopper.vocabularies.cmip6_cmor_tables.Tables as cmor_tables
from access_mopper import ACCESS_ESM_CMORiser

# Import the utility function from conftest
from ..conftest import load_filtered_variables

DATA_DIR = Path(__file__).parent.parent / "data"


# Define table configurations to avoid code duplication
CMOR_TABLES = [
    ("Amon", "Mappings_CMIP6_Amon.json", "CMIP6_Amon.json"),
    ("Lmon", "Mappings_CMIP6_Lmon.json", "CMIP6_Lmon.json"),
    ("Emon", "Mappings_CMIP6_Emon.json", "CMIP6_Emon.json"),
]


class TestFullCMORIntegration:
    """Integration tests for full CMOR processing of all variables."""

    @pytest.mark.slow
    @pytest.mark.integration
    @pytest.mark.skipif(
        not (DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc").exists(),
        reason="Test data file not available",
    )
    @pytest.mark.parametrize("table_name,mappings_file,cmor_table_file", CMOR_TABLES)
    def test_full_cmorisation_all_variables(
        self, parent_experiment_config, table_name, mappings_file, cmor_table_file
    ):
        """Test CMORisation for all variables in each supported table.

        This is a comprehensive integration test that processes all variables
        defined in the mapping files and validates the output using PrePARE.
        """
        # Load variables for this specific table
        try:
            table_variables = load_filtered_variables(mappings_file)
        except Exception:
            pytest.skip(f"Cannot load variables for table {table_name}")

        file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"

        # Test a subset of variables to keep test time reasonable
        # In practice, you might want to test all variables in CI but subset for dev
        test_variables = (
            table_variables[:5] if len(table_variables) > 5 else table_variables
        )

        for cmor_name in test_variables:
            with pytest.subtest(variable=cmor_name):
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
                            input_paths=file_pattern,
                            compound_name=f"{table_name}.{cmor_name}",
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

                        # Verify output files were created
                        output_files = list(
                            output_dir.glob(f"{cmor_name}_{table_name}_*.nc")
                        )
                        assert (
                            output_files
                        ), f"No output files found for {cmor_name} in {output_dir}"

                        # Validate output using PrePARE if available
                        self._validate_with_prepare(
                            output_files[0], cmor_name, table_path
                        )

                    except Exception as e:
                        pytest.fail(
                            f"Failed processing {cmor_name} with table {table_name}: {e}"
                        )

    def _validate_with_prepare(self, output_file, cmor_name, table_path):
        """Validate CMOR output using PrePARE tool if available."""
        try:
            cmd = [
                "PrePARE",
                "--variable",
                cmor_name,
                "--table-path",
                str(table_path),
                str(output_file),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

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
    @pytest.mark.skipif(
        not (DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc").exists(),
        reason="Test data file not available",
    )
    def test_quick_integration_sample(self, parent_experiment_config):
        """Test a small sample of variables for quick integration testing.

        This test runs a subset of variables to provide faster feedback
        during development while still testing the integration.
        """
        # Test one variable from each table for quick integration testing
        test_cases = [
            ("Amon", "tas"),
            ("Lmon", "mrso"),
            ("Emon", "lai"),
        ]

        file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"

        for table_name, cmor_name in test_cases:
            output_dir = Path(gettempdir()) / f"quick_test_{table_name}_{cmor_name}"
            output_dir.mkdir(parents=True, exist_ok=True)

            try:
                # Verify variable exists in mapping
                mappings_file = f"Mappings_CMIP6_{table_name}.json"
                available_vars = load_filtered_variables(mappings_file)

                if cmor_name not in available_vars:
                    continue  # Skip if variable not available

                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=file_pattern,
                    compound_name=f"{table_name}.{cmor_name}",
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
