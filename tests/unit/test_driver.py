"""
Unit tests for the ACCESS_ESM_CMORiser driver class.

These tests focus on the initialization and configuration of the main
CMORiser interface without requiring actual data processing.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from access_moppy.driver import ACCESS_ESM_CMORiser


class TestACCESSESMCMORiser:
    """Unit tests for ACCESS_ESM_CMORiser driver class."""

    @pytest.fixture
    def valid_config(self):
        """Valid configuration for CMORiser initialization."""
        return {
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-5",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
            "activity_id": "CMIP",
        }

    @pytest.mark.unit
    def test_init_with_minimal_params(self, valid_config, temp_dir):
        """Test initialization with minimal required parameters."""
        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.input_paths == ["test.nc"]
            assert cmoriser.compound_name == "Amon.tas"
            assert cmoriser.output_path == Path(temp_dir)
            assert cmoriser.experiment_id == "historical"
            assert cmoriser.source_id == "ACCESS-ESM1-5"

    @pytest.mark.unit
    def test_init_with_multiple_input_paths(self, valid_config, temp_dir):
        """Test initialization with multiple input files."""
        input_files = ["file1.nc", "file2.nc", "file3.nc"]

        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=input_files,
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.input_paths == input_files

    @pytest.mark.unit
    def test_init_with_parent_info(self, valid_config, temp_dir):
        """Test initialization with parent experiment information."""
        parent_info = {
            "parent_experiment_id": "piControl",
            "parent_activity_id": "CMIP",
            "parent_source_id": "ACCESS-ESM1-5",
            "parent_variant_label": "r1i1p1f1",
            "parent_time_units": "days since 0001-01-01 00:00:00",
            "parent_mip_era": "CMIP6",
            "branch_time_in_child": 0.0,
            "branch_time_in_parent": 54786.0,
            "branch_method": "standard",
        }

        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                parent_info=parent_info,
                **valid_config,
            )

            # Should use provided parent info instead of defaults
            assert cmoriser.parent_info == parent_info

    @pytest.mark.unit
    def test_init_with_drs_root(self, valid_config, temp_dir):
        """Test initialization with DRS root specification."""
        drs_root = temp_dir / "drs_structure"

        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                drs_root=str(drs_root),
                **valid_config,
            )

            assert cmoriser.drs_root == Path(drs_root)

    @pytest.mark.unit
    def test_compound_name_parsing(self, valid_config, temp_dir):
        """Test that compound names are parsed correctly."""
        test_cases = [
            ("Amon.tas", "Amon", "tas"),
            ("Omon.tos", "Omon", "tos"),
            ("Lmon.mrso", "Lmon", "mrso"),
            ("day.pr", "day", "pr"),
        ]

        for compound_name, expected_table, expected_var in test_cases:
            with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
                mock_load.return_value = {expected_var: {"units": "K"}}

                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name=compound_name,
                    output_path=temp_dir,
                    **valid_config,
                )

                # Check that the compound name is stored correctly
                assert cmoriser.compound_name == compound_name
                # Check that mappings were loaded for the correct compound name
                mock_load.assert_called_with(compound_name)

    @pytest.mark.unit
    def test_output_path_conversion(self, valid_config):
        """Test that output path is properly converted to Path object."""
        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            # Test with string path using secure temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                test_output_path = str(Path(temp_dir) / "test_output")
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=test_output_path,
                    **valid_config,
                )

                assert isinstance(cmoriser.output_path, Path)
                assert cmoriser.output_path == Path(test_output_path)

    @pytest.mark.unit
    def test_default_parent_info_used(self, valid_config, temp_dir):
        """Test that default parent info is used when none provided."""
        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            # Should use default parent info when none provided
            assert "parent_experiment_id" in cmoriser.parent_info
            assert cmoriser.parent_info["parent_experiment_id"] == "piControl"

    @pytest.mark.unit
    def test_variable_mapping_loaded(self, valid_config, temp_dir):
        """Test that variable mapping is loaded correctly."""
        mock_mapping = {
            "tas": {
                "CF standard Name": "air_temperature",
                "units": "K",
                "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            }
        }

        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = mock_mapping

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.variable_mapping == mock_mapping
            mock_load.assert_called_once_with("Amon.tas")

    @pytest.mark.unit
    def test_missing_required_params(self, temp_dir):
        """Test that missing required parameters raise appropriate errors."""
        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            # Test missing experiment_id parameter - should raise TypeError
            with pytest.raises(TypeError, match="experiment_id"):
                # Intentionally omit experiment_id to test error handling
                # This is expected to fail with TypeError
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    source_id="ACCESS-ESM1-5",
                    variant_label="r1i1p1f1",
                    grid_label="gn",
                    activity_id="CMIP",
                    # experiment_id intentionally omitted for testing
                )

    @pytest.mark.unit
    def test_drs_root_path_conversion(self, valid_config, temp_dir):
        """Test DRS root path conversion from string to Path."""
        with patch("access_moppy.driver.load_cmip6_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            # Use secure temporary directory for DRS root path
            with tempfile.TemporaryDirectory() as drs_temp_dir:
                test_drs_path = str(Path(drs_temp_dir) / "drs")
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    drs_root=test_drs_path,
                    **valid_config,
                )

                assert isinstance(cmoriser.drs_root, Path)
                assert cmoriser.drs_root == Path(test_drs_path)
