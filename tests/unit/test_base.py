from unittest.mock import patch

import pytest
import xarray as xr

from access_mopper.base import BaseCMORiser


class TestBaseCMORiser:
    """Unit tests for BaseCMORiser class."""

    def test_init_with_valid_params(self, mock_config, temp_dir):
        """Test initialization with valid parameters."""
        cmoriser = BaseCMORiser(
            input_paths=["test.nc"],
            compound_name="Amon.tas",
            output_path=temp_dir,
            **mock_config,
        )

        assert cmoriser.experiment_id == "historical"
        assert cmoriser.compound_name == "Amon.tas"
        assert cmoriser.mip_table == "Amon"
        assert cmoriser.cmor_name == "tas"

    def test_init_with_invalid_compound_name(self, mock_config, temp_dir):
        """Test initialization fails with invalid compound name."""
        with pytest.raises(ValueError, match="Invalid compound_name format"):
            BaseCMORiser(
                input_paths=["test.nc"],
                compound_name="invalid",
                output_path=temp_dir,
                **mock_config,
            )

    def test_init_with_missing_required_params(self, temp_dir):
        """Test initialization fails with missing required parameters."""
        with pytest.raises(TypeError):
            BaseCMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                # Missing required CMIP6 metadata
            )

    @patch("access_mopper.base.xr.open_mfdataset")
    def test_load_data_single_file(
        self, mock_open_mfdataset, mock_netcdf_dataset, mock_config, temp_dir
    ):
        """Test loading data from single file."""
        mock_open_mfdataset.return_value = mock_netcdf_dataset

        cmoriser = BaseCMORiser(
            input_paths=["test.nc"],
            compound_name="Amon.tas",
            output_path=temp_dir,
            **mock_config,
        )

        result = cmoriser._load_input_data()

        mock_open_mfdataset.assert_called_once()
        assert isinstance(result, xr.Dataset)

    @patch("access_mopper.base.xr.open_mfdataset")
    def test_load_data_multiple_files(
        self, mock_open_mfdataset, mock_netcdf_dataset, mock_config, temp_dir
    ):
        """Test loading data from multiple files."""
        mock_open_mfdataset.return_value = mock_netcdf_dataset

        cmoriser = BaseCMORiser(
            input_paths=["test1.nc", "test2.nc"],
            compound_name="Amon.tas",
            output_path=temp_dir,
            **mock_config,
        )

        cmoriser._load_input_data()

        mock_open_mfdataset.assert_called_once_with(
            ["test1.nc", "test2.nc"],
            combine="by_coords",
            data_vars="minimal",
            coords="minimal",
            compat="override",
        )

    def test_compound_name_parsing(self, mock_config, temp_dir):
        """Test parsing of compound name into table and variable."""
        test_cases = [
            ("Amon.tas", "Amon", "tas"),
            ("Omon.tos", "Omon", "tos"),
            ("day.pr", "day", "pr"),
            ("6hrPlevPt.ua", "6hrPlevPt", "ua"),
        ]

        for compound_name, expected_table, expected_var in test_cases:
            cmoriser = BaseCMORiser(
                input_paths=["test.nc"],
                compound_name=compound_name,
                output_path=temp_dir,
                **mock_config,
            )
            assert cmoriser.mip_table == expected_table
            assert cmoriser.cmor_name == expected_var

    def test_output_path_creation(self, mock_config, temp_dir):
        """Test that output path is created if it doesn't exist."""
        non_existent_path = temp_dir / "new_output_dir"
        assert not non_existent_path.exists()

        BaseCMORiser(
            input_paths=["test.nc"],
            compound_name="Amon.tas",
            output_path=non_existent_path,
            **mock_config,
        )

        # This should create the directory
        assert non_existent_path.exists()
