from unittest.mock import patch

from access_moppy import ACCESS_ESM_CMORiser
from tests.mocks.mock_data import (
    create_mock_2d_ocean_dataset,
    create_mock_3d_ocean_dataset,
    create_mock_atmosphere_dataset,
)


class TestCMORiserIntegration:
    """Integration tests with mocked large datasets."""

    @patch("access_moppy.base.xr.open_mfdataset")
    def test_full_cmorisation_workflow_mocked(
        self, mock_open_mfdataset, mock_config, temp_dir
    ):
        """Test complete CMORisation workflow with mocked data."""
        # Create mock dataset with realistic atmosphere data
        mock_dataset = create_mock_atmosphere_dataset(
            variables=["temp"], n_time=12, n_lat=145, n_lon=192
        )
        mock_open_mfdataset.return_value = mock_dataset

        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=["mock_file.nc"],
            compound_name="Amon.tas",
            output_path=temp_dir,
            **mock_config,
        )

        # Mock the write method to avoid actual file I/O during integration test
        with patch.object(cmoriser, "write") as mock_write:
            cmoriser.run()

            # Verify the workflow completed
            assert hasattr(cmoriser, "tas")
            mock_write.assert_not_called()  # We're testing run() only

    @patch("access_moppy.base.xr.open_mfdataset")
    def test_memory_efficient_processing(self, mock_open_mfdataset, temp_dir):
        """Test that large datasets are processed efficiently."""
        # Create a chunked dataset to simulate large data
        large_dataset = create_mock_atmosphere_dataset(
            variables=["temp"],
            n_time=1000,  # Large time dimension
            n_lat=180,
            n_lon=360,
        ).chunk({"time": 100, "lat": 90, "lon": 180})

        mock_open_mfdataset.return_value = large_dataset

        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=["large_mock_file.nc"],
            compound_name="Amon.tas",
            output_path=temp_dir,
            experiment_id="historical",
            source_id="ACCESS-ESM1-5",
            variant_label="r1i1p1f1",
            grid_label="gn",
            activity_id="CMIP",
        )

        # This should not load all data into memory at once
        cmoriser.cmoriser.load_dataset()
        # result = cmoriser.ds

        # Raw name of temperature in ACCESS-ESM1_6
        raw_name = "fld_s03i236"
        # Verify it's still chunked (lazy loading)
        assert hasattr(cmoriser[raw_name].data, "chunks")

    @patch("access_moppy.base.xr.open_mfdataset")
    def test_multiple_variables_workflow(
        self, mock_open_mfdataset, mock_config, temp_dir
    ):
        """Test workflow with multiple variables in dataset."""
        # Create dataset with multiple variables
        mock_dataset = create_mock_atmosphere_dataset(
            variables=["temp", "precip", "psl"]
        )
        mock_open_mfdataset.return_value = mock_dataset

        # Test different compound names
        compound_names = ["Amon.tas", "Amon.pr", "Amon.psl"]

        for compound_name in compound_names:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["mock_file.nc"],
                compound_name=compound_name,
                output_path=temp_dir,
                **mock_config,
            )

            with patch.object(cmoriser, "write"):
                cmoriser.run()

            # Verify correct variable is being processed
            expected_var = compound_name.split(".")[1]
            assert cmoriser.cmoriser.cmor_name == expected_var

    @patch("access_moppy.base.xr.open_mfdataset")
    def test_multiple_ocean_variables_workflow(
        self, mock_open_mfdataset, mock_config_om2, temp_dir
    ):
        """Test workflow with multiple variables in dataset."""
        # Create dataset with multiple variables
        mock_dataset = create_mock_2d_ocean_dataset()

        mock_open_mfdataset.return_value = mock_dataset

        # Test different compound names
        compound_name = "Omon.tos"

        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=["mock_file.nc"],
            compound_name=compound_name,
            output_path=temp_dir,
            **mock_config_om2,
        )

        with patch.object(cmoriser, "write"):
            cmoriser.run()

        # Verify correct variable is being processed
        expected_var = compound_name.split(".")[1]
        assert cmoriser.cmoriser.cmor_name == expected_var

    @patch("access_moppy.base.xr.open_mfdataset")
    def test_3d_ocean_variables_workflow(
        self, mock_open_mfdataset, mock_config_om2, temp_dir
    ):
        """Test workflow with 3D ocean variables (depth-dependent)."""
        # Create 3D ocean dataset
        mock_dataset = create_mock_3d_ocean_dataset(
            nt=12, nz=50, ny=300, nx=360, variables=["pot_temp"]
        )

        mock_open_mfdataset.return_value = mock_dataset

        # Test with 3D ocean variable
        compound_name = "Omon.thetao"

        cmoriser = ACCESS_ESM_CMORiser(
            input_paths=["mock_3d_file.nc"],
            compound_name=compound_name,
            output_path=temp_dir,
            **mock_config_om2,
        )

        with patch.object(cmoriser, "write"):
            cmoriser.run()

        # Get the processed dataset
        processed_ds = cmoriser.to_dataset()

        # Verify correct variable is being processed
        expected_var = compound_name.split(".")[1]
        assert cmoriser.cmoriser.cmor_name == expected_var

        # Verify processed dataset has depth dimension
        assert "lev" in processed_ds.dims

        # Verify processed data has correct dimensions (CMOR might rename dimensions)
        assert len(processed_ds[expected_var].dims) == 4  # time, lev, j, i
        assert processed_ds[expected_var].dims == ("time", "lev", "j", "i")

        # Verify data shape includes depth dimension (50 levels)
        assert 50 in processed_ds[expected_var].shape
        assert processed_ds[expected_var].shape == (12, 50, 300, 360)

        # Verify main variable exists
        assert (
            expected_var in processed_ds.data_vars
        ), f"{expected_var} should be in data variables"

        # Verify coordinate variables
        assert (
            "latitude" in processed_ds.data_vars
        ), "latitude should be in data variables"
        assert (
            "longitude" in processed_ds.data_vars
        ), "longitude should be in data variables"
        assert processed_ds["latitude"].dims == (
            "j",
            "i",
        ), "latitude should have (j, i) dimensions"
        assert processed_ds["longitude"].dims == (
            "j",
            "i",
        ), "longitude should have (j, i) dimensions"

        # Verify vertices if they exist
        if "vertices_latitude" in processed_ds.data_vars:
            assert processed_ds["vertices_latitude"].dims == (
                "j",
                "i",
                "vertices",
            ), "vertices_latitude should have (j, i, vertices) dimensions"
        if "vertices_longitude" in processed_ds.data_vars:
            assert processed_ds["vertices_longitude"].dims == (
                "j",
                "i",
                "vertices",
            ), "vertices_longitude should have (j, i, vertices) dimensions"

        # Verify time has bounds attribute
        assert (
            "bounds" in processed_ds["time"].attrs
        ), "time coordinate should have bounds attribute"
        assert (
            processed_ds["time"].attrs["bounds"] == "time_bnds"
        ), "time bounds attribute should point to time_bnds"
