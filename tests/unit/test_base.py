"""
Unit tests for the CMORiser base class.

These tests focus on the core functionality of the CMORiser class
without requiring complex dependencies or data files.
"""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import dask.array as da
import netCDF4 as nc
import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser


class TestCMIP6CMORiser:
    """Unit tests for CMORiser base class."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary object."""
        vocab = Mock()
        vocab.get_table = Mock(return_value={"tas": {"units": "K"}})
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping."""
        return {
            "CF standard Name": "air_temperature",
            "units": "K",
            "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            "positive": None,
        }

    @pytest.mark.unit
    def test_init_with_valid_params(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with valid parameters."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.input_paths == ["test.nc"]
        assert cmoriser.output_path == str(temp_dir)
        assert cmoriser.cmor_name == "tas"  # Should be extracted from compound_name
        assert cmoriser.vocab == mock_vocab
        assert cmoriser.mapping == mock_mapping

    @pytest.mark.unit
    def test_init_with_multiple_input_paths(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with multiple input files."""
        input_files = ["test1.nc", "test2.nc", "test3.nc"]
        cmoriser = CMORiser(
            input_paths=input_files,
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.input_paths == input_files

    @pytest.mark.unit
    def test_init_with_single_input_path_string(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test initialization with single input path as string."""
        cmoriser = CMORiser(
            input_paths="single_file.nc",
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.input_paths == ["single_file.nc"]

    @pytest.mark.unit
    def test_init_with_drs_root(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with DRS root path."""
        drs_root = temp_dir / "drs"
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
            drs_root=str(drs_root),
        )

        assert cmoriser.drs_root == Path(drs_root)

    @pytest.mark.unit
    def test_version_date_format(self, mock_vocab, mock_mapping, temp_dir):
        """Test that version date is set correctly."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        # Check that version_date is a string in YYYYMMDD format
        assert isinstance(cmoriser.version_date, str)
        assert len(cmoriser.version_date) == 8
        assert cmoriser.version_date.isdigit()

    @pytest.mark.unit
    def test_type_mapping_attribute(self, mock_vocab, mock_mapping, temp_dir):
        """Test that type_mapping is available as class attribute."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        # type_mapping should be available from utilities
        assert hasattr(cmoriser, "type_mapping")
        assert cmoriser.type_mapping is not None

    @pytest.mark.unit
    def test_dataset_proxy_methods(self, mock_vocab, mock_mapping, temp_dir):
        """Test that the CMORiser can proxy dataset operations."""
        # Create a mock dataset
        mock_dataset = Mock()
        mock_dataset.test_attr = "test_value"
        mock_dataset.__getitem__ = Mock(return_value="dataset_item")
        mock_dataset.__setitem__ = Mock()
        mock_dataset.__repr__ = Mock(return_value="<Dataset representation>")

        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        # Set the dataset
        cmoriser.ds = mock_dataset

        # Test __getitem__ proxy
        result = cmoriser["test_key"]
        assert result == "dataset_item"
        mock_dataset.__getitem__.assert_called_with("test_key")

        # Test __getattr__ proxy
        assert cmoriser.test_attr == "test_value"

        # Test __setitem__ proxy
        cmoriser["new_key"] = "new_value"
        mock_dataset.__setitem__.assert_called_with("new_key", "new_value")

        # Test __repr__ proxy
        repr_result = repr(cmoriser)
        assert repr_result == "<Dataset representation>"

    @pytest.mark.unit
    def test_dataset_none_initially(self, mock_vocab, mock_mapping, temp_dir):
        """Test that dataset is None initially."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.ds is None

    @pytest.mark.unit
    def test_getattr_fallback(self, mock_vocab, mock_mapping, temp_dir):
        """Test __getattr__ behavior when dataset is None."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        # When ds is None, getattr should raise AttributeError
        with pytest.raises(AttributeError):
            _ = cmoriser.nonexistent_attribute


class TestCMIP6CMORiserWrite:
    """Unit tests for CMORiser.write() method with memory validation and string coordinate handling."""

    # ==================== Fixtures ====================

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary object."""
        vocab = Mock()
        vocab.get_table = Mock(
            return_value={"tas": {"units": "K"}, "baresoilFrac": {"units": "%"}}
        )
        # Add mock for get_required_attribute_names() to return a list of required attributes
        vocab.get_required_attribute_names = Mock(
            return_value=[
                "variable_id",
                "table_id",
                "source_id",
                "experiment_id",
                "variant_label",
                "grid_label",
                "activity_id",
                "institution_id",
                "mip_era",
                "creation_date",
                "tracking_id",
            ]
        )
        # Add mock for generate_filename() to return a proper filename string
        vocab.generate_filename = Mock(
            return_value="tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_200001-200012.nc"
        )
        # Add mock for standardize_missing_values() to return the input data unchanged
        vocab.standardize_missing_values = Mock(side_effect=lambda x, **kwargs: x)
        # Add mock for get_cmip_missing_value() to return a standard missing value
        vocab.get_cmip_missing_value = Mock(return_value=1e20)
        # Add mock for build_drs_path() to return a Path
        vocab.build_drs_path = Mock(return_value=Path("/mock/drs/path"))
        # Add __class__.__name__ for CMIP6 detection
        vocab.__class__.__name__ = "CMIP6Vocabulary"
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping."""
        return {
            "CF standard Name": "air_temperature",
            "units": "K",
            "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            "positive": None,
        }

    @pytest.fixture
    def sample_dataset(self):
        """
        Create a sample xarray Dataset for testing (no string coordinates).

        Dataset structure:
        - tas: main variable (12 time steps × 10 lat × 10 lon, float32)
        - time_bnds: time bounds
        - All required CMIP6 global attributes included
        """
        time = np.arange(12)
        lat = np.arange(10)
        lon = np.arange(10)

        data = np.random.rand(12, 10, 10).astype(np.float32)

        ds = xr.Dataset(
            {
                "tas": (["time", "lat", "lon"], data, {"_FillValue": 1e20}),
                "time_bnds": (["time", "bnds"], np.zeros((12, 2))),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": ("lat", lat),
                "lon": ("lon", lon),
            },
            attrs={
                "variable_id": "tas",
                "table_id": "Amon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            },
        )
        return ds

    @pytest.fixture
    def sample_dask_dataset(self):
        """
        Create a sample Dask-backed xarray Dataset for testing chunked write.
        """
        nt, ny, nx = 24, 30, 36

        time = np.arange(nt)
        yt_ocean = np.linspace(-89.5, 89.5, ny)
        xt_ocean = np.linspace(0.5, 359.5, nx)

        # Create Dask array
        data = da.from_array(
            np.random.rand(nt, ny, nx).astype(np.float32),
            chunks=(6, ny, nx),  # Chunk along time dimension
        )

        ds = xr.Dataset(
            {
                "tos": (
                    ["time", "yt_ocean", "xt_ocean"],
                    data,
                    {"_FillValue": np.float32(-1e20)},
                ),
                "time_bnds": (["time", "nv"], np.zeros((nt, 2))),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {
                        "units": "days since 2000-01-01 00:00:00",
                        "calendar": "standard",
                    },
                ),
                "yt_ocean": ("yt_ocean", yt_ocean),
                "xt_ocean": ("xt_ocean", xt_ocean),
                "nv": ("nv", [1.0, 2.0]),
            },
            attrs={
                "variable_id": "tos",
                "table_id": "Omon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "activity_id": "CMIP",
                "institution_id": "CSIRO",
            },
        )
        return ds

    @pytest.fixture
    def sample_dataset_missing_attrs(self):
        """Create a dataset missing required CMIP6 attributes."""
        ds = xr.Dataset(
            {"tas": (["time"], np.zeros(10))},
            coords={
                "time": (
                    "time",
                    np.arange(10),
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
            },
            attrs={"variable_id": "tas"},  # Missing other required attrs
        )
        return ds

    @pytest.fixture
    def dataset_with_scalar_string_coord(self):
        """
        Create dataset with scalar byte string coordinate (mimics land variables).

        Example: baresoilFrac with 'type' coordinate = b'bare_ground'
        """
        time = np.arange(12)
        lat = np.linspace(-90, 90, 10)
        lon = np.linspace(0, 360, 10)

        data = np.random.rand(12, 10, 10).astype(np.float32)

        ds = xr.Dataset(
            {
                "baresoilFrac": (
                    ["time", "lat", "lon"],
                    data,
                    {"_FillValue": 1e20, "units": "%"},
                ),
                "time_bnds": (["time", "bnds"], np.zeros((12, 2))),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": ("lat", lat),
                "lon": ("lon", lon),
                "type": np.array(b"bare_ground", dtype="|S11"),
            },
            attrs={
                "variable_id": "baresoilFrac",
                "table_id": "Lmon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            },
        )
        return ds

    @pytest.fixture
    def dataset_with_array_string_coord(self):
        """
        Create dataset with array string coordinate.

        Example: Multi-region data with region names.
        """
        time = np.arange(12)
        region = np.array(["land", "ocean", "ice"], dtype="|S5")

        data = np.random.rand(12, 3).astype(np.float32)

        ds = xr.Dataset(
            {
                "regionTemp": (
                    ["time", "region"],
                    data,
                    {"_FillValue": 1e20, "units": "K"},
                ),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "region": ("region", region),
            },
            attrs={
                "variable_id": "regionTemp",
                "table_id": "Amon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            },
        )
        return ds

    @pytest.fixture
    def dataset_with_unicode_string_coord(self):
        """
        Create dataset with Unicode string coordinate (tests conversion).
        """
        time = np.arange(12)
        lat = np.linspace(-90, 90, 10)
        lon = np.linspace(0, 360, 10)

        data = np.random.rand(12, 10, 10).astype(np.float32)

        ds = xr.Dataset(
            {
                "baresoilFrac": (
                    ["time", "lat", "lon"],
                    data,
                    {"_FillValue": 1e20, "units": "%"},
                ),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": ("lat", lat),
                "lon": ("lon", lon),
                "type": "bare_ground",  # Unicode string
            },
            attrs={
                "variable_id": "baresoilFrac",
                "table_id": "Lmon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            },
        )
        return ds

    @pytest.fixture
    def dataset_with_array_unicode_string_coord(self):
        """
        Create dataset with an *array* Unicode string coordinate (dtype kind 'U').

        Mimics the landCoverFrac 'type' coordinate produced by calc_landcover
        (17 CABLE vegetation types, with empty strings at indices 11 & 12).
        """
        time = np.arange(3)
        lat = np.linspace(-90, 90, 4)
        lon = np.linspace(0, 360, 5)
        n_types = 5

        data = np.random.rand(3, n_types, 4, 5).astype(np.float32)

        # Unicode array (dtype kind 'U') with two empty-string slots
        veg_types = np.array(
            ["Evergreen_Needleleaf", "Evergreen_Broadleaf", "", "", "Shrub"],
            dtype=str,  # dtype=str gives kind 'U'
        )

        ds = xr.Dataset(
            {
                "landCoverFrac": (
                    ["time", "type", "lat", "lon"],
                    data,
                    {"_FillValue": 1e20, "units": "%"},
                ),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": ("lat", lat),
                "lon": ("lon", lon),
                "type": ("type", veg_types),
            },
            attrs={
                "variable_id": "landCoverFrac",
                "table_id": "Lmon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            },
        )
        return ds

    @pytest.fixture
    def dataset_with_multiple_string_coords(self):
        """
        Create dataset with multiple string coordinates.
        """
        time = np.arange(12)
        lat = np.linspace(-90, 90, 10)
        lon = np.linspace(0, 360, 10)

        data = np.random.rand(12, 10, 10).astype(np.float32)

        ds = xr.Dataset(
            {
                "baresoilFrac": (
                    ["time", "lat", "lon"],
                    data,
                    {"_FillValue": 1e20, "units": "%"},
                ),
            },
            coords={
                "time": (
                    "time",
                    time,
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": ("lat", lat),
                "lon": ("lon", lon),
                "type": np.array(b"bare_ground", dtype="|S11"),
                "region": np.array(b"global", dtype="|S6"),
            },
            attrs={
                "variable_id": "baresoilFrac",
                "table_id": "Lmon",
                "source_id": "ACCESS-ESM1-5",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            },
        )
        return ds

    @pytest.fixture
    def cmoriser_with_dataset(self, mock_vocab, mock_mapping, sample_dataset, temp_dir):
        """Create a CMORiser instance with a valid dataset attached."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )
        cmoriser.ds = sample_dataset
        cmoriser.cmor_name = "tas"
        return cmoriser

    @pytest.fixture
    def cmoriser_with_dask_dataset(
        self, mock_vocab, mock_mapping, sample_dask_dataset, temp_dir
    ):
        """Create a CMORiser instance with a Dask-backed dataset."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Omon.tos",
            enable_chunking=True,
            chunk_size_mb=4.0,
            enable_compression=True,
            compression_level=4,
        )
        cmoriser.ds = sample_dask_dataset
        cmoriser.cmor_name = "tos"
        return cmoriser

    # ==================== Attribute Validation Tests ====================

    @pytest.mark.unit
    def test_write_raises_error_when_missing_required_attributes(
        self, mock_vocab, mock_mapping, sample_dataset_missing_attrs, temp_dir, caplog
    ):
        """
        Test that write() raises ValueError when required CMIP6 attributes are missing.

        Required attributes: variable_id, table_id, source_id, experiment_id,
                           variant_label, grid_label
        """
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )
        cmoriser.ds = sample_dataset_missing_attrs
        cmoriser.cmor_name = "tas"

        # Mock psutil to avoid memory checks
        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            import logging

            with caplog.at_level(logging.WARNING, logger="access_moppy.base"):
                cmoriser.write()

            # Check that warning was logged
            assert "Missing required global attributes" in caplog.text

    # ==================== Memory Estimation Tests ====================

    @pytest.mark.unit
    def test_write_data_size_estimation(self, cmoriser_with_dataset):
        """
        Test that data size estimation is reasonable.

        Sample dataset: float32 (4 bytes) × 12 × 10 × 10 = 4,800 bytes for main var
        With 1.5x overhead factor, total should be well under 1 GB.
        """
        ds = cmoriser_with_dataset.ds

        # Calculate expected size manually
        total_size = 0
        for var in ds.variables:
            vdat = ds[var]
            var_size = vdat.dtype.itemsize
            for dim in vdat.dims:
                var_size *= ds.sizes[dim]
            total_size += var_size

        expected_size_with_overhead = int(total_size * 1.5)

        # Verify the size is small (test data should be < 1 MB)
        assert expected_size_with_overhead < 1 * 1024**2

    # ==================== Direct Write Tests ====================

    @pytest.mark.unit
    def test_write_creates_file_direct(self, cmoriser_with_dataset, temp_dir):
        """Test that write() creates a NetCDF file with direct write (non-Dask)."""
        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser_with_dataset.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            assert len(output_files) == 1

    @pytest.mark.unit
    def test_write_creates_correct_filename(self, cmoriser_with_dataset, temp_dir):
        """Test that write() creates file with correct CMIP6 filename format."""
        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser_with_dataset.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            filename = output_files[0].name

            assert filename.startswith("tas_")
            assert "_Amon_" in filename
            assert "_ACCESS-ESM1-5_" in filename
            assert "_historical_" in filename
            assert "_r1i1p1f1_" in filename
            assert "_gn_" in filename
            assert filename.endswith(".nc")

    @pytest.mark.unit
    def test_write_creates_valid_netcdf_structure(
        self, cmoriser_with_dataset, temp_dir
    ):
        """Test that write() creates a valid NetCDF file with correct structure."""
        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser_with_dataset.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                # Check dimensions
                assert "time" in ds_out.dims
                assert "lat" in ds_out.dims
                assert "lon" in ds_out.dims

                # Check main variable
                assert "tas" in ds_out.data_vars

                # Check global attributes
                assert ds_out.attrs["variable_id"] == "tas"
                assert ds_out.attrs["table_id"] == "Amon"
                assert ds_out.attrs["source_id"] == "ACCESS-ESM1-5"
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_preserves_data_values(self, cmoriser_with_dataset, temp_dir):
        """Test that write() preserves data values correctly."""
        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            original_data = cmoriser_with_dataset.ds["tas"].values.copy()

            cmoriser_with_dataset.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                np.testing.assert_array_almost_equal(
                    ds_out["tas"].values, original_data
                )
            finally:
                ds_out.close()

    # ==================== Chunked Write Tests ====================

    @pytest.mark.unit
    def test_write_uses_chunked_write_for_dask_array(
        self, cmoriser_with_dask_dataset, temp_dir, caplog
    ):
        """Test that write() uses chunked writing for Dask arrays."""
        import logging

        with caplog.at_level(logging.DEBUG, logger="access_moppy.base"):
            cmoriser_with_dask_dataset.write()

        # Should indicate chunked writing
        assert "Using chunked writing" in caplog.text
        assert "timesteps/chunk" in caplog.text

    @pytest.mark.unit
    def test_write_chunked_creates_valid_file(
        self, cmoriser_with_dask_dataset, temp_dir
    ):
        """Test that chunked write creates a valid NetCDF file."""
        cmoriser_with_dask_dataset.write()

        output_files = list(Path(temp_dir).glob("*.nc"))
        assert len(output_files) == 1

        ds_out = xr.open_dataset(output_files[0])
        try:
            assert "tos" in ds_out.data_vars
            assert ds_out.sizes["time"] == 24  # All timesteps written
        finally:
            ds_out.close()

    @pytest.mark.unit
    def test_write_chunked_preserves_data_values(
        self, cmoriser_with_dask_dataset, temp_dir
    ):
        """Test that chunked write preserves data values correctly."""
        # Compute original data before write
        original_data = cmoriser_with_dask_dataset.ds["tos"].values.copy()

        cmoriser_with_dask_dataset.write()

        output_files = list(Path(temp_dir).glob("*.nc"))
        ds_out = xr.open_dataset(output_files[0])

        try:
            np.testing.assert_array_almost_equal(ds_out["tos"].values, original_data)
        finally:
            ds_out.close()

    # ==================== System Memory Check Tests ====================

    @pytest.mark.unit
    def test_write_proceeds_when_system_memory_sufficient(
        self, cmoriser_with_dataset, temp_dir
    ):
        """
        Test that write() proceeds normally when system memory is sufficient.

        Scenario: Non-dask (eager) dataset, plenty of system memory available.
        Expected: File is created successfully.
        """
        with patch("psutil.virtual_memory") as mock_mem:
            # Mock sufficient available memory (16 GB)
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser_with_dataset.write()

            # Verify output file was created
            output_files = list(Path(temp_dir).glob("*.nc"))
            assert len(output_files) == 1

    # ==================== Import Error Handling Tests ====================

    @pytest.mark.unit
    def test_write_eager_raises_memory_error_when_oom(
        self, cmoriser_with_dataset, temp_dir
    ):
        """
        Test that the eager (non-dask) write path raises MemoryError when the
        estimated data size exceeds available system memory.
        """
        with patch("psutil.virtual_memory") as mock_mem:
            # Report only 1 byte available — guaranteed to trigger OOM
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=1,
            )

            with pytest.raises(MemoryError, match="exceeds available system memory"):
                cmoriser_with_dataset.write()

    # ==================== Output File Tests ====================

    @pytest.mark.unit
    def test_write_creates_correct_cmip6_filename(
        self, cmoriser_with_dataset, temp_dir
    ):
        """
        Test that write() creates file with correct CMIP6 filename format.

        Expected format: {var}_{table}_{source}_{exp}_{variant}_{grid}_{timerange}.nc
        Example: tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_200001-200012.nc
        """
        with patch("psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser_with_dataset.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            assert len(output_files) == 1

            filename = output_files[0].name

            # Check filename components
            assert filename.startswith("tas_")
            assert "_Amon_" in filename
            assert "_ACCESS-ESM1-5_" in filename
            assert "_historical_" in filename
            assert "_r1i1p1f1_" in filename
            assert "_gn_" in filename
            assert filename.endswith(".nc")

    # ==================== Logging Tests ====================

    @pytest.mark.unit
    def test_write_prints_output_path(self, cmoriser_with_dataset, temp_dir, caplog):
        """
        Test that write() logs the output file path after completion.
        """
        import logging

        with patch("psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            with caplog.at_level(logging.INFO, logger="access_moppy.base"):
                cmoriser_with_dataset.write()

            assert "CMORised output written to" in caplog.text
            assert str(temp_dir) in caplog.text

    # ==================== String Coordinate Preparation Tests ====================

    @pytest.mark.unit
    def test_prepare_string_coordinates_detects_scalar_byte_string(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test detection of scalar byte string coordinates."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        string_coords_info = cmoriser._prepare_string_coordinates()

        assert "type" in string_coords_info
        assert string_coords_info["type"]["is_scalar"] is True
        assert string_coords_info["type"]["strlen_size"] == 11
        assert string_coords_info["type"]["strlen_dim"] == "type_strlen"
        assert string_coords_info["type"]["dims"] == ()

    @pytest.mark.unit
    def test_prepare_string_coordinates_detects_array_string(
        self, mock_vocab, mock_mapping, dataset_with_array_string_coord, temp_dir
    ):
        """Test detection of array string coordinates."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.regionTemp",
        )
        cmoriser.ds = dataset_with_array_string_coord
        cmoriser.cmor_name = "regionTemp"

        string_coords_info = cmoriser._prepare_string_coordinates()

        assert "region" in string_coords_info
        assert string_coords_info["region"]["is_scalar"] is False
        assert string_coords_info["region"]["strlen_size"] == 5
        assert string_coords_info["region"]["dims"] == ("region",)

    @pytest.mark.unit
    def test_prepare_string_coordinates_converts_unicode(
        self, mock_vocab, mock_mapping, dataset_with_unicode_string_coord, temp_dir
    ):
        """Test Unicode to byte string conversion."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_unicode_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        string_coords_info = cmoriser._prepare_string_coordinates()

        assert "type" in string_coords_info
        values = string_coords_info["type"]["values"]
        assert isinstance(values, (bytes, np.ndarray))

    @pytest.mark.unit
    def test_prepare_string_coordinates_handles_unicode_array_with_empty_strings(
        self,
        mock_vocab,
        mock_mapping,
        dataset_with_array_unicode_string_coord,
        temp_dir,
    ):
        """Array unicode coord (dtype kind 'U') with empty-string slots is handled.

        Exercises the materialise-as-list branch in _prepare_string_coordinates so
        that max() does not exhaust the iterator before the encode step, and verifies
        that max_len is at least 1 even when some entries are empty strings.
        """
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.landCoverFrac",
        )
        cmoriser.ds = dataset_with_array_unicode_string_coord
        cmoriser.cmor_name = "landCoverFrac"

        string_coords_info = cmoriser._prepare_string_coordinates()

        assert "type" in string_coords_info
        info = string_coords_info["type"]
        assert info["is_scalar"] is False
        # max_len must be >= 1 (not 0) even though some slots are empty strings
        assert info["strlen_size"] >= 1
        # The longest non-empty label is "Evergreen_Needleleaf" (20 chars)
        assert info["strlen_size"] == 20
        # values must be a byte-string array with the correct shape
        assert isinstance(info["values"], np.ndarray)
        assert info["values"].dtype.kind == "S"
        assert info["values"].shape == (5,)

    @pytest.mark.unit
    def test_prepare_string_coordinates_empty_when_no_strings(
        self, mock_vocab, mock_mapping, sample_dataset, temp_dir
    ):
        """Test that empty dict is returned when no string coordinates exist."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )
        cmoriser.ds = sample_dataset
        cmoriser.cmor_name = "tas"

        string_coords_info = cmoriser._prepare_string_coordinates()

        assert string_coords_info == {}

    @pytest.mark.unit
    def test_prepare_string_coordinates_handles_multiple_coords(
        self, mock_vocab, mock_mapping, dataset_with_multiple_string_coords, temp_dir
    ):
        """Test detection of multiple string coordinates."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_multiple_string_coords
        cmoriser.cmor_name = "baresoilFrac"

        string_coords_info = cmoriser._prepare_string_coordinates()

        assert len(string_coords_info) == 2
        assert "type" in string_coords_info
        assert "region" in string_coords_info

    @pytest.mark.unit
    def test_prepare_string_coordinates_calculates_max_length(
        self, mock_vocab, mock_mapping, dataset_with_array_string_coord, temp_dir
    ):
        """Test that maximum string length is calculated correctly for arrays."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.regionTemp",
        )
        cmoriser.ds = dataset_with_array_string_coord
        cmoriser.cmor_name = "regionTemp"

        string_coords_info = cmoriser._prepare_string_coordinates()

        # "ocean" is 5 chars, should be the max
        assert string_coords_info["region"]["strlen_size"] == 5

    # ==================== String Coordinate Write Integration Tests ====================

    @pytest.mark.unit
    def test_write_scalar_string_coord_stays_in_coords(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that scalar string coordinate remains in coords after write/read."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            assert len(output_files) == 1

            ds_out = xr.open_dataset(output_files[0])
            try:
                assert "type" in ds_out.coords
                assert "type" not in ds_out.data_vars
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_string_coord_has_correct_dtype(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that string coordinate has correct |S dtype after write."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                assert ds_out["type"].dtype.kind == "S"
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_string_coord_has_correct_encoding(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that string coordinate has correct CF-compliant encoding."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                encoding = ds_out["type"].encoding
                assert encoding.get("dtype") == np.dtype("S1")
                assert encoding.get("char_dim_name") == "type_strlen"
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_adds_coordinates_attribute(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that coordinates attribute is added to main variable."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))

            # Use netCDF4 to read the file directly (xarray may decode attributes differently)
            with nc.Dataset(output_files[0], "r") as ds_nc:
                coords_attr = ds_nc.variables["baresoilFrac"].getncattr("coordinates")
                assert "type" in coords_attr

    @pytest.mark.unit
    def test_write_preserves_string_value(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that string value is preserved correctly."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                type_value = ds_out["type"].values
                if isinstance(type_value, bytes):
                    assert type_value == b"bare_ground"
                elif isinstance(type_value, np.ndarray):
                    if type_value.ndim == 0:
                        assert type_value.item() == b"bare_ground"
                    else:
                        assert b"bare_ground" in type_value
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_array_string_coord_dimensions(
        self, mock_vocab, mock_mapping, dataset_with_array_string_coord, temp_dir
    ):
        """Test that array string coordinate has correct dimensions."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.regionTemp",
        )
        cmoriser.ds = dataset_with_array_string_coord
        cmoriser.cmor_name = "regionTemp"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))

            # Use netCDF4 to check dimensions (xarray hides strlen dimension)
            with nc.Dataset(output_files[0], "r") as ds_nc:
                assert "region" in ds_nc.variables
                assert "region_strlen" in ds_nc.dimensions
                assert ds_nc.dimensions["region_strlen"].size == 5

            # Also verify with xarray that region coordinate exists
            ds_out = xr.open_dataset(output_files[0])
            try:
                assert "region" in ds_out.coords
                assert len(ds_out["region"]) == 3
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_unicode_converted_to_bytes(
        self, mock_vocab, mock_mapping, dataset_with_unicode_string_coord, temp_dir
    ):
        """Test that Unicode strings are converted to byte strings."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_unicode_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                assert ds_out["type"].dtype.kind == "S"
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_prints_string_coord_detection(
        self,
        mock_vocab,
        mock_mapping,
        dataset_with_scalar_string_coord,
        temp_dir,
        caplog,
    ):
        """Test that string coordinate detection is logged."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        import logging

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            with caplog.at_level(logging.DEBUG, logger="access_moppy.base"):
                cmoriser.write()

            assert "Detected string coordinate 'type'" in caplog.text
            assert "String coordinates processed: type" in caplog.text

    @pytest.mark.unit
    def test_write_preserves_numerical_data_with_string_coords(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that numerical data values are preserved when string coords present."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        original_data = dataset_with_scalar_string_coord["baresoilFrac"].values.copy()

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                np.testing.assert_array_almost_equal(
                    ds_out["baresoilFrac"].values, original_data
                )
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_preserves_string_coord_attributes(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that string coordinate attributes are preserved."""
        # Add attributes to type coordinate
        dataset_with_scalar_string_coord["type"].attrs["long_name"] = "Surface type"
        dataset_with_scalar_string_coord["type"].attrs["standard_name"] = "area_type"

        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))
            ds_out = xr.open_dataset(output_files[0])

            try:
                assert ds_out["type"].attrs.get("long_name") == "Surface type"
                assert ds_out["type"].attrs.get("standard_name") == "area_type"
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_multiple_string_coords(
        self, mock_vocab, mock_mapping, dataset_with_multiple_string_coords, temp_dir
    ):
        """Test writing multiple string coordinates."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_multiple_string_coords
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))

            # Use netCDF4 to check dimensions and attributes
            with nc.Dataset(output_files[0], "r") as ds_nc:
                # Check strlen dimensions exist
                assert "type_strlen" in ds_nc.dimensions
                assert "region_strlen" in ds_nc.dimensions

                # Check coordinates attribute
                coords_attr = ds_nc.variables["baresoilFrac"].getncattr("coordinates")
                assert "type" in coords_attr
                assert "region" in coords_attr

            # Verify with xarray that coordinates exist
            ds_out = xr.open_dataset(output_files[0])
            try:
                assert "type" in ds_out.coords
                assert "region" in ds_out.coords
            finally:
                ds_out.close()

    @pytest.mark.unit
    def test_write_creates_strlen_dimension(
        self, mock_vocab, mock_mapping, dataset_with_scalar_string_coord, temp_dir
    ):
        """Test that strlen dimension is created correctly."""
        cmoriser = CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.baresoilFrac",
        )
        cmoriser.ds = dataset_with_scalar_string_coord
        cmoriser.cmor_name = "baresoilFrac"

        with patch("access_moppy.base.psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            cmoriser.write()

            output_files = list(Path(temp_dir).glob("*.nc"))

            # Use netCDF4 to check dimension (xarray hides it after decoding)
            with nc.Dataset(output_files[0], "r") as ds_nc:
                assert "type_strlen" in ds_nc.dimensions
                assert ds_nc.dimensions["type_strlen"].size == 11


def _make_write_cmoriser(tmp_path, ds, cmor_name):
    """
    Create a minimal CMORiser wired for write() without file IO setup.
    All heavy dependencies (chunking, compression, drs_root) are disabled.
    """
    cmoriser = object.__new__(CMORiser)
    cmoriser.ds = ds
    cmoriser.cmor_name = cmor_name
    cmoriser.compound_name = f"Amon.{cmor_name}"
    cmoriser.output_path = str(tmp_path)
    cmoriser.drs_root = None
    cmoriser.enable_compression = False
    cmoriser.compression_level = 0
    cmoriser.chunker = None
    cmoriser.enable_chunking = False

    vocab = MagicMock()
    vocab.get_required_attribute_names.return_value = []
    vocab.generate_filename.return_value = f"{cmor_name}_test.nc"
    cmoriser.vocab = vocab

    return cmoriser


class TestWriteDecodedTimeEncoding:
    """
    Cover the branch in write() that encodes datetime64 / cftime time
    coordinates back to float64 before writing to netCDF4.

    Both Phase 1 (createVariable dtype = "f8") and Phase 2
    (date2num encoding path) are exercised by reading back the written file.
    """

    @pytest.mark.unit
    def test_write_datetime64_time_encoded_to_float(self, tmp_path):
        """datetime64 time is encoded to numeric float64 in the output file."""
        import pandas as pd

        time = pd.date_range("2020-01-01", periods=2, freq="MS")
        ds = xr.Dataset(
            {
                "tas": xr.DataArray(
                    np.array([[280.0, 281.0]]).T,
                    dims=["time", "lat"],
                    coords={"time": time, "lat": [0.0]},
                    attrs={"units": "K", "standard_name": "air_temperature"},
                )
            }
        )
        ds["time"].attrs = {
            "units": "days since 2020-01-01",
            "calendar": "standard",
            "standard_name": "time",
            "axis": "T",
        }
        ds.attrs = {}

        assert np.issubdtype(ds["time"].dtype, np.datetime64)

        cmoriser = _make_write_cmoriser(tmp_path, ds, "tas")
        cmoriser.write()

        # Read back with netCDF4 (bypasses xarray auto-decoding)
        out = tmp_path / "tas_test.nc"
        with nc.Dataset(out, "r") as f:
            assert f["time"].dtype in (np.float64, np.float32)
            # 2020-01-01 = 0 days, 2020-02-01 = 31 days since 2020-01-01
            assert f["time"][0] == pytest.approx(0.0, abs=1.0)
            assert f["time"][1] == pytest.approx(31.0, abs=1.0)

    @pytest.mark.unit
    def test_write_cftime_time_encoded_to_float(self, tmp_path):
        """cftime (dtype=object) time is encoded to numeric float64 in the output file."""
        cf_time = xr.cftime_range(
            "2020-01-01", periods=2, freq="MS", calendar="gregorian"
        )
        ds = xr.Dataset(
            {
                "tas": xr.DataArray(
                    np.array([[280.0, 281.0]]).T,
                    dims=["time", "lat"],
                    coords={"time": cf_time, "lat": [0.0]},
                    attrs={"units": "K"},
                )
            }
        )
        ds["time"].attrs = {
            "units": "days since 2020-01-01",
            "calendar": "gregorian",
            "standard_name": "time",
            "axis": "T",
        }
        ds.attrs = {}

        assert ds["time"].dtype == object  # cftime

        cmoriser = _make_write_cmoriser(tmp_path, ds, "tas")
        cmoriser.write()

        out = tmp_path / "tas_test.nc"
        with nc.Dataset(out, "r") as f:
            assert f["time"].dtype in (np.float64, np.float32)
            assert f["time"][0] == pytest.approx(0.0, abs=1.0)
            assert f["time"][1] == pytest.approx(31.0, abs=1.0)

    @pytest.mark.unit
    def test_write_numeric_time_unchanged(self, tmp_path):
        """Numeric float64 time (non-decoded) passes through write() unchanged."""
        time_values = np.array([0.0, 31.0], dtype=np.float64)
        ds = xr.Dataset(
            {
                "tas": xr.DataArray(
                    np.array([[280.0, 281.0]]).T,
                    dims=["time", "lat"],
                    coords={
                        "time": xr.Variable(
                            "time",
                            time_values,
                            attrs={
                                "units": "days since 2020-01-01",
                                "calendar": "standard",
                            },
                        ),
                        "lat": [0.0],
                    },
                    attrs={"units": "K"},
                )
            }
        )
        ds.attrs = {}

        cmoriser = _make_write_cmoriser(tmp_path, ds, "tas")
        cmoriser.write()

        out = tmp_path / "tas_test.nc"
        with nc.Dataset(out, "r") as f:
            assert f["time"][0] == pytest.approx(0.0)
            assert f["time"][1] == pytest.approx(31.0)


class TestPreprocessAuxTimeCoords:
    """Tests that _preprocess drops time_0/time_1 auxiliary UM coordinates.

    The fix must run inside _preprocess (per-file, before xr.open_mfdataset
    combines files), because join='outer' unions every distinct coordinate
    value into a full dimension during the combine step.

    We mock xr.open_mfdataset so that the preprocess kwarg receives a
    dataset that already has time_1/time_0 as non-dimension coordinates.
    This is necessary because with decode_cf=False, xarray does not parse
    the NetCDF 'coordinates' attribute, so aux coords read from real files
    appear as data variables and are filtered out before _preprocess can
    call drop_vars.  The mock lets us test the coordinate-dropping branch
    directly.
    """

    @pytest.fixture
    def mock_vocab(self):
        vocab = Mock()
        vocab.__class__.__name__ = "CMIP6Vocabulary"
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        return {
            "CF standard Name": "soil_carbon_content",
            "units": "kg m-2",
            "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            "positive": None,
        }

    @pytest.fixture
    def base_cmoriser(self, mock_vocab, mock_mapping, tmp_path):
        # Create a minimal valid NetCDF file so the probe in load_dataset succeeds
        dummy_nc = tmp_path / "dummy.nc"
        ds_dummy = xr.Dataset(
            {"cSoil": (["time", "lat", "lon"], np.ones((1, 2, 2), dtype="f4"))},
            coords={
                "time": (
                    "time",
                    [0.0],
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": ("lat", [0.0, 1.0]),
                "lon": ("lon", [0.0, 1.0]),
            },
        )
        ds_dummy.to_netcdf(str(dummy_nc))
        return CMORiser(
            input_paths=[str(dummy_nc)],
            output_path=str(tmp_path),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Lmon.cSoil",
            validate_frequency=False,
            enable_chunking=False,
        )

    def _make_open_mfdataset_mock(self, extra_coords):
        """Return a side_effect for xr.open_mfdataset that applies the
        preprocess kwarg to a single-file dataset containing extra_coords
        as non-dimension coordinates.  This simulates what open_mfdataset
        does per file, bypassing the decode_cf=False coordinate-detection
        limitation of the real NetCDF backend."""

        def fake_open_mfdataset(paths, *, preprocess=None, **kwargs):
            ds = xr.Dataset(
                {"cSoil": (["time", "lat", "lon"], np.ones((1, 2, 2), dtype="f4"))},
                coords={
                    "time": (
                        "time",
                        [0.0],
                        {"units": "days since 2000-01-01", "calendar": "standard"},
                    ),
                    "lat": ("lat", [0.0, 1.0]),
                    "lon": ("lon", [0.0, 1.0]),
                    **extra_coords,
                },
            )
            if preprocess is not None:
                ds = preprocess(ds)
            return ds

        return fake_open_mfdataset

    @pytest.mark.unit
    def test_preprocess_drops_time_1(self, base_cmoriser):
        """time_1 as a non-dimension coordinate must be dropped by _preprocess."""
        mock_fn = self._make_open_mfdataset_mock({"time_1": ("time", [-0.5])})
        with patch("access_moppy.base.xr.open_mfdataset", side_effect=mock_fn):
            with patch.object(base_cmoriser, "_normalize_missing_values_early"):
                base_cmoriser.load_dataset(required_vars={"cSoil"})

        assert "time_1" not in base_cmoriser.ds.coords
        assert "time_1" not in base_cmoriser.ds.data_vars
        assert "cSoil" in base_cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_preprocess_drops_time_0(self, base_cmoriser):
        """time_0 as a non-dimension coordinate must be dropped by _preprocess."""
        mock_fn = self._make_open_mfdataset_mock({"time_0": ("time", [-1.0])})
        with patch("access_moppy.base.xr.open_mfdataset", side_effect=mock_fn):
            with patch.object(base_cmoriser, "_normalize_missing_values_early"):
                base_cmoriser.load_dataset(required_vars={"cSoil"})

        assert "time_0" not in base_cmoriser.ds.coords
        assert "time_0" not in base_cmoriser.ds.data_vars
        assert "cSoil" in base_cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_preprocess_drops_both_time_0_and_time_1(self, base_cmoriser):
        """Both time_0 and time_1 must be dropped when both are present."""
        mock_fn = self._make_open_mfdataset_mock(
            {"time_0": ("time", [-1.0]), "time_1": ("time", [-0.5])}
        )
        with patch("access_moppy.base.xr.open_mfdataset", side_effect=mock_fn):
            with patch.object(base_cmoriser, "_normalize_missing_values_early"):
                base_cmoriser.load_dataset(required_vars={"cSoil"})

        assert "time_0" not in base_cmoriser.ds.coords
        assert "time_1" not in base_cmoriser.ds.coords
        assert "cSoil" in base_cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_preprocess_no_aux_coords_unaffected(self, base_cmoriser):
        """Files without time_0/time_1 must load normally without error."""
        mock_fn = self._make_open_mfdataset_mock({})
        with patch("access_moppy.base.xr.open_mfdataset", side_effect=mock_fn):
            with patch.object(base_cmoriser, "_normalize_missing_values_early"):
                base_cmoriser.load_dataset(required_vars={"cSoil"})

        assert "cSoil" in base_cmoriser.ds.data_vars


class TestLoadDatasetFxFile:
    """Tests for the time-independent (fx) file loading branch of load_dataset.

    When the first input file has no 'time' dimension, load_dataset must use
    xr.open_dataset (not open_mfdataset) to avoid adding a spurious time axis,
    and must honour the optional required_vars filtering.
    """

    @pytest.fixture
    def mock_vocab(self):
        vocab = Mock()
        vocab.__class__.__name__ = "CMIP6Vocabulary"
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        return {
            "CF standard Name": "soil_moisture_content_at_field_capacity",
            "units": "kg m-2",
            "dimensions": {"lat": "lat", "lon": "lon"},
            "positive": None,
        }

    @pytest.fixture
    def fx_nc(self, tmp_path):
        """Minimal fx NetCDF file: two spatial-only variables, no time dimension."""
        nc_path = tmp_path / "fx.nc"
        ds = xr.Dataset(
            {
                "mrsofc": (["lat", "lon"], np.ones((3, 4), dtype="f4")),
                "sftlf": (["lat", "lon"], np.full((3, 4), 100.0, dtype="f4")),
            },
            coords={
                "lat": ("lat", [-30.0, 0.0, 30.0]),
                "lon": ("lon", [0.0, 90.0, 180.0, 270.0]),
            },
        )
        ds.to_netcdf(str(nc_path))
        return nc_path

    @pytest.fixture
    def fx_cmoriser(self, mock_vocab, mock_mapping, fx_nc, tmp_path):
        return CMORiser(
            input_paths=[str(fx_nc)],
            output_path=str(tmp_path),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="fx.mrsofc",
            enable_chunking=False,
        )

    # ------------------------------------------------------------------
    # Baseline: no required_vars
    # ------------------------------------------------------------------

    @pytest.mark.unit
    def test_fx_file_loaded_without_time_dim(self, fx_cmoriser):
        """Loading an fx file must not produce a 'time' dimension."""
        with patch.object(fx_cmoriser, "_normalize_missing_values_early"):
            fx_cmoriser.load_dataset()

        assert "time" not in fx_cmoriser.ds.dims
        assert "mrsofc" in fx_cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_fx_file_without_required_vars_keeps_all_vars(self, fx_cmoriser):
        """Without required_vars all data variables in the file are retained."""
        with patch.object(fx_cmoriser, "_normalize_missing_values_early"):
            fx_cmoriser.load_dataset()

        assert "mrsofc" in fx_cmoriser.ds.data_vars
        assert "sftlf" in fx_cmoriser.ds.data_vars

    # ------------------------------------------------------------------
    # required_vars filtering
    # ------------------------------------------------------------------

    @pytest.mark.unit
    def test_fx_file_required_vars_keeps_only_requested(self, fx_cmoriser):
        """required_vars restricts the loaded dataset to the named variable."""
        with patch.object(fx_cmoriser, "_normalize_missing_values_early"):
            fx_cmoriser.load_dataset(required_vars={"mrsofc"})

        assert "mrsofc" in fx_cmoriser.ds.data_vars
        assert "sftlf" not in fx_cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_fx_file_required_vars_silently_ignores_absent_vars(self, fx_cmoriser):
        """Variables listed in required_vars but absent from the file are silently dropped."""
        with patch.object(fx_cmoriser, "_normalize_missing_values_early"):
            fx_cmoriser.load_dataset(required_vars={"mrsofc", "nonexistent_var"})

        assert "mrsofc" in fx_cmoriser.ds.data_vars
        assert "nonexistent_var" not in fx_cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_fx_file_required_vars_preserves_spatial_dims(self, fx_cmoriser):
        """Spatial dimensions (lat/lon) are preserved after required_vars filtering."""
        with patch.object(fx_cmoriser, "_normalize_missing_values_early"):
            fx_cmoriser.load_dataset(required_vars={"mrsofc"})

        assert "lat" in fx_cmoriser.ds.dims
        assert "lon" in fx_cmoriser.ds.dims

    @pytest.mark.unit
    def test_fx_file_data_values_preserved(self, fx_cmoriser):
        """Data values read from the fx file are unmodified after loading."""
        with patch.object(fx_cmoriser, "_normalize_missing_values_early"):
            fx_cmoriser.load_dataset(required_vars={"mrsofc"})

        np.testing.assert_array_equal(fx_cmoriser.ds["mrsofc"].values, 1.0)

    # ------------------------------------------------------------------
    # UM fx files: source file carries time=1 that must be squeezed out
    # ------------------------------------------------------------------

    @pytest.mark.unit
    def test_fx_file_with_um_time1_dim_is_squeezed(
        self, mock_vocab, mock_mapping, tmp_path
    ):
        """UM fx files always have a size-1 time dimension in the source.

        After loading, that dimension must be dropped so downstream CMOR
        processing sees (lat, lon) rather than (time=1, lat, lon).
        """
        nc_path = tmp_path / "fx_with_time.nc"
        ds = xr.Dataset(
            {"mrsofc": (["time", "lat", "lon"], np.ones((1, 3, 4), dtype="f4"))},
            coords={
                "time": (["time"], [0.0], {"units": "days since 2000-01-01"}),
                "lat": ("lat", [-30.0, 0.0, 30.0]),
                "lon": ("lon", [0.0, 90.0, 180.0, 270.0]),
            },
        )
        ds.to_netcdf(str(nc_path))

        cmoriser = CMORiser(
            input_paths=[str(nc_path)],
            output_path=str(tmp_path),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="fx.mrsofc",
            enable_chunking=False,
        )
        with patch.object(cmoriser, "_normalize_missing_values_early"):
            cmoriser.load_dataset(required_vars={"mrsofc"})

        assert (
            "time" not in cmoriser.ds.dims
        ), "time=1 from UM source file should be squeezed out for fx variables"
        assert cmoriser.ds["mrsofc"].dims == ("lat", "lon")


class TestHasTimeProbeLogic:
    """Tests for the _has_time probe fallback in load_dataset (base.py).

    When none of the required variables exist in the probe file,
    _probe_target_vars is empty and the original any([]) returned False,
    silently falling into the fx branch and loading only one file.

    The fix: check whether any data variable in the probe file has a time
    dimension, so time-series files still use open_mfdataset.
    """

    @pytest.fixture
    def mock_vocab(self):
        vocab = Mock()
        vocab.__class__.__name__ = "CMIP6Vocabulary"
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        return {
            "CF standard Name": "geopotential_height",
            "units": "m",
            "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            "positive": None,
        }

    @pytest.fixture
    def time_series_nc_without_target(self, tmp_path):
        """NetCDF file that is time-dependent but lacks the required target variable.

        Uses a non-bounds data variable so it appears in _probe.data_vars
        after being read back with decode_cf=False.
        """
        nc_path = tmp_path / "timeseries_no_target.nc"
        ds = xr.Dataset(
            # A real data variable with a time dim — but NOT the required fld_s16i201
            {"some_other_var": (["time", "lat"], np.zeros((3, 5)))},
            coords={
                "time": (
                    ["time"],
                    np.arange(3, dtype=float),
                    {"units": "days since 2000-01-01", "calendar": "standard"},
                ),
                "lat": np.linspace(-90, 90, 5),
            },
        )
        ds.to_netcdf(str(nc_path))
        return nc_path

    @pytest.fixture
    def static_nc_without_target(self, tmp_path):
        """NetCDF file with no time dimension and no target variable."""
        nc_path = tmp_path / "static_no_target.nc"
        ds = xr.Dataset(
            {"lat_bnds": (["lat", "bnds"], np.zeros((3, 2)))},
            coords={"lat": ("lat", [-30.0, 0.0, 30.0])},
        )
        ds.to_netcdf(str(nc_path))
        return nc_path

    @pytest.mark.unit
    def test_time_series_file_uses_open_mfdataset_when_target_absent(
        self, mock_vocab, mock_mapping, time_series_nc_without_target, tmp_path
    ):
        """When required vars are absent from the probe but the file IS time-dependent,
        load_dataset must take the open_mfdataset path (_has_time=True).

        Before the fix: any([]) == False → _has_time=False → only first file opened.
        After the fix: infer from other data vars → _has_time=True → open_mfdataset.
        """
        cmoriser = CMORiser(
            input_paths=[str(time_series_nc_without_target)],
            output_path=str(tmp_path),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="day.zg",
            validate_frequency=False,
            enable_chunking=False,
        )

        with (
            patch("access_moppy.base.xr.open_mfdataset") as mock_mfd,
            patch.object(cmoriser, "_normalize_missing_values_early"),
        ):
            mock_mfd.return_value = xr.Dataset()
            cmoriser.load_dataset(required_vars={"fld_s16i201"})

        mock_mfd.assert_called_once()

    @pytest.mark.unit
    def test_static_file_uses_open_dataset_when_target_absent(
        self, mock_vocab, mock_mapping, static_nc_without_target, tmp_path
    ):
        """When required vars are absent and the file has no time dimension at all,
        load_dataset must take the open_dataset path (_has_time=False).
        """
        cmoriser = CMORiser(
            input_paths=[str(static_nc_without_target)],
            output_path=str(tmp_path),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="fx.orog",
            validate_frequency=False,
            enable_chunking=False,
        )

        with (
            patch("access_moppy.base.xr.open_mfdataset") as mock_mfd,
            patch.object(cmoriser, "_normalize_missing_values_early"),
        ):
            cmoriser.load_dataset(required_vars={"fld_s00i033"})

        mock_mfd.assert_not_called()


# ---------------------------------------------------------------------------
# _check_units
# ---------------------------------------------------------------------------


class TestCheckUnits:
    """Tests for CMORiser._check_units."""

    def _make_cmoriser(self, mapping):
        """Return a minimal mock with a .mapping attribute."""
        obj = MagicMock(spec=CMORiser)
        obj.mapping = mapping
        return obj

    @pytest.mark.unit
    def test_no_raise_when_units_match(self):
        """No exception when declared units equal expected units."""
        obj = self._make_cmoriser({"tas": {"units": "K"}})
        CMORiser._check_units(obj, "tas", "K")  # must not raise

    @pytest.mark.unit
    def test_raises_on_mismatch(self):
        """ValueError raised when declared units differ from expected."""
        obj = self._make_cmoriser({"tas": {"units": "degC"}})
        with pytest.raises(ValueError, match="Mapping units mismatch for tas"):
            CMORiser._check_units(obj, "tas", "K")

    @pytest.mark.unit
    def test_no_raise_when_declared_missing(self):
        """No exception when the mapping has no 'units' key (declared is None)."""
        obj = self._make_cmoriser({"tas": {}})
        CMORiser._check_units(obj, "tas", "K")  # declared is None → skip

    @pytest.mark.unit
    def test_no_raise_when_expected_empty(self):
        """No exception when expected is an empty string."""
        obj = self._make_cmoriser({"tas": {"units": "K"}})
        CMORiser._check_units(obj, "tas", "")  # expected falsy → skip

    @pytest.mark.unit
    def test_no_raise_when_variable_not_in_mapping(self):
        """No exception when cmor_name is absent from mapping (declared is None)."""
        obj = self._make_cmoriser({})
        CMORiser._check_units(obj, "tas", "K")  # mapping.get returns {} → declared None

    @pytest.mark.unit
    def test_error_message_contains_declared_and_expected_units(self):
        """Error message mentions both the declared and expected unit strings."""
        obj = self._make_cmoriser({"pr": {"units": "kg m-2 s-1"}})
        with pytest.raises(ValueError, match="kg m-2 s-1") as exc_info:
            CMORiser._check_units(obj, "pr", "mm d-1")
        assert "mm d-1" in str(exc_info.value)
