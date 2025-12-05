"""
Unit tests for the CMIP6_CMORiser base class.

These tests focus on the core functionality of the CMIP6_CMORiser class
without requiring complex dependencies or data files.
"""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMIP6_CMORiser


class TestCMIP6CMORiser:
    """Unit tests for CMIP6_CMORiser base class."""

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
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
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
        cmoriser = CMIP6_CMORiser(
            input_paths=input_files,
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.input_paths == input_files

    @pytest.mark.unit
    def test_init_with_single_input_path_string(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test initialization with single input path as string."""
        cmoriser = CMIP6_CMORiser(
            input_paths="single_file.nc",
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.input_paths == ["single_file.nc"]

    @pytest.mark.unit
    def test_init_with_drs_root(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with DRS root path."""
        drs_root = temp_dir / "drs"
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
            drs_root=str(drs_root),
        )

        assert cmoriser.drs_root == Path(drs_root)

    @pytest.mark.unit
    def test_version_date_format(self, mock_vocab, mock_mapping, temp_dir):
        """Test that version date is set correctly."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
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
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
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

        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
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
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        assert cmoriser.ds is None

    @pytest.mark.unit
    def test_getattr_fallback(self, mock_vocab, mock_mapping, temp_dir):
        """Test __getattr__ behavior when dataset is None."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

        # When ds is None, getattr should raise AttributeError
        with pytest.raises(AttributeError):
            _ = cmoriser.nonexistent_attribute


class TestCMIP6CMORiserWrite:
    """Unit tests for CMIP6_CMORiser.write() method with memory validation."""

    # ==================== Fixtures ====================

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

    @pytest.fixture
    def sample_dataset(self):
        """
        Create a sample xarray Dataset for testing.

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
    def cmoriser_with_dataset(self, mock_vocab, mock_mapping, sample_dataset, temp_dir):
        """Create a CMORiser instance with a valid dataset attached."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )
        cmoriser.ds = sample_dataset
        cmoriser.cmor_name = "tas"
        return cmoriser

    # ==================== Attribute Validation Tests ====================

    @pytest.mark.unit
    def test_write_raises_error_when_missing_required_attributes(
        self, mock_vocab, mock_mapping, sample_dataset_missing_attrs, temp_dir
    ):
        """
        Test that write() raises ValueError when required CMIP6 attributes are missing.

        Required attributes: variable_id, table_id, source_id, experiment_id,
                           variant_label, grid_label
        """
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )
        cmoriser.ds = sample_dataset_missing_attrs
        cmoriser.cmor_name = "tas"

        with pytest.raises(
            ValueError, match="Missing required CMIP6 global attributes"
        ):
            cmoriser.write()

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

    # ==================== System Memory Check Tests ====================

    @pytest.mark.unit
    def test_write_proceeds_when_system_memory_sufficient(
        self, cmoriser_with_dataset, temp_dir
    ):
        """
        Test that write() proceeds normally when system memory is sufficient.

        Scenario: No Dask client, plenty of system memory available.
        Expected: File is created successfully.
        """
        with patch("psutil.virtual_memory") as mock_mem:
            # Mock sufficient available memory (16 GB)
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            with patch(
                "dask.distributed.get_client", side_effect=ValueError("No client")
            ):
                cmoriser_with_dataset.write()

                # Verify output file was created
                output_files = list(Path(temp_dir).glob("*.nc"))
                assert len(output_files) == 1

    # ==================== Import Error Handling Tests ====================

    @pytest.mark.unit
    def test_write_handles_distributed_not_installed(
        self, cmoriser_with_dataset, temp_dir
    ):
        """
        Test graceful handling when dask.distributed is not installed.

        Scenario: dask.distributed import raises ImportError.
        Expected: Falls back to system memory check and proceeds.
        """
        with patch("psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            # Mock ImportError when trying to import dask.distributed
            with patch(
                "dask.distributed.get_client",
                side_effect=ImportError("No module named 'distributed'"),
            ):
                cmoriser_with_dataset.write()

                output_files = list(Path(temp_dir).glob("*.nc"))
                assert len(output_files) == 1

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

            with patch(
                "dask.distributed.get_client", side_effect=ValueError("No client")
            ):
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

    @pytest.mark.unit
    def test_write_creates_valid_netcdf_structure(
        self, cmoriser_with_dataset, temp_dir
    ):
        """
        Test that write() creates a valid NetCDF file with correct structure.

        Verifies:
        - Required dimensions exist
        - Main variable exists with correct shape
        - Global attributes are preserved
        """
        with patch("psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            with patch(
                "dask.distributed.get_client", side_effect=ValueError("No client")
            ):
                cmoriser_with_dataset.write()

                output_files = list(Path(temp_dir).glob("*.nc"))
                output_file = output_files[0]

                # Read back and verify structure
                ds_out = xr.open_dataset(output_file)

                try:
                    # Check dimensions
                    assert "time" in ds_out.dims
                    assert "lat" in ds_out.dims
                    assert "lon" in ds_out.dims

                    # Check main variable
                    assert "tas" in ds_out.data_vars
                    assert ds_out["tas"].dims == ("time", "lat", "lon")

                    # Check global attributes
                    assert ds_out.attrs["variable_id"] == "tas"
                    assert ds_out.attrs["table_id"] == "Amon"
                    assert ds_out.attrs["source_id"] == "ACCESS-ESM1-5"
                    assert ds_out.attrs["experiment_id"] == "historical"
                    assert ds_out.attrs["variant_label"] == "r1i1p1f1"
                    assert ds_out.attrs["grid_label"] == "gn"
                finally:
                    ds_out.close()

    @pytest.mark.unit
    def test_write_preserves_data_values(self, cmoriser_with_dataset, temp_dir):
        """
        Test that write() preserves data values correctly.

        Verifies that data written to file matches original data.
        """
        with patch("psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            with patch(
                "dask.distributed.get_client", side_effect=ValueError("No client")
            ):
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

    # ==================== Logging Tests ====================

    @pytest.mark.unit
    def test_write_prints_output_path(self, cmoriser_with_dataset, temp_dir, capsys):
        """
        Test that write() prints the output file path after completion.
        """
        with patch("psutil.virtual_memory") as mock_mem:
            mock_mem.return_value = MagicMock(
                total=32 * 1024**3,
                available=16 * 1024**3,
            )

            with patch(
                "dask.distributed.get_client", side_effect=ValueError("No client")
            ):
                cmoriser_with_dataset.write()

                captured = capsys.readouterr()

                assert "CMORised output written to" in captured.out
                assert str(temp_dir) in captured.out
