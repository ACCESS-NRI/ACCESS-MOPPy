from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.base import CMIP6_CMORiser
from access_moppy.ocean import (
    CMIP6_Ocean_CMORiser_OM2,
    CMIP6_Ocean_CMORiser_OM3,
)
from tests.mocks.mock_data import (
    create_mock_om2_dataset,
    create_mock_om3_dataset,
)


class TestCMIP6OceanCMORiserOM2:
    """Unit tests for CMIP6_Ocean_CMORiser_OM2 (B-grid)."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary for OM2."""
        vocab = Mock()
        vocab.source_id = "ACCESS-OM2"
        vocab.variable = {"units": "K", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(
            return_value={
                "variable_id": "tos",
                "table_id": "Omon",
                "source_id": "ACCESS-OM2",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            }
        )
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping for ocean."""
        return {
            "tos": {
                "model_variables": ["surface_temp"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.fixture
    def mock_om2_dataset(self):
        """Create mock OM2 dataset."""
        return create_mock_om2_dataset(nt=12, ny=30, nx=36)

    @pytest.mark.unit
    def test_infer_grid_type_t_grid(
        self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir
    ):
        """Test that T-grid is inferred from xt_ocean/yt_ocean coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = mock_om2_dataset

            grid_type, symmetric = cmoriser.infer_grid_type()

            assert grid_type == "T"
            assert symmetric is None  # MOM5 doesn't use symmetric memory

    @pytest.mark.unit
    def test_infer_grid_type_u_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that U-grid is inferred from xu_ocean/yt_ocean coordinates."""
        ds = xr.Dataset(
            coords={
                "xu_ocean": ("xu_ocean", np.arange(10)),
                "yt_ocean": ("yt_ocean", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.uo",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "U"

    @pytest.mark.unit
    def test_get_dim_rename_om2(self, mock_vocab, mock_mapping, temp_dir):
        """Test dimension renaming for ACCESS-OM2."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            dim_rename = cmoriser._get_dim_rename()

            assert dim_rename["xt_ocean"] == "i"
            assert dim_rename["yt_ocean"] == "j"
            assert dim_rename["xu_ocean"] == "i"
            assert dim_rename["yu_ocean"] == "j"
            assert dim_rename["st_ocean"] == "lev"

    @pytest.mark.unit
    def test_arakawa_grid_type(self, mock_vocab, mock_mapping, temp_dir):
        """Test that ACCESS-OM2 uses B-grid (Arakawa B)."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            assert cmoriser.arakawa == "B"

    @pytest.mark.unit
    def test_time_bnds_loaded_and_preserved(
        self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir
    ):
        """Test that time_bnds is loaded with other variables and preserved in output."""
        with patch("access_moppy.ocean.Supergrid"):
            # Mock load_dataset to avoid file I/O
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_om2_dataset

                # Run the processing
                cmoriser.select_and_process_variables()

                # Verify time_bnds is in the output dataset
                assert "time_bnds" in cmoriser.ds.data_vars

                # Verify only cmor_name and time_bnds are kept as data variables
                assert set(cmoriser.ds.data_vars) == {"tos", "time_bnds"}

    @pytest.mark.unit
    def test_time_bnds_dimensions_in_used_coords(
        self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir
    ):
        """Test that time_bnds dimensions are identified as used coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_om2_dataset

                # Run the processing
                cmoriser.select_and_process_variables()

                # Verify time_bnds dimensions are preserved
                assert "time" in cmoriser.ds.coords
                assert "nv" in cmoriser.ds.coords  # nv is dimension for time_bnds

                # Verify time_bnds has correct dimensions
                assert cmoriser.ds["time_bnds"].dims == ("time", "nv")

    @pytest.mark.unit
    def test_auto_calculate_time_bnds_when_missing(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test that time_bnds is automatically calculated when missing from source data."""
        # Create dataset WITHOUT time_bnds
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                    {
                        "long_name": "Sea surface temperature",
                        "units": "K",
                    },
                ),
            },
            coords={
                "time": pd.date_range("2000-01-15", periods=12, freq="MS"),
                "yt_ocean": ("yt_ocean", np.arange(30), {"units": "degrees_N"}),
                "xt_ocean": ("xt_ocean", np.arange(36), {"units": "degrees_E"}),
            },
            attrs={"title": "ACCESS-OM2", "grid_type": "mosaic"},
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                # Run processing - should automatically calculate time_bnds
                cmoriser.select_and_process_variables()

                # Verify time_bnds was created
                assert "time_bnds" in cmoriser.ds.data_vars
                assert cmoriser.ds["time_bnds"].shape == (12, 2)

                # Verify dimensions
                assert cmoriser.ds["time_bnds"].dims == ("time", "nv")

                # Verify nv coordinate exists
                assert "nv" in cmoriser.ds.coords
                assert len(cmoriser.ds["nv"]) == 2

    @pytest.mark.unit
    def test_required_vars_includes_time_bnds(
        self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir
    ):
        """Test that time_bnds is included in required_vars during loading."""
        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset") as mock_load:
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_om2_dataset

                # Run processing
                cmoriser.select_and_process_variables()

                # Verify load_dataset was called with time_bnds in required_vars
                mock_load.assert_called_once()
                call_args = mock_load.call_args
                required_vars = call_args.kwargs.get("required_vars") or call_args[0][0]
                assert "time_bnds" in required_vars
                assert "surface_temp" in required_vars  # model variable

    @pytest.mark.unit
    def test_calculated_time_bnds_values_monthly_first_end(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test that calculated time_bnds has correct month boundaries."""
        # Use proper month-start dates
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
            },
            coords={
                # Generate time centered on mid-month (typical for monthly averages)
                "time": pd.date_range("2000-01-01", periods=12, freq="MS")
                + pd.Timedelta(days=14),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )

        print(ds_no_time_bnds["time"].values)

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                cmoriser.select_and_process_variables()

                time_bnds = cmoriser.ds["time_bnds"]

                # Check first month (January 2000)
                # Bounds should be [2000-01-01, 2000-02-01]
                print(time_bnds.values)
                first_lower = pd.Timestamp(time_bnds[0, 0].values)
                first_upper = pd.Timestamp(time_bnds[0, 1].values)

                assert first_lower.year == 2000
                assert first_lower.month == 1
                assert first_lower.day == 1

                assert first_upper.year == 2000
                assert first_upper.month == 2
                assert first_upper.day == 1

                # Check last month (December 2000)
                last_lower = pd.Timestamp(time_bnds[11, 0].values)
                last_upper = pd.Timestamp(time_bnds[11, 1].values)

                assert last_lower.year == 2000
                assert last_lower.month == 12
                assert last_lower.day == 1

                assert last_upper.year == 2001
                assert last_upper.month == 1
                assert last_upper.day == 1

    @pytest.mark.unit
    def test_calculated_time_bnds_values_monthly_range(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test that calculated time_bnds has correct structure and reasonable values."""
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
            },
            coords={
                # Monthly time coordinate (mid-month)
                "time": pd.date_range("2000-01-15", periods=12, freq="MS")
                + pd.Timedelta(days=14),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )

        ds_no_time_bnds["time"].attrs["units"] = "days since 1850-01-01"

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                cmoriser.select_and_process_variables()

                time_bnds = cmoriser.ds["time_bnds"]

                # Verify shape
                assert time_bnds.shape == (12, 2)
                assert time_bnds.dims == ("time", "nv")

                # For each time step, verify bounds make sense
                for i in range(12):
                    lower = pd.Timestamp(time_bnds[i, 0].values)
                    upper = pd.Timestamp(time_bnds[i, 1].values)

                    # Lower bound should be before upper bound
                    assert (
                        lower < upper
                    ), f"Lower bound >= upper bound at index {i}: [{lower}, {upper}]"

                    # Bounds should span about 1 month (28-31 days)
                    days_span = (upper - lower).days
                    assert (
                        28 <= days_span <= 31
                    ), f"Unexpected time span {days_span} days at index {i}, expected 28-31 days"

                # Verify all bounds are in year 2000-2001 range (reasonable for test data)
                all_bnds = time_bnds.values.flatten()
                years = [pd.Timestamp(b).year for b in all_bnds]
                assert all(
                    y in [2000, 2001] for y in years
                ), f"Unexpected years in bounds: {set(years)}"

                # Verify bounds have proper attributes
                assert "long_name" in time_bnds.attrs
                assert "units" in time_bnds.attrs

    @pytest.mark.unit
    def test_debug_time_bnds_calculation(self, mock_vocab, mock_mapping, temp_dir):
        """Debug test to see what time_bnds are actually calculated."""
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
            },
            coords={
                "time": pd.date_range("2000-01-15", periods=12, freq="MS")
                + pd.Timedelta(days=14),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                cmoriser.select_and_process_variables()

                # Print what we got
                print("\n=== Debug: Time values ===")
                for i, t in enumerate(cmoriser.ds["time"].values[:3]):
                    print(f"time[{i}]: {pd.Timestamp(t)}")

                print("\n=== Debug: Time bounds ===")
                for i in range(3):
                    lower = pd.Timestamp(cmoriser.ds["time_bnds"][i, 0].values)
                    upper = pd.Timestamp(cmoriser.ds["time_bnds"][i, 1].values)
                    print(f"time_bnds[{i}]: [{lower}, {upper}]")

    @pytest.mark.unit
    def test_existing_time_bnds_not_overwritten(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test that existing time_bnds is NOT overwritten."""
        # Create dataset with existing time_bnds (with special marker values)
        time = pd.date_range("2000-01-15", periods=12, freq="MS")

        # Special time_bnds with marker values to verify it's not overwritten
        existing_time_bnds = np.zeros((12, 2), dtype="datetime64[ns]")
        marker_time = np.datetime64("1999-12-31")  # Special marker
        existing_time_bnds[:, 0] = marker_time
        existing_time_bnds[:, 1] = marker_time + np.timedelta64(1, "D")

        ds_with_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
                "time_bnds": (
                    ["time", "nv"],
                    existing_time_bnds,
                    {"long_name": "time bounds"},
                ),
            },
            coords={
                "time": time,
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
                "nv": [0, 1],
            },
            attrs={"title": "ACCESS-OM2"},
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_with_bnds

                cmoriser.select_and_process_variables()

                # Verify original time_bnds was kept (marker value still there)
                assert cmoriser.ds["time_bnds"][0, 0].values == marker_time
                assert "time_bnds" in cmoriser.ds.data_vars

    @pytest.mark.unit
    def test_time_bnds_attributes(self, mock_vocab, mock_mapping, temp_dir):
        """Test that calculated time_bnds has proper attributes."""
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
            },
            coords={
                "time": (
                    "time",
                    pd.date_range("2000-01-15", periods=12, freq="MS"),
                    {
                        "long_name": "time",
                        "units": "days since 0001-01-01 00:00:00",
                        "calendar": "PROLEPTIC_GREGORIAN",
                    },
                ),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                cmoriser.select_and_process_variables()

                time_bnds = cmoriser.ds["time_bnds"]

                # Check attributes
                assert "long_name" in time_bnds.attrs
                assert time_bnds.attrs["long_name"] == "time bounds"
                assert "units" in time_bnds.attrs

    @pytest.mark.unit
    def test_only_tos_and_time_bnds_kept(self, mock_vocab, mock_mapping, temp_dir):
        """Test that only CMOR variable and time_bnds are kept in final dataset."""
        # Create dataset with extra variables that should be dropped
        ds_with_extras = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
                "extra_var1": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36),
                ),
                "extra_var2": (["yt_ocean", "xt_ocean"], np.random.rand(30, 36)),
            },
            coords={
                "time": pd.date_range("2000-01-15", periods=12, freq="MS"),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
            attrs={"title": "ACCESS-OM2"},
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_with_extras

                cmoriser.select_and_process_variables()

                # Only tos and time_bnds should remain
                assert set(cmoriser.ds.data_vars) == {"tos", "time_bnds"}

                # Extra variables should be dropped
                assert "extra_var1" not in cmoriser.ds
                assert "extra_var2" not in cmoriser.ds
                assert "surface_temp" not in cmoriser.ds  # Original var was renamed

    @pytest.mark.unit
    def test_nv_coordinate_preserved(self, mock_vocab, mock_mapping, temp_dir):
        """Test that nv coordinate is preserved (needed by time_bnds)."""
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
            },
            coords={
                "time": pd.date_range("2000-01-15", periods=12, freq="MS"),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
            attrs={"title": "ACCESS-OM2"},
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                cmoriser.select_and_process_variables()

                # nv should be in coordinates
                assert "nv" in cmoriser.ds.coords

                # time should be in coordinates
                assert "time" in cmoriser.ds.coords

                # Spatial coordinates should be preserved (renamed)
                assert "j" in cmoriser.ds.coords  # Renamed from yt_ocean
                assert "i" in cmoriser.ds.coords  # Renamed from xt_ocean

    @pytest.mark.unit
    def test_error_when_time_missing_and_cannot_calculate(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test that error is raised when time coordinate is missing and time_bnds cannot be calculated."""
        # Create dataset without time coordinate
        ds_no_time = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["yt_ocean", "xt_ocean"],
                    np.random.rand(30, 36).astype(np.float32),
                ),
            },
            coords={
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time

                # Should raise error because time_bnds cannot be calculated without time
                with pytest.raises(
                    ValueError, match="time_bnds is required.*could not be calculated"
                ):
                    cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_time_bnds_continuous_coverage(self, mock_vocab, mock_mapping, temp_dir):
        """Test that calculated time_bnds provides continuous coverage (no gaps)."""
        ds_no_time_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36).astype(np.float32),
                ),
            },
            coords={
                "time": pd.date_range("2000-01-15", periods=12, freq="MS"),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )

        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, "load_dataset", return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_time_bnds

                cmoriser.select_and_process_variables()

                time_bnds = cmoriser.ds["time_bnds"]

                # Upper bound of month i should equal lower bound of month i+1
                for i in range(len(time_bnds) - 1):
                    assert (
                        time_bnds[i, 1].values == time_bnds[i + 1, 0].values
                    ), f"Gap in time_bnds between index {i} and {i+1}"


class TestCMIP6OceanCMORiserOM3:
    """Unit tests for CMIP6_Ocean_CMORiser_OM3 (C-grid)."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary for OM3."""
        vocab = Mock()
        vocab.source_id = "ACCESS-OM3"
        vocab.variable = {"units": "degC", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(
            return_value={
                "variable_id": "tos",
                "table_id": "Omon",
                "source_id": "ACCESS-OM3",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            }
        )
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping."""
        return {
            "tos": {
                "model_variables": ["tos"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.fixture
    def mock_om3_dataset(self):
        """Create mock OM3 dataset."""
        return create_mock_om3_dataset(nt=12, ny=30, nx=36)

    @pytest.mark.unit
    def test_infer_grid_type_t_grid(
        self, mock_vocab, mock_mapping, mock_om3_dataset, temp_dir
    ):
        """Test that T-grid is inferred from xh/yh coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = mock_om3_dataset

            grid_type, symmetric = cmoriser.infer_grid_type()

            assert grid_type == "T"
            assert symmetric is True  # MOM6 uses symmetric memory

    @pytest.mark.unit
    def test_infer_grid_type_u_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that U-grid is inferred from xq/yh coordinates."""
        ds = xr.Dataset(
            coords={
                "xq": ("xq", np.arange(10)),
                "yh": ("yh", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.uo",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "U"

    @pytest.mark.unit
    def test_infer_grid_type_v_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that V-grid is inferred from xh/yq coordinates."""
        ds = xr.Dataset(
            coords={
                "xh": ("xh", np.arange(10)),
                "yq": ("yq", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.vo",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "V"

    @pytest.mark.unit
    def test_infer_grid_type_c_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that C-grid (corner) is inferred from xq/yq coordinates."""
        ds = xr.Dataset(
            coords={
                "xq": ("xq", np.arange(10)),
                "yq": ("yq", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.var",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "C"

    @pytest.mark.unit
    def test_get_dim_rename_om3(self, mock_vocab, mock_mapping, temp_dir):
        """Test dimension renaming for ACCESS-OM3."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            dim_rename = cmoriser._get_dim_rename()

            assert dim_rename["xh"] == "i"
            assert dim_rename["yh"] == "j"
            assert dim_rename["xq"] == "i"
            assert dim_rename["yq"] == "j"
            assert dim_rename["zl"] == "lev"

    @pytest.mark.unit
    def test_arakawa_grid_type(self, mock_vocab, mock_mapping, temp_dir):
        """Test that ACCESS-OM3 uses C-grid (Arakawa C)."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            assert cmoriser.arakawa == "C"
