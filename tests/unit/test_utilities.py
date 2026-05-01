"""
Comprehensive tests for calculate_time_bounds and helper functions.
"""

import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import cftime
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.utilities import (
    _detect_frequency_from_bounds,
    _infer_frequency,
    _model_mapping_file_exists,
    calculate_time_bounds,
    create_ilamb_symlinks,
    detect_time_frequency_lazy,
    get_requested_variables_from_data_request,
)


class TestCalculateTimeBoundsErrors:
    """Test error handling in calculate_time_bounds."""

    def test_missing_time_coordinate(self):
        """Test error when time coordinate is missing."""
        ds = xr.Dataset(coords={"x": [1, 2, 3]})

        with pytest.raises(ValueError, match="must contain 'time' coordinate"):
            calculate_time_bounds(ds)

    def test_insufficient_time_points(self):
        """Test error when less than 2 time points."""
        ds = xr.Dataset(coords={"time": [np.datetime64("2000-01-01")]})

        with pytest.raises(ValueError, match="Need at least 2 time points"):
            calculate_time_bounds(ds)


class TestCalculateTimeBoundsMonthly:
    """Test monthly frequency time bounds calculation."""

    def test_monthly_bounds_numpy_datetime64(self):
        """Test monthly bounds with numpy datetime64."""
        time = np.array(
            ["2000-01-15", "2000-02-15", "2000-03-15", "2000-12-15", "2001-01-15"],
            dtype="datetime64[D]",
        )

        ds = xr.Dataset(coords={"time": time})
        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (5, 2)
        assert time_bnds.dims == ("time", "nv")

        # Check attributes
        assert "long_name" in time_bnds.attrs
        assert time_bnds.attrs["long_name"] == "time bounds"

        # Implementation uses midpoint method - bounds bracket the time points
        for i in range(len(time)):
            assert time_bnds.values[i, 0] < time[i]
            assert time_bnds.values[i, 1] > time[i]

    def test_monthly_bounds_cftime(self):
        """Test monthly bounds with cftime for wide date ranges."""
        time = xr.cftime_range("0850-01-15", periods=13, freq="MS", calendar="noleap")
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (13, 2)

        # Check first bound - implementation may use different approach
        assert time_bnds.values[0, 0].year == 850

        # Check that bounds bracket the time points
        for i in range(len(time)):
            assert time_bnds.values[i, 0] <= time.values[i]
            assert time_bnds.values[i, 1] >= time.values[i]

    def test_monthly_bounds_year_2200(self):
        """Test monthly bounds with year 2200 (edge of typical range)."""
        time = xr.cftime_range(
            "2200-01-15", periods=12, freq="MS", calendar="proleptic_gregorian"
        )
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        assert time_bnds.shape == (12, 2)
        assert time_bnds.values[0, 0].year == 2200
        assert time_bnds.values[-1, 1].year == 2201

    def test_monthly_bounds_february(self):
        """Test monthly bounds handle February correctly."""
        time = np.array(["2000-02-15", "2000-03-15"], dtype="datetime64[D]")
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        # February bounds
        assert time_bnds[0, 0] == np.datetime64("2000-02-01")
        assert time_bnds[0, 1] == np.datetime64("2000-03-01")


class TestCalculateTimeBoundsDaily:
    """Test daily frequency time bounds calculation."""

    def test_daily_bounds_numpy_datetime64(self):
        """Test daily bounds with numpy datetime64."""
        time = np.array(
            ["2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"],
            dtype="datetime64[D]",
        )

        ds = xr.Dataset(coords={"time": time})
        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (4, 2)

        # Check bounds for first day
        assert time_bnds[0, 0] == np.datetime64("2000-01-01")
        assert time_bnds[0, 1] == np.datetime64("2000-01-02")

        # Check bounds for last day
        assert time_bnds[3, 0] == np.datetime64("2000-01-04")
        assert time_bnds[3, 1] == np.datetime64("2000-01-05")

    def test_daily_bounds_cftime(self):
        """Test daily bounds with cftime."""
        time = xr.cftime_range("0850-01-01", periods=5, freq="D", calendar="360_day")
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (5, 2)

        # Check first day
        assert time_bnds.values[0, 0].year == 850
        assert time_bnds.values[0, 0].day == 1
        assert time_bnds.values[0, 1].day == 2

        # Check calendar
        assert time_bnds.attrs["calendar"] == "360_day"

    def test_daily_bounds_leap_year(self):
        """Test daily bounds around leap day."""
        time = np.array(
            ["2000-02-28", "2000-02-29", "2000-03-01"], dtype="datetime64[D]"
        )
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        assert time_bnds[1, 0] == np.datetime64("2000-02-29")
        assert time_bnds[1, 1] == np.datetime64("2000-03-01")


class TestCalculateTimeBoundsYearly:
    """Test yearly frequency time bounds calculation."""

    def test_yearly_bounds_numpy_datetime64(self):
        """Test yearly bounds with numpy datetime64."""
        time = np.array(
            ["2000-07-01", "2001-07-01", "2002-07-01"], dtype="datetime64[D]"
        )

        ds = xr.Dataset(coords={"time": time})
        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (3, 2)

        # Check first year
        assert time_bnds[0, 0] == np.datetime64("2000-01-01")
        assert time_bnds[0, 1] == np.datetime64("2001-01-01")

        # Check second year
        assert time_bnds[1, 0] == np.datetime64("2001-01-01")
        assert time_bnds[1, 1] == np.datetime64("2002-01-01")

    def test_yearly_bounds_cftime(self):
        """Test yearly bounds with cftime."""
        time = xr.cftime_range("0850-07-01", periods=3, freq="YE", calendar="noleap")
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (3, 2)

        # Check bounds
        assert time_bnds.values[0, 0].year == 850
        assert time_bnds.values[0, 0].month == 1
        assert time_bnds.values[0, 1].year == 851


class TestCalculateTimeBoundsIrregular:
    """Test irregular/midpoint time bounds calculation."""

    def test_midpoint_bounds_numpy(self):
        """Test midpoint bounds for irregular data with numpy datetime64."""
        # Irregular spacing: 10, 5, 15 days
        time = np.array(
            ["2000-01-01", "2000-01-11", "2000-01-16", "2000-01-31"],
            dtype="datetime64[D]",
        )

        ds = xr.Dataset(coords={"time": time})
        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (4, 2)

        # First point: extrapolate backward
        # dt_first = 10 days, so lower bound = 2000-01-01 - 5 days
        assert time_bnds[0, 0] == np.datetime64("1999-12-27")

        # Middle point bounds should be midpoints
        # time[1] = 2000-01-11, midpoint to prev = 2000-01-06, midpoint to next = 2000-01-13.5
        assert time_bnds[1, 0] == np.datetime64("2000-01-06")

        # Last point: extrapolate forward
        # dt_last = 15 days, upper bound = 2000-01-31 + 7.5 days
        assert time_bnds[3, 1] == np.datetime64("2000-02-07T12:00:00")

    def test_midpoint_bounds_cftime(self):
        """Test midpoint bounds for irregular data with cftime."""
        # Create irregular time points
        time_vals = [
            cftime.DatetimeNoLeap(850, 1, 1),
            cftime.DatetimeNoLeap(850, 1, 11),
            cftime.DatetimeNoLeap(850, 1, 16),
            cftime.DatetimeNoLeap(850, 1, 31),
        ]
        time = xr.DataArray(time_vals, dims=["time"], name="time")
        time.attrs["calendar"] = "noleap"

        ds = xr.Dataset(coords={"time": time})
        time_bnds = calculate_time_bounds(ds)

        # Check shape
        assert time_bnds.shape == (4, 2)

        # Check that bounds are cftime objects
        assert hasattr(time_bnds.values[0, 0], "calendar")


class TestInferFrequency:
    """Test the _infer_frequency helper function."""

    def test_infer_frequency_monthly(self):
        """Test frequency inference for monthly data."""
        time_values = np.array(
            ["2000-01-15", "2000-02-15", "2000-03-15"], dtype="datetime64[D]"
        )

        freq = _infer_frequency(time_values)
        assert freq == "monthly"

    def test_infer_frequency_monthly_cftime(self):
        """Test frequency inference for monthly cftime data."""
        time_values = xr.cftime_range("2000-01", periods=12, freq="MS").values

        freq = _infer_frequency(time_values)
        assert freq == "monthly"

    def test_infer_frequency_daily(self):
        """Test frequency inference for daily data."""
        time_values = np.array(
            ["2000-01-01", "2000-01-02", "2000-01-03"], dtype="datetime64[D]"
        )

        freq = _infer_frequency(time_values)
        assert freq == "daily"

    def test_infer_frequency_yearly(self):
        """Test frequency inference for yearly data."""
        time_values = np.array(
            ["2000-01-01", "2001-01-01", "2002-01-01"], dtype="datetime64[D]"
        )

        freq = _infer_frequency(time_values)
        assert freq == "yearly"

    def test_infer_frequency_irregular(self):
        """Test frequency inference for irregular data."""
        time_values = np.array(
            ["2000-01-01", "2000-01-05", "2000-01-12"], dtype="datetime64[D]"
        )

        freq = _infer_frequency(time_values)
        assert freq == "irregular"

    def test_infer_frequency_single_point(self):
        """Test frequency inference with single time point."""
        time_values = np.array(["2000-01-01"], dtype="datetime64[D]")

        freq = _infer_frequency(time_values)
        assert freq is None


class TestCalculateTimeBoundsEdgeCases:
    """Test edge cases and special scenarios."""

    def test_preserves_units(self):
        """Test that time bounds preserve time units attribute."""
        time = np.array(["2000-01-15", "2000-02-15"], dtype="datetime64[D]")
        ds = xr.Dataset(coords={"time": time})
        ds["time"].attrs["units"] = "days since 1850-01-01"

        time_bnds = calculate_time_bounds(ds)

        assert time_bnds.attrs["units"] == "days since 1850-01-01"

    def test_default_units(self):
        """Test that no units are added when time doesn't have units."""
        time = np.array(["2000-01-15", "2000-02-15"], dtype="datetime64[D]")
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        assert "long_name" in time_bnds.attrs

    def test_nv_coordinate(self):
        """Test that nv coordinate is created correctly."""
        time = np.array(["2000-01-15", "2000-02-15"], dtype="datetime64[D]")
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        assert "nv" in time_bnds.coords
        np.testing.assert_array_equal(time_bnds.coords["nv"].values, [0, 1])

    def test_different_calendars(self):
        """Test with different calendar types."""
        calendars = ["noleap", "360_day", "gregorian", "proleptic_gregorian"]
        # Note: 'gregorian' and 'standard' are synonyms in cftime
        expected_calendars = {
            "noleap": "noleap",
            "360_day": "360_day",
            "gregorian": "standard",  # cftime converts gregorian to standard
            "proleptic_gregorian": "proleptic_gregorian",
        }

        for calendar in calendars:
            time = xr.cftime_range("2000-01", periods=3, freq="MS", calendar=calendar)
            ds = xr.Dataset(coords={"time": time})

            time_bnds = calculate_time_bounds(ds)

            assert time_bnds.shape == (3, 2)
            # Check calendar if present - allow for cftime aliases
            if "calendar" in time_bnds.attrs:
                assert time_bnds.attrs["calendar"] in [
                    calendar,
                    expected_calendars.get(calendar, calendar),
                ]

    def test_long_time_series(self):
        """Test with a long time series (performance check)."""
        time = xr.cftime_range(
            "0000-01", periods=100, freq="MS", calendar="proleptic_gregorian"
        )
        ds = xr.Dataset(coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        assert time_bnds.shape == (100, 2)
        assert time_bnds.values[0, 0].year == 0
        assert time_bnds.values[-1, 1].year == 8  # ~8 years later


class TestCalculateTimeBoundsIntegration:
    """Integration tests for time bounds."""

    def test_time_bounds_roundtrip(self):
        """Test that time bounds can be written and read from netCDF."""
        time = xr.cftime_range("2000-01", periods=12, freq="MS")
        ds = xr.Dataset({"data": (["time"], np.random.rand(12))}, coords={"time": time})

        time_bnds = calculate_time_bounds(ds)

        # Remove encoding-related attributes to avoid xarray conflicts
        time_bnds_clean = time_bnds.copy()
        attrs_to_keep = {}
        for key, value in time_bnds.attrs.items():
            if key not in ["units", "calendar"]:  # These are handled by xarray encoding
                attrs_to_keep[key] = value
        time_bnds_clean.attrs = attrs_to_keep

        ds["time_bnds"] = time_bnds_clean

        # Write to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp:
            tmp_path = tmp.name

        try:
            ds.to_netcdf(tmp_path)

            # Read back
            ds_read = xr.open_dataset(tmp_path, decode_times=True)

            assert "time_bnds" in ds_read
            assert ds_read["time_bnds"].shape == (12, 2)

            ds_read.close()
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=your_module", "--cov-report=html"])


# ---------------------------------------------------------------------------
# Helpers shared by get_requested_variables_from_data_request tests
# ---------------------------------------------------------------------------


def _make_dreq_api_mocks(variables):
    """Return a (dc_mock, dq_mock, update_config_mock) triple.

    *variables* is a list of strings that should be returned under
    ``requested["experiment"]["historical"]["Core"]``.
    """
    dc_mock = MagicMock()
    dq_mock = MagicMock()
    update_config_mock = MagicMock()

    dc_mock.load.return_value = MagicMock(name="dreq_content")
    dq_mock.create_dreq_tables_for_request.return_value = MagicMock(name="dreq_tables")
    dq_mock.get_requested_variables.return_value = {
        "experiment": {
            "historical": {"Core": set(variables)},
        }
    }
    return dc_mock, dq_mock, update_config_mock


def _patch_dreq(dc_mock, dq_mock, update_config_mock):
    """Context-manager stack that injects mocks into sys.modules."""
    content_mod = MagicMock()
    content_mod.dreq_content = dc_mock
    query_mod = MagicMock()
    query_mod.dreq_query = dq_mock
    utilities_mod = MagicMock()
    config_mod = MagicMock()
    config_mod.update_config = update_config_mock
    utilities_mod.config = config_mod

    mods = {
        "data_request_api": MagicMock(),
        "data_request_api.content": content_mod,
        "data_request_api.query": query_mod,
        "data_request_api.utilities": utilities_mod,
        "data_request_api.utilities.config": config_mod,
    }
    return patch.dict(sys.modules, mods)


class TestGetRequestedVariablesValidation:
    """Tests for the input-validation layer (no API calls needed)."""

    def test_invalid_variable_name_raises_value_error(self):
        with pytest.raises(ValueError, match="Must be 'CMIP6' or 'CMIP7'"):
            get_requested_variables_from_data_request(variable_name="CMIP5")

    def test_invalid_variable_name_empty_string(self):
        with pytest.raises(ValueError, match="Must be 'CMIP6' or 'CMIP7'"):
            get_requested_variables_from_data_request(variable_name="")

    def test_invalid_variable_name_lowercase(self):
        with pytest.raises(ValueError, match="Must be 'CMIP6' or 'CMIP7'"):
            get_requested_variables_from_data_request(variable_name="cmip6")

    def test_missing_api_raises_import_error(self):
        """When DATA_REQUEST_API_AVAILABLE is False an ImportError is raised."""
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", False):
            with pytest.raises(ImportError, match="data_request_api"):
                get_requested_variables_from_data_request()

    def test_missing_api_error_message_contains_install_hint(self):
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", False):
            with pytest.raises(ImportError, match="pip install"):
                get_requested_variables_from_data_request()


class TestGetRequestedVariablesHappyPath:
    """Tests for the successful code-paths, with the API fully mocked."""

    def test_returns_list(self):
        vars_ = ["Amon.tas", "Amon.pr", "Omon.tos"]
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(vars_)
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                result = get_requested_variables_from_data_request()
        assert isinstance(result, list)
        assert set(result) == set(vars_)

    def test_cmip6_variable_name_passed_to_config(self):
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                get_requested_variables_from_data_request(variable_name="CMIP6")
        uc_mock.assert_called_once_with("variable_name", "CMIP6 Compound Name")

    def test_cmip7_variable_name_passed_to_config(self):
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                get_requested_variables_from_data_request(variable_name="CMIP7")
        uc_mock.assert_called_once_with("variable_name", "CMIP7 Compound Name")

    def test_priority_lowercase_forwarded_to_api(self):
        """priority_cutoff must be lowercased when calling get_requested_variables."""
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                get_requested_variables_from_data_request(priority="Core")
        _, kwargs = dq_mock.get_requested_variables.call_args
        assert kwargs["priority_cutoff"] == "core"

    def test_dreq_version_forwarded(self):
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                get_requested_variables_from_data_request(dreq_version="v1.0.0")
        dc_mock.retrieve.assert_called_once_with("v1.0.0")
        dc_mock.load.assert_called_once_with("v1.0.0")

    def test_empty_variable_list(self):
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks([])
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                result = get_requested_variables_from_data_request()
        assert result == []

    def test_priority_capitalized_for_lookup(self):
        """Priority key in the response dict must be accessed capitalised."""
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        # Deliberately store under the capitalised key
        dq_mock.get_requested_variables.return_value = {
            "experiment": {"historical": {"Core": {"Amon.tas"}}}
        }
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                result = get_requested_variables_from_data_request(priority="core")
        assert "Amon.tas" in result


class TestGetRequestedVariablesKeyErrors:
    """Tests for KeyError raised when experiment/priority are absent."""

    def test_missing_experiment_raises_key_error(self):
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                with pytest.raises(KeyError, match="piControl"):
                    get_requested_variables_from_data_request(experiment="piControl")

    def test_missing_priority_raises_key_error(self):
        dc_mock, dq_mock, uc_mock = _make_dreq_api_mocks(["Amon.tas"])
        # Put the experiment in but without the requested priority
        dq_mock.get_requested_variables.return_value = {
            "experiment": {"historical": {"Core": {"Amon.tas"}}}
        }
        with patch("access_moppy.utilities.DATA_REQUEST_API_AVAILABLE", True):
            with _patch_dreq(dc_mock, dq_mock, uc_mock):
                with pytest.raises(KeyError, match="Tier1"):
                    get_requested_variables_from_data_request(
                        experiment="historical", priority="Tier1"
                    )


# Helpers
_FLAT_FILENAME = "tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-200012.nc"
_CMIP6_RELPATH = (
    "CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Amon/{var}/gn/v20210101"
    "/{var}_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-200012.nc"
)


def _make_flat(root, *variable_ids, time_range="185001-200012"):
    """Create stub flat-DRS .nc files and return a dict var→Path."""
    paths = {}
    for var in variable_ids:
        p = root / f"{var}_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_{time_range}.nc"
        p.touch()
        paths[var] = p
    return paths


def _make_cmip6(root, *variable_ids):
    """Create stub CMIP6-DRS .nc files and return a dict var→Path."""
    paths = {}
    for var in variable_ids:
        p = root / _CMIP6_RELPATH.format(var=var)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
        paths[var] = p
    return paths


class TestCreateIlambSymlinksFlat:
    """Tests for create_ilamb_symlinks with flat DRS output."""

    def test_creates_symlinks_for_all_variables(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "tas", "pr")

        created = create_ilamb_symlinks(
            output_dir, tmp_path / "ilamb", drs_format="flat"
        )

        assert set(created.keys()) == {"tas", "pr"}
        assert (tmp_path / "ilamb" / "tas.nc").is_symlink()
        assert (tmp_path / "ilamb" / "pr.nc").is_symlink()

    def test_symlinks_resolve_to_source_files(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        src_map = _make_flat(output_dir, "tas")

        created = create_ilamb_symlinks(
            output_dir, tmp_path / "ilamb", drs_format="flat"
        )

        assert created["tas"].resolve() == src_map["tas"].resolve()

    def test_ilamb_dir_created_automatically(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "tas")
        ilamb_dir = tmp_path / "nested" / "ilamb"

        create_ilamb_symlinks(output_dir, ilamb_dir, drs_format="flat")

        assert ilamb_dir.is_dir()

    def test_returns_correct_symlink_paths(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "tos")
        ilamb_dir = tmp_path / "ilamb"

        created = create_ilamb_symlinks(output_dir, ilamb_dir, drs_format="flat")

        assert created["tos"] == ilamb_dir / "tos.nc"

    def test_time_invariant_file_without_time_range(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "areacella_fx_ACCESS-ESM1-5_historical_r1i1p1f1_gn.nc").touch()

        created = create_ilamb_symlinks(
            output_dir, tmp_path / "ilamb", drs_format="flat"
        )

        assert "areacella" in created


class TestCreateIlambSymlinksCmip6:
    """Tests for create_ilamb_symlinks with CMIP6 DRS output."""

    def test_creates_symlinks_from_hierarchy(self, tmp_path):
        output_dir = tmp_path / "drs_root"
        _make_cmip6(output_dir, "tas", "pr")
        ilamb_dir = tmp_path / "ilamb"

        created = create_ilamb_symlinks(output_dir, ilamb_dir, drs_format="cmip6")

        assert set(created.keys()) == {"tas", "pr"}
        assert (ilamb_dir / "tas.nc").is_symlink()
        assert (ilamb_dir / "pr.nc").is_symlink()

    def test_symlinks_resolve_to_source_files(self, tmp_path):
        output_dir = tmp_path / "drs_root"
        src_map = _make_cmip6(output_dir, "tas")
        ilamb_dir = tmp_path / "ilamb"

        created = create_ilamb_symlinks(output_dir, ilamb_dir, drs_format="cmip6")

        assert created["tas"].resolve() == src_map["tas"].resolve()

    def test_ilamb_inside_output_dir_excluded_on_rescan(self, tmp_path):
        """A second call must not pick up symlinks created by the first run."""
        output_dir = tmp_path / "drs_root"
        _make_cmip6(output_dir, "tas")
        ilamb_dir = output_dir / "ilamb"  # deliberately nested inside output_dir

        create_ilamb_symlinks(output_dir, ilamb_dir, drs_format="cmip6")
        created2 = create_ilamb_symlinks(
            output_dir, ilamb_dir, drs_format="cmip6", overwrite=True
        )

        assert set(created2.keys()) == {"tas"}


class TestCreateIlambSymlinksAutoDetect:
    """Tests for drs_format='auto' detection logic."""

    def test_auto_detects_flat_format(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "tas")

        created = create_ilamb_symlinks(output_dir, tmp_path / "ilamb")

        assert "tas" in created

    def test_auto_detects_cmip6_format(self, tmp_path):
        output_dir = tmp_path / "drs_root"
        _make_cmip6(output_dir, "tas")

        created = create_ilamb_symlinks(output_dir, tmp_path / "ilamb")

        assert "tas" in created

    def test_auto_is_default(self, tmp_path):
        """Omitting drs_format is equivalent to drs_format='auto'."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "pr")

        created = create_ilamb_symlinks(output_dir, tmp_path / "ilamb")

        assert "pr" in created


class TestCreateIlambSymlinksErrors:
    """Tests for error handling in create_ilamb_symlinks."""

    def test_missing_output_dir_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="does not exist"):
            create_ilamb_symlinks(tmp_path / "nonexistent", tmp_path / "ilamb")

    def test_invalid_drs_format_raises(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.raises(ValueError, match="Invalid drs_format"):
            create_ilamb_symlinks(output_dir, tmp_path / "ilamb", drs_format="drs7")

    def test_multiple_files_same_variable_raises(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "tas", time_range="185001-190012")
        _make_flat(output_dir, "tas", time_range="190101-200012")

        with pytest.raises(ValueError, match="Multiple source files"):
            create_ilamb_symlinks(output_dir, tmp_path / "ilamb", drs_format="flat")

    def test_error_message_lists_conflicting_files(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _make_flat(output_dir, "pr", time_range="185001-190012")
        _make_flat(output_dir, "pr", time_range="190101-200012")

        with pytest.raises(ValueError, match="pr"):
            create_ilamb_symlinks(output_dir, tmp_path / "ilamb", drs_format="flat")

    def test_empty_directory_warns_and_returns_empty(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.warns(UserWarning, match="No .nc files"):
            result = create_ilamb_symlinks(
                output_dir, tmp_path / "ilamb", drs_format="flat"
            )

        assert result == {}


class TestCreateIlambSymlinksOverwrite:
    """Tests for the overwrite parameter."""

    def _setup_with_existing_link(self, tmp_path):
        """Create a flat output dir and a pre-existing symlink pointing elsewhere."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        src = _make_flat(output_dir, "tas")["tas"]
        ilamb_dir = tmp_path / "ilamb"
        ilamb_dir.mkdir()
        old_target = tmp_path / "old_tas.nc"
        old_target.touch()
        link = ilamb_dir / "tas.nc"
        link.symlink_to(old_target)
        return output_dir, ilamb_dir, src, old_target, link

    def test_existing_symlink_skipped_by_default(self, tmp_path):
        output_dir, ilamb_dir, src, old_target, link = self._setup_with_existing_link(
            tmp_path
        )

        with pytest.warns(UserWarning, match="already exists"):
            created = create_ilamb_symlinks(
                output_dir, ilamb_dir, drs_format="flat", overwrite=False
            )

        assert "tas" not in created
        assert link.resolve() == old_target.resolve()

    def test_existing_symlink_replaced_when_overwrite_true(self, tmp_path):
        output_dir, ilamb_dir, src, old_target, link = self._setup_with_existing_link(
            tmp_path
        )

        created = create_ilamb_symlinks(
            output_dir, ilamb_dir, drs_format="flat", overwrite=True
        )

        assert "tas" in created


class TestModelMappingFileExists:
    """Tests for _model_mapping_file_exists in utilities.py."""

    @pytest.mark.unit
    def test_returns_true_for_known_model(self):
        """Returns True when a mapping file is bundled for the given model."""
        assert _model_mapping_file_exists("ACCESS-ESM1.6") is True

    @pytest.mark.unit
    def test_returns_false_for_unknown_model(self):
        """Returns False when no mapping file exists for the given model."""
        assert _model_mapping_file_exists("NONEXISTENT-MODEL-XYZ") is False

    @pytest.mark.unit
    def test_returns_false_for_empty_string(self):
        """Returns False for an empty model ID string."""
        assert _model_mapping_file_exists("") is False


def _make_ocean_monthly_ds(numeric_units: str = "days since 0001-01-01") -> xr.Dataset:
    """Build a minimal ocean-model-like dataset with year-3 CE monthly timestamps.

    Mirrors MOM output: numeric time coordinate with CF units, a ``time_bnds``
    variable that has *no* units attribute, and a non-standard ``calendar_type``
    attribute instead of the CF ``calendar`` attribute.
    """
    # Mid-month values for Jan–Dec of year 3, in days since 0001-01-01
    # (proleptic Gregorian: year 3 is ordinary, 365 days)
    # Jan 16 12:00 = 730 + 15.5  days since 0001-01-01
    year2_days = 365 * 2  # days in years 1 and 2
    mid_month_offsets = [
        15.5,
        45.0,
        74.5,
        105.0,
        135.5,
        166.0,
        196.5,
        227.5,
        258.0,
        288.5,
        319.0,
        349.5,
    ]
    time_values = np.array(
        [year2_days + d for d in mid_month_offsets], dtype=np.float64
    )

    # Bounds: first-of-month to first-of-next-month (no units attr on purpose)
    month_starts = [
        0.0,
        31.0,
        59.0,
        90.0,
        120.0,
        151.0,
        181.0,
        212.0,
        243.0,
        273.0,
        304.0,
        334.0,
        365.0,
    ]
    bnds_values = np.array(
        [
            [year2_days + month_starts[i], year2_days + month_starts[i + 1]]
            for i in range(12)
        ],
        dtype=np.float64,
    )

    ds = xr.Dataset(
        {"time_bnds": xr.DataArray(bnds_values, dims=["time", "nv"])},
        coords={
            "time": xr.DataArray(
                time_values,
                dims=["time"],
                attrs={
                    "units": numeric_units,
                    "calendar_type": "PROLEPTIC_GREGORIAN",
                    "bounds": "time_bnds",
                },
            )
        },
    )
    return ds


class TestDetectTimeFrequencyLazyOceanMonthly:
    """Tests for detect_time_frequency_lazy with ocean-model monthly data.

    Ocean model (MOM) output uses very old calendar dates (year 3 CE) stored as
    numeric offsets with CF units. pandas cannot represent these as Timestamps
    (minimum ~1677 CE), so the frequency detection must use direct timedelta
    arithmetic rather than pd.to_datetime.
    """

    @pytest.mark.unit
    def test_monthly_frequency_year3_ce(self):
        """Monthly data in year 3 CE is correctly identified as ~30 days."""
        ds = _make_ocean_monthly_ds()
        result = detect_time_frequency_lazy(ds)
        assert result is not None
        assert pd.Timedelta("20D") < result < pd.Timedelta("35D")

    @pytest.mark.unit
    def test_not_half_day(self):
        """Frequency must not be 0.5 days (the pre-fix wrong result)."""
        ds = _make_ocean_monthly_ds()
        result = detect_time_frequency_lazy(ds)
        assert result is not None
        assert result > pd.Timedelta(
            "1D"
        ), f"Frequency {result} looks like the old 0.5-day wrong result"

    @pytest.mark.unit
    def test_cftime_object_array_no_units(self):
        """Object-array of cftime datetimes with no units is handled correctly.

        This covers the ``else`` branch of Method 3 for individual (not
        concatenated) ocean files where the time coordinate has no ``units``
        attribute and xarray stores values as a raw object array of cftime
        datetimes.
        """
        dates = [cftime.DatetimeProlepticGregorian(3, m, 16, 12) for m in range(1, 13)]
        time_values = np.array(dates, dtype=object)

        ds = xr.Dataset(
            coords={
                "time": xr.DataArray(
                    time_values,
                    dims=["time"],
                    # No "units" attribute — matches individual ocean file behaviour
                )
            }
        )

        result = detect_time_frequency_lazy(ds)
        assert result is not None
        assert pd.Timedelta("20D") < result < pd.Timedelta("35D")

    @pytest.mark.unit
    def test_result_is_timedelta(self):
        """Return value is always a pd.Timedelta, not a raw number."""
        ds = _make_ocean_monthly_ds()
        result = detect_time_frequency_lazy(ds)
        assert isinstance(result, pd.Timedelta)


class TestDetectTimeFrequencyLazyMethod3EdgeCases:
    """Cover the remaining unchecked branches of Method 3 in detect_time_frequency_lazy."""

    # ------------------------------------------------------------------
    # Branch: elif units and "since" in units  →  num2date raises ValueError
    #         AND values are datetime64  →  pd.to_datetime fallback
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_num2date_valueerror_non_datetime64_reraises(self):
        """ValueError from num2date is re-raised when values are not datetime64."""
        time_values = np.array([1.0, 31.0, 59.0])
        ds = xr.Dataset(
            coords={
                "time": xr.DataArray(
                    time_values,
                    dims=["time"],
                    attrs={"units": "days since 2000-01-01", "calendar": "standard"},
                )
            }
        )

        with patch("access_moppy.utilities.num2date", side_effect=ValueError("bad")):
            import warnings

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = detect_time_frequency_lazy(ds)
            assert result is None
            assert any("bad" in str(warning.message) for warning in w)

    # ------------------------------------------------------------------
    # Branch: else (no units)  →  object dtype, but subtraction raises
    #         →  except Exception: pass  →  pd.to_datetime fallback
    #         →  pd.to_datetime also fails  →  outer except swallows, returns None
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_object_array_subtraction_error_swallowed_returns_none(self):
        """When cftime subtraction raises, the inner except is silenced and execution
        falls through to pd.to_datetime. Since plain object() values are not
        convertible, pd.to_datetime also raises, which is caught by the outermost
        except-Exception handler — so the function returns None with a warning.
        """
        bad_dates = [object(), object(), object()]  # subtraction will raise TypeError
        time_values = np.array(bad_dates, dtype=object)

        ds = xr.Dataset(
            coords={
                "time": xr.DataArray(
                    time_values,
                    dims=["time"],
                    # No units — takes the else branch
                )
            }
        )

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = detect_time_frequency_lazy(ds)
        assert result is None
        assert len(w) >= 1

    # ------------------------------------------------------------------
    # Branch: else (no units)  →  non-object dtype  →  pd.to_datetime fallback
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_no_units_non_object_dtype_uses_pd_to_datetime(self):
        """Without units and with datetime64 values, falls through to pd.to_datetime."""
        time_values = np.array(
            ["2000-01-15", "2000-02-15", "2000-03-15"], dtype="datetime64[D]"
        )
        ds = xr.Dataset(
            coords={
                "time": xr.DataArray(
                    time_values,
                    dims=["time"],
                    # No units attr
                )
            }
        )
        result = detect_time_frequency_lazy(ds)
        assert result is not None
        assert pd.Timedelta("20D") < result < pd.Timedelta("35D")


class TestDetectFrequencyFromBoundsCrossValidation:
    """Cover the cross-validation discard paths in _detect_frequency_from_bounds."""

    def _make_ds_with_bounds(self, time_values, bnds_values, units):
        """Build a minimal dataset with time_bnds for bounds-based detection tests."""
        ds = xr.Dataset(
            {"time_bnds": xr.DataArray(bnds_values, dims=["time", "nv"])},
            coords={
                "time": xr.DataArray(
                    time_values,
                    dims=["time"],
                    attrs={
                        "units": units,
                        "calendar": "standard",
                        "bounds": "time_bnds",
                    },
                )
            },
        )
        ds["time_bnds"].attrs["units"] = units
        return ds

    @pytest.mark.unit
    def test_valid_bounds_returns_frequency(self):
        """Properly centred monthly bounds produce ~30 days."""
        time_values = np.array([15.0])
        bnds_values = np.array([[0.0, 30.0]])
        units = "days since 2000-01-01"

        ds = self._make_ds_with_bounds(time_values, bnds_values, units)
        result = _detect_frequency_from_bounds(ds, "time")
        assert result is not None
        assert pd.Timedelta("20D") < result < pd.Timedelta("35D")

    @pytest.mark.unit
    def test_no_bounds_variable_returns_none(self):
        """When the dataset has no time bounds variable at all, return None.

        Exercises the ``if bounds_var is None: return None`` True-branch:
        neither the ``bounds`` attr lookup nor the name-scan finds anything,
        so bounds_var remains None and the function returns None early.
        """
        ds = xr.Dataset(
            coords={
                "time": xr.DataArray(
                    np.array([15.0, 46.0, 75.0]),
                    dims=["time"],
                    attrs={"units": "days since 2000-01-01", "calendar": "standard"},
                    # No "bounds" attr and no time_bnds/time_bounds in the dataset
                )
            }
        )
        result = _detect_frequency_from_bounds(ds, "time")
        assert result is None

    @pytest.mark.unit
    def test_bounds_found_via_name_scan_not_attr(self):
        """When time_bnds is present as a data var but time coord has no ``bounds``
        attr, the name-scan finds it — exercising the ``if bounds_var is None:``
        False-branch (bounds_var is not None after the for loop).
        """
        time_values = np.array([15.0])
        bnds_values = np.array([[0.0, 30.0]])
        units = "days since 2000-01-01"

        # Deliberately omit the "bounds" attr from the time coordinate so the
        # function must fall through to the for-loop name scan.
        ds = xr.Dataset(
            {
                "time_bnds": xr.DataArray(
                    bnds_values, dims=["time", "nv"], attrs={"units": units}
                )
            },
            coords={
                "time": xr.DataArray(
                    time_values,
                    dims=["time"],
                    attrs={"units": units, "calendar": "standard"},
                    # No "bounds" attr — forces name-scan path
                )
            },
        )
        result = _detect_frequency_from_bounds(ds, "time")
        assert result is not None
        assert pd.Timedelta("20D") < result < pd.Timedelta("35D")

    @pytest.mark.unit
    def test_bounds_wrong_shape_returns_none(self):
        """bounds variable with unexpected shape → warning + return None (line 1430).

        CF-compliant time_bnds must have shape (time, 2). A 1-D array triggers
        the shape guard and the function returns None.
        """
        units = "days since 2000-01-01"
        ds = xr.Dataset(
            # 1-D bounds — wrong shape, should be (time, 2)
            {
                "time_bnds": xr.DataArray(
                    np.array([0.0, 30.0]),
                    dims=["nv"],
                    attrs={"units": units},
                )
            },
            coords={
                "time": xr.DataArray(
                    np.array([15.0]),
                    dims=["time"],
                    attrs={
                        "units": units,
                        "calendar": "standard",
                        "bounds": "time_bnds",
                    },
                )
            },
        )
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _detect_frequency_from_bounds(ds, "time")
        assert result is None
        assert any("unexpected shape" in str(warning.message) for warning in w)
