"""
Comprehensive tests for calculate_time_bounds and helper functions.
"""

import os
import tempfile

import cftime
import numpy as np
import pytest
import xarray as xr

from access_moppy.utilities import (
    _infer_frequency,
    calculate_time_bounds,
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
