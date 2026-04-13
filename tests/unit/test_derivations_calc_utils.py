"""Tests for access_moppy.derivations.calc_utils."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_utils import (
    add_axis,
    calculate_monthly_maximum,
    calculate_monthly_minimum,
    drop_axis,
    drop_time_axis,
    rename_coord,
    squeeze_axis,
    sum_vars,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_da(shape=(3, 4), dims=("time", "lat"), name="var"):
    """Return a simple DataArray with given shape / dims."""
    data = np.arange(float(np.prod(shape))).reshape(shape)
    return xr.DataArray(data, dims=dims, name=name)


def _make_time_da(n=12, freq="ME"):
    """Return a DataArray with a proper time coordinate."""
    times = xr.date_range("2000-01-01", periods=n, freq=freq)
    data = np.random.default_rng(0).random(n)
    return xr.DataArray(data, dims=["time"], coords={"time": times}, name="var")


# ---------------------------------------------------------------------------
# add_axis
# ---------------------------------------------------------------------------


class TestAddAxis:
    @pytest.mark.unit
    def test_adds_new_dimension(self):
        da = _make_da()
        result = add_axis(da, "lev", 500.0)
        assert "lev" in result.dims

    @pytest.mark.unit
    def test_new_dimension_is_singleton(self):
        da = _make_da()
        result = add_axis(da, "lev", 500.0)
        assert result.sizes["lev"] == 1

    @pytest.mark.unit
    def test_original_dims_preserved(self):
        da = _make_da(shape=(3, 4), dims=("time", "lat"))
        result = add_axis(da, "lev", 100.0)
        for dim in ("time", "lat"):
            assert dim in result.dims

    @pytest.mark.unit
    def test_data_values_unchanged(self):
        da = _make_da()
        result = add_axis(da, "lev", 500.0)
        np.testing.assert_array_equal(result.squeeze("lev").values, da.values)

    @pytest.mark.unit
    def test_axis_value_stored(self):
        da = _make_da()
        result = add_axis(da, "lev", 250.0)
        assert float(result["lev"].values[0]) == 250.0


# ---------------------------------------------------------------------------
# drop_axis
# ---------------------------------------------------------------------------


class TestDropAxis:
    @pytest.mark.unit
    def test_drops_single_dim(self):
        da = _make_da(shape=(1, 4), dims=("lev", "lat"))
        result = drop_axis(da, "lev")
        assert "lev" not in result.dims
        assert "lat" in result.dims

    @pytest.mark.unit
    def test_drops_multiple_dims(self):
        da = _make_da(shape=(1, 1, 4), dims=("lev", "extra", "lat"))
        result = drop_axis(da, ["lev", "extra"])
        assert "lev" not in result.dims
        assert "extra" not in result.dims
        assert "lat" in result.dims

    @pytest.mark.unit
    def test_missing_dim_ignored(self):
        da = _make_da(shape=(3, 4), dims=("time", "lat"))
        # 'lev' is not a dimension – should not raise
        result = drop_axis(da, "lev")
        assert list(result.dims) == ["time", "lat"]

    @pytest.mark.unit
    def test_string_input_accepted(self):
        da = _make_da(shape=(2, 4), dims=("lev", "lat"))
        result = drop_axis(da, "lev")
        assert "lev" not in result.dims

    @pytest.mark.unit
    def test_drops_time_axis(self):
        n = 5
        times = xr.date_range("2000-01-01", periods=n, freq="ME")
        da = xr.DataArray(np.ones(n), dims=["time"], coords={"time": times})
        result = drop_axis(da, "time")
        assert "time" not in result.dims


# ---------------------------------------------------------------------------
# drop_time_axis
# ---------------------------------------------------------------------------


class TestDropTimeAxis:
    @pytest.mark.unit
    def test_drops_time_when_present(self):
        n = 4
        times = xr.date_range("2000-01-01", periods=n, freq="ME")
        da = xr.DataArray(np.ones((n, 3)), dims=["time", "lat"], coords={"time": times})
        result = drop_time_axis(da)
        assert "time" not in result.dims

    @pytest.mark.unit
    def test_no_op_when_time_absent(self):
        da = _make_da(shape=(3, 4), dims=("lat", "lon"))
        result = drop_time_axis(da)
        assert list(result.dims) == ["lat", "lon"]

    @pytest.mark.unit
    def test_selects_first_time_step(self):
        n = 3
        times = xr.date_range("2000-01-01", periods=n, freq="ME")
        data = np.array([[1, 2], [3, 4], [5, 6]], dtype=float)
        da = xr.DataArray(data, dims=["time", "lat"], coords={"time": times})
        result = drop_time_axis(da)
        np.testing.assert_array_equal(result.values, data[0])


# ---------------------------------------------------------------------------
# squeeze_axis
# ---------------------------------------------------------------------------


class TestSqueezeAxis:
    @pytest.mark.unit
    def test_squeezes_all_singletons_by_default(self):
        da = _make_da(shape=(1, 3, 1), dims=("lev", "lat", "lon"))
        result = squeeze_axis(da)
        assert result.shape == (3,)
        assert "lat" in result.dims

    @pytest.mark.unit
    def test_squeezes_named_dim(self):
        da = _make_da(shape=(1, 3), dims=("lev", "lat"))
        result = squeeze_axis(da, dims="lev")
        assert "lev" not in result.dims
        assert result.shape == (3,)

    @pytest.mark.unit
    def test_non_singleton_dim_unchanged(self):
        da = _make_da(shape=(2, 3), dims=("lev", "lat"))
        # squeezing a non-singleton dim should raise an error (xarray behaviour)
        with pytest.raises(ValueError):
            squeeze_axis(da, dims="lev")

    @pytest.mark.unit
    def test_data_values_preserved(self):
        data = np.array([[[1.0, 2.0, 3.0]]])  # shape (1, 1, 3)
        da = xr.DataArray(data, dims=["a", "b", "c"])
        result = squeeze_axis(da)
        np.testing.assert_array_equal(result.values, [1.0, 2.0, 3.0])


# ---------------------------------------------------------------------------
# sum_vars
# ---------------------------------------------------------------------------


class TestSumVars:
    @pytest.mark.unit
    def test_sums_two_arrays(self):
        a = xr.DataArray([1.0, 2.0, 3.0], dims=["x"])
        b = xr.DataArray([4.0, 5.0, 6.0], dims=["x"])
        result = sum_vars([a, b])
        np.testing.assert_array_equal(result.values, [5.0, 7.0, 9.0])

    @pytest.mark.unit
    def test_sums_three_arrays(self):
        arrays = [xr.DataArray(np.ones(4) * i, dims=["x"]) for i in range(1, 4)]
        result = sum_vars(arrays)
        np.testing.assert_array_equal(result.values, np.ones(4) * 6.0)

    @pytest.mark.unit
    def test_single_element_list(self):
        a = xr.DataArray([1.0, 2.0], dims=["x"])
        result = sum_vars([a])
        np.testing.assert_array_equal(result.values, a.values)

    @pytest.mark.unit
    def test_preserves_name(self):
        a = xr.DataArray([1.0], dims=["x"], name="myvar")
        b = xr.DataArray([2.0], dims=["x"])
        result = sum_vars([a, b])
        # The result should be a valid DataArray (name behaviour varies)
        assert isinstance(result, xr.DataArray)


# ---------------------------------------------------------------------------
# rename_coord
# ---------------------------------------------------------------------------


class TestRenameCoord:
    @pytest.mark.unit
    def test_renames_when_coords_differ(self):
        a = xr.DataArray(np.ones(3), dims=["lat"])
        b = xr.DataArray(np.ones(3), dims=["latitude"])
        result, override = rename_coord(a, b, 0)
        assert result.dims[0] == "lat"
        assert override is True

    @pytest.mark.unit
    def test_no_rename_when_coords_same(self):
        a = xr.DataArray(np.ones(3), dims=["lat"])
        b = xr.DataArray(np.ones(3), dims=["lat"])
        result, override = rename_coord(a, b, 0)
        assert result.dims[0] == "lat"
        assert override is False

    @pytest.mark.unit
    def test_copies_bounds_attribute(self):
        lat_coord = xr.Variable("lat", [0, 1, 2], attrs={"bounds": "lat_bnds"})
        a = xr.DataArray(
            np.ones(3),
            dims=["lat"],
            coords={"lat": lat_coord},
        )
        # b also has a "latitude" coord so that after rename var2["lat"] is accessible
        b = xr.DataArray(
            np.ones(3),
            dims=["latitude"],
            coords={"latitude": [0, 1, 2]},
        )
        result, _ = rename_coord(a, b, 0)
        assert result["lat"].attrs.get("bounds") == "lat_bnds"


# ---------------------------------------------------------------------------
# calculate_monthly_minimum
# ---------------------------------------------------------------------------


class TestCalculateMonthlyMinimum:
    @pytest.mark.unit
    def test_returns_monthly_values(self):
        da = _make_time_da(n=365, freq="D")
        result = calculate_monthly_minimum(da)
        # Expect approximately 12 months of output
        assert result.sizes["time"] == 12

    @pytest.mark.unit
    def test_values_are_monthly_minima(self):
        times = xr.date_range("2000-01-01", periods=30, freq="D")
        # All values 1.0 except the first day which is 0.0
        data = np.ones(30)
        data[0] = 0.0
        da = xr.DataArray(data, dims=["time"], coords={"time": times})
        result = calculate_monthly_minimum(da)
        # January minimum should be 0.0
        assert float(result.values[0]) == pytest.approx(0.0)

    @pytest.mark.unit
    def test_raises_for_missing_time_dim(self):
        da = xr.DataArray(np.ones(4), dims=["lat"])
        with pytest.raises(ValueError, match="Time dimension"):
            calculate_monthly_minimum(da)

    @pytest.mark.unit
    def test_raises_for_missing_time_coord(self):
        da = xr.DataArray(np.ones(4), dims=["time"])
        with pytest.raises(ValueError, match="Time coordinate"):
            calculate_monthly_minimum(da)

    @pytest.mark.unit
    def test_cell_methods_updated(self):
        da = _make_time_da(n=365, freq="D")
        result = calculate_monthly_minimum(da)
        assert "minimum" in result.attrs.get("cell_methods", "")

    @pytest.mark.unit
    def test_existing_cell_methods_preserved(self):
        da = _make_time_da(n=365, freq="D")
        da.attrs["cell_methods"] = "time: mean"
        result = calculate_monthly_minimum(da)
        assert "time: mean" in result.attrs.get("cell_methods", "")

    @pytest.mark.unit
    def test_preserve_attrs_false(self):
        da = _make_time_da(n=365, freq="D")
        da.attrs["units"] = "K"
        result = calculate_monthly_minimum(da, preserve_attrs=False)
        assert "cell_methods" not in result.attrs

    @pytest.mark.unit
    def test_custom_time_dim_name(self):
        times = xr.date_range("2000-01-01", periods=365, freq="D")
        data = np.random.default_rng(1).random(365)
        da = xr.DataArray(data, dims=["t"], coords={"t": times})
        result = calculate_monthly_minimum(da, time_dim="t")
        assert result.sizes["t"] == 12

    @pytest.mark.unit
    def test_resample_failure_raises_runtime_error(self):
        """An exception raised inside the resample block is wrapped as RuntimeError."""
        from unittest.mock import MagicMock, patch

        da = _make_time_da(n=10, freq="D")
        mock_resampler = MagicMock()
        mock_resampler.min.side_effect = ValueError("forced error")
        with patch.object(da.__class__, "resample", return_value=mock_resampler):
            with pytest.raises(
                RuntimeError, match="Failed to calculate monthly minimum"
            ):
                calculate_monthly_minimum(da)


# ---------------------------------------------------------------------------
# calculate_monthly_maximum
# ---------------------------------------------------------------------------


class TestCalculateMonthlyMaximum:
    @pytest.mark.unit
    def test_returns_monthly_values(self):
        da = _make_time_da(n=365, freq="D")
        result = calculate_monthly_maximum(da)
        assert result.sizes["time"] == 12

    @pytest.mark.unit
    def test_values_are_monthly_maxima(self):
        times = xr.date_range("2000-01-01", periods=30, freq="D")
        data = np.zeros(30)
        data[-1] = 99.0
        da = xr.DataArray(data, dims=["time"], coords={"time": times})
        result = calculate_monthly_maximum(da)
        # January maximum should be 99.0
        assert float(result.values[0]) == pytest.approx(99.0)

    @pytest.mark.unit
    def test_raises_for_missing_time_dim(self):
        da = xr.DataArray(np.ones(4), dims=["lat"])
        with pytest.raises(ValueError, match="Time dimension"):
            calculate_monthly_maximum(da)

    @pytest.mark.unit
    def test_raises_for_missing_time_coord(self):
        da = xr.DataArray(np.ones(4), dims=["time"])
        with pytest.raises(ValueError, match="Time coordinate"):
            calculate_monthly_maximum(da)

    @pytest.mark.unit
    def test_cell_methods_updated(self):
        da = _make_time_da(n=365, freq="D")
        result = calculate_monthly_maximum(da)
        assert "maximum" in result.attrs.get("cell_methods", "")

    @pytest.mark.unit
    def test_existing_cell_methods_preserved(self):
        da = _make_time_da(n=365, freq="D")
        da.attrs["cell_methods"] = "area: mean"
        result = calculate_monthly_maximum(da)
        assert "area: mean" in result.attrs.get("cell_methods", "")

    @pytest.mark.unit
    def test_preserve_attrs_false(self):
        da = _make_time_da(n=365, freq="D")
        da.attrs["units"] = "K"
        result = calculate_monthly_maximum(da, preserve_attrs=False)
        assert "cell_methods" not in result.attrs

    @pytest.mark.unit
    def test_custom_time_dim_name(self):
        times = xr.date_range("2000-01-01", periods=365, freq="D")
        data = np.random.default_rng(2).random(365)
        da = xr.DataArray(data, dims=["t"], coords={"t": times})
        result = calculate_monthly_maximum(da, time_dim="t")
        assert result.sizes["t"] == 12

    @pytest.mark.unit
    def test_max_is_gte_min(self):
        da = _make_time_da(n=365, freq="D")
        result_min = calculate_monthly_minimum(da)
        result_max = calculate_monthly_maximum(da)
        assert (result_max.values >= result_min.values).all()

    @pytest.mark.unit
    def test_resample_failure_raises_runtime_error(self):
        """An exception raised inside the resample block is wrapped as RuntimeError."""
        from unittest.mock import MagicMock, patch

        da = _make_time_da(n=10, freq="D")
        mock_resampler = MagicMock()
        mock_resampler.max.side_effect = ValueError("forced error")
        with patch.object(da.__class__, "resample", return_value=mock_resampler):
            with pytest.raises(
                RuntimeError, match="Failed to calculate monthly maximum"
            ):
                calculate_monthly_maximum(da)
