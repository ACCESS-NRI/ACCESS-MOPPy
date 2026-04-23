import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.derivations.calc_utils import (
    calculate_monthly_maximum,
    calculate_monthly_minimum,
)


class TestEvaluateExpression:
    """Tests for evaluate_expression() — covers the 'formula' key fix."""

    def _make_context(self):
        time = pd.date_range("2000-01-01", periods=5, freq="D")
        da = xr.DataArray(np.ones((5, 3)), dims=["time", "x"], coords={"time": time})
        return {"var1": da, "var2": da.copy()}

    def test_operation_key(self):
        """Original 'operation' key still works."""
        ctx = self._make_context()
        expr = {"operation": "add", "operands": ["var1", "var2"]}
        result = evaluate_expression(expr, ctx)
        assert float(result.isel(time=0, x=0)) == pytest.approx(2.0)

    def test_literal(self):
        """Literal values are returned as-is."""
        result = evaluate_expression({"literal": 42}, {})
        assert result == 42

    def test_string_lookup(self):
        """String expressions are looked up in context."""
        ctx = self._make_context()
        result = evaluate_expression("var1", ctx)
        assert result is ctx["var1"]

    def test_numeric_passthrough(self):
        """Numeric values are returned directly."""
        assert evaluate_expression(3.14, {}) == pytest.approx(3.14)


class TestCalculateMonthlyMinimum:
    """Tests for calculate_monthly_minimum() — covers decode_cf and ME frequency fix."""

    def _daily_da_encoded(self, n_days=365):
        """Daily DataArray with undecoded (float64) time — simulates decode_cf=False."""
        time_vals = np.arange(n_days, dtype="float64")
        da = xr.DataArray(
            np.random.rand(n_days, 4, 8).astype("float32"),
            dims=["time", "lat", "lon"],
            coords={"time": ("time", time_vals)},
        )
        da["time"].attrs = {
            "units": "days since 2000-01-01",
            "calendar": "proleptic_gregorian",
        }
        return da

    def _daily_da_decoded(self, n_days=365):
        """Daily DataArray with decoded cftime time."""
        time = xr.date_range("2000-01-01", periods=n_days, freq="D", use_cftime=True)
        return xr.DataArray(
            np.random.rand(n_days, 4, 8).astype("float32"),
            dims=["time", "lat", "lon"],
            coords={"time": time},
        )

    def test_encoded_time_produces_monthly_output(self):
        """Encoded float64 time is decoded and resampled to monthly."""
        da = self._daily_da_encoded()
        result = calculate_monthly_minimum(da)
        assert result.sizes["time"] == 12

    def test_decoded_time_produces_monthly_output(self):
        """Already-decoded cftime time is resampled to monthly without error."""
        da = self._daily_da_decoded()
        result = calculate_monthly_minimum(da)
        assert result.sizes["time"] == 12

    def test_missing_time_dim_raises(self):
        da = xr.DataArray(np.ones((5, 3)), dims=["lat", "lon"])
        with pytest.raises(ValueError, match="Time dimension"):
            calculate_monthly_minimum(da)

    def test_missing_time_coord_raises(self):
        da = xr.DataArray(np.ones((5, 3)), dims=["time", "lon"])
        with pytest.raises(ValueError, match="Time coordinate"):
            calculate_monthly_minimum(da)


class TestCalculateMonthlyMaximum:
    """Tests for calculate_monthly_maximum() — mirrors minimum tests."""

    def _daily_da_encoded(self, n_days=365):
        time_vals = np.arange(n_days, dtype="float64")
        da = xr.DataArray(
            np.random.rand(n_days, 4, 8).astype("float32"),
            dims=["time", "lat", "lon"],
            coords={"time": ("time", time_vals)},
        )
        da["time"].attrs = {
            "units": "days since 2000-01-01",
            "calendar": "proleptic_gregorian",
        }
        return da

    def test_encoded_time_produces_monthly_output(self):
        da = self._daily_da_encoded()
        result = calculate_monthly_maximum(da)
        assert result.sizes["time"] == 12

    def test_max_greater_than_min(self):
        """Monthly max must be >= monthly min for same input."""
        da = self._daily_da_encoded()
        mn = calculate_monthly_minimum(da)
        mx = calculate_monthly_maximum(da)
        assert (mx.values >= mn.values).all()


class TestCalcLandcover:
    """Tests for calc_landcover() — covers the refactored explicit-args signature."""

    def _make_inputs(self, n_tiles=17, nlat=3, nlon=4):
        """Return (tilefrac, landfrac) DataArrays compatible with calc_landcover."""
        rng = np.random.default_rng(0)
        tilefrac = xr.DataArray(
            rng.uniform(0, 1, (nlat, n_tiles, nlon)).astype(np.float32),
            dims=["lat", "pseudo_level_0", "lon"],
        )
        landfrac = xr.DataArray(
            rng.uniform(0, 1, (nlat, nlon)).astype(np.float32),
            dims=["lat", "lon"],
        )
        return tilefrac, landfrac

    @pytest.mark.unit
    def test_cable_output_dimension_named_type(self):
        """calc_landcover renames the pseudo-level dim to 'type' (CMIP out_name)."""
        from access_moppy.derivations.calc_land import calc_landcover

        tilefrac, landfrac = self._make_inputs(n_tiles=17)
        result = calc_landcover(tilefrac, landfrac, model="cable")
        assert "type" in result.dims
        assert "pseudo_level_0" not in result.dims

    @pytest.mark.unit
    def test_cable_output_is_percentage(self):
        """Values must be 0–100 (percentage, not fraction)."""
        from access_moppy.derivations.calc_land import calc_landcover

        tilefrac = xr.DataArray(
            np.full((2, 17, 3), 0.5, dtype=np.float32),
            dims=["lat", "pseudo_level_0", "lon"],
        )
        landfrac = xr.DataArray(
            np.full((2, 3), 0.5, dtype=np.float32), dims=["lat", "lon"]
        )
        result = calc_landcover(tilefrac, landfrac, model="cable")
        # 0.5 * 0.5 * 100 = 25 %
        np.testing.assert_allclose(result.values, 25.0, rtol=1e-5)

    @pytest.mark.unit
    def test_cable_vegtype_coordinate_has_17_entries(self):
        """CABLE model must produce exactly 17 vegetation-type labels."""
        from access_moppy.derivations.calc_land import calc_landcover

        tilefrac, landfrac = self._make_inputs(n_tiles=17)
        result = calc_landcover(tilefrac, landfrac, model="cable")
        assert result.sizes["type"] == 17

    @pytest.mark.unit
    def test_cable_vegtype_coordinate_values(self):
        """Spot-check a few known CABLE vegetation-type labels."""
        from access_moppy.derivations.calc_land import calc_landcover

        tilefrac, landfrac = self._make_inputs(n_tiles=17)
        result = calc_landcover(tilefrac, landfrac, model="cable")
        type_values = result["type"].values.tolist()
        assert type_values[0] == "Evergreen_Needleleaf"
        assert type_values[4] == "Shrub"
        assert type_values[16] == "Ice"

    @pytest.mark.unit
    def test_cmip6_output_dimension_named_type(self):
        """CMIP6 model path also renames the pseudo-level dim to 'type'."""
        from access_moppy.derivations.calc_land import calc_landcover

        tilefrac, landfrac = self._make_inputs(n_tiles=4)
        result = calc_landcover(tilefrac, landfrac, model="cmip6")
        assert "type" in result.dims
        assert result.sizes["type"] == 4

    @pytest.mark.unit
    def test_nan_values_are_filled_with_zero(self):
        """NaN inputs must be replaced by 0.0 in the output."""
        from access_moppy.derivations.calc_land import calc_landcover

        # cmip6 has 4 vegetation types — pseudo_level_0 must match
        tilefrac = xr.DataArray(
            np.array(
                [[[np.nan, 1.0], [0.5, 0.5], [0.3, 0.3], [0.2, 0.2]]], dtype=np.float32
            ),
            dims=["lat", "pseudo_level_0", "lon"],
        )
        landfrac = xr.DataArray(
            np.array([[1.0, 1.0]], dtype=np.float32), dims=["lat", "lon"]
        )
        result = calc_landcover(tilefrac, landfrac, model="cmip6")
        # NaN * anything = NaN, then fillna(0) → 0
        assert float(result.isel(lat=0, type=0, lon=0)) == pytest.approx(0.0)

    @pytest.mark.unit
    def test_type_coord_units_are_empty(self):
        """The 'type' coordinate must carry an empty-string units attribute."""
        from access_moppy.derivations.calc_land import calc_landcover

        tilefrac, landfrac = self._make_inputs(n_tiles=17)
        result = calc_landcover(tilefrac, landfrac, model="cable")
        assert result["type"].attrs.get("units") == ""
