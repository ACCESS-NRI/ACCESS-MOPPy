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
