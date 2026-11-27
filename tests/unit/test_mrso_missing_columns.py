"""Numerical regressions for missing soil moisture columns."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings


@pytest.mark.unit
@pytest.mark.parametrize("model_id", ["ACCESS-ESM1-5", "ACCESS-ESM1-6"])
@pytest.mark.parametrize(
    "compound_name",
    [
        "day.mrso",
        "Lmon.mrso",
        "land.mrso.tavg-u-hxy-lnd.day.GLB",
        "land.mrso.tavg-u-hxy-lnd.mon.GLB",
    ],
)
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "dask"])
@pytest.mark.parametrize("dtype", ["float32", "float64"])
def test_mrso_preserves_missing_columns_and_valid_zeros(
    model_id, compound_name, lazy, dtype, tmp_path
):
    # Wet land, an all-missing column, dry land, an incomplete column,
    # and another valid column with a different total.
    values = np.array([[1, np.nan, 0, 1, 2]] * 6, dtype=dtype)
    values[2, 3] = np.nan
    coords = {"time": [0], "lat": [-30.0], "lon": np.arange(5)}
    raw = xr.DataArray(
        values[None, :, None, :],
        dims=["time", "soil_model_level_number", "lat", "lon"],
        coords={**coords, "soil_model_level_number": np.arange(1, 7)},
    )
    if lazy:
        raw = raw.chunk({"soil_model_level_number": 2, "lon": 2})

    mapping = load_model_mappings(compound_name, model_id)["mrso"]
    result = evaluate_expression(mapping["calculation"], {"fld_s08i223": raw})

    assert result.dims == ("time", "lat", "lon")
    assert result.dtype == np.dtype(dtype)
    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        np.array([[[6, np.nan, 0, np.nan, 12]]], dtype=dtype),
        dims=["time", "lat", "lon"],
        coords=coords,
    )
    xr.testing.assert_equal(result.compute(), expected)
    assert result.mean().compute().item() == 6

    path = tmp_path / "mrso.nc"
    result.rename("mrso").to_netcdf(path, encoding={"mrso": {"_FillValue": 1e20}})
    with xr.open_dataset(path) as decoded:
        xr.testing.assert_equal(decoded.mrso, expected)
    with xr.open_dataset(path, mask_and_scale=False) as encoded:
        fill_value = encoded.mrso.attrs["_FillValue"]
        np.testing.assert_array_equal(
            encoded.mrso.values, [[[6, fill_value, 0, fill_value, 12]]]
        )
