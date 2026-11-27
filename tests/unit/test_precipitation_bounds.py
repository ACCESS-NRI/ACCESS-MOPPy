"""Numerical regressions for convective precipitation and snowfall bounds."""

from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from access_moppy.atmosphere import Atmosphere_CMORiser
from access_moppy.utilities import load_model_mappings


@pytest.mark.unit
@pytest.mark.parametrize("model_id", ["ACCESS-ESM1-5", "ACCESS-ESM1-6"])
@pytest.mark.parametrize("table,frequency", [("day", "day"), ("Amon", "mon")])
@pytest.mark.parametrize("mip_era", ["CMIP6", "CMIP7"])
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "dask"])
@pytest.mark.parametrize("dtype", ["float32", "float64"])
@pytest.mark.parametrize(
    ("name", "raw_values"),
    [
        (
            "prc",
            {
                # Clipping the negative component of the fifth cell separately
                # would inflate the positive sum from 2e-5 to 3e-5.
                "fld_s05i205": [-1e-12, 0, 1e-5, np.nan, -1e-5, 1e-5],
                "fld_s05i206": [0, 0, 3e-5, 1e-5, 3e-5, np.nan],
            },
        ),
        ("prsn", {"fld_s05i215": [-1e-12, 0, 4e-5, np.nan, 2e-5, np.nan]}),
    ],
)
def test_precipitation_is_nonnegative_and_preserves_missing_values(
    model_id, table, frequency, mip_era, lazy, dtype, name, raw_values, tmp_path
):
    compound_name = f"{table}.{name}"
    mapping_name = (
        f"atmos.{name}.tavg-u-hxy-u.{frequency}.GLB"
        if mip_era == "CMIP7"
        else compound_name
    )
    mapping = load_model_mappings(mapping_name, model_id)

    dims = ("time", "lat", "lon")
    coords = {"time": [0], "lat": [-30.0], "lon": np.arange(6)}
    raw = xr.Dataset(
        {
            field: (dims, np.array(values, dtype=dtype)[None, None, :])
            for field, values in raw_values.items()
        },
        coords=coords,
    )
    if lazy:
        raw = raw.chunk({"lon": 2})

    vocab = Mock(variable={"dimensions": list(dims)})
    vocab.axes = {dim: {"out_name": dim} for dim in dims}
    vocab._get_axes.return_value = ({}, {})
    vocab._get_required_bounds_variables.return_value = ({}, {})
    cmoriser = Atmosphere_CMORiser(
        input_data=raw,
        output_path=str(tmp_path),
        vocab=vocab,
        variable_mapping=mapping,
        compound_name=compound_name,
        enable_chunking=False,
    )
    cmoriser.select_and_process_variables()
    result = cmoriser.ds[name]

    assert result.dims == dims
    assert result.dtype == np.dtype(dtype)
    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        np.array([[[0, 0, 4e-5, np.nan, 2e-5, np.nan]]], dtype=dtype),
        dims=dims,
        coords=coords,
    )
    xr.testing.assert_allclose(result.compute(), expected, rtol=5e-7, atol=0)
