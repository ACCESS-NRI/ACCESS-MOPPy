"""Numerical regressions for the annual product-flux recipes in issue #802."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings


@pytest.mark.unit
@pytest.mark.parametrize("model_id", ["ACCESS-ESM1-5", "ACCESS-ESM1-6"])
@pytest.mark.parametrize("mip_era", ["CMIP6", "CMIP7"])
@pytest.mark.parametrize("tile_dim", ["pseudo_level_0", "pseudo_level"])
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "dask"])
@pytest.mark.parametrize(
    ("name", "fields", "annual_kg"),
    [
        ("fDeforestToProduct", {895: 1.0}, 1.0),
        ("fProductDecomp", {907: 1.0, 908: 2.0, 909: 4.0}, 7.0),
    ],
)
def test_product_flux_sums_tiles_and_converts_annual_units(
    model_id, mip_era, tile_dim, lazy, name, fields, annual_kg
):
    """Annual g m-2 inputs need no tile fractions and become kg m-2 s-1.

    Retain the existing land-fraction scaling and ocean behavior here;
    the coastal normalization audit is tracked separately in #808.
    """
    compound_name = (
        f"Emon.{name}" if mip_era == "CMIP6" else f"land.{name}.tavg-u-hxy-lnd.mon.glb"
    )
    mapping = load_model_mappings(compound_name, model_id)[name]
    times = np.array(["1951-06-15", "1952-06-15"], dtype="datetime64[ns]")
    coords = {"time": times, "lat": [-31.25], "lon": [0.0, 1.875, 3.75, 5.625]}
    # Unequal contributions sum to 1000 g in year one, 2000 g in year two.
    values = np.broadcast_to(
        np.array([[100.0, 300.0, 600.0], [200.0, 600.0, 1200.0]])[:, :, None, None],
        (2, 3, 1, 4),
    ).copy()
    values[:, :, :, -1] = np.nan  # Native missing product values over ocean.
    raw = xr.DataArray(
        values,
        dims=["time", tile_dim, "lat", "lon"],
        coords={**coords, tile_dim: [1, 2, 3]},
        attrs={"units": "g m-2 yr-1"},
    )
    landfrac = xr.DataArray(
        [[1.0, 0.5, 0.1, 0.0]],
        dims=["lat", "lon"],
        coords={key: coords[key] for key in ("lat", "lon")},
    )
    if lazy:
        raw = raw.chunk({"time": 1, tile_dim: 2, "lon": 2})
        landfrac = landfrac.chunk({"lon": 2})
    context = {f"fld_s03i{stash}": raw * factor for stash, factor in fields.items()}
    context["fld_s03i395"] = landfrac
    assert set(mapping["model_variables"]) == set(context)

    result = evaluate_expression(mapping["calculation"], context)

    assert result.dims == ("time", "lat", "lon")
    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        np.array([[[1.0, 0.5, 0.1, 0.0]], [[2.0, 1.0, 0.2, 0.0]]])
        * annual_kg
        / (365.2422 * 86400),
        dims=["time", "lat", "lon"],
        coords=coords,
    )
    xr.testing.assert_allclose(result.compute(), expected, rtol=1e-12, atol=0)
