"""Numerical mapping regressions for the five land means in issue #808."""

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
    ("name", "fields", "land_values", "units"),
    [
        ("cSoil", {858: 1, 859: 2, 860: 4}, [0.357, 0.714, 1.071], "kg m-2"),
        (
            "cVeg",
            {851: 1, 852: 2, 853: 4, 854: 8},
            [0.765, 1.53, 2.295],
            "kg m-2",
        ),
        ("lai", {893: 1}, [51.0, 102.0, 153.0], "1"),
        (
            "fBNF",
            {885: 1},
            np.array([51.0, 102.0, 153.0]) / (1000 * 86400),
            "kg m-2 s-1",
        ),
        ("snc", {}, [70.0, 0.0, 100.0], "%"),
    ],
)
def test_baseline_land_means_keep_coastal_values_and_mask_ocean(
    model_id, mip_era, tile_dim, lazy, name, fields, land_values, units
):
    """Unequal tiles retain their weights, conversions and land-area denominator."""
    realm = "landIce" if name == "snc" else "land"
    table = {"snc": "LImon", "cSoil": "Emon", "fBNF": "Emon"}.get(name, "Lmon")
    request = (
        f"{table}.{name}"
        if mip_era == "CMIP6"
        else f"{realm}.{name}.tavg-u-hxy-lnd.mon.glb"
    )
    mapping = load_model_mappings(request, model_id)[name]
    assert mapping["units"] == units
    coords = {
        "time": np.array(
            ["1952-06-15", "1952-07-15", "1952-08-15"], dtype="datetime64[ns]"
        ),
        "lat": [-31.25],
        "lon": [0.0, 1.875, 3.75, 5.625, 7.5, 9.375],
    }

    def tiles(values):
        array = np.broadcast_to(
            np.asarray(values, dtype=float)[:, :, None, None], (3, 3, 1, 6)
        ).copy()
        # Exercise zero, missing and finite native ocean data. The land mask
        # must be applied even when the tile calculation returns a finite value.
        array[:, :, :, 3] = 0
        array[:, :, :, 4] = np.nan
        result = xr.DataArray(
            array,
            dims=["time", tile_dim, "lat", "lon"],
            coords={**coords, tile_dim: [1, 2, 3]},
        )
        return result.chunk({"time": 1, tile_dim: 2, "lon": 2}) if lazy else result

    raw = tiles([[10, 30, 80], [20, 60, 160], [30, 90, 240]])
    landfrac = xr.DataArray(
        [[1.0, 0.5, 0.1, 0.0, 0.0, 0.0]],
        dims=["lat", "lon"],
        coords={key: coords[key] for key in ("lat", "lon")},
    )
    context = {f"fld_s03i{stash}": raw * factor for stash, factor in fields.items()}
    context["fld_s03i317"] = tiles([[0.2, 0.3, 0.5]] * 3)
    context["fld_s03i395"] = landfrac.chunk({"lon": 2}) if lazy else landfrac
    if name == "snc":
        # Positive snow amount is a presence test: retain partial cover,
        # snow-free land and complete cover, independently of snow depth.
        context["fld_s08i236"] = tiles([[1, 0, 20], [0, 0, 0], [1, 2, 3]])
    assert set(mapping["model_variables"]) == set(context)

    result = evaluate_expression(mapping["calculation"], context)

    assert result.dims == ("time", "lat", "lon")
    assert (result.chunks is not None) == lazy
    # CMIP6 LImon.snc is explicitly a whole-cell percentage. Only the
    # CMIP7 monthly product changes its averaging domain to a land mean.
    whole_cell = name == "snc" and mip_era == "CMIP6"
    spatial = (
        [1, 0.5, 0.1, 0, 0, 0] if whole_cell else [1, 1, 1, np.nan, np.nan, np.nan]
    )
    expected = xr.DataArray(
        np.asarray(land_values)[:, None, None] * np.array(spatial)[None, None, :],
        dims=["time", "lat", "lon"],
        coords=coords,
    )
    actual = result.compute()
    xr.testing.assert_allclose(actual, expected, rtol=1e-12, atol=0)
    if whole_cell:
        assert bool((actual.isel(lon=slice(3, None)) == 0).all())
    else:
        assert bool(actual.isel(lon=slice(3, None)).isnull().all())

    # Integrate over land area, never over the full atmospheric cell area.
    area = xr.DataArray([2, 3, 5, 7, 11, 13], dims="lon", coords={"lon": coords["lon"]})
    integral = (actual * area * (1 if whole_cell else landfrac)).sum(["lat", "lon"])
    np.testing.assert_allclose(
        integral, np.asarray(land_values) * 4, rtol=1e-12, atol=0
    )


@pytest.mark.unit
@pytest.mark.parametrize("model_id", ["ACCESS-ESM1-5", "ACCESS-ESM1-6"])
def test_snc_override_selects_only_cmip7_monthly_land_mean(model_id):
    """Legacy request syntax must still select the output vocabulary's domain."""
    original = load_model_mappings("LImon.snc", model_id)
    branded = load_model_mappings("landIce.snc.tavg-u-hxy-lnd.mon.glb", model_id)
    explicit = load_model_mappings("LImon.snc", model_id, cmip_version="CMIP7")
    assert branded == explicit
    assert original["snc"]["calculation"]["operation"] == "calc_snc"
    assert explicit["snc"]["calculation"]["operation"] == "mask_land"
    # Loading a version override must not leak into subsequent requests.
    assert load_model_mappings("LImon.snc", model_id) == original
    assert load_model_mappings("LImon.snc", model_id, cmip_version="CMIP6") == original
    for request in ("day.snc", "ImonAnt.snc", "ImonGre.snc"):
        other = load_model_mappings(request, model_id, cmip_version="CMIP7")["snc"]
        assert other["calculation"] == original["snc"]["calculation"]
        assert "calculation_overrides" not in other
