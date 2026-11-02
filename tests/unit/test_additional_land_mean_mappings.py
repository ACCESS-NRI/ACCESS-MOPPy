"""Land-area normalization of the 19 additional recipes found in #808."""

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
    ("name", "fields", "uses_tiles", "land_value"),
    [
        ("cLand", list(range(851, 861)) + [898, 899, 900], True, 20.445),
        ("cLeaf", [852], True, 0.102),
        ("cLitter", [855, 856, 857], True, 0.918),
        ("cProduct", [898, 899, 900], False, 17.64),
        ("cRoot", [854], True, 0.204),
        ("nLand", list(range(861, 871)) + [901, 902, 903], True, 26.625),
        ("nLitter", [864, 865, 866], True, 2.295),
        ("nMineral", [870], True, 1.02),
        ("nProduct", [901, 902, 903], False, 18.72),
        ("nSoil", [867, 868, 869, 870], True, 3.774),
        ("nVeg", [861, 862, 863], True, 1.836),
        ("fNdep", [884], True, 1734 / (1000 * 86400)),
        ("fNgas", [920], True, 3570 / (1000 * 86400)),
        ("fNleach", [918], True, 3468 / (1000 * 86400)),
        ("fNloss", [918, 920], True, 7038 / (1000 * 86400)),
        ("fNnetmin", [917], True, 3417 / (1000 * 86400)),
        ("fNup", [919], True, 3519 / (1000 * 86400)),
        ("fDeforestToProduct", [895], False, 5.4 / (365.2422 * 86400)),
        ("fProductDecomp", [907, 908, 909], False, 20.88 / (365.2422 * 86400)),
    ],
)
def test_additional_land_means(
    model_id, mip_era, tile_dim, lazy, name, fields, uses_tiles, land_value
):
    """Keep tile-density weights, sum product contributions, and mask ocean."""
    table = "Lmon" if name in {"cLeaf", "cLitter", "cProduct", "cRoot"} else "Emon"
    request = (
        f"{table}.{name}"
        if mip_era == "CMIP6"
        else f"land.{name}.tavg-u-hxy-lnd.mon.glb"
    )
    mapping = load_model_mappings(request, model_id)[name]
    assert mapping["units"] == ("kg m-2 s-1" if name.startswith("f") else "kg m-2")
    coords = {"time": [0, 1, 2], "lat": [-31.25], "lon": [0, 1, 2, 3, 4, 5]}

    def tiles(values):
        array = np.broadcast_to(
            np.asarray(values)[:, :, None, None], (3, 3, 1, 6)
        ).copy()
        # Zero, missing and finite ocean inputs must all produce a masked cell.
        array[:, :, :, 3] = 0
        array[:, :, :, 4] = np.nan
        result = xr.DataArray(
            array,
            dims=["time", tile_dim, "lat", "lon"],
            coords={**coords, tile_dim: [1, 2, 3]},
        )
        return result.chunk({"time": 1, tile_dim: 2, "lon": 2}) if lazy else result

    # Weighted sum = 51, plain tile sum = 120. Each diagnostic has a distinct
    # coefficient, so swapped fields, double product weighting and bad unit
    # constants are visible in the explicitly specified expected values above.
    raw = tiles([[10.0, 30, 80], [20, 60, 160], [30, 90, 240]])
    fraction = xr.DataArray(
        [[1.0, 0.5, 0.1, 0, 0, 0]],
        dims=["lat", "lon"],
        coords={key: coords[key] for key in ("lat", "lon")},
    )
    if lazy:
        fraction = fraction.chunk({"lon": 2})
    context = {f"fld_s03i{n}": raw * (n - 850) for n in fields}
    context["fld_s03i395"] = fraction
    required = set(context) | ({"fld_s03i317"} if uses_tiles else set())
    assert set(mapping["model_variables"]) == required
    context["fld_s03i317"] = tiles([[0.2, 0.3, 0.5]] * 3)
    if name == "cRoot":
        # The former leaf input is available but must not determine cRoot.
        context["fld_s03i852"] = raw * 999

    result = evaluate_expression(mapping["calculation"], context)
    assert result.dims == ("time", "lat", "lon")
    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        land_value
        * np.array([1, 2, 3])[:, None, None]
        * np.array([1, 1, 1, np.nan, np.nan, np.nan])[None, None, :],
        dims=["time", "lat", "lon"],
        coords=coords,
    )
    actual = result.compute()
    xr.testing.assert_allclose(actual, expected, rtol=1e-12, atol=0)
    assert bool(actual.isel(lon=slice(3, None)).isnull().all())
    area = xr.DataArray([2, 3, 5, 7, 11, 13], dims="lon", coords={"lon": coords["lon"]})
    integral = (actual * area * fraction).sum(["lat", "lon"])
    np.testing.assert_allclose(
        integral, land_value * np.array([1, 2, 3]) * 4, rtol=1e-12, atol=0
    )
