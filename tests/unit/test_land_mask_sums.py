"""Numerical regressions for the land-only frozen soil and transpiration sums."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings


@pytest.mark.unit
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "dask"])
def test_mrfso_requires_complete_soil_columns(lazy):
    # Frozen land, ocean, unfrozen land, missing moisture, missing ice fraction.
    water = np.array([[4, np.nan, 4, 4, 4]] * 6, dtype=np.float32)
    ice = np.array([[0.5, np.nan, 0, 0.5, 0.5]] * 6, dtype=np.float32)
    water[0, 3] = np.nan
    ice[0, 4] = np.nan
    dim = "soil_model_level_number"
    raw = xr.Dataset(
        {
            "fld_s08i223": ((dim, "lon"), water),
            "fld_s08i230": ((dim, "lon"), ice),
        }
    )
    if lazy:
        raw = raw.chunk({dim: 2, "lon": 2})

    mapping = load_model_mappings(
        "landIce.mrfso.tavg-u-hxy-lnd.mon.GLB", "ACCESS-ESM1-6"
    )["mrfso"]
    result = evaluate_expression(mapping["calculation"], raw)

    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        np.array([12, np.nan, 0, np.nan, np.nan], dtype=np.float32), dims="lon"
    )
    xr.testing.assert_identical(result.compute(), expected)


@pytest.mark.unit
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "dask"])
@pytest.mark.parametrize("dim", ["pseudo_level", "pseudo_level_0"])
def test_tran_preserves_land_domain_and_requires_occupied_tiles(lazy, dim, tmp_path):
    # Multiple occupied tiles, missing/zero-weight ocean, true zero on land,
    # missing occupied tile, all occupied tiles missing, and unknown coverage.
    flux = np.array(
        [
            [4, np.nan, 4, 0, 8, np.nan, 8],
            [8, np.nan, np.nan, np.nan, np.nan, 8, 8],
        ],
        dtype=np.float32,
    )
    tilefrac = np.array(
        [
            [0.25, np.nan, 0, 1, 0.75, 1, 0.75],
            [0.75, np.nan, 0, 0, 0.25, 0, np.nan],
        ],
        dtype=np.float32,
    )
    raw = xr.Dataset(
        {"fld_s03i174": ((dim, "lon"), flux), "fld_s03i317": ((dim, "lon"), tilefrac)}
    )
    if lazy:
        raw = raw.chunk({dim: 1, "lon": 2})

    mapping = load_model_mappings("land.tran.tavg-u-hxy-lnd.mon.GLB", "ACCESS-ESM1-6")[
        "tran"
    ]
    result = evaluate_expression(mapping["calculation"], raw)

    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        np.array([7, np.nan, np.nan, 0, np.nan, np.nan, np.nan], dtype=np.float32),
        dims="lon",
    )
    xr.testing.assert_identical(result.compute(), expected)

    path = tmp_path / "tran.nc"
    result.rename("tran").to_netcdf(path, encoding={"tran": {"_FillValue": 1e20}})
    with xr.open_dataset(path) as decoded:
        xr.testing.assert_identical(decoded.tran, expected.rename("tran"))
