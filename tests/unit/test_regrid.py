from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from access_moppy.regrid import (
    RegridConfig,
    RegridError,
    apply_optional_regridding,
    build_target_grid,
    select_regrid_method,
)


def test_select_regrid_method_auto_rules():
    assert (
        select_regrid_method(
            "Amon.pr", {"standard_name": "precipitation_flux"}, {"enabled": True}
        )
        == "conservative"
    )
    assert (
        select_regrid_method(
            "Omon.tos",
            {"standard_name": "sea_surface_temperature", "units": "K"},
            {"enabled": True},
        )
        == "bilinear"
    )
    assert (
        select_regrid_method("fx.sftlf", {"units": "%"}, {"enabled": True})
        == "nearest_s2d"
    )
    assert (
        select_regrid_method("SImon.siconc", {"units": "%"}, {"enabled": True})
        == "conservative"
    )


@pytest.mark.parametrize("name", ["uo", "vo", "tauu"])
def test_select_regrid_method_refuses_vector_fields(name):
    with pytest.raises(RegridError, match="vector"):
        select_regrid_method(name, {}, {"enabled": True, "method": "auto"})


def test_explicit_variable_method_override():
    cfg = {
        "enabled": True,
        "method": "auto",
        "variable_methods": {"tos": "nearest_s2d"},
    }
    assert select_regrid_method("Omon.tos", {"units": "K"}, cfg) == "nearest_s2d"


def test_target_grid_has_cmip_style_bounds():
    target = build_target_grid("cmip7-1x1")
    assert target.sizes["lat"] == 180
    assert target.sizes["lon"] == 360
    assert target["lat"].attrs["bounds"] == "lat_bnds"
    assert target["lon"].attrs["bounds"] == "lon_bnds"


def test_apply_cached_weights_updates_metadata(tmp_path: Path):
    ds = xr.Dataset(
        data_vars={
            "tas": (("time", "lat", "lon"), np.arange(4, dtype=float).reshape(1, 2, 2)),
        },
        coords={
            "time": [0],
            "lat": ("lat", [-0.5, 0.5]),
            "lon": ("lon", [0.5, 1.5]),
        },
        attrs={"source_id": "TEST", "grid_label": "gn"},
    )
    ds["tas"].attrs.update({"units": "K", "cell_measures": "area: areacella"})

    weights_path = tmp_path / "weights.nc"
    xr.Dataset(
        {
            "row": ("n_s", np.array([1, 2, 3, 4])),
            "col": ("n_s", np.array([1, 2, 3, 4])),
            "S": ("n_s", np.ones(4)),
        }
    ).to_netcdf(weights_path)

    out = apply_optional_regridding(
        ds,
        "tas",
        RegridConfig.from_config(
            {
                "enabled": True,
                "target_grid": "cmip7-1x1",
                "method": "bilinear",
                "weights": {"path": str(weights_path), "mode": "reuse"},
            }
        ),
    )

    assert out.attrs["grid_label"] == "gr"
    assert out["tas"].dims == ("time", "lat", "lon")
    assert out["tas"].shape == (1, 180, 360)
    np.testing.assert_array_equal(
        out["tas"].isel(time=0).values.ravel()[:4], np.arange(4)
    )
    assert "lat_bnds" in out
    assert "lon_bnds" in out
    assert out["tas"].attrs["coordinates"] == "time lat lon"
    assert "cell_measures" not in out["tas"].attrs


def test_conservative_generation_requires_bounds(tmp_path: Path):
    ds = xr.Dataset(
        {
            "pr": (
                ("lat", "lon"),
                np.ones((2, 2)),
                {"standard_name": "precipitation_flux"},
            )
        },
        coords={"lat": [-0.5, 0.5], "lon": [0.5, 1.5]},
    )
    with pytest.raises(RegridError, match="requires source cell bounds"):
        apply_optional_regridding(
            ds,
            "pr",
            {
                "enabled": True,
                "target_grid": "cmip7-1x1",
                "method": "conservative",
                "weights": {"mode": "create", "cache_dir": str(tmp_path)},
            },
        )
