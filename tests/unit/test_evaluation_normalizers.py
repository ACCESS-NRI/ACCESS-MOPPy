"""Synthetic contracts for bounded native evaluation normalizers."""

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from access_moppy.evaluation.bgc import derive_fgco2, derive_pht_estimate
from access_moppy.evaluation.cice import normalize_cice
from access_moppy.evaluation.mom6 import complete_grid_from_supergrid, normalize_mom6


def _mom6(name="thetao"):
    bounds = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    axes = {
        "yh": [-70.0, -60.0],
        "xh": [0.0, 1.0],
        "yq": [-75.0, -65.0, -55.0],
        "xq": [-0.5, 0.5, 1.5],
    }
    grid_vars = {}
    for dims, suffix, wet, area in (
        (("yh", "xh"), "", "wet", "areacello"),
        (("yh", "xq"), "_u", "wet_u", "areacello_cu"),
        (("yq", "xh"), "_v", "wet_v", "areacello_cv"),
    ):
        shape = tuple(len(axes[d]) for d in dims)
        grid_vars["geolat" + suffix] = (
            dims,
            np.broadcast_to(np.array(axes[dims[0]])[:, None], shape),
            {"units": "degrees_north"},
        )
        grid_vars["geolon" + suffix] = (
            dims,
            np.broadcast_to(np.array(axes[dims[1]])[None, :], shape),
            {"units": "degrees_east"},
        )
        mask = np.ones(shape)
        mask[-1, -1] = 0
        grid_vars[wet] = (dims, mask)
        grid_vars[area] = (dims, np.full(shape, 2.0), {"units": "m2"})
    grid = xr.Dataset(grid_vars, coords={k: (k, v) for k, v in axes.items()})
    dims = {"uo": ("yh", "xq"), "vo": ("yq", "xh"), "tauuo": ("yh", "xq")}.get(
        name, ("yh", "xh")
    )
    three_d = name in {"thetao", "so", "uo", "vo", "thkcello"}
    field_dims = ("time", "z_l", *dims) if three_d else ("time", *dims)
    shape = (
        (2, 2, *[len(axes[d]) for d in dims])
        if three_d
        else (2, *[len(axes[d]) for d in dims])
    )
    values = np.full(shape, 3.0)
    if name == "thkcello":
        values[:, 0] = 10.0
        values[:, 1] = 20.0
    units = {
        "thetao": "degC",
        "so": "psu",
        "uo": "m s-1",
        "vo": "m s-1",
        "tauuo": "N m-2",
        "hfds": "W m-2",
        "thkcello": "m",
    }
    native = xr.Dataset(
        {
            name: (
                field_dims,
                da.from_array(values, chunks=(1,) * len(shape)),
                {"units": units[name]},
            ),
            "time_bnds": (("time", "nv"), bounds),
        },
        coords={
            "time": bounds[:, 1],
            "z_l": ("z_l", [50.0, 300.0], {"units": "m"}),
            "z_i": ("z_i", [0.0, 100.0, 500.0], {"units": "m"}),
            **{k: (k, v) for k, v in axes.items() if k in dims},
        },
    )
    native.time.attrs["bounds"] = "time_bnds"
    return native, grid


@pytest.mark.parametrize(
    "name,component",
    [
        ("thetao", "T"),
        ("so", "T"),
        ("uo", "Cu"),
        ("vo", "Cv"),
        ("tauuo", "Cu"),
        ("hfds", "T"),
    ],
)
def test_mom6_geometry_mask_and_lazy_values(name, component):
    native, grid = _mom6(name)
    result = normalize_mom6(native, grid, {"native_grid": component}, name)
    assert result.attrs["native_grid_component"] == component
    assert isinstance(result[name].data, da.Array)
    assert np.isnan(
        result[name]
        .isel({result[name].dims[-2]: -1, result[name].dims[-1]: -1})
        .compute()
    ).all()
    assert result.time.dt.month.values.tolist() == [1, 2]
    if name in {"thetao", "so", "uo", "vo"}:
        np.testing.assert_allclose(result.lev_bnds.values, [[0, 100], [100, 500]])


def test_mom6_area_volume_depth_and_rejections():
    native, grid = _mom6("thkcello")
    area = normalize_mom6(native, grid, {"native_grid": "T"}, "areacello")
    assert area.areacello.dims == ("yh", "xh")
    volume = normalize_mom6(native, grid, {"native_grid": "T"}, "volcello")
    assert isinstance(volume.volcello.data, da.Array)
    np.testing.assert_allclose(
        volume.volcello.isel(time=0, lev=0).compute().values, [[20, 20], [20, np.nan]]
    )
    assert float(volume.volcello.isel(time=0).sum().compute()) == 180.0
    limited = normalize_mom6(
        native,
        grid,
        {"native_grid": "T", "depth_selection": {"maximum": 200}},
        "volcello",
    )
    np.testing.assert_allclose(limited.lev_bnds.values, [[0, 100]])
    with pytest.raises(ValueError, match="Declared grid"):
        normalize_mom6(native, grid, {}, "volcello")
    with pytest.raises(ValueError, match="time bounds"):
        normalize_mom6(
            native.drop_vars("time_bnds"), grid, {"native_grid": "T"}, "volcello"
        )
    with pytest.raises(ValueError, match="size mismatch"):
        normalize_mom6(
            native, grid.isel(xh=slice(0, 1)), {"native_grid": "T"}, "volcello"
        )
    bad = grid.copy()
    bad["geolat"] = bad.geolat.where(bad.wet > 0)
    with pytest.raises(ValueError, match="finite positions"):
        normalize_mom6(native, bad, {"native_grid": "T"}, "volcello")


def test_supergrid_reconstructs_dry_positions():
    _, grid = _mom6("uo")
    sy = np.array([-75.0, -70.0, -65.0, -60.0, -55.0])
    sx = np.array([-0.5, 0.0, 0.5, 1.0, 1.5])
    supergrid = xr.Dataset(
        {
            "x": (
                ("nyp", "nxp"),
                np.broadcast_to(sx[None, :], (5, 5)),
                {"units": "degrees_east"},
            ),
            "y": (
                ("nyp", "nxp"),
                np.broadcast_to(sy[:, None], (5, 5)),
                {"units": "degrees_north"},
            ),
        }
    )
    for suffix, wet in (("", "wet"), ("_u", "wet_u"), ("_v", "wet_v")):
        for stem in ("geolat", "geolon"):
            grid[stem + suffix] = grid[stem + suffix].where(grid[wet] > 0)
    complete = complete_grid_from_supergrid(grid, supergrid)
    assert np.isfinite(complete.geolon_u).all()
    np.testing.assert_allclose(
        complete.geolon_u.values, np.broadcast_to(sx[None, ::2], (2, 3))
    )


def test_mom6_rejects_noncontiguous_time_invalid_depth_and_fill():
    native, grid = _mom6("thetao")
    spec = {"native_grid": "T"}
    gap = native.copy()
    shifted = gap.time_bnds.values.copy()
    shifted[1] += np.timedelta64(1, "D")
    gap["time_bnds"] = (("time", "nv"), shifted)
    with pytest.raises(ValueError, match="contiguous"):
        normalize_mom6(gap, grid, spec, "thetao")
    bad_depth = native.assign_coords(z_i=("z_i", [0.0, 101.0, 100.0]))
    bad_depth.z_i.attrs["units"] = "m"
    with pytest.raises(ValueError, match="depth centers and bounds"):
        normalize_mom6(bad_depth, grid, spec, "thetao")
    filled = native.copy()
    values = native.thetao.data.map_blocks(
        lambda block: np.where(block == 3, 1e20, block)
    )
    filled["thetao"] = xr.DataArray(
        values,
        dims=native.thetao.dims,
        coords=native.thetao.coords,
        attrs={"units": "degC", "_FillValue": 1e20},
    )
    output = normalize_mom6(filled, grid, spec, "thetao")
    assert isinstance(output.thetao.data, da.Array)
    assert np.isnan(output.thetao.isel(time=0, lev=0, yh=0, xh=0).compute())


def _cice():
    bounds = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    native = xr.Dataset(
        {
            "aice_m": (
                ("time", "nj", "ni"),
                da.from_array(
                    np.array([[[0.1, 0.2], [0.3, 0.9]], [[0.2, 0.3], [0.4, 0.8]]]),
                    chunks=(1, 2, 2),
                ),
                {"units": "1"},
            ),
            "time_bounds": (("time", "nv"), bounds),
        },
        coords={"time": bounds[:, 1]},
    )
    native.time.attrs["bounds"] = "time_bounds"
    grid = xr.Dataset(
        {
            "tarea": (("nj", "ni"), [[1e4, 2e4], [3e4, 0]], {"units": "cm2"}),
            "TLAT": (("nj", "ni"), [[-70, -80], [70, 80]], {"units": "degrees_north"}),
            "TLON": (("nj", "ni"), [[0, 10], [0, 10]], {"units": "degrees_east"}),
            "tmask": (("nj", "ni"), [[1, 1], [1, 0]]),
        }
    )
    native = native.assign_coords(TLAT=grid.TLAT, TLON=grid.TLON)
    return native, grid


def test_cice_conversion_area_laziness_and_rejections():
    native, grid = _cice()
    spec = {"native_grid": "T"}
    result = normalize_cice(native, grid, spec)
    assert isinstance(result.siconc.data, da.Array)
    np.testing.assert_allclose(result.siconc.isel(nj=0, ni=0).compute(), [10, 20])
    np.testing.assert_allclose(result.areacello.isel(nj=0).compute(), [1, 2])
    assert np.isnan(result.siconc.isel(nj=1, ni=1).compute()).all()
    assert result.time.dt.month.values.tolist() == [1, 2]
    assert "siconc" not in normalize_cice(xr.Dataset(), grid, spec, "areacello")
    with pytest.raises(ValueError, match="native_grid"):
        normalize_cice(native, grid, {})
    with pytest.raises(ValueError, match="units disagrees"):
        normalize_cice(native, grid, {**spec, "concentration_units": "%"})
    with pytest.raises(ValueError, match="size mismatch"):
        normalize_cice(native, grid.isel(ni=slice(0, 1)), spec)
    with pytest.raises(ValueError, match="time bounds"):
        normalize_cice(native.drop_vars("time_bounds"), grid, spec)


def test_cice_missing_coordinates_require_explicit_grid_audit():
    native, grid = _cice()
    native = native.drop_vars(["TLAT", "TLON"])
    with pytest.raises(ValueError, match="grid_identity_verified"):
        normalize_cice(native, grid, {"native_grid": "T"})
    result = normalize_cice(
        native,
        grid,
        {
            "native_grid": "T",
            "grid_identity_verified": True,
            "grid_reference": "verified-run-grid-checksum",
        },
    )
    assert result.attrs["grid_reference"] == "verified-run-grid-checksum"
    wrong = native.assign_coords(TLAT=grid.TLAT + 1, TLON=grid.TLON)
    with pytest.raises(ValueError, match="latitude mismatch"):
        normalize_cice(
            wrong,
            grid,
            {
                "native_grid": "T",
                "grid_identity_verified": True,
                "grid_reference": "cannot-override-coordinate-mismatch",
            },
        )


def test_cice_masked_land_coordinates_and_bounds_are_valid():
    native, grid = _cice()
    for name in ("TLAT", "TLON"):
        grid[name] = grid[name].where(grid.tmask > 0)
        grid[name].attrs["bounds"] = name + "_bounds"
        grid[name + "_bounds"] = grid[name].expand_dims(vertices=4, axis=-1)
        native = native.assign_coords({name: grid[name]})
    out = normalize_cice(native, grid, {"native_grid": "T"})
    assert np.isnan(out.latitude[-1, -1])
    assert np.isnan(out.vertices_latitude[-1, -1]).all()
    invalid = grid.copy()
    invalid.TLAT.values[0, 0] = np.nan
    with pytest.raises(ValueError, match="finite wet"):
        normalize_cice(native, invalid, {"native_grid": "T"})


@pytest.mark.parametrize("component", ["mom6", "cice"])
def test_cf_masked_static_grid_cells_stay_excluded(component):
    if component == "mom6":
        native, grid = _mom6("thetao")
        name, field, normalizer = "wet", "thetao", normalize_mom6
    else:
        native, grid = _cice()
        name, field, normalizer = "tmask", "siconc", normalize_cice
    grid[name] = grid[name].where(grid[name] > 0)
    with pytest.raises(ValueError, match="declared CF missing"):
        normalizer(native, grid, {"native_grid": "T"}, field)
    grid[name].encoding["_FillValue"] = 1e30
    out = normalizer(native, grid, {"native_grid": "T"}, field)
    assert np.isnan(
        out[field].isel({out[field].dims[-2]: -1, out[field].dims[-1]: -1}).compute()
    ).all()


def test_bgc_flux_is_run_audited_and_lazy():
    shape = (2, 2, 2)
    native = xr.Dataset(
        {
            name: (
                ("time", "yh", "xh"),
                da.full(shape, value, chunks=(1, 2, 2)),
                {"units": units},
            )
            for name, value, units in (
                ("dic_stf", 2.2e-5, "mole/m^2/sec"),
                ("salt_flux_added", 0.000346, "kg m-2 s-1"),
                ("friver", 0.002, "kg m-2 s-1"),
            )
        }
    )
    spec = {
        "parameters": {
            "dic_global": 0.0019,
            "sal_global": 34.6,
            "runoff_dic_mol_kg": 0.001,
        },
        "dic_surface_components_verified": True,
        "run_id": "real-run",
        "run_config_reference": "namelist@sha256:abc",
    }
    result = derive_fgco2(native, spec)
    assert isinstance(result.data, da.Array)
    np.testing.assert_allclose(result.compute(), 1.2011e-8, rtol=1e-12)
    with pytest.raises(ValueError, match="runoff_dic"):
        derive_fgco2(
            native, {**spec, "parameters": {"dic_global": 0.0019, "sal_global": 34.6}}
        )
    with pytest.raises(ValueError, match="run_config_reference"):
        derive_fgco2(native, {**spec, "run_config_reference": ""})
    with pytest.raises(ValueError, match="Verify"):
        derive_fgco2(native, {**spec, "dic_surface_components_verified": False})


def test_pht_requires_explicit_estimate_opt_in():
    native, _ = _mom6()
    native = native.rename({"thetao": "htotal", "z_l": "zl"})
    native.htotal.data = da.full(
        native.htotal.shape, 1e-8, chunks=native.htotal.data.chunks
    )
    native.htotal.attrs["units"] = "mol/kg"
    spec = {
        "co2_calc": "mocsy",
        "allow_pht_estimate": True,
        "run_id": "real-run",
        "run_config_reference": "namelist@sha256:abc",
    }
    result = derive_pht_estimate(native, spec)
    assert isinstance(result.data, da.Array)
    np.testing.assert_allclose(result.compute(), 8.0)
    assert "estimate" in result.attrs["derivation_status"]
    with pytest.raises(ValueError, match="allow_pht_estimate"):
        derive_pht_estimate(native, {**spec, "allow_pht_estimate": False})
    with pytest.raises(ValueError, match="mocsy"):
        derive_pht_estimate(native, {**spec, "co2_calc": "ocmip2"})
