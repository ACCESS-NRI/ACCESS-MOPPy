"""Native geometry contracts inside the existing component CMORisers."""

from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from access_moppy.ocean import Ocean_CMORiser_OM3
from access_moppy.ocean_supergrid import Supergrid
from access_moppy.sea_ice import SeaIce_CMORiser


def _vocab(source="ACCESS-OM3"):
    vocab = Mock()
    vocab.source_id = source
    vocab.variable = {"dimensions": "longitude latitude time"}
    return vocab


def _supergrid():
    sx = np.array([-0.5, 0.0, 0.5, 1.0, 1.5])
    sy = np.array([-75.0, -70.0, -65.0, -60.0, -55.0])
    return xr.Dataset(
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


def _mom_grid():
    return xr.Dataset(
        {
            "geolat": (
                ("yh", "xh"),
                [[-70.0, -70.0], [-60.0, -60.0]],
                {"units": "degrees_north"},
            ),
            "geolon": (
                ("yh", "xh"),
                [[0.0, 1.0], [0.0, 1.0]],
                {"units": "degrees_east"},
            ),
            "wet": (("yh", "xh"), [[1, 1], [1, 0]]),
            "areacello": (("yh", "xh"), [[2.0, 2.0], [2.0, 0.0]], {"units": "m2"}),
        },
        coords={"yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
    )


def _cice_grid():
    corners_lat = np.broadcast_to(np.array([[-71.0, -71.0, -69.0, -69.0]]), (2, 2, 4))
    corners_lon = np.broadcast_to(np.array([[-0.5, 0.5, 0.5, -0.5]]), (2, 2, 4))
    grid = xr.Dataset(
        {
            "TLAT": (
                ("nj", "ni"),
                [[-70.0, -70.0], [-60.0, -60.0]],
                {"units": "degrees_north"},
            ),
            "TLON": (
                ("nj", "ni"),
                [[0.0, 1.0], [0.0, 1.0]],
                {"units": "degrees_east"},
            ),
            "latt_bounds": (
                ("nj", "ni", "nvertices"),
                corners_lat,
                {"units": "degrees_north"},
            ),
            "lont_bounds": (
                ("nj", "ni", "nvertices"),
                corners_lon,
                {"units": "degrees_east"},
            ),
            "tarea": (("nj", "ni"), [[1e4, 2e4], [3e4, 0.0]], {"units": "m^2"}),
            "tmask": (("nj", "ni"), [[1, 1], [1, 0]]),
        }
    )
    return grid


def test_explicit_supergrid_avoids_resolution_lookup_and_has_corners():
    grid = Supergrid(dataset=_supergrid())
    assert grid.supergrid_path is None
    info = grid.extract_grid("T", "C", True)
    assert info["latitude"].shape == (2, 2)
    assert info["vertices_latitude"].shape == (2, 2, 4)
    with pytest.raises(ValueError, match="odd"):
        Supergrid(dataset=_supergrid().isel(nxp=slice(None, -1)))
    actual_units = _supergrid()
    actual_units.x.attrs["units"] = "degrees"
    actual_units.y.attrs["units"] = "degrees"
    assert Supergrid(dataset=actual_units).extract_grid("T", "C", True)[
        "latitude"
    ].shape == (
        2,
        2,
    )
    actual_units.x.attrs["units"] = "metres"
    with pytest.raises(ValueError, match="angular degree"):
        Supergrid(dataset=actual_units)


def test_mom6_merges_only_required_static_operands_with_exact_horizontal_grid(tmp_path):
    native = xr.Dataset(
        {"thkcello": (("time", "yh", "xh"), np.ones((2, 2, 2)))},
        coords={"time": [0, 1], "yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
    )
    grid = _mom_grid()
    grid["unrelated_diagnostic"] = (("time", "yh", "xh"), np.zeros((1, 2, 2)))
    grid = grid.assign_coords(time=[99])
    kwargs = dict(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="Omon.volcello",
        vocab=_vocab(),
        variable_mapping={
            "volcello": {
                "model_variables": ["thkcello", "areacello"],
                "calculation": {"type": "formula", "operation": "multiply"},
            }
        },
        supergrid_data=_supergrid(),
    )
    cm = Ocean_CMORiser_OM3(grid_data=grid, **kwargs)
    assert cm.input_dataset.sizes["time"] == 2
    assert "areacello" in cm.input_dataset
    assert "unrelated_diagnostic" not in cm.input_dataset
    with pytest.raises(ValueError, match="exact"):
        Ocean_CMORiser_OM3(grid_data=grid.assign_coords(xh=[2.0, 3.0]), **kwargs)
    timed_area = grid.copy()
    timed_area["areacello"] = grid.areacello.expand_dims(time=[99])
    with pytest.raises(ValueError, match="must not have a time axis"):
        Ocean_CMORiser_OM3(grid_data=timed_area, **kwargs)


def test_mom6_field_is_masked_and_supergrid_identity_checked(tmp_path):
    native = xr.Dataset(
        {"thetao": (("time", "yh", "xh"), np.full((1, 2, 2), 3.0))},
        coords={"time": [0], "yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
    )
    cm = Ocean_CMORiser_OM3(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="Omon.thetao",
        vocab=_vocab(),
        variable_mapping={
            "thetao": {"model_variables": ["thetao"], "calculation": {"type": "direct"}}
        },
        grid_data=_mom_grid(),
        supergrid_data=_supergrid(),
    )
    cm.ds = cm.input_dataset.copy()
    cm._validate_native_grid()
    assert np.isnan(cm.ds.thetao.isel(time=0, yh=1, xh=1))
    assert cm.infer_grid_type() == ("T", True)
    cm.grid_type = "T"
    cm.grid_info = cm.supergrid.extract_grid("T", "C", True)
    cm._validate_native_grid_info()
    bad = _mom_grid()
    bad["geolon"] = bad.geolon + 2.0
    cm.grid_data = bad
    with pytest.raises(ValueError, match="disagrees"):
        cm._validate_native_grid_info()


def test_mom6_depth_bounds_from_native_interfaces(tmp_path):
    native = xr.Dataset(
        {"thetao": (("time", "z_l", "yh", "xh"), np.ones((1, 2, 2, 2)))},
        coords={
            "time": [0],
            "z_l": ("z_l", [50.0, 300.0], {"units": "m"}),
            "z_i": ("z_i", [0.0, 100.0, 500.0], {"units": "m"}),
            "yh": [-70.0, -60.0],
            "xh": [0.0, 1.0],
        },
    )
    cm = Ocean_CMORiser_OM3(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="Omon.thetao",
        vocab=_vocab(),
        variable_mapping={
            "thetao": {"model_variables": ["thetao"], "calculation": {"type": "direct"}}
        },
        grid_data=_mom_grid(),
        supergrid_data=_supergrid(),
    )
    cm.ds = cm.input_dataset.copy()
    cm._add_depth_bounds_from_edges({"lev_bnds": {}}, {"z_l": "lev"})
    np.testing.assert_allclose(cm.ds.lev_bnds.values, [[0.0, 100.0], [100.0, 500.0]])
    cm.ds = cm.ds.drop_vars("lev_bnds")
    with pytest.raises(ValueError, match="requires z_i"):
        cm._add_depth_bounds_from_edges({"lev_bnds": {}}, {"z_l": "lev"})


def test_mom6_monthly_bounds_set_time_midpoints_and_reject_partial_months(tmp_path):
    bounds = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    native = xr.Dataset(
        {
            "thetao": (("time", "yh", "xh"), np.ones((2, 2, 2))),
            "time_bnds": (("time", "bnds"), bounds),
        },
        coords={"time": bounds[:, 1], "yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
    )
    native.time.attrs["bounds"] = "time_bnds"
    cm = Ocean_CMORiser_OM3(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="Omon.thetao",
        vocab=_vocab(),
        variable_mapping={
            "thetao": {"model_variables": ["thetao"], "calculation": {"type": "direct"}}
        },
        grid_data=_mom_grid(),
        supergrid_data=_supergrid(),
    )
    cm.ds = cm.input_dataset.copy()
    cm._normalise_native_time_bounds()
    assert cm.ds.time.dt.month.values.tolist() == [1, 2]
    np.testing.assert_array_equal(cm.ds.time_bnds, bounds)
    cm.ds["time_bnds"][0, 0] = np.datetime64("2000-01-02")
    with pytest.raises(ValueError, match="complete calendar months"):
        cm._normalise_native_time_bounds()


def test_cice_native_corners_mask_and_month_midpoints(tmp_path):
    bounds = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    native = xr.Dataset(
        {
            "aice_m": (("time", "nj", "ni"), np.full((2, 2, 2), 0.5), {"units": "1"}),
            "time_bounds": (("time", "d2"), bounds),
        },
        coords={"time": bounds[:, 1]},
    )
    native.time.attrs["bounds"] = "time_bounds"
    grid = _cice_grid()
    cm = SeaIce_CMORiser(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="SImon.siconc",
        vocab=_vocab(),
        variable_mapping={
            "siconc": {"model_variables": ["aice_m"], "calculation": {"type": "direct"}}
        },
        grid_data=grid,
        grid_identity_verified=True,
        grid_reference="static-grid@sha256:test",
    )
    cm.ds = cm.input_dataset.copy()
    cm.ds["siconc"] = cm.ds.aice_m * 100.0
    cm._validate_native_grid()
    assert np.isnan(cm.ds.siconc.isel(time=0, nj=1, ni=1))
    info = cm._native_grid_info("T")
    assert info["vertices_longitude"].shape == (2, 2, 4)
    cm._normalise_time_bounds()
    assert cm.ds.time.dt.month.values.tolist() == [1, 2]
    assert cm.ds.time.attrs["bounds"] == "time_bnds"
    cm.ds["time_bnds"][0, 0] = np.datetime64("2000-01-02")
    with pytest.raises(ValueError, match="complete calendar months"):
        cm._normalise_time_bounds()


def test_cice_missing_grid_identity_evidence_rejected(tmp_path):
    native = xr.Dataset(
        {"aice_m": (("time", "nj", "ni"), np.full((1, 2, 2), 0.5))},
        coords={"time": [0]},
    )
    cm = SeaIce_CMORiser(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="SImon.siconc",
        vocab=_vocab(),
        variable_mapping={
            "siconc": {"model_variables": ["aice_m"], "calculation": {"type": "direct"}}
        },
        grid_data=_cice_grid(),
    )
    cm.ds = cm.input_dataset.copy()
    cm.ds["siconc"] = cm.ds.aice_m * 100.0
    with pytest.raises(ValueError, match="grid_reference"):
        cm._validate_native_grid()


def test_cice_unrelated_static_ncat_does_not_conflict_with_native(tmp_path):
    native = xr.Dataset(
        {
            "aice_m": (("time", "nj", "ni"), np.full((1, 2, 2), 0.5)),
            "NCAT": ((), 5),
        },
        coords={"time": [0]},
    )
    grid = _cice_grid()
    grid["NCAT"] = xr.DataArray(6)
    cm = SeaIce_CMORiser(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="SImon.siconc",
        vocab=_vocab(),
        variable_mapping={
            "siconc": {"model_variables": ["aice_m"], "calculation": {"type": "direct"}}
        },
        grid_data=grid,
        grid_identity_verified=True,
        grid_reference="static-grid@sha256:test",
    )
    assert cm.input_dataset.NCAT.item() == 5


def test_cice_area_validation_and_unrelated_latlon_coords(tmp_path):
    native = xr.Dataset(
        {
            "aice_m": (("time", "nj", "ni"), np.ones((1, 2, 2))),
            "ULAT": (("nj", "ni"), np.full((2, 2), -60.0), {"bounds": "latu_bounds"}),
            "ULON": (("nj", "ni"), np.full((2, 2), 30.0)),
            "latu_bounds": (("nj", "ni", "nvertices"), np.zeros((2, 2, 4))),
        },
        coords={"time": [0]},
    ).set_coords(["ULAT", "ULON"])
    grid = _cice_grid()
    cm = SeaIce_CMORiser(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="Ofx.areacello",
        vocab=_vocab(),
        variable_mapping={
            "areacello": {
                "model_variables": ["tarea"],
                "calculation": {"type": "direct"},
            }
        },
        grid_data=grid,
        grid_identity_verified=True,
        grid_reference="static-grid@sha256:test",
    )
    cm.ds = cm.input_dataset.copy()
    cm.ds["areacello"] = cm.ds.tarea
    cm._validate_native_grid()
    cm.grid_type = "T"
    cm.ds["latitude"] = xr.DataArray(np.zeros((2, 2)), dims=("nj", "ni"))
    cm._discard_staggered_native_coords()
    assert "ULAT" not in cm.ds and "ULON" not in cm.ds
    assert "latu_bounds" not in cm.ds and "latitude" in cm.ds
    cm.ds["areacello"] = cm.ds.areacello * 2
    with pytest.raises(ValueError, match="convert native tarea"):
        cm._validate_native_grid()


def test_cice_inactive_default_fill_geometry_is_nan_but_valid_land_is_kept(tmp_path):
    native = xr.Dataset(
        {"aice_m": (("time", "nj", "ni"), np.ones((1, 2, 2)))},
        coords={"time": [0]},
    )
    grid = _cice_grid()
    grid["tmask"] = xr.DataArray(
        [[1.0, 0.0], [1.0, np.nan]],
        dims=("nj", "ni"),
        attrs={"units": "1"},
    )
    grid["tmask"].encoding["_FillValue"] = 1e30
    for name, fill in (
        ("TLAT", 1e30),
        ("TLON", 1e30),
        ("latt_bounds", 9.969209968386869e36),
        ("lont_bounds", 9.969209968386869e36),
    ):
        values = grid[name].values.copy()
        values[1, 1] = fill
        grid[name] = xr.DataArray(
            values, dims=grid[name].dims, attrs=dict(grid[name].attrs)
        )
    cm = SeaIce_CMORiser(
        input_data=native,
        output_path=str(tmp_path),
        compound_name="SImon.siconc",
        vocab=_vocab(),
        variable_mapping={
            "siconc": {"model_variables": ["aice_m"], "calculation": {"type": "direct"}}
        },
        grid_data=grid,
        grid_identity_verified=True,
        grid_reference="static-grid@sha256:test",
    )
    cm.ds = cm.input_dataset.copy()
    cm.ds["siconc"] = cm.ds.aice_m
    cm._validate_native_grid()
    info = cm._native_grid_info("T")
    assert np.isnan(info["latitude"].values[1, 1])
    assert np.isnan(info["longitude"].values[1, 1])
    assert np.isnan(info["vertices_latitude"].values[1, 1]).all()
    assert np.isnan(info["vertices_longitude"].values[1, 1]).all()
    assert np.isfinite(info["latitude"].values[0, 1])
    assert np.isfinite(info["vertices_latitude"].values[0, 1]).all()
    assert grid["latt_bounds"].values[1, 1, 0] > 1e30
    grid["latt_bounds"].values[0, 0, 0] = 9.969209968386869e36
    with pytest.raises(ValueError, match="wet TLAT corners"):
        cm._native_grid_info("T")


@pytest.mark.parametrize("component", ["mom6", "cice"])
@pytest.mark.parametrize("decoded", [False, True])
def test_declared_missing_native_mask_is_not_wet(tmp_path, component, decoded):
    if component == "mom6":
        grid, mask_name, marker = _mom_grid(), "wet", 1e20
        native = xr.Dataset(
            {"thetao": (("time", "yh", "xh"), np.ones((1, 2, 2)))},
            coords={"time": [0], "yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
        )
        klass, name = Ocean_CMORiser_OM3, "thetao"
        extras = {"supergrid_data": _supergrid()}
    else:
        grid, mask_name, marker = _cice_grid(), "tmask", 1e30
        native = xr.Dataset(
            {"aice_m": (("time", "nj", "ni"), np.ones((1, 2, 2)))},
            coords={"time": [0]},
        )
        klass, name = SeaIce_CMORiser, "siconc"
        extras = {
            "grid_identity_verified": True,
            "grid_reference": "static-grid@sha256:test",
        }
    mask = grid[mask_name].astype(float).copy()
    mask.values[-1, -1] = np.nan if decoded else marker
    if decoded:
        mask.encoding["_FillValue"] = marker
    else:
        mask.attrs["missing_value"] = marker
    grid[mask_name] = mask
    model_name = "thetao" if component == "mom6" else "aice_m"

    def cmoriser(active_grid):
        cm = klass(
            input_data=native,
            output_path=str(tmp_path),
            compound_name=f"{'Omon' if component == 'mom6' else 'SImon'}.{name}",
            vocab=_vocab(),
            variable_mapping={
                name: {
                    "model_variables": [model_name],
                    "calculation": {"type": "direct"},
                }
            },
            grid_data=active_grid,
            **extras,
        )
        cm.ds = cm.input_dataset.copy()
        cm.ds[name] = cm.ds[model_name]
        return cm

    cm = cmoriser(grid)
    cm._validate_native_grid()
    assert np.isnan(cm.ds[name].isel(time=0)[-1, -1])
    bad = grid.copy()
    bad_mask = bad[mask_name].copy()
    bad_mask.values[-1, -1] = 2
    bad[mask_name] = bad_mask
    with pytest.raises(ValueError, match="zero/one"):
        cmoriser(bad)._validate_native_grid()
    if decoded:
        unexplained = grid.copy()
        unexplained[mask_name].encoding.clear()
        with pytest.raises(ValueError, match="zero/one|undeclared"):
            cmoriser(unexplained)._validate_native_grid()
