"""Real CMOR run, Iris conversion, and NetCDF writing agree for native grids."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.ocean_supergrid import Supergrid

iris = pytest.importorskip("iris")


def _project_cv():
    return {
        "activity_id": {"ACCESS-DEV": "ACCESS model development"},
        "institution_id": {"ACCESS-NRI": "ACCESS-NRI"},
        "experiment_id": {
            "om3-real-run": {
                "experiment": "ACCESS-OM3 development run",
                "description": "An explicitly identified local evaluation run",
                "activity_id": ["ACCESS-DEV"],
                "parent_experiment_id": ["none"],
                "required_model_components": ["OGCM"],
                "sub_experiment_id": ["none"],
            }
        },
        "source_id": {
            "ACCESS-OM3": {
                "label": "ACCESS-OM3 development configuration",
                "institution_id": ["ACCESS-NRI"],
                "model_component": {
                    "ocean": {
                        "description": "MOM6 ocean",
                        "native_nominal_resolution": "25 km",
                    },
                    "seaIce": {
                        "description": "CICE sea ice",
                        "native_nominal_resolution": "25 km",
                    },
                },
            }
        },
    }


def _months():
    return np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )


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


def _mom6_u_inputs():
    supergrid = _supergrid()
    geometry = Supergrid(dataset=supergrid).extract_grid("U", "C", True)
    grid = xr.Dataset(
        {
            "geolat_u": (
                ("yh", "xq"),
                geometry["latitude"].values,
                {"units": "degrees_north"},
            ),
            "geolon_u": (
                ("yh", "xq"),
                geometry["longitude"].values,
                {"units": "degrees_east"},
            ),
            "wet_u": (("yh", "xq"), [[1, 1, 1], [1, 1, 0]]),
        },
        coords={"yh": [-70.0, -60.0], "xq": [-0.5, 0.5, 1.5]},
    )
    bounds = _months()
    native = xr.Dataset(
        {
            "tauuo": (
                ("time", "yh", "xq"),
                np.array(
                    [
                        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
                        [[2.0, 3.0, 4.0], [5.0, 6.0, 7.0]],
                    ]
                ),
                {"units": "N m-2"},
            ),
            "time_bnds": (("time", "bnds"), bounds),
        },
        coords={"time": bounds[:, 1], "yh": [-70.0, -60.0], "xq": [-0.5, 0.5, 1.5]},
    )
    native.time.attrs["bounds"] = "time_bnds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    return native, grid, supergrid


def _mom6_3d_inputs():
    supergrid = _supergrid()
    geometry = Supergrid(dataset=supergrid).extract_grid("T", "C", True)
    grid = xr.Dataset(
        {
            "geolat": (
                ("yh", "xh"),
                geometry["latitude"].values,
                {"units": "degrees_north"},
            ),
            "geolon": (
                ("yh", "xh"),
                geometry["longitude"].values,
                {"units": "degrees_east"},
            ),
            "wet": (("yh", "xh"), [[1, 1], [1, 0]]),
            "areacello": (("yh", "xh"), [[2.0, 2.0], [2.0, 0.0]], {"units": "m2"}),
        },
        coords={"yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
    )
    bounds = _months()
    native = xr.Dataset(
        {
            "thetao": (
                ("time", "z_l", "yh", "xh"),
                np.ones((2, 2, 2, 2)),
                {"units": "degC"},
            ),
            "time_bnds": (("time", "bnds"), bounds),
        },
        coords={
            "time": bounds[:, 1],
            "z_l": ("z_l", [0.5, 1.5], {"units": "m"}),
            "z_i": ("z_i", [0.0, 1.0, 2.0], {"units": "m"}),
            "yh": [-70.0, -60.0],
            "xh": [0.0, 1.0],
        },
    )
    native.time.attrs["bounds"] = "time_bnds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    return native, grid, supergrid


def _cice_inputs():
    corners_lat = np.broadcast_to([[-71.0, -71.0, -69.0, -69.0]], (2, 2, 4))
    corners_lon = np.broadcast_to([[-0.5, 0.5, 0.5, -0.5]], (2, 2, 4))
    grid = xr.Dataset(
        {
            "TLAT": (
                ("nj", "ni"),
                [[-70.0, -70.0], [-60.0, -60.0]],
                {"units": "degrees_north", "bounds": "TLAT_bnds"},
            ),
            "TLON": (
                ("nj", "ni"),
                [[0.0, 1.0], [0.0, 1.0]],
                {"units": "degrees_east", "bounds": "TLON_bnds"},
            ),
            "TLAT_bnds": (("nj", "ni", "nv4"), corners_lat, {"units": "degrees_north"}),
            "TLON_bnds": (("nj", "ni", "nv4"), corners_lon, {"units": "degrees_east"}),
            "tarea": (("nj", "ni"), [[1e4, 2e4], [3e4, 0.0]], {"units": "cm2"}),
            "tmask": (("nj", "ni"), [[1, 1], [1, 0]]),
        }
    )
    bounds = _months()
    native = xr.Dataset(
        {
            "aice_m": (
                ("time", "nj", "ni"),
                np.array([[[0.1, 0.2], [0.3, 0.9]], [[0.2, 0.3], [0.4, 0.8]]]),
                {"units": "1"},
            ),
            "time_bounds": (("time", "d2"), bounds),
        },
        coords={"time": bounds[:, 1]},
    )
    native.time.attrs["bounds"] = "time_bounds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    return native, grid


def _assert_same_cube(before, after, *, area_type=False):
    assert before.var_name == after.var_name
    assert before.units == after.units
    np.testing.assert_array_equal(
        np.ma.getmaskarray(before.data), np.ma.getmaskarray(after.data)
    )
    np.testing.assert_allclose(before.data.compressed(), after.data.compressed())
    for name in ("time", "latitude", "longitude"):
        lhs, rhs = before.coord(name), after.coord(name)
        assert lhs.units == rhs.units
        np.testing.assert_allclose(lhs.points, rhs.points)
        np.testing.assert_allclose(lhs.bounds, rhs.bounds)
    assert before.coord("time").units.calendar == after.coord("time").units.calendar
    for cube in (before, after):
        assert "CMORised using ACCESS-MOPPy" in cube.attributes["cmorisation"]
        assert cube.attributes["experiment_id"] == "om3-real-run"
        assert (
            "not a CMIP publication"
            in cube.attributes["access_moppy_publication_status"]
        )
    if area_type:
        assert (
            before.coord("area_type").points[0]
            == after.coord("area_type").points[0]
            == "sea_ice"
        )
        assert before.coord("area_type").units == after.coord("area_type").units


@pytest.mark.unit
@pytest.mark.parametrize("component", ["mom6_u", "cice"])
def test_real_cmorised_cube_matches_written_netcdf(tmp_path, component):
    pytest.importorskip("ncdata")
    if component == "mom6_u":
        native, grid, supergrid = _mom6_u_inputs()
        compound, options = "Omon.tauuo", {"supergrid_data": supergrid}
    else:
        native, grid = _cice_inputs()
        compound = "SImon.siconc"
        options = {
            "grid_identity_verified": True,
            "grid_reference": "static-grid@sha256:test",
        }
    driver = ACCESS_ESM_CMORiser(
        native,
        compound_name=compound,
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        activity_id="ACCESS-DEV",
        model_id="ACCESS-OM3",
        cmip_version="CMIP6",
        project_cv=_project_cv(),
        grid_data=grid,
        output_path=tmp_path,
        enable_resampling=False,
        split_years=None,
        **options,
    )
    try:
        driver.run()
        before = driver.to_iris()
        assert before.has_lazy_data()
        driver.write()
        assert len(driver.cmoriser.written_files) == 1
        after = iris.load_cube(str(driver.cmoriser.written_files[0]))
        _assert_same_cube(before, after, area_type=component == "cice")
    finally:
        driver.close()


@pytest.mark.unit
def test_mom6_depth_bounds_survive_real_cmor_write_and_reload(tmp_path):
    pytest.importorskip("ncdata")
    native, grid, supergrid = _mom6_3d_inputs()
    driver = ACCESS_ESM_CMORiser(
        native,
        compound_name="Omon.thetao",
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        activity_id="ACCESS-DEV",
        model_id="ACCESS-OM3",
        cmip_version="CMIP6",
        project_cv=_project_cv(),
        grid_data=grid,
        supergrid_data=supergrid,
        output_path=tmp_path,
        enable_resampling=False,
        split_years=None,
    )
    try:
        driver.run()
        assert "lev_bnds" in driver.to_dataset()
        before = driver.to_iris()
        depth = before.coord("depth")
        assert depth.has_bounds()
        np.testing.assert_allclose(depth.bounds, [[0.0, 1.0], [1.0, 2.0]])
        driver.write()
        after = iris.load_cube(str(driver.cmoriser.written_files[0]))
        reloaded_depth = after.coord("depth")
        assert reloaded_depth.has_bounds()
        np.testing.assert_allclose(reloaded_depth.bounds, depth.bounds)
        _assert_same_cube(before, after)
    finally:
        driver.close()


@pytest.mark.unit
def test_fgco2_scalar_depth_survives_real_cmor_write_and_reload(tmp_path):
    pytest.importorskip("ncdata")
    native, grid, supergrid = _mom6_3d_inputs()
    native = native.drop_vars(["thetao", "z_l", "z_i"])
    for name, value, units in (
        ("dic_stf", 0.01, "mol m-2 s-1"),
        ("salt_flux_added", 0.002, "kg m-2 s-1"),
        ("friver", 3.0, "kg m-2 s-1"),
    ):
        native[name] = (
            ("time", "yh", "xh"),
            np.full((2, 2, 2), value),
            {"units": units},
        )
    driver = ACCESS_ESM_CMORiser(
        native,
        compound_name="Omon.fgco2",
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        activity_id="ACCESS-DEV",
        model_id="ACCESS-OM3",
        cmip_version="CMIP6",
        project_cv=_project_cv(),
        grid_data=grid,
        supergrid_data=supergrid,
        calculation_kwargs={
            "parameters": {
                "dic_global": 0.0019,
                "sal_global": 34.6,
                "runoff_dic_mol_kg": 0.001,
            },
            "run_id": "om3-real-run",
            "run_config_reference": "audited input.nml and field_table",
            "dic_surface_components_verified": True,
        },
        output_path=tmp_path,
        enable_resampling=False,
        split_years=None,
    )
    try:
        driver.run()
        original_attrs = dict(driver.to_dataset()["fgco2"].attrs)
        before = driver.to_iris()
        assert driver.to_dataset()["fgco2"].attrs == original_attrs
        depth = before.coord("depth")
        np.testing.assert_allclose(depth.points, [0.0])
        assert not depth.has_bounds()
        driver.write()
        after = iris.load_cube(str(driver.cmoriser.written_files[0]))
        reloaded_depth = after.coord("depth")
        assert reloaded_depth.units == depth.units
        np.testing.assert_allclose(reloaded_depth.points, depth.points)
        assert not reloaded_depth.has_bounds()
        _assert_same_cube(before, after)
    finally:
        driver.close()


@pytest.mark.unit
@pytest.mark.parametrize("component", ["mom6", "cice"])
def test_fixed_ocean_area_roundtrips_without_time_axis(tmp_path, component):
    pytest.importorskip("ncdata")
    if component == "mom6":
        _, grid, supergrid = _mom6_3d_inputs()
        native = xr.Dataset({"areacello": grid.areacello})
        options = {"supergrid_data": supergrid}
    else:
        _, grid = _cice_inputs()
        native = xr.Dataset(
            {"areacello": grid.tarea * 1e-4},
            attrs={"source_area_units": "cm2"},
        )
        options = {
            "grid_component": "sea_ice",
            "grid_identity_verified": True,
            "grid_reference": "static-grid@sha256:test",
        }
    driver = ACCESS_ESM_CMORiser(
        native,
        compound_name="Ofx.areacello",
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        activity_id="ACCESS-DEV",
        model_id="ACCESS-OM3",
        cmip_version="CMIP6",
        project_cv=_project_cv(),
        grid_data=grid,
        output_path=tmp_path,
        enable_resampling=False,
        split_years=None,
        **options,
    )
    try:
        driver.run()
        before = driver.to_iris()
        assert before.shape == (2, 2)
        assert not before.coords("time")
        driver.write()
        after = iris.load_cube(str(driver.cmoriser.written_files[0]))
        assert not after.coords("time")
        assert before.units == after.units
        np.testing.assert_array_equal(
            np.ma.getmaskarray(before.data), np.ma.getmaskarray(after.data)
        )
        np.testing.assert_allclose(before.data.compressed(), after.data.compressed())
        np.testing.assert_allclose(
            after.data.compressed(),
            [2.0, 2.0, 2.0] if component == "mom6" else [1.0, 2.0, 3.0],
        )
        for name in ("latitude", "longitude"):
            np.testing.assert_allclose(
                before.coord(name).points, after.coord(name).points
            )
            np.testing.assert_allclose(
                before.coord(name).bounds, after.coord(name).bounds
            )
        assert after.attributes["experiment_id"] == "om3-real-run"
    finally:
        driver.close()
