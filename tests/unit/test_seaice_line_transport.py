"""Numerical and written-output regressions for the two strait diagnostics."""

from copy import deepcopy
from unittest.mock import patch

import dask.array as da
import netCDF4
import numpy as np
import pytest
import xarray as xr

from access_moppy import ACCESS_ESM_CMORiser
from access_moppy.derivations import (
    calc_line_transport,
    calc_monthly_ice_mass_transport,
)
from access_moppy.derivations.calc_seaice import _transport_grid
from access_moppy.file_discovery import discover_files


@pytest.fixture
def transport_input():
    shape = (2, 300, 360)
    ds = xr.Dataset(coords={"time": [15.5, 45.5]})
    ds.time.attrs = {
        "units": "days since 2000-01-01 00:00:00",
        "calendar": "proleptic_gregorian",
        "bounds": "time_bounds",
    }
    ds["time_bounds"] = (("time", "d2"), [[0.0, 31.0], [31.0, 60.0]])
    for name, value, units in [
        ("aice", 0.5, "1"),
        ("siu", 2.0, "m/s"),
        ("siv", 4.0, "m/s"),
        ("simass", 917.0, "kg m^-2"),
        ("hi", 1.0, "m"),
    ]:
        ds[name] = xr.DataArray(
            np.full(shape, value, dtype="float32"),
            dims=("time", "nj", "ni"),
            attrs={"units": units, "coordinates": "TLON TLAT time"},
        )
    ds["uvel"], ds["vvel"] = ds.siu.copy(), ds.siv.copy()
    for name in ("TLAT", "TLON", "ULAT", "ULON"):
        coords = np.zeros(shape[1:])
        for line in _transport_grid()["lines"]:
            for p in line["points"]:
                coords[p["j"], p["i"]] = p[name]
        ds[name] = xr.DataArray(
            coords,
            dims=("nj", "ni"),
            attrs={
                "units": "degrees_north" if name.endswith("LAT") else "degrees_east"
            },
        )
    return ds


def calculate(ds, quantity="aice", **kwargs):
    return calc_line_transport(
        ds[quantity], ds.siu, ds.siv, ds.TLAT, ds.TLON, ds.ULAT, ds.ULON, **kwargs
    )


@pytest.fixture
def daily_transport_input(transport_input):
    """Two complete months including leap February, without allocating repeats."""
    ds = xr.Dataset(coords={"time": np.arange(60) + 0.5})
    ds.time.attrs = transport_input.time.attrs.copy()
    ds["time_bounds"] = (
        ("time", "d2"),
        np.column_stack([np.arange(60), np.arange(1, 61)]),
    )
    for name in ("aice", "siu", "siv"):
        field = transport_input[name].isel(time=0, drop=True)
        ds[name] = field.expand_dims(time=ds.time)
    ds["sithick"] = xr.full_like(ds.aice, 2.0).assign_attrs(units="m")
    for name in ("TLAT", "TLON", "ULAT", "ULON"):
        ds[name] = transport_input[name]
    ds.time.attrs = transport_input.time.attrs.copy()
    return ds


def monthly_mass(ds, **kwargs):
    ds = ds.rename(
        {"aice": "simassacrossline", "time_bounds": "time_bnds", "d2": "bnds"}
    )
    ds.time.attrs["bounds"] = "time_bnds"
    return calc_monthly_ice_mass_transport(ds, **kwargs)


@pytest.fixture
def uniform_edges(monkeypatch):
    grid = deepcopy(_transport_grid())
    for line in grid["lines"]:
        for p in line["points"]:
            p["edge_length"] = 2.0 if p["component"] == "x" else 3.0
    monkeypatch.setattr(
        "access_moppy.derivations.calc_seaice._transport_grid", lambda: grid
    )


@pytest.mark.parametrize("lazy", [False, True])
def test_known_fluxes_and_reversed_flow(transport_input, uniform_edges, lazy):
    ds = transport_input
    ds["siu"][1] *= -1
    ds["siv"][1] *= -1
    if lazy:
        ds = ds.chunk({"time": 1})
    result = calculate(ds)
    assert isinstance(result.data, da.Array) == lazy
    # x/y point counts: (1,17), (2,7), (12,9), (0,2).
    expected = np.array([104.0, 46.0, 78.0, 12.0])
    np.testing.assert_allclose(result, [expected, -expected])
    assert result.dims == ("time", "siline")
    assert set(result.coords) == {"time", "siline"}
    assert result.attrs["units"] == "m2 s-1"
    assert list(result.siline.values) == [
        "fram_strait",
        "canadian_archipelago",
        "barents_opening",
        "bering_strait",
    ]


def test_mass_uses_mass_per_grid_area_once(transport_input, uniform_edges):
    result = calculate(transport_input, "simass", quantity_kind="mass")
    np.testing.assert_allclose(
        result, np.tile(np.array([104.0, 46.0, 78.0, 12.0]) * 1834, (2, 1))
    )
    assert result.attrs["units"] == "kg s-1"
    assert "approximate" in result.attrs["comment"]
    legacy = calculate(transport_input, "hi", quantity_kind="mass", ice_density=900.0)
    np.testing.assert_allclose(legacy, result * 900 / 917)


def test_zero_ice_and_wholly_missing_straits_are_distinct(transport_input):
    ds = transport_input
    ds["siu"][:] = np.nan
    ds["siv"][:] = np.nan
    ds["aice"][0] = 0
    ds["aice"][1] = np.nan
    result = calculate(ds)
    np.testing.assert_array_equal(result[0], 0)
    assert np.isnan(result[1]).all()


@pytest.mark.parametrize(
    "name,bad_units", [("aice", "%"), ("siu", "cm/s"), ("TLAT", "unknown")]
)
def test_incorrect_units_are_rejected(transport_input, name, bad_units):
    transport_input[name].attrs["units"] = bad_units
    with pytest.raises(ValueError, match="units"):
        calculate(transport_input)


def test_other_grid_is_rejected_even_when_shape_matches(transport_input):
    transport_input["TLON"] += 0.1
    transport_input.TLON.attrs["units"] = "degrees_east"
    with pytest.raises(ValueError, match="does not match"):
        calculate(transport_input)
    with pytest.raises(ValueError, match="require.*grid"):
        calculate(transport_input.isel(ni=slice(0, 359)))


def test_time_indexes_must_match(transport_input):
    ds = transport_input
    with pytest.raises(ValueError, match="align"):
        calc_line_transport(
            ds.aice,
            ds.siu.assign_coords(time=[15.0, 45.0]),
            ds.siv,
            ds.TLAT,
            ds.TLON,
            ds.ULAT,
            ds.ULON,
        )


def test_coordinate_conventions_and_dimension_order(transport_input):
    ds = transport_input
    expected = calculate(ds)
    ds["TLON"] = (ds.TLON % 360).assign_attrs(units="degrees_east")
    ds["ULON"] = (ds.ULON % 360).assign_attrs(units="degrees_east")
    ds["TLAT"] = np.deg2rad(ds.TLAT).assign_attrs(units="radians")
    xr.testing.assert_equal(calculate(ds.transpose("ni", "time", "nj", "d2")), expected)


@pytest.mark.parametrize(
    "model,version",
    [
        ("ACCESS-ESM1-5", "CMIP6"),
        ("ACCESS-ESM1-6", "CMIP6"),
        ("ACCESS-ESM1-6", "CMIP7"),
    ],
)
@pytest.mark.parametrize(
    "variable,units", [("siareaacrossline", "m2 s-1"), ("simassacrossline", "kg s-1")]
)
@pytest.mark.parametrize("file_input", [False, True])
def test_full_pipeline_writes_strait_labels(
    transport_input,
    tmp_path,
    model,
    version,
    variable,
    units,
    parent_experiment_config,
    file_input,
    daily_transport_input,
):
    compound = (
        f"SImon.{variable}"
        if version == "CMIP6"
        else f"seaIce.{variable}.tavg-u-ht-u.mon.glb"
    )
    daily_mass = model == "ACCESS-ESM1-6" and variable == "simassacrossline"
    if daily_mass:
        transport_input = daily_transport_input
    input_data = transport_input
    if file_input:
        input_data = tmp_path / "ice_input.nc"
        transport_input.to_netcdf(
            input_data,
            encoding={
                "time": {"_FillValue": None},
                **{
                    name: {"zlib": True, "complevel": 1}
                    for name in transport_input.data_vars
                },
            },
        )
    # No supergrid download, latitude/longitude attachment, or horizontal-grid
    # inference should be needed for a line result.
    with patch(
        "access_moppy.sea_ice.Supergrid",
        side_effect=AssertionError("unexpected supergrid"),
    ):
        with ACCESS_ESM_CMORiser(
            input_data=input_data,
            compound_name=compound,
            cmip_version=version,
            source_id=model,
            experiment_id="historical",
            variant_label="r1i1p1f1",
            parent_info={
                **parent_experiment_config,
                "parent_source_id": model,
                "parent_mip_era": version,
            },
            output_path=tmp_path,
            enable_chunking=True,
            enable_resampling=daily_mass,
        ) as driver:
            driver.run(write_output=True)
            files = driver.cmoriser.written_files
            assert len(files) == 1
    with netCDF4.Dataset(files[0]) as nc:
        field = nc[variable]
        assert field.dimensions == ("time", "line")
        assert field.shape == (2, 4)
        assert field.units == units
        assert field.coordinates == "line"
        assert field.cell_methods == "time: mean"
        assert "cell_measures" not in field.ncattrs()
        assert "latitude" not in nc.variables and "longitude" not in nc.variables
        assert not ({"ni", "nj", "i", "j", "vertices"} & set(nc.dimensions))
        assert nc["line"].dtype.str == "|S1"
        assert nc["line"].standard_name == "region"
        assert list(netCDF4.chartostring(nc["line"][:])) == [
            "fram_strait",
            "canadian_archipelago",
            "barents_opening",
            "bering_strait",
        ]
        assert nc["time"].bounds == "time_bnds"
        assert "_FillValue" not in nc["time"].ncattrs()
        np.testing.assert_array_equal(nc["time_bnds"][:], [[0.0, 31.0], [31.0, 60.0]])
        if variable == "simassacrossline":
            assert "approximate" in field.comment
            assert "excluding snow" in field.comment
            if daily_mass:
                assert "daily" in field.comment
                assert "0.001" in field.comment


@pytest.mark.parametrize("lazy", [False, True])
def test_daily_mass_multiplies_before_averaging(
    daily_transport_input, uniform_edges, lazy
):
    ds = daily_transport_input
    first_half = ds.time < 15
    ds["sithick"] = (
        xr.where(first_half, 1.0, 3.0)
        .broadcast_like(ds.sithick)
        .assign_attrs(units="m")
    )
    for name in ("siu", "siv"):
        ds[name] = (
            xr.where(first_half, 3.0, 1.0)
            .broadcast_like(ds[name])
            .assign_attrs(units="m/s")
        )
    if lazy:
        ds = ds.chunk({"time": 10})
    result = monthly_mass(ds)
    assert isinstance(result.simassacrossline.data, da.Array) == lazy
    edges = np.array([53, 25, 51, 6])
    np.testing.assert_allclose(
        result.simassacrossline, np.tile(917 * 0.5 * 3 * edges, (2, 1))
    )
    # Multiplying monthly means would give a different January flux.
    wrong = float(ds.sithick[:31, 0, 0].mean() * ds.siu[:31, 0, 0].mean())
    assert not np.isclose(wrong, 3)
    np.testing.assert_array_equal(result.time, [15.5, 45.5])
    np.testing.assert_array_equal(result.time_bnds, [[0, 31], [31, 60]])


@pytest.mark.parametrize(
    "concentration,expected_zero",
    [
        (0.0, True),
        (0.001, True),
        (0.00101, False),
        (np.nan, False),
        (-0.1, False),
        (1.1, False),
    ],
)
def test_daily_mass_low_ice_masks_are_explicit(
    daily_transport_input, concentration, expected_zero
):
    ds = daily_transport_input
    ds["aice"] = xr.full_like(ds.aice, concentration)
    ds["sithick"] = xr.full_like(ds.sithick, np.nan)
    ds["siu"] = xr.full_like(ds.siu, np.nan)
    ds["siv"] = xr.full_like(ds.siv, np.nan)
    result = monthly_mass(ds).simassacrossline
    if expected_zero:
        np.testing.assert_array_equal(result, 0)
    else:
        assert np.isnan(result).all()


def test_daily_mass_preserves_small_valid_flux(daily_transport_input):
    ds = daily_transport_input
    ds["aice"] = xr.full_like(ds.aice, 0.0005)
    assert (monthly_mass(ds).simassacrossline > 0).all()


def test_daily_mass_missing_ice_day_propagates_to_month(daily_transport_input):
    ds = daily_transport_input.copy(deep=True)
    # One missing contribution must not disappear in either the line sum or
    # the monthly mean, and it must not contaminate February/other straits.
    ds["sithick"][0, 279, 267] = np.nan
    result = monthly_mass(ds).simassacrossline
    assert np.isnan(result[0, 0])
    assert np.isfinite(result[0, 1:]).all()
    assert np.isfinite(result[1]).all()


@pytest.mark.parametrize(
    "problem,match",
    [
        ("missing_bounds", "daily time bounds"),
        ("monthly", "one-day bounds"),
        ("gap", "contiguous"),
        ("overlap", "contiguous"),
        ("partial", "complete calendar months"),
        ("time_outside", "within their bounds"),
        ("no_units", "CF time units"),
    ],
)
def test_daily_mass_rejects_incomplete_or_wrong_frequency(
    daily_transport_input, problem, match
):
    ds = daily_transport_input.copy(deep=True)
    if problem == "missing_bounds":
        ds = ds.drop_vars("time_bounds")
        ds = ds.rename({"aice": "simassacrossline"})
        with pytest.raises(ValueError, match=match):
            calc_monthly_ice_mass_transport(ds)
        return
    if problem == "monthly":
        ds = ds.isel(time=[0])
        ds["time_bounds"][:] = [[0, 31]]
    elif problem == "gap":
        ds = ds.isel(time=[i for i in range(60) if i != 2])
    elif problem == "overlap":
        ds["time_bounds"][1] = ds.time_bounds[0]
    elif problem == "partial":
        ds = ds.isel(time=slice(1, None))
    elif problem == "time_outside":
        ds = ds.assign_coords(time=ds.time + 2)
        ds.time.attrs = daily_transport_input.time.attrs.copy()
    elif problem == "no_units":
        ds.time.attrs.pop("units")
    with pytest.raises(ValueError, match=match):
        monthly_mass(ds)


@pytest.mark.parametrize(
    "calendar,days,midpoints", [("360_day", 60, [15, 45]), ("noleap", 59, [15.5, 45])]
)
def test_daily_mass_respects_calendar(daily_transport_input, calendar, days, midpoints):
    ds = daily_transport_input.isel(time=slice(days))
    ds.time.attrs["calendar"] = calendar
    result = monthly_mass(ds)
    np.testing.assert_array_equal(result.time, midpoints)
    assert result.time.attrs["calendar"] == calendar


def test_daily_mass_accepts_decoded_time_and_hours(daily_transport_input):
    ds = daily_transport_input.copy()
    expected = monthly_mass(ds)
    decoded = monthly_mass(xr.decode_cf(ds))
    np.testing.assert_allclose(decoded.simassacrossline, expected.simassacrossline)
    ds = ds.assign_coords(time=ds.time * 24)
    ds.time.attrs = {
        **daily_transport_input.time.attrs,
        "units": "hours since 2000-01-01",
    }
    ds["time_bounds"] = ds.time_bounds * 24
    ds.time_bounds.attrs = {}
    ds.time_bounds.encoding = {}
    result = monthly_mass(ds)
    np.testing.assert_allclose(result.simassacrossline, expected.simassacrossline)
    np.testing.assert_allclose(result.time, expected.time * 24)


@pytest.mark.parametrize("layout", ["packed", "split"])
def test_mass_file_discovery_uses_daily_inputs(tmp_path, layout):
    folder = tmp_path / "output000/ice"
    folder.mkdir(parents=True)
    (folder / "iceh-1monthly-mean_1850-01.nc").touch()
    if layout == "packed":
        paths = [folder / "iceh-1daily-mean_1850-01.nc"]
    else:
        paths = [
            folder / f"access-esm1p6.cice5.2d.{name}.1day.mean.1850.nc"
            for name in ("aice", "sithick", "siu", "siv")
        ]
    for path in paths:
        path.touch()
    result = discover_files(
        tmp_path, "SImon.simassacrossline", model_id="ACCESS-ESM1-6"
    )
    assert set(map(str, result)) == set(map(str, paths))
    area = discover_files(tmp_path, "SImon.siareaacrossline", model_id="ACCESS-ESM1-6")
    assert list(map(str, area)) == [str(folder / "iceh-1monthly-mean_1850-01.nc")]


@pytest.mark.parametrize(
    "problem,match",
    [
        ("bounds_shape", "time, 2"),
        ("nonfinite", "finite"),
        ("bounds_units", "same units"),
        ("area_units", "fractional aice"),
        ("thickness_units", "sithick in m"),
        ("thickness_dims", "time, nj, ni"),
        ("density", "ice_density"),
        ("threshold", "ice_threshold"),
    ],
)
def test_daily_mass_rejects_invalid_metadata(daily_transport_input, problem, match):
    ds = daily_transport_input.copy(deep=True)
    kwargs = {}
    if problem == "bounds_shape":
        ds = ds.drop_vars("time_bounds")
        ds["time_bounds"] = (("time", "nv"), np.zeros((60, 3)))
        ds = ds.rename({"nv": "d2"})
    elif problem == "nonfinite":
        ds = ds.assign_coords(time=np.full(60, np.nan))
        ds.time.attrs = daily_transport_input.time.attrs.copy()
    elif problem == "bounds_units":
        ds.time_bounds.attrs["units"] = "hours since 2000-01-01"
    elif problem == "area_units":
        ds.aice.attrs["units"] = "%"
    elif problem == "thickness_units":
        ds.sithick.attrs["units"] = "cm"
    elif problem == "thickness_dims":
        ds["sithick"] = ds.sithick.isel(nj=0, drop=True)
    elif problem == "density":
        kwargs["ice_density"] = 0
    elif problem == "threshold":
        kwargs["ice_threshold"] = -0.001
    with pytest.raises(ValueError, match=match):
        monthly_mass(ds, **kwargs)


@pytest.mark.parametrize(
    "problem",
    [
        "kind",
        "density_on_area",
        "negative_density",
        "density_on_mass",
        "field_dims",
        "coordinate_dims",
    ],
)
def test_line_transport_rejects_invalid_contract(transport_input, problem):
    ds = transport_input
    kwargs = {}
    if problem == "kind":
        kwargs["quantity_kind"] = "volume"
    elif problem == "density_on_area":
        kwargs["ice_density"] = 900
    elif problem == "negative_density":
        kwargs.update(quantity_kind="mass", ice_density=-900)
    elif problem == "density_on_mass":
        kwargs.update(quantity_kind="mass", ice_density=900)
    elif problem == "field_dims":
        ds = ds.isel(time=0, drop=True)
    elif problem == "coordinate_dims":
        ds["TLAT"] = ds.TLAT.isel(nj=0, drop=True)
    with pytest.raises(ValueError):
        calculate(ds, **kwargs)
