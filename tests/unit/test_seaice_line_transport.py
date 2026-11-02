"""Numerical and written-output regressions for the two strait diagnostics."""

from copy import deepcopy
from unittest.mock import patch

import dask.array as da
import netCDF4
import numpy as np
import pytest
import xarray as xr

from access_moppy import ACCESS_ESM_CMORiser
from access_moppy.derivations import calc_line_transport
from access_moppy.derivations.calc_seaice import _transport_grid


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
):
    compound = (
        f"SImon.{variable}"
        if version == "CMIP6"
        else f"seaIce.{variable}.tavg-u-ht-u.mon.glb"
    )
    input_data = transport_input
    if file_input:
        input_data = tmp_path / "ice_input.nc"
        transport_input.to_netcdf(input_data, encoding={"time": {"_FillValue": None}})
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
            enable_resampling=False,
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
