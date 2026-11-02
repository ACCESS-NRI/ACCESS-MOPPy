"""Soil interfaces, conservative topsoil integration and coordinate association."""

from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from access_moppy.atmosphere import Atmosphere_CMORiser
from access_moppy.derivations.calc_land import (
    cable_soil_geometry,
    calc_mrsfl,
    calc_mrsll,
    calc_mrsol,
    calc_topsoil,
)


def soil(values, levels=None):
    values = np.asarray(values, dtype=float)
    return xr.DataArray(
        values,
        dims=("soil_model_level_number",),
        coords={"soil_model_level_number": levels or list(range(1, len(values) + 1))},
    )


def test_interfaces_are_model_thicknesses_not_midpoint_extrapolation():
    centres, bounds = cable_soil_geometry([1, 2, 3, 4, 5, 6])
    np.testing.assert_allclose(
        np.diff(bounds), np.array([0.022, 0.058, 0.154, 0.409, 1.085, 2.872])[:, None]
    )
    np.testing.assert_allclose(bounds[:, 0], [0, 0.022, 0.080, 0.234, 0.643, 1.728])
    np.testing.assert_allclose(centres, [0.011, 0.051, 0.157, 0.4385, 1.1855, 3.164])
    assert bounds[-1, 1] == 4.6


@pytest.mark.parametrize("levels", [[0, 1], [1, 7], [1, 1], [2, 1], [1.2, 2], []])
def test_reject_ambiguous_or_unknown_layers(levels):
    with pytest.raises(ValueError, match="CABLE soil levels"):
        cable_soil_geometry(levels)


@pytest.mark.parametrize("function", [calc_mrsol, calc_mrsfl, calc_mrsll])
def test_layered_values_and_missing_values_are_unchanged(function):
    water = soil([2, 3, np.nan, 5, 6, 7])
    fraction = xr.ones_like(water) * 0.25
    result = function(water) if function is calc_mrsol else function(water, fraction)
    expected = water if function is calc_mrsol else water * fraction
    np.testing.assert_array_equal(result.values, expected.values)
    assert result.dims == ("depth",)
    assert result.depth.attrs["units"] == "m"
    np.testing.assert_allclose(result.depth[-1], 3.164)


def test_topsoil_integrates_uniform_density_to_exactly_ten_centimetres():
    thickness = np.array([0.022, 0.058, 0.154, 0.409, 1.085, 2.872])
    # Density 300 kg m-3 yields 30 kg m-2 in the requested 0.1 m interval.
    result = calc_topsoil(soil(300 * thickness).chunk())
    assert hasattr(result.data, "compute")
    np.testing.assert_allclose(result.compute(), 30)


def test_topsoil_partial_layer_fraction_uses_edges():
    np.testing.assert_allclose(calc_topsoil(soil([2, 3, 154, 1000, 2000, 3000])), 25)


@pytest.mark.parametrize("missing_layer", [0, 1, 2])
def test_missing_contributing_layer_is_not_filled(missing_layer):
    data = np.ones(6)
    data[missing_layer] = np.nan
    assert np.isnan(calc_topsoil(soil(data)))


def test_missing_deep_layers_do_not_affect_topsoil():
    np.testing.assert_allclose(
        calc_topsoil(soil([2, 3, 154, np.nan, np.nan, np.nan])), 25
    )


@pytest.mark.parametrize("levels", [[1, 2], [1, 3, 4], [2, 3, 4]])
def test_topsoil_requires_all_contributing_layers(levels):
    with pytest.raises(ValueError, match="requires CABLE layers"):
        calc_topsoil(soil(np.ones(len(levels)), levels))


def coordinate_setup(operation, ds, meta):
    cmoriser = object.__new__(Atmosphere_CMORiser)
    cmoriser.cmor_name = "water"
    cmoriser.mapping = {"water": {"calculation": {"operation": operation}}}
    cmoriser.vocab = SimpleNamespace(axes={"soil": {"out_name": "depth", **meta}})
    cmoriser.ds = ds
    cmoriser._set_soil_depth_coordinates()
    return cmoriser.ds


@pytest.mark.parametrize(
    "units,value,bounds", [("cm", "5", "0 10"), ("m", "0.05", "0 0.1")]
)
def test_reduced_output_replaces_detached_indices_with_associated_scalar(
    units, value, bounds
):
    ds = xr.Dataset(
        {"water": ("time", [25.0])}, coords={"depth": np.arange(1, 7), "time": [1]}
    )
    result = coordinate_setup(
        "calc_topsoil", ds, {"units": units, "value": value, "bounds_values": bounds}
    )
    assert result.depth.dims == ()
    assert result.depth.item() == float(value)
    assert "depth" not in result.dims
    assert result.depth.attrs == {"units": units, "bounds": "depth_bnds"}
    np.testing.assert_array_equal(result.depth_bnds, [float(v) for v in bounds.split()])
    assert result.water.attrs["coordinates"] == "depth"
    np.testing.assert_array_equal(result.water, ds.water)


def test_layered_output_attaches_bounds_for_selected_model_layers():
    water = calc_mrsol(soil([2, 3, 4], [1, 3, 6]))
    result = coordinate_setup(
        "calc_mrsol", water.to_dataset(name="water"), {"units": "m"}
    )
    assert result.depth_bnds.dims == ("depth", "bnds")
    np.testing.assert_allclose(
        result.depth_bnds, [[0, 0.022], [0.08, 0.234], [1.728, 4.6]]
    )
    assert result.depth.attrs["bounds"] == "depth_bnds"
