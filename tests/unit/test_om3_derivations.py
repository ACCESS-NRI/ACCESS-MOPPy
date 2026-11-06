"""Physical units and run-specific assumptions in the OM3 CMOR mappings."""

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_om3 import (
    calc_cice_area,
    calc_cice_siconc,
    calc_om3_fgco2,
    calc_om3_volcello,
)


def field(value, units):
    return xr.DataArray(
        da.full((2, 2, 2), value, chunks=(1, 2, 2)),
        dims=("time", "yh", "xh"),
        attrs={"units": units},
    )


def test_audited_carbon_removes_virtual_and_river_terms():
    stf = field(0.01, "mol m-2 s-1")
    salt = field(0.002, "kg m-2 s-1")
    river = field(3.0, "kg m-2 s-1")
    kwargs = dict(
        parameters=dict(dic_global=0.0019, sal_global=34.6, runoff_dic_mol_kg=0.001),
        run_id="actual-run",
        run_config_reference="audited input.nml and field_table",
        dic_surface_components_verified=True,
    )
    result = calc_om3_fgco2(stf, salt, river, **kwargs)
    assert isinstance(result.data, da.Array)
    expected = (0.01 - 0.0019 / 34.6 * 1000 * 0.002 - 0.001 * 3) * 0.012011
    np.testing.assert_allclose(result.compute(), expected)
    assert result.attrs["positive"] == "down"
    with pytest.raises(ValueError, match="Verify"):
        calc_om3_fgco2(
            stf, salt, river, **{**kwargs, "dic_surface_components_verified": False}
        )
    with pytest.raises(ValueError, match="run_id"):
        calc_om3_fgco2(stf, salt, river, **{**kwargs, "run_config_reference": ""})


@pytest.mark.parametrize("shifted_dim", ["time", "yh"])
def test_carbon_rejects_misaligned_native_fields(shifted_dim):
    coords = {"time": [0, 1], "yh": [10, 20], "xh": [30, 40]}
    stf = field(0.01, "mol m-2 s-1").assign_coords(coords)
    salt = field(0.002, "kg m-2 s-1").assign_coords(coords)
    river = field(3.0, "kg m-2 s-1").assign_coords(coords)
    salt = salt.assign_coords({shifted_dim: salt[shifted_dim] + 1})
    kwargs = dict(
        parameters=dict(dic_global=0.0019, sal_global=34.6, runoff_dic_mol_kg=0.001),
        run_id="actual-run",
        run_config_reference="audited input.nml and field_table",
        dic_surface_components_verified=True,
    )
    with pytest.raises(ValueError, match="align|index"):
        calc_om3_fgco2(stf, salt, river, **kwargs)


def test_cice_units_and_masked_values():
    source = xr.DataArray(
        [0.0, 0.5, 1e20], dims="i", attrs={"units": "1", "_FillValue": 1e20}
    )
    np.testing.assert_allclose(calc_cice_siconc(source), [0.0, 50.0, np.nan])
    area = xr.DataArray([1e4, 2e4], dims="i", attrs={"units": "cm2"})
    np.testing.assert_allclose(calc_cice_area(area), [1.0, 2.0])
    with pytest.raises(ValueError, match="units"):
        calc_cice_siconc(source.assign_attrs(units="unknown"))


def test_native_volume_uses_matching_t_cell_area():
    thickness = field(10.0, "m").expand_dims(z_l=[5.0])
    area = xr.DataArray(
        [[2.0, 3.0], [4.0, 1e20]],
        dims=("yh", "xh"),
        attrs={"units": "m2", "_FillValue": 1e20},
    )
    result = calc_om3_volcello(thickness, area)
    assert isinstance(result.data, da.Array)
    np.testing.assert_allclose(
        result.isel(time=0, z_l=0).compute(), [[20, 30], [40, np.nan]]
    )
    with pytest.raises(ValueError, match="units"):
        calc_om3_volcello(thickness.assign_attrs(units="kg"), area)
