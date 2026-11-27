import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings


@pytest.mark.unit
@pytest.mark.parametrize(
    "compound_name", ["Omon.po4", "ocnBgchem.po4.tavg-ol-hxy-sea.mon.glb"]
)
@pytest.mark.parametrize(
    ("model_id", "native_units", "nitrate_scale"),
    [("ACCESS-ESM1-5", "mmol m-3", 0.001), ("ACCESS-ESM1-6", "mol kg-1", 1035.0)],
)
def test_phosphate_converts_native_nitrate_to_mol_per_cubic_metre(
    compound_name, model_id, native_units, nitrate_scale
):
    mapping = load_model_mappings(compound_name, model_id)["po4"]
    # Express representative surface and deep-ocean concentrations in native units.
    values_mol_m3 = np.array([[[[0.02, np.nan]], [[0.032, 0.0]]]], dtype="float32")
    native = xr.DataArray(
        values_mol_m3 / nitrate_scale,
        dims=("time", "st_ocean", "yt_ocean", "xt_ocean"),
        coords={"st_ocean": [5.0, 1500.0], "xt_ocean": [0.5, 1.5]},
        attrs={"units": native_units},
    )

    phosphate = evaluate_expression(mapping["calculation"], {"no3": native})

    assert mapping["units"] == "mol m-3"
    assert mapping["model_variable_units"] == {"no3": native_units}
    expected = native.copy(data=values_mol_m3.astype("float64") / 16.0)
    xr.testing.assert_allclose(phosphate, expected, rtol=2e-7, atol=0)
