"""Protect the model-specific soil field-capacity resources."""

import numpy as np

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings


def test_esm16_field_capacity_uses_native_mass_without_changing_esm15():
    compound_name = "fx.mrsofc"
    modern_mapping = load_model_mappings(compound_name, "ACCESS-ESM1-6")["mrsofc"]
    legacy_mapping = load_model_mappings(compound_name, "ACCESS-ESM1-5")["mrsofc"]
    assert modern_mapping["model_variables"] == []
    assert legacy_mapping["calculation"]["args"][0] == {
        "literal": "fx.mrsofc_ACCESS-ESM.nc"
    }

    modern = evaluate_expression(modern_mapping["calculation"], {})
    legacy = evaluate_expression(legacy_mapping["calculation"], {})
    assert modern.dims == ("lat", "lon")
    assert modern.attrs["units"] == "kg m-2"
    for axis in modern.dims:
        np.testing.assert_array_equal(modern[axis], legacy[axis])

    values = modern.values
    np.testing.assert_array_equal(np.isnan(values), np.isnan(legacy))
    assert np.count_nonzero(np.isfinite(values)) == 10865
    assert np.count_nonzero(values == 0) == 3434
    # Maximum independently reconstructed from STASH 41 in the UM restart.
    np.testing.assert_allclose(np.nanmax(values), 1766.021525742412, rtol=1e-12)
