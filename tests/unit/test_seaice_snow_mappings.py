"""Snow thickness must use the density of the model's native snow mass."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings


@pytest.mark.unit
@pytest.mark.parametrize(
    ("model_id", "snow_density"),
    [("ACCESS-ESM1-6", 330.0), ("ACCESS-ESM1-5", 317.0)],
)
@pytest.mark.parametrize(
    "compound_name",
    [
        "SImon.sisnthick",
        "SIday.sisnthick",
        "seaIce.snd.tavg-u-hxy-sn.mon.glb",
        "seaIce.snd.tavg-u-hxy-sn.day.glb",
    ],
)
@pytest.mark.parametrize("lazy", [False, True], ids=["numpy", "dask"])
def test_snow_thickness_uses_model_density(model_id, snow_density, compound_name, lazy):
    """CICE5 uses 330 kg/m³; preserve ESM1.5's existing conversion separately.

    CICE5 2026.01.000 and 2026.07.000 define rhos=330 in the ACCESS driver,
    and write sisnmass as rhos * vsno (snow volume per grid-cell area).
    """
    mapping = load_model_mappings(compound_name, model_id)["sisnthick"]
    siconc = xr.DataArray(
        [100.0, 50.0, 0.01, 1e-12, 0.0, np.nan, 50.0],
        dims="cell",
        coords={"cell": np.arange(7)},
    )
    # One metre over full/threshold ice cover, 0.2 m over half cover,
    # residual ice, open water, and missing concentration/mass respectively.
    sisnmass = xr.DataArray(
        [
            snow_density,
            snow_density / 10,
            snow_density * 1e-4,
            0.001,
            10.0,
            10.0,
            np.nan,
        ],
        dims=siconc.dims,
        coords=siconc.coords,
    )
    if lazy:
        siconc = siconc.chunk(cell=3)
        sisnmass = sisnmass.chunk(cell=3)

    result = evaluate_expression(
        mapping["calculation"], {"siconc": siconc, "sisnmass": sisnmass}
    )

    assert (result.chunks is not None) == lazy
    expected = xr.DataArray(
        [1.0, 0.2, 1.0, 0.0, 0.0, np.nan, np.nan],
        dims=siconc.dims,
        coords=siconc.coords,
    )
    xr.testing.assert_allclose(result.compute(), expected)
