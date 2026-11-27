"""Explicit project supergrids must carry a coherent two-dimensional grid."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.ocean_supergrid import Supergrid


@pytest.mark.unit
def test_explicit_supergrid_rejects_non_dataset_input():
    with pytest.raises(TypeError, match="xarray Dataset"):
        Supergrid(dataset=np.ones((5, 5)))


@pytest.mark.unit
def test_explicit_supergrid_requires_both_geographic_arrays():
    x = xr.DataArray(np.ones((5, 5)), dims=("nyp", "nxp"))
    with pytest.raises(ValueError, match="requires x and y"):
        Supergrid(dataset=xr.Dataset({"x": x}))


@pytest.mark.unit
def test_explicit_supergrid_rejects_misaligned_axes():
    x = xr.DataArray(np.ones((5, 5)), dims=("nyp", "nxp"))
    y = xr.DataArray(np.ones((5, 5)), dims=("other_y", "other_x"))
    with pytest.raises(ValueError, match="matching 2D dimensions"):
        Supergrid(dataset=xr.Dataset({"x": x, "y": y}))
