"""Tests for access_moppy.derivations.calc_aerosol."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_aerosol import optical_depth
from access_moppy.derivations.calc_utils import sum_vars

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pseudo_level_da(nlev=5, nlat=4, nlon=4, dim_name="pseudo_level"):
    """Return a DataArray with a pseudo_level dimension."""
    rng = np.random.default_rng(0)
    data = rng.random((nlev, nlat, nlon))
    return xr.DataArray(
        data,
        dims=[dim_name, "lat", "lon"],
        coords={dim_name: np.arange(nlev)},
    )


# ---------------------------------------------------------------------------
# optical_depth
# ---------------------------------------------------------------------------


class TestOpticalDepth:
    @pytest.mark.unit
    def test_basic_single_wavelength(self):
        da = _make_pseudo_level_da(nlev=5)
        result = optical_depth([da], lwave=2)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_output_shape_matches_spatial_dims(self):
        nlat, nlon = 4, 4
        da = _make_pseudo_level_da(nlev=5, nlat=nlat, nlon=nlon)
        result = optical_depth([da], lwave=0)
        # pseudo_level dim is selected (scalar coord), only lat and lon remain
        assert result.shape == (nlat, nlon)

    @pytest.mark.unit
    def test_two_arrays_are_summed(self):
        da1 = _make_pseudo_level_da(nlev=3)
        da2 = _make_pseudo_level_da(nlev=3)
        result = optical_depth([da1, da2], lwave=1)

        # Manual computation
        expected = sum_vars([da1.sel(pseudo_level=1), da2.sel(pseudo_level=1)])
        np.testing.assert_allclose(result.values, expected.values)

    @pytest.mark.unit
    def test_selects_correct_wavelength(self):
        nlev = 4
        da = _make_pseudo_level_da(nlev=nlev)
        for lwave in range(nlev):
            result = optical_depth([da], lwave=lwave)
            np.testing.assert_allclose(
                result.squeeze().values, da.sel(pseudo_level=lwave).values
            )

    @pytest.mark.unit
    def test_raises_without_pseudo_level_dim(self):
        """Raises ValueError when pseudo_level is not in dims."""
        da = xr.DataArray(
            np.ones((3, 4, 4)),
            dims=["lev", "lat", "lon"],
        )
        with pytest.raises(ValueError, match="pseudo_level"):
            optical_depth([da], lwave=0)

    @pytest.mark.unit
    def test_custom_pseudo_level_dim_name(self):
        """Works with a dim name that contains 'pseudo_level'."""
        da = _make_pseudo_level_da(nlev=3, dim_name="pseudo_level_2")
        result = optical_depth([da], lwave=0)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_output_renamed_to_pseudo_level(self):
        """The selected pseudo_level dim is renamed to 'pseudo_level' in coordinates."""
        da = _make_pseudo_level_da(nlev=3, dim_name="pseudo_level_2")
        result = optical_depth([da], lwave=1)
        # After sel the dim becomes a scalar coordinate, renamed to pseudo_level
        assert "pseudo_level" in result.coords
