"""Tests for access_moppy.derivations.calc_land."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_land import calc_rootd, calc_snc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NT = 3  # time steps
NTILES = 4  # pseudo-level (tile) dimension
NJ = 4  # lat
NI = 6  # lon


def _make_inputs(tilefrac_data, snow_data, landfrac_data=None):
    """Build (tilefrac, snow_tile, landfrac) DataArrays from numpy arrays."""
    times = xr.date_range("2000-01-01", periods=NT, freq="ME")

    tilefrac = xr.DataArray(
        tilefrac_data,
        dims=["time", "pseudo_level_0", "lat", "lon"],
        coords={"time": times},
        attrs={"units": "1"},
    )
    snow_tile = xr.DataArray(
        snow_data,
        dims=["time", "pseudo_level_0", "lat", "lon"],
        coords={"time": times},
        attrs={"units": "kg m-2"},
    )
    if landfrac_data is None:
        landfrac_data = np.ones((NJ, NI))
    landfrac = xr.DataArray(
        landfrac_data,
        dims=["lat", "lon"],
        attrs={"units": "1"},
    )
    return tilefrac, snow_tile, landfrac


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCalcSnc:
    """Tests for calc_snc()."""

    def test_all_tiles_snowy_equals_tilefrac_sum(self):
        """When all tiles have snow, snc == 100 * sum(tilefrac) * landfrac."""
        tf = np.full((NT, NTILES, NJ, NI), 0.25)  # each tile = 0.25, sum = 1.0
        sn = np.ones((NT, NTILES, NJ, NI))  # all tiles have snow
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn)

        result = calc_snc(tilefrac, snow_tile, landfrac)

        assert result.dims == ("time", "lat", "lon")
        np.testing.assert_allclose(result.values, 100.0)

    def test_no_snow_returns_zero(self):
        """When no tile has snow, snc == 0."""
        tf = np.full((NT, NTILES, NJ, NI), 0.25)
        sn = np.zeros((NT, NTILES, NJ, NI))  # no snow anywhere
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn)

        result = calc_snc(tilefrac, snow_tile, landfrac)

        np.testing.assert_allclose(result.values, 0.0)

    def test_partial_snow_coverage(self):
        """Only tiles with snow > 0 contribute to snc."""
        # 4 tiles of equal fraction 0.25; only tile index 0 has snow
        tf = np.full((NT, NTILES, NJ, NI), 0.25)
        sn = np.zeros((NT, NTILES, NJ, NI))
        sn[:, 0, :, :] = 1.0  # only tile 0 has snow
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn)

        result = calc_snc(tilefrac, snow_tile, landfrac)

        # Only tile 0 (frac=0.25) contributes; landfrac=1 → 25 %
        np.testing.assert_allclose(result.values, 25.0)

    def test_landfrac_scaling(self):
        """snc is scaled by land fraction."""
        tf = np.full((NT, NTILES, NJ, NI), 0.25)
        sn = np.ones((NT, NTILES, NJ, NI))  # all tiles have snow
        lf = np.full((NJ, NI), 0.5)  # 50 % land
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn, lf)

        result = calc_snc(tilefrac, snow_tile, landfrac)

        # sum(tilefrac) = 1.0; * 0.5 landfrac * 100 = 50 %
        np.testing.assert_allclose(result.values, 50.0)

    def test_no_nan_in_output(self):
        """fillna(0) ensures output contains no NaN values."""
        tf = np.full((NT, NTILES, NJ, NI), 0.25)
        sn = np.ones((NT, NTILES, NJ, NI))
        lf = np.full((NJ, NI), 1.0)
        lf[0, 0] = np.nan  # inject NaN into land fraction
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn, lf)

        result = calc_snc(tilefrac, snow_tile, landfrac)

        assert not np.any(np.isnan(result.values))

    def test_output_bounds(self):
        """snc values must lie within [0, 100]."""
        rng = np.random.default_rng(0)
        tf = rng.uniform(0, 1, size=(NT, NTILES, NJ, NI))
        # normalise so tile fractions sum to ≤ 1
        tf /= tf.sum(axis=1, keepdims=True).clip(min=1)
        sn = rng.uniform(0, 10, size=(NT, NTILES, NJ, NI))
        lf = rng.uniform(0, 1, size=(NJ, NI))
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn, lf)

        result = calc_snc(tilefrac, snow_tile, landfrac)

        assert float(result.min()) >= 0.0
        assert float(result.max()) <= 100.0 + 1e-6

    def test_dask_compatible(self):
        """Result can be computed lazily with dask-backed arrays."""
        pytest.importorskip("dask")  # skip if dask not installed

        tf = np.full((NT, NTILES, NJ, NI), 0.25)
        sn = np.ones((NT, NTILES, NJ, NI))
        tilefrac, snow_tile, landfrac = _make_inputs(tf, sn)

        # chunk along the tile dimension
        tilefrac = tilefrac.chunk({"time": 1})
        snow_tile = snow_tile.chunk({"time": 1})

        result = calc_snc(tilefrac, snow_tile, landfrac)

        # result should still be a dask-backed DataArray (lazy)
        assert result.chunks is not None, "result should be dask-backed"
        # and it should compute to the correct value
        np.testing.assert_allclose(result.compute().values, 100.0)

    def test_mismatched_pseudo_level_name_handled(self):
        """snow_tile with a different pseudo-level dim name is handled correctly."""
        times = xr.date_range("2000-01-01", periods=NT, freq="ME")
        tf = xr.DataArray(
            np.full((NT, NTILES, NJ, NI), 0.25),
            dims=["time", "pseudo_level_0", "lat", "lon"],
            coords={"time": times},
        )
        # snow_tile uses a different internal name for the tile dimension
        sn = xr.DataArray(
            np.ones((NT, NTILES, NJ, NI)),
            dims=["time", "tile", "lat", "lon"],
            coords={"time": times},
        )
        lf = xr.DataArray(np.ones((NJ, NI)), dims=["lat", "lon"])

        result = calc_snc(tf, sn, lf)

        np.testing.assert_allclose(result.values, 100.0)


# ---------------------------------------------------------------------------
# Helpers for calc_rootd
# ---------------------------------------------------------------------------

NT_R = 2  # time steps
N_TILES = 17  # tiles 1-17 (pseudo_level_1 coordinate values)
NJ_R = 3
NI_R = 4


def _make_tilefrac(data):
    """Build a tilefrac DataArray with pseudo_level_1 coords 1-17."""
    times = xr.date_range("2000-01-01", periods=NT_R, freq="ME")
    return xr.DataArray(
        data,
        dims=["time", "pseudo_level_1", "lat", "lon"],
        coords={
            "time": times,
            "pseudo_level_1": np.arange(1, N_TILES + 1),
        },
        attrs={"units": "1"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCalcRootd:
    """Tests for calc_rootd() — issue #335."""

    def test_vegetated_returns_4p6(self):
        """Grid cells with any vegetated tile (1-13) get 4.6 m."""
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        data[:, 0, :, :] = 0.5  # tile 1 (index 0) is vegetated
        tilefrac = _make_tilefrac(data)

        result = calc_rootd(tilefrac)

        assert result.dims == ("lat", "lon")
        np.testing.assert_allclose(result.values, 4.6)

    def test_non_veg_land_returns_zero(self):
        """Land cells with only non-veg tiles (14-17) get 0 m."""
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        data[:, 13, :, :] = 1.0  # tile 14 (index 13) — ice/urban/barren/lake
        tilefrac = _make_tilefrac(data)

        result = calc_rootd(tilefrac)

        assert result.dims == ("lat", "lon")
        np.testing.assert_allclose(result.values, 0.0)

    def test_ocean_returns_nan(self):
        """Cells with all-zero tile fractions (ocean) get NaN (missing)."""
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        tilefrac = _make_tilefrac(data)

        result = calc_rootd(tilefrac)

        assert result.dims == ("lat", "lon")
        assert np.all(np.isnan(result.values))

    def test_mixed_grid(self):
        """Spatial mix: veg / non-veg land / ocean all in the same array."""
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        # lat 0: vegetated
        data[:, 0, 0, :] = 0.8  # tile 1
        # lat 1: non-veg land only (tile 14)
        data[:, 13, 1, :] = 1.0
        # lat 2: ocean — all zeros

        tilefrac = _make_tilefrac(data)
        result = calc_rootd(tilefrac)

        np.testing.assert_allclose(result.values[0, :], 4.6)  # veg
        np.testing.assert_allclose(result.values[1, :], 0.0)  # non-veg land
        assert np.all(np.isnan(result.values[2, :]))  # ocean

    def test_veg_appears_in_only_one_timestep(self):
        """Vegetation present in any timestep is enough to report 4.6 m."""
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        data[0, 2, :, :] = 0.3  # tile 3, timestep 0 only
        tilefrac = _make_tilefrac(data)

        result = calc_rootd(tilefrac)

        np.testing.assert_allclose(result.values, 4.6)

    def test_no_time_dimension_removed(self):
        """Output has no time dimension regardless of input."""
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        data[:, 0, :, :] = 0.5
        tilefrac = _make_tilefrac(data)

        result = calc_rootd(tilefrac)

        assert "time" not in result.dims

    def test_dask_compatible(self):
        """Function returns a lazy dask-backed array without calling compute."""
        pytest.importorskip("dask")

        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        data[:, 0, :, :] = 0.5
        tilefrac = _make_tilefrac(data).chunk({"time": 1})

        result = calc_rootd(tilefrac)

        assert result.chunks is not None, "result should be dask-backed (lazy)"
        np.testing.assert_allclose(result.compute().values, 4.6)

    def test_pseudo_level_0_without_coord_is_supported(self):
        """Regression: works when tile dimension is pseudo_level_0 with no coord."""
        times = xr.date_range("2000-01-01", periods=NT_R, freq="ME")
        data = np.zeros((NT_R, N_TILES, NJ_R, NI_R))
        data[:, 0, :, :] = 0.5
        tilefrac = xr.DataArray(
            data,
            dims=["time", "pseudo_level_0", "lat", "lon"],
            coords={"time": times},
        )

        result = calc_rootd(tilefrac)

        assert result.dims == ("lat", "lon")
        np.testing.assert_allclose(result.values, 4.6)

    def test_missing_pseudo_level_dimension_raises(self):
        """Defensive path: raise when tilefrac has no pseudo_level dimension."""
        times = xr.date_range("2000-01-01", periods=NT_R, freq="ME")
        data = np.zeros((NT_R, NJ_R, NI_R))
        tilefrac = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": times},
        )

        with pytest.raises(ValueError, match="No pseudo_level dimension found"):
            calc_rootd(tilefrac)
