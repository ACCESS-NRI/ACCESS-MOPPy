"""Tests for access_moppy.derivations.calc_seaice."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_seaice import (
    calc_hemi_seaice,
    calc_seaice_extent,
    calc_siarean,
    calc_siareas,
    calc_siextentn,
    calc_siextents,
    calc_sisnmassn,
    calc_sisnmasss,
    calc_sivoln,
    calc_sivols,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NJ = 8  # number of j-cells (latitude-like)
NI = 6  # number of i-cells (longitude-like)
NT = 3  # time steps


def _make_seaice_grid():
    """Return (siconc, tarea) on a simple (nj, ni) grid.

    The northern half is nj//2 … nj-1.
    siconc values are set so that north half ≥ south half for easy assertions.
    """
    rng = np.random.default_rng(42)
    siconc_data = rng.uniform(0.1, 0.8, size=(NT, NJ, NI)).astype(float)
    tarea_data = np.full((NJ, NI), 1e10, dtype=float)  # 1e10 m² per cell

    times = xr.date_range("2000-01-01", periods=NT, freq="ME")

    siconc = xr.DataArray(
        siconc_data,
        dims=["time", "nj", "ni"],
        coords={"time": times},
        attrs={"units": "1"},
    )
    tarea = xr.DataArray(
        tarea_data,
        dims=["nj", "ni"],
        attrs={"units": "m2"},
    )
    return siconc, tarea


def _make_sivol_grid():
    """Return (sivol, tarea) for volume calculations."""
    rng = np.random.default_rng(7)
    sivol_data = rng.uniform(0.5, 3.0, size=(NT, NJ, NI)).astype(float)
    tarea_data = np.full((NJ, NI), 1e10, dtype=float)

    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    sivol = xr.DataArray(sivol_data, dims=["time", "nj", "ni"], coords={"time": times})
    tarea = xr.DataArray(tarea_data, dims=["nj", "ni"])
    return sivol, tarea


def _make_sisnmass_grid():
    """Return (sisnmass, tarea) for snow-mass calculations."""
    rng = np.random.default_rng(13)
    sisnmass_data = rng.uniform(0.1, 1.0, size=(NT, NJ, NI)).astype(float)
    tarea_data = np.full((NJ, NI), 1e10, dtype=float)

    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    sisnmass = xr.DataArray(
        sisnmass_data, dims=["time", "nj", "ni"], coords={"time": times}
    )
    tarea = xr.DataArray(tarea_data, dims=["nj", "ni"])
    return sisnmass, tarea


def _make_hemi_grid(nlat=10, nlon=8):
    """Return (aice, areacello) with a proper 'lat' coordinate for calc_hemi_seaice."""
    rng = np.random.default_rng(99)
    lats = np.linspace(-90, 90, nlat)
    lons = np.linspace(0, 360, nlon, endpoint=False)
    times = xr.date_range("2000-01-01", periods=2, freq="ME")

    aice_data = rng.uniform(0.0, 1.0, size=(2, nlat, nlon))
    area_data = np.full((nlat, nlon), 1e12)  # 1e12 m² per cell

    aice = xr.DataArray(
        aice_data,
        dims=["time", "lat", "lon"],
        coords={"time": times, "lat": lats, "lon": lons},
    )
    areacello = xr.DataArray(
        area_data,
        dims=["lat", "lon"],
        coords={"lat": lats, "lon": lons},
    )
    return aice, areacello


# ---------------------------------------------------------------------------
# calc_hemi_seaice
# ---------------------------------------------------------------------------


class TestCalcHemiSeaice:
    @pytest.mark.unit
    def test_north_area_calculation(self):
        aice, areacello = _make_hemi_grid()
        result = calc_hemi_seaice(aice, areacello, "north", extent=False)
        assert isinstance(result, xr.DataArray)
        assert "time" in result.dims

    @pytest.mark.unit
    def test_south_area_calculation(self):
        aice, areacello = _make_hemi_grid()
        result = calc_hemi_seaice(aice, areacello, "south", extent=False)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_north_extent_calculation(self):
        aice, areacello = _make_hemi_grid()
        result = calc_hemi_seaice(aice, areacello, "north", extent=True)
        assert isinstance(result, xr.DataArray)
        # Extent is always non-negative
        assert float(result.min()) >= 0.0

    @pytest.mark.unit
    def test_south_extent_calculation(self):
        aice, areacello = _make_hemi_grid()
        result = calc_hemi_seaice(aice, areacello, "south", extent=True)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_invalid_hemisphere_raises(self):
        aice, areacello = _make_hemi_grid()
        with pytest.raises(ValueError, match="invalid hemisphere"):
            calc_hemi_seaice(aice, areacello, "east")

    @pytest.mark.unit
    def test_raises_when_no_lat_coord(self):
        """Raises ValueError when no latitude coordinate can be found."""
        da = xr.DataArray(np.ones((2, 4, 4)), dims=["time", "x", "y"])
        area = xr.DataArray(np.ones((4, 4)), dims=["x", "y"])
        with pytest.raises(ValueError, match="latitude"):
            calc_hemi_seaice(da, area, "north")

    @pytest.mark.unit
    def test_extent_uses_15pct_threshold(self):
        """Cells with concentration < 15 % should not contribute to extent."""
        nlat, nlon = 4, 4
        lats = np.array([-45.0, -15.0, 15.0, 45.0])
        lons = np.linspace(0, 360, nlon, endpoint=False)

        # All northern cells have ice = 0.05 (< threshold)
        aice_data = np.full((1, nlat, nlon), 0.05)
        area_data = np.full((nlat, nlon), 1e12)

        aice = xr.DataArray(
            aice_data,
            dims=["time", "lat", "lon"],
            coords={"lat": lats, "lon": lons, "time": [0]},
        )
        areacello = xr.DataArray(
            area_data, dims=["lat", "lon"], coords={"lat": lats, "lon": lons}
        )

        result = calc_hemi_seaice(aice, areacello, "north", extent=True)
        # Below 15 % threshold → extent should be 0
        assert float(result.squeeze().values) == pytest.approx(0.0)

    @pytest.mark.unit
    def test_lat_coord_from_tarea(self):
        """Latitude coordinate found in tarea when absent in invar."""
        nlat, nlon = 6, 4
        lats = np.linspace(-60, 60, nlat)

        aice_data = np.full((1, nlat, nlon), 0.5)
        area_data = np.full((nlat, nlon), 1e12)

        # aice has no lat coordinate
        aice = xr.DataArray(aice_data, dims=["time", "j", "i"], coords={"time": [0]})
        # areacello has lat coordinate
        areacello = xr.DataArray(
            area_data,
            dims=["j", "i"],
            coords={"lat": (["j", "i"], np.tile(lats, (nlon, 1)).T)},
        )

        result = calc_hemi_seaice(aice, areacello, "north", extent=False)
        assert isinstance(result, xr.DataArray)


# ---------------------------------------------------------------------------
# calc_seaice_extent
# ---------------------------------------------------------------------------


class TestCalcSeaiceExtent:
    @pytest.mark.unit
    def test_north_returns_dataarray(self):
        aice, areacello = _make_hemi_grid()
        result = calc_seaice_extent(aice, areacello, region="north")
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_south_returns_dataarray(self):
        aice, areacello = _make_hemi_grid()
        result = calc_seaice_extent(aice, areacello, region="south")
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_units_conversion(self):
        """Result should be in 1e6 km² (m² / 1e12)."""
        nlat, nlon = 4, 4
        lats = np.array([10.0, 20.0, 30.0, 40.0])  # all north
        lons = np.linspace(0, 360, nlon, endpoint=False)
        aice_data = np.full((1, nlat, nlon), 1.0)  # all cells > 15 %
        area_data = np.full((nlat, nlon), 1e12)  # 1e12 m² each → 1 × 1e6 km² each

        aice = xr.DataArray(
            aice_data,
            dims=["time", "lat", "lon"],
            coords={"lat": lats, "lon": lons, "time": [0]},
        )
        areacello = xr.DataArray(
            area_data, dims=["lat", "lon"], coords={"lat": lats, "lon": lons}
        )

        result = calc_seaice_extent(aice, areacello, region="north")
        # Each of the 16 cells contributes 1 × 1e6 km² → total = 16
        assert float(result.squeeze().values) == pytest.approx(16.0)

    @pytest.mark.unit
    def test_extent_non_negative(self):
        aice, areacello = _make_hemi_grid()
        result = calc_seaice_extent(aice, areacello, region="north")
        assert float(result.min()) >= 0.0


# ---------------------------------------------------------------------------
# calc_siarean / calc_siareas
# ---------------------------------------------------------------------------


class TestCalcSiarean:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siarean(siconc, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_has_time_dim(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siarean(siconc, tarea)
        assert "time" in result.dims

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siarean(siconc, tarea)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_non_negative(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siarean(siconc, tarea)
        assert float(result.min()) >= 0.0

    @pytest.mark.unit
    def test_unit_conversion_from_m2_to_1e6km2(self):
        """A single-cell grid with siconc=1.0 (fraction) and area=1e12 m² should give 1.0 unit."""
        siconc = xr.DataArray(
            [[[1.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        tarea = xr.DataArray([[1e12]], dims=["nj", "ni"])
        result = calc_siarean(siconc, tarea)
        # siconc * tarea / 1e12 = 1.0
        assert float(result.squeeze().values) == pytest.approx(1.0)


class TestCalcSiareas:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siareas(siconc, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siareas(siconc, tarea)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_non_negative(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siareas(siconc, tarea)
        assert float(result.min()) >= 0.0

    @pytest.mark.unit
    def test_north_plus_south_approximately_total(self):
        """North area + south area should equal total area (within floating point)."""
        siconc, tarea = _make_seaice_grid()
        north = calc_siarean(siconc, tarea)
        south = calc_siareas(siconc, tarea)
        total = (siconc * tarea).sum(["ni", "nj"]) / 1e12
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# calc_sivoln / calc_sivols
# ---------------------------------------------------------------------------


class TestCalcSivoln:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sivol, tarea = _make_sivol_grid()
        result = calc_sivoln(sivol, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sivol, tarea = _make_sivol_grid()
        result = calc_sivoln(sivol, tarea)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_unit_conversion_1e9(self):
        """1 m × 1e9 m² area = 1e9 m³ = 1 × 1e3 km³ (divide by 1e9)."""
        sivol = xr.DataArray(
            [[[1.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        tarea = xr.DataArray([[1e9]], dims=["nj", "ni"])
        result = calc_sivoln(sivol, tarea)
        assert float(result.squeeze().values) == pytest.approx(1.0)


class TestCalcSivols:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sivol, tarea = _make_sivol_grid()
        result = calc_sivols(sivol, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sivol, tarea = _make_sivol_grid()
        result = calc_sivols(sivol, tarea)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_north_plus_south_equals_total(self):
        sivol, tarea = _make_sivol_grid()
        north = calc_sivoln(sivol, tarea)
        south = calc_sivols(sivol, tarea)
        total = (sivol * tarea).sum(["ni", "nj"]) / 1e9
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# calc_sisnmassn / calc_sisnmasss
# ---------------------------------------------------------------------------


class TestCalcSisnmassn:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sisnmass, tarea = _make_sisnmass_grid()
        result = calc_sisnmassn(sisnmass, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sisnmass, tarea = _make_sisnmass_grid()
        result = calc_sisnmassn(sisnmass, tarea)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_non_negative(self):
        sisnmass, tarea = _make_sisnmass_grid()
        result = calc_sisnmassn(sisnmass, tarea)
        assert float(result.min()) >= 0.0


class TestCalcSisnmasss:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sisnmass, tarea = _make_sisnmass_grid()
        result = calc_sisnmasss(sisnmass, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sisnmass, tarea = _make_sisnmass_grid()
        result = calc_sisnmasss(sisnmass, tarea)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_north_plus_south_equals_total(self):
        sisnmass, tarea = _make_sisnmass_grid()
        north = calc_sisnmassn(sisnmass, tarea)
        south = calc_sisnmasss(sisnmass, tarea)
        total = (sisnmass * tarea).sum(["ni", "nj"])
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# calc_siextentn / calc_siextents
# ---------------------------------------------------------------------------


class TestCalcSiextentn:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siextentn(siconc, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_threshold_is_15pct(self):
        """Only cells with siconc > 0.15 (fraction) should count."""
        siconc = xr.DataArray(
            [[[0.10, 0.20]]],  # shape (1, 1, 2): first<0.15, second>0.15
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        tarea = xr.DataArray([[1e12, 1e12]], dims=["nj", "ni"])
        result = calc_siextentn(siconc, tarea)
        # Only one cell > 0.15 → 1e12 m² / 1e12 = 1.0
        assert float(result.squeeze().values) == pytest.approx(1.0)

    @pytest.mark.unit
    def test_non_negative(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siextentn(siconc, tarea)
        assert float(result.min()) >= 0.0


class TestCalcSiextents:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea = _make_seaice_grid()
        result = calc_siextents(siconc, tarea)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_north_plus_south_lte_total_extent(self):
        """Sum of north + south extent ≤ total cells (by construction)."""
        siconc, tarea = _make_seaice_grid()
        north = calc_siextentn(siconc, tarea)
        south = calc_siextents(siconc, tarea)
        total = ((siconc > 0.15) * tarea).sum(["ni", "nj"]) / 1e12
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)
