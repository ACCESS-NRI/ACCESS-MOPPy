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
    calc_sisnconc,
    calc_sisnmassn,
    calc_sisnmasss,
    calc_sisnthick,
    calc_sivoln,
    calc_sivols,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NJ = 8  # number of j-cells (latitude-like)
NI = 6  # number of i-cells (longitude-like)
NT = 3  # time steps


def _make_tlat(nj=NJ, ni=NI, first_north_row=None):
    """T-cell latitude on (nj, ni); rows from ``first_north_row`` on are north."""
    if first_north_row is None:
        first_north_row = nj // 2
    rows = np.arange(nj) - first_north_row + 0.5  # equator between two rows
    lat = np.repeat((rows * 10.0)[:, None], ni, axis=1)
    return xr.DataArray(lat, dims=["nj", "ni"], attrs={"units": "degrees_north"})


def _make_seaice_grid():
    """Return (siconc, tarea, tlat) on a simple (nj, ni) grid.

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
    return siconc, tarea, _make_tlat()


def _make_sivol_grid():
    """Return (sivol, tarea, tlat) for volume calculations."""
    rng = np.random.default_rng(7)
    sivol_data = rng.uniform(0.5, 3.0, size=(NT, NJ, NI)).astype(float)
    tarea_data = np.full((NJ, NI), 1e10, dtype=float)

    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    sivol = xr.DataArray(sivol_data, dims=["time", "nj", "ni"], coords={"time": times})
    tarea = xr.DataArray(tarea_data, dims=["nj", "ni"])
    return sivol, tarea, _make_tlat()


def _make_sisnmass_grid():
    """Return (sisnmass, tarea, tlat) for snow-mass calculations."""
    rng = np.random.default_rng(13)
    sisnmass_data = rng.uniform(0.1, 1.0, size=(NT, NJ, NI)).astype(float)
    tarea_data = np.full((NJ, NI), 1e10, dtype=float)

    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    sisnmass = xr.DataArray(
        sisnmass_data, dims=["time", "nj", "ni"], coords={"time": times}
    )
    tarea = xr.DataArray(tarea_data, dims=["nj", "ni"])
    return sisnmass, tarea, _make_tlat()


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
        with pytest.raises(ValueError, match="[Ii]nvalid hemisphere"):
            calc_hemi_seaice(aice, areacello, "east")

    @pytest.mark.unit
    def test_invalid_hemisphere_error_lists_supported(self):
        """Error message must list the supported hemisphere values."""
        aice, areacello = _make_hemi_grid()
        with pytest.raises(ValueError, match="'east'") as exc_info:
            calc_hemi_seaice(aice, areacello, "east")
        msg = str(exc_info.value)
        assert "'north'" in msg
        assert "'south'" in msg

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
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siarean(siconc, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_has_time_dim(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siarean(siconc, tarea, tlat)
        assert "time" in result.dims

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siarean(siconc, tarea, tlat)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_non_negative(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siarean(siconc, tarea, tlat)
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
        tlat = xr.DataArray([[45.0]], dims=["nj", "ni"])
        result = calc_siarean(siconc, tarea, tlat)
        # siconc * tarea / 1e12 = 1.0
        assert float(result.squeeze().values) == pytest.approx(1.0)


class TestCalcSiareas:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siareas(siconc, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siareas(siconc, tarea, tlat)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_non_negative(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siareas(siconc, tarea, tlat)
        assert float(result.min()) >= 0.0

    @pytest.mark.unit
    def test_north_plus_south_approximately_total(self):
        """North area + south area should equal total area (within floating point)."""
        siconc, tarea, tlat = _make_seaice_grid()
        north = calc_siarean(siconc, tarea, tlat)
        south = calc_siareas(siconc, tarea, tlat)
        total = (siconc * tarea).sum(["ni", "nj"]) / 1e12
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# calc_sivoln / calc_sivols
# ---------------------------------------------------------------------------


class TestCalcSivoln:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sivol, tarea, tlat = _make_sivol_grid()
        result = calc_sivoln(sivol, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sivol, tarea, tlat = _make_sivol_grid()
        result = calc_sivoln(sivol, tarea, tlat)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_unit_conversion_1e12(self):
        """1 m × 1e12 m² area = 1e12 m³ = 1e3 km³, i.e. 1 in units of 1e3 km³."""
        sivol = xr.DataArray(
            [[[1.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        tarea = xr.DataArray([[1e12]], dims=["nj", "ni"])
        tlat = xr.DataArray([[80.0]], dims=["nj", "ni"])
        result = calc_sivoln(sivol, tarea, tlat)
        assert float(result.squeeze().values) == pytest.approx(1.0)


class TestCalcSivols:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sivol, tarea, tlat = _make_sivol_grid()
        result = calc_sivols(sivol, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sivol, tarea, tlat = _make_sivol_grid()
        result = calc_sivols(sivol, tarea, tlat)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_north_plus_south_equals_total(self):
        sivol, tarea, tlat = _make_sivol_grid()
        north = calc_sivoln(sivol, tarea, tlat)
        south = calc_sivols(sivol, tarea, tlat)
        total = (sivol * tarea).sum(["ni", "nj"]) / 1e12
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# calc_sisnmassn / calc_sisnmasss
# ---------------------------------------------------------------------------


class TestCalcSisnmassn:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sisnmass, tarea, tlat = _make_sisnmass_grid()
        result = calc_sisnmassn(sisnmass, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sisnmass, tarea, tlat = _make_sisnmass_grid()
        result = calc_sisnmassn(sisnmass, tarea, tlat)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_non_negative(self):
        sisnmass, tarea, tlat = _make_sisnmass_grid()
        result = calc_sisnmassn(sisnmass, tarea, tlat)
        assert float(result.min()) >= 0.0


class TestCalcSisnmasss:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sisnmass, tarea, tlat = _make_sisnmass_grid()
        result = calc_sisnmasss(sisnmass, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        sisnmass, tarea, tlat = _make_sisnmass_grid()
        result = calc_sisnmasss(sisnmass, tarea, tlat)
        assert "nj" not in result.dims
        assert "ni" not in result.dims

    @pytest.mark.unit
    def test_north_plus_south_equals_total(self):
        sisnmass, tarea, tlat = _make_sisnmass_grid()
        north = calc_sisnmassn(sisnmass, tarea, tlat)
        south = calc_sisnmasss(sisnmass, tarea, tlat)
        total = (sisnmass * tarea).sum(["ni", "nj"])
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# calc_siextentn / calc_siextents
# ---------------------------------------------------------------------------


class TestCalcSiextentn:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siextentn(siconc, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_threshold_is_15pct(self):
        """Cells count from siconc >= 0.15 (fraction), inclusive."""
        siconc = xr.DataArray(
            [[[0.10, 0.15, 0.20]]],  # below, exactly at, above the threshold
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        tarea = xr.DataArray([[1e12, 1e12, 1e12]], dims=["nj", "ni"])
        tlat = xr.DataArray([[70.0, 70.0, 70.0]], dims=["nj", "ni"])
        result = calc_siextentn(siconc, tarea, tlat)
        # Two cells >= 0.15 → 2e12 m² / 1e12 = 2.0
        assert float(result.squeeze().values) == pytest.approx(2.0)

    @pytest.mark.unit
    def test_non_negative(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siextentn(siconc, tarea, tlat)
        assert float(result.min()) >= 0.0


class TestCalcSiextents:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        siconc, tarea, tlat = _make_seaice_grid()
        result = calc_siextents(siconc, tarea, tlat)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_north_plus_south_lte_total_extent(self):
        """Sum of north + south extent ≤ total cells (by construction)."""
        siconc, tarea, tlat = _make_seaice_grid()
        north = calc_siextentn(siconc, tarea, tlat)
        south = calc_siextents(siconc, tarea, tlat)
        total = ((siconc >= 0.15) * tarea).sum(["ni", "nj"]) / 1e12
        np.testing.assert_allclose((north + south).values, total.values, rtol=1e-10)


# ---------------------------------------------------------------------------
# Hemispheric sums on the real CICE layout
# ---------------------------------------------------------------------------

_HEMI_FUNCS = [
    (calc_siarean, "north"),
    (calc_siareas, "south"),
    (calc_sivoln, "north"),
    (calc_sivols, "south"),
    (calc_sisnmassn, "north"),
    (calc_sisnmasss, "south"),
    (calc_siextentn, "north"),
    (calc_siextents, "south"),
]


class TestHemisphereSums:
    @pytest.mark.unit
    @pytest.mark.parametrize(("func", "hemi"), _HEMI_FUNCS)
    def test_hemisphere_follows_latitude_not_row_index(self, func, hemi):
        """The ACCESS tripolar equator is not at nj // 2 (rows 136/137 of 300).

        Ice only in the rows between the equator and the middle row must be
        counted in the northern hemisphere, not the southern.
        """
        nj, first_north = 10, 3
        tlat = _make_tlat(nj=nj, ni=2, first_north_row=first_north)
        field = xr.DataArray(np.zeros((1, nj, 2)), dims=["time", "nj", "ni"])
        field[:, first_north : nj // 2, :] = 1.0  # north of the equator, below nj//2
        tarea = xr.DataArray(np.full((nj, 2), 1e12), dims=["nj", "ni"])
        result = float(func(field, tarea, tlat).squeeze())
        assert (result > 0) == (hemi == "north")

    @pytest.mark.unit
    @pytest.mark.parametrize(("func", "hemi"), _HEMI_FUNCS)
    def test_ocean_grid_area_is_rejected(self, func, hemi):
        """An ocean (yt_ocean, xt_ocean) area must not broadcast against (nj, ni).

        That pairing built a (time, nj, ni, yt_ocean, xt_ocean) outer product
        and ran the job out of memory.
        """
        field, _, tlat = _make_seaice_grid()
        areacello = xr.DataArray(np.ones((NJ, NI)), dims=["yt_ocean", "xt_ocean"])
        with pytest.raises(ValueError, match="tarea dims"):
            func(field, areacello, tlat)

    @pytest.mark.unit
    @pytest.mark.parametrize(("func", "hemi"), _HEMI_FUNCS)
    def test_land_cells_are_skipped(self, func, hemi):
        """CICE writes NaN over land; it must not turn the total into NaN."""
        field, tarea, tlat = _make_seaice_grid()
        field = field.where(field.ni != 0)  # first column is land
        result = func(field, tarea, tlat)
        assert not np.isnan(result.values).any()

    @pytest.mark.unit
    def test_extent_ignores_values_above_one(self):
        """Fill or bad values above a fraction of 1 do not count as extent."""
        siconc = xr.DataArray([[[1.0, 5.0]]], dims=["time", "nj", "ni"])
        tarea = xr.DataArray([[1e12, 1e12]], dims=["nj", "ni"])
        tlat = xr.DataArray([[-70.0, -70.0]], dims=["nj", "ni"])
        assert float(calc_siextents(siconc, tarea, tlat).squeeze()) == pytest.approx(
            1.0
        )

    @pytest.mark.unit
    def test_snow_mass_is_in_kg(self):
        """sisnmass (kg m-2) × area (m²) summed gives kg, with no rescaling."""
        sisnmass = xr.DataArray([[[2.0]]], dims=["time", "nj", "ni"])
        tarea = xr.DataArray([[3.0]], dims=["nj", "ni"])
        tlat = xr.DataArray([[-60.0]], dims=["nj", "ni"])
        assert float(calc_sisnmasss(sisnmass, tarea, tlat).squeeze()) == 6.0


# ---------------------------------------------------------------------------
# Helpers for sisnconc / sisnthick (siconc in %, sisnmass in kg m-2)
# ---------------------------------------------------------------------------


def _make_snow_grid():
    """Return (sisnmass, siconc) with siconc in % (0-100) and some zero-ice cells."""
    times = xr.date_range("2000-01-01", periods=NT, freq="ME")

    # siconc in %: some cells at 0 (no ice)
    siconc_data = np.array(
        [[[0.0, 50.0, 100.0], [25.0, 75.0, 0.0]]] * NT, dtype=float
    )  # shape (NT, 2, 3)
    sisnmass_data = np.full((NT, 2, 3), 31.7, dtype=float)  # kg m-2

    siconc = xr.DataArray(
        siconc_data, dims=["time", "nj", "ni"], coords={"time": times}
    )
    sisnmass = xr.DataArray(
        sisnmass_data, dims=["time", "nj", "ni"], coords={"time": times}
    )
    return sisnmass, siconc


# ---------------------------------------------------------------------------
# calc_sisnconc
# ---------------------------------------------------------------------------


class TestCalcSisnconc:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        _, siconc = _make_snow_grid()
        result = calc_sisnconc(siconc)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_preserves_dims(self):
        _, siconc = _make_snow_grid()
        result = calc_sisnconc(siconc)
        assert result.dims == siconc.dims

    @pytest.mark.unit
    def test_binary_output(self):
        """Result must contain only 0.0 or 100.0."""
        _, siconc = _make_snow_grid()
        result = calc_sisnconc(siconc)
        unique = np.unique(result.values)
        assert set(unique).issubset({0.0, 100.0})

    @pytest.mark.unit
    def test_one_where_ice_present(self):
        """Cells with siconc > 0 should return 1.0."""
        siconc = xr.DataArray(
            [[[50.0, 100.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        result = calc_sisnconc(siconc)
        np.testing.assert_array_equal(result.values, [[[100.0, 100.0]]])

    @pytest.mark.unit
    def test_zero_where_no_ice(self):
        """Cells with siconc == 0 should return 0.0."""
        siconc = xr.DataArray(
            [[[0.0, 0.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        result = calc_sisnconc(siconc)
        np.testing.assert_array_equal(result.values, [[[0.0, 0.0]]])

    @pytest.mark.unit
    def test_mixed_ice_no_ice(self):
        """Mixed grid: ice-present cells → 1, no-ice cells → 0."""
        siconc = xr.DataArray(
            [[[0.0, 30.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        result = calc_sisnconc(siconc)
        np.testing.assert_array_equal(result.values, [[[0.0, 100.0]]])


# ---------------------------------------------------------------------------
# calc_sisnthick
# ---------------------------------------------------------------------------


class TestCalcSisnthick:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sisnmass, siconc = _make_snow_grid()
        result = calc_sisnthick(sisnmass, siconc)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_preserves_dims(self):
        sisnmass, siconc = _make_snow_grid()
        result = calc_sisnthick(sisnmass, siconc)
        assert result.dims == sisnmass.dims

    @pytest.mark.unit
    def test_zero_where_no_ice(self):
        """Cells with siconc == 0 should return 0.0, not NaN."""
        siconc = xr.DataArray(
            [[[0.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        sisnmass = xr.DataArray(
            [[[10.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        result = calc_sisnthick(sisnmass, siconc)
        assert float(result.squeeze().values) == pytest.approx(0.0)
        assert not np.isnan(float(result.squeeze().values))

    @pytest.mark.unit
    def test_known_value_full_ice_cover(self):
        """sisnmass=317 kg m-2, siconc=100% → sisnthick = 317/(317*1.0) = 1.0 m."""
        siconc = xr.DataArray(
            [[[100.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        sisnmass = xr.DataArray(
            [[[317.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        result = calc_sisnthick(sisnmass, siconc)
        assert float(result.squeeze().values) == pytest.approx(1.0)

    @pytest.mark.unit
    def test_known_value_partial_ice_cover(self):
        """sisnmass=31.7 kg m-2, siconc=50% → sisnthick = 31.7/(317*0.5) = 0.2 m."""
        siconc = xr.DataArray(
            [[[50.0]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        sisnmass = xr.DataArray(
            [[[31.7]]],
            dims=["time", "nj", "ni"],
            coords={"time": xr.date_range("2000-01-01", periods=1, freq="ME")},
        )
        result = calc_sisnthick(sisnmass, siconc)
        assert float(result.squeeze().values) == pytest.approx(0.2)

    @pytest.mark.unit
    def test_non_negative(self):
        sisnmass, siconc = _make_snow_grid()
        result = calc_sisnthick(sisnmass, siconc)
        assert float(result.min()) >= 0.0

    @pytest.mark.unit
    def test_no_nan_where_ice_present(self):
        """No NaN values should appear where sea ice is present."""
        sisnmass, siconc = _make_snow_grid()
        result = calc_sisnthick(sisnmass, siconc)
        ice_present = siconc > 0
        assert not np.any(np.isnan(result.values[ice_present.values]))
