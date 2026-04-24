"""Tests for access_moppy.derivations.calc_ocean."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_ocean import (
    calc_areacello,
    calc_global_ave_ocean,
    calc_hfds,
    calc_hfgeou,
    calc_msftbarot,
    calc_overturning_streamfunction,
    calc_rsdoabsorb,
    calc_total_mass_transport,
    calc_umo_corrected,
    calc_vmo_corrected,
    calc_zostoga,
    ocean_floor,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NT = 2
NZ = 4  # depth levels
NY = 6  # lat
NX = 8  # lon
RNG = np.random.default_rng(42)


def _make_3d_ocean_da(dims=None):
    """Return a 3-D ocean DataArray on (time, st_ocean, yt_ocean, xt_ocean)."""
    if dims is None:
        dims = ["time", "st_ocean", "yt_ocean", "xt_ocean"]
    data = RNG.random((NT, NZ, NY, NX))
    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    return xr.DataArray(
        data,
        dims=dims,
        coords={"time": times},
    )


def _make_2d_ocean_da():
    """Return a 2-D ocean DataArray on (yt_ocean, xt_ocean)."""
    data = RNG.random((NY, NX)) * 1e11
    return xr.DataArray(data, dims=["yt_ocean", "xt_ocean"])


def _make_surface_da():
    """Return a 2-D surface DataArray on (time, yt_ocean, xt_ocean)."""
    data = RNG.random((NT, NY, NX)) * 100.0
    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    return xr.DataArray(
        data, dims=["time", "yt_ocean", "xt_ocean"], coords={"time": times}
    )


# ---------------------------------------------------------------------------
# calc_global_ave_ocean
# ---------------------------------------------------------------------------


class TestCalcGlobalAveOcean:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        var = _make_3d_ocean_da()
        rho_dzt = _make_3d_ocean_da()
        area_t = _make_2d_ocean_da()
        result = calc_global_ave_ocean(var, rho_dzt, area_t)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_spatial_dims_removed(self):
        var = _make_3d_ocean_da()
        rho_dzt = _make_3d_ocean_da()
        area_t = _make_2d_ocean_da()
        result = calc_global_ave_ocean(var, rho_dzt, area_t)
        for dim in ("st_ocean", "yt_ocean", "xt_ocean"):
            assert dim not in result.dims

    @pytest.mark.unit
    def test_time_dim_preserved(self):
        var = _make_3d_ocean_da()
        rho_dzt = _make_3d_ocean_da()
        area_t = _make_2d_ocean_da()
        result = calc_global_ave_ocean(var, rho_dzt, area_t)
        assert "time" in result.dims

    @pytest.mark.unit
    def test_uniform_field_returns_same_value(self):
        """Weighted average of a uniform field equals that field's value."""
        var = xr.DataArray(
            np.ones((NT, NZ, NY, NX)) * 5.0,
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        rho_dzt = xr.DataArray(
            np.ones((NT, NZ, NY, NX)),
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        area_t = xr.DataArray(
            np.ones((NY, NX)),
            dims=["yt_ocean", "xt_ocean"],
        )
        result = calc_global_ave_ocean(var, rho_dzt, area_t)
        np.testing.assert_allclose(result.values, 5.0)


# ---------------------------------------------------------------------------
# calc_rsdoabsorb
# ---------------------------------------------------------------------------


class TestCalcRsdoabsorb:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        sw_heat = _make_3d_ocean_da()
        swflux = _make_surface_da()
        result = calc_rsdoabsorb(sw_heat, swflux)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_output_has_same_depth_size(self):
        sw_heat = _make_3d_ocean_da()
        swflux = _make_surface_da()
        result = calc_rsdoabsorb(sw_heat, swflux)
        assert result.sizes["st_ocean"] == sw_heat.sizes["st_ocean"]

    @pytest.mark.unit
    def test_surface_layer_includes_flux(self):
        """Surface layer of output = sw_heat[z=0] + swflux."""
        sw_heat = _make_3d_ocean_da()
        swflux = _make_surface_da()
        result = calc_rsdoabsorb(sw_heat, swflux)

        expected_surface = sw_heat.isel(st_ocean=0) + swflux
        np.testing.assert_allclose(
            result.isel(st_ocean=0).values, expected_surface.values, rtol=1e-10
        )

    @pytest.mark.unit
    def test_deeper_layers_unchanged(self):
        """Deeper layers should equal the original sw_heat values."""
        sw_heat = _make_3d_ocean_da()
        swflux = _make_surface_da()
        result = calc_rsdoabsorb(sw_heat, swflux)

        for z in range(1, NZ):
            np.testing.assert_allclose(
                result.isel(st_ocean=z).values,
                sw_heat.isel(st_ocean=z).values,
                rtol=1e-10,
            )


# ---------------------------------------------------------------------------
# calc_zostoga
# ---------------------------------------------------------------------------


class TestCalcZostoga:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        pot_temp = xr.DataArray(
            np.ones((NT, NZ, NY, NX)) * 10.0,
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        dzt = xr.DataArray(
            np.ones((NT, NZ, NY, NX)) * 100.0,
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        areacello = xr.DataArray(
            np.ones((NY, NX)) * 1e10,
            dims=["yt_ocean", "xt_ocean"],
        )
        result = calc_zostoga(pot_temp, dzt, areacello)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_time_dim_preserved(self):
        pot_temp = xr.DataArray(
            np.ones((NT, NZ, NY, NX)) * 10.0,
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        dzt = xr.DataArray(
            np.ones((NT, NZ, NY, NX)),
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        areacello = xr.DataArray(
            np.ones((NY, NX)),
            dims=["yt_ocean", "xt_ocean"],
        )
        result = calc_zostoga(pot_temp, dzt, areacello)
        assert "time" in result.dims

    @pytest.mark.unit
    def test_zero_thermosteric_at_reference_temp(self):
        """At the reference temperature (4°C), thermosteric change should be ~0."""
        pot_temp = xr.DataArray(
            np.ones((NT, NZ, NY, NX)) * 4.0,  # exactly at reference
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        dzt = xr.DataArray(
            np.ones((NT, NZ, NY, NX)) * 10.0,
            dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        )
        areacello = xr.DataArray(
            np.ones((NY, NX)),
            dims=["yt_ocean", "xt_ocean"],
        )
        result = calc_zostoga(pot_temp, dzt, areacello)
        np.testing.assert_allclose(result.values, 0.0, atol=1e-10)


# ---------------------------------------------------------------------------
# calc_overturning_streamfunction
# ---------------------------------------------------------------------------


class TestCalcOverturningStreamfunction:
    def _make_transport(self):
        data = RNG.random((NT, NZ, NY, NX)) * 1e9
        times = xr.date_range("2000-01-01", periods=NT, freq="ME")
        return xr.DataArray(
            data,
            dims=["time", "st_ocean", "yt_ocean", "xu_ocean"],
            coords={"time": times},
        )

    @pytest.mark.unit
    def test_returns_dataarray(self):
        ty = self._make_transport()
        result = calc_overturning_streamfunction(ty)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_longitude_dim_removed(self):
        ty = self._make_transport()
        result = calc_overturning_streamfunction(ty)
        assert "xu_ocean" not in result.dims

    @pytest.mark.unit
    def test_with_gm_component(self):
        ty = self._make_transport()
        gm = self._make_transport()
        result = calc_overturning_streamfunction(ty, gm_trans=gm)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_with_submeso_component(self):
        ty = self._make_transport()
        submeso = self._make_transport()
        result = calc_overturning_streamfunction(ty, submeso_trans=submeso)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_sverdrup_conversion(self):
        ty = self._make_transport()
        result_kg = calc_overturning_streamfunction(ty, to_sverdrups=False)
        result_sv = calc_overturning_streamfunction(ty, to_sverdrups=True)
        np.testing.assert_allclose(result_sv.values, result_kg.values * 1e-9)


# ---------------------------------------------------------------------------
# calc_total_mass_transport / calc_umo_corrected / calc_vmo_corrected
# ---------------------------------------------------------------------------


def _make_transport_da():
    data = RNG.random((NT, NZ, NY, NX)) * 1e8
    times = xr.date_range("2000-01-01", periods=NT, freq="ME")
    return xr.DataArray(
        data,
        dims=["time", "st_ocean", "yt_ocean", "xt_ocean"],
        coords={"time": times},
    )


class TestCalcTotalMassTransport:
    @pytest.mark.unit
    def test_resolved_only(self):
        resolved = _make_transport_da()
        result = calc_total_mass_transport(resolved)
        np.testing.assert_array_equal(result.values, resolved.values)

    @pytest.mark.unit
    def test_with_gm_component(self):
        resolved = _make_transport_da()
        gm = _make_transport_da()
        result = calc_total_mass_transport(resolved, gm_trans=gm)
        assert result.shape == resolved.shape

    @pytest.mark.unit
    def test_with_submeso_component(self):
        resolved = _make_transport_da()
        submeso = _make_transport_da()
        result = calc_total_mass_transport(resolved, submeso_trans=submeso)
        assert result.shape == resolved.shape

    @pytest.mark.unit
    def test_with_both_components(self):
        resolved = _make_transport_da()
        gm = _make_transport_da()
        submeso = _make_transport_da()
        result = calc_total_mass_transport(resolved, gm_trans=gm, submeso_trans=submeso)
        assert result.shape == resolved.shape


class TestCalcUmoCorrected:
    @pytest.mark.unit
    def test_resolves_to_total_mass_transport(self):
        tx = _make_transport_da()
        gm = _make_transport_da()
        result_umo = calc_umo_corrected(tx, tx_trans_gm=gm)
        result_total = calc_total_mass_transport(tx, gm_trans=gm)
        np.testing.assert_array_equal(result_umo.values, result_total.values)

    @pytest.mark.unit
    def test_no_gm_no_submeso(self):
        tx = _make_transport_da()
        result = calc_umo_corrected(tx)
        np.testing.assert_array_equal(result.values, tx.values)


class TestCalcVmoCorrected:
    @pytest.mark.unit
    def test_resolves_to_total_mass_transport(self):
        ty = _make_transport_da()
        result_vmo = calc_vmo_corrected(ty)
        np.testing.assert_array_equal(result_vmo.values, ty.values)

    @pytest.mark.unit
    def test_with_gm_and_submeso(self):
        ty = _make_transport_da()
        gm = _make_transport_da()
        submeso = _make_transport_da()
        result = calc_vmo_corrected(ty, ty_trans_gm=gm, ty_trans_submeso=submeso)
        assert result.shape == ty.shape


# ---------------------------------------------------------------------------
# ocean_floor
# ---------------------------------------------------------------------------


class TestOceanFloor:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        var = _make_3d_ocean_da()
        result = ocean_floor(var)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_depth_dim_removed(self):
        var = _make_3d_ocean_da()
        result = ocean_floor(var)
        assert "st_ocean" not in result.dims

    @pytest.mark.unit
    def test_selects_bottom_non_nan_value(self):
        """Verify that the deepest non-NaN value is returned for each column."""
        # Column 0: valid at all depths
        # Column 1: NaN below depth index 1
        data = np.array([[1.0, 2.0], [3.0, np.nan], [5.0, np.nan], [7.0, np.nan]])
        var = xr.DataArray(data, dims=["st_ocean", "xt_ocean"])
        result = ocean_floor(var, depth_dim="st_ocean")
        assert float(result.isel(xt_ocean=0).values) == pytest.approx(7.0)
        assert float(result.isel(xt_ocean=1).values) == pytest.approx(2.0)

    @pytest.mark.unit
    def test_all_nan_column_is_nan(self):
        """Columns with all NaN should return NaN."""
        data = np.array([[np.nan, 1.0], [np.nan, 2.0]])
        var = xr.DataArray(data, dims=["st_ocean", "xt_ocean"])
        result = ocean_floor(var, depth_dim="st_ocean")
        assert np.isnan(float(result.isel(xt_ocean=0).values))
        assert not np.isnan(float(result.isel(xt_ocean=1).values))

    @pytest.mark.unit
    def test_preserves_time_dimension(self):
        """ocean_floor should reduce depth but keep time and horizontal dims."""
        var = _make_3d_ocean_da()
        result = ocean_floor(var)
        assert "time" in result.dims
        assert result.sizes["time"] == NT
        assert result.sizes["yt_ocean"] == NY
        assert result.sizes["xt_ocean"] == NX
        assert "st_ocean" not in result.dims

    @pytest.mark.unit
    def test_tob_kelvin_to_celsius_conversion(self):
        """tob mapping chains ocean_floor then kelvin_to_celsius; verify output is in degC."""
        KELVIN_OFFSET = 273.15
        # Bottom-most valid value at each column is 290 K (shallow) and 275 K (deep)
        # Shape: (st_ocean=3, xt_ocean=2)
        data = np.array(
            [
                [300.0, 280.0],  # surface
                [295.0, np.nan],  # mid
                [290.0, np.nan],  # bottom (col 0)
            ]
        )
        var = xr.DataArray(data, dims=["st_ocean", "xt_ocean"])
        floor_values = ocean_floor(var, depth_dim="st_ocean")
        # Apply the kelvin_to_celsius lambda used in the mapping
        result = floor_values - KELVIN_OFFSET
        assert float(result.isel(xt_ocean=0).values) == pytest.approx(
            290.0 - KELVIN_OFFSET
        )
        assert float(result.isel(xt_ocean=1).values) == pytest.approx(
            280.0 - KELVIN_OFFSET
        )
        # Values should now be in a realistic ocean degC range
        assert float(result.max().values) < 100.0
        assert float(result.min().values) > -10.0


# ---------------------------------------------------------------------------
# calc_areacello
# ---------------------------------------------------------------------------


class TestCalcAreacello:
    def _make_area_and_ht(self, with_time=False):
        ny, nx = 4, 6
        area_data = np.ones((ny, nx)) * 1e11

        if with_time:
            times = xr.date_range("2000-01-01", periods=NT, freq="ME")
            area_data_t = np.broadcast_to(area_data, (NT, ny, nx)).copy()
            area_t = xr.DataArray(
                area_data_t,
                dims=["time", "yt_ocean", "xt_ocean"],
                coords={"time": times},
            )
        else:
            area_t = xr.DataArray(area_data, dims=["yt_ocean", "xt_ocean"])

        ht_data = np.ones((ny, nx)) * 500.0
        ht_data[0, 0] = 0.0  # land cell

        ht = xr.DataArray(ht_data, dims=["yt_ocean", "xt_ocean"])
        return area_t, ht

    @pytest.mark.unit
    def test_returns_dataarray(self):
        area_t, ht = self._make_area_and_ht()
        result = calc_areacello(area_t, ht)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_land_cells_masked(self):
        area_t, ht = self._make_area_and_ht()
        result = calc_areacello(area_t, ht)
        # Cell where ht == 0 should be NaN (masked)
        assert np.isnan(float(result.isel(yt_ocean=0, xt_ocean=0).values))

    @pytest.mark.unit
    def test_ocean_cells_preserved(self):
        area_t, ht = self._make_area_and_ht()
        result = calc_areacello(area_t, ht)
        # Non-land cell should retain the original area value
        assert float(result.isel(yt_ocean=1, xt_ocean=0).values) == pytest.approx(1e11)

    @pytest.mark.unit
    def test_drops_time_dim_by_default(self):
        area_t, ht = self._make_area_and_ht(with_time=True)
        result = calc_areacello(area_t, ht, drop_time=True)
        assert "time" not in result.dims

    @pytest.mark.unit
    def test_keeps_time_dim_when_requested(self):
        area_t, ht = self._make_area_and_ht(with_time=True)
        result = calc_areacello(area_t, ht, drop_time=False)
        assert "time" in result.dims


# ---------------------------------------------------------------------------
# calc_hfgeou
# ---------------------------------------------------------------------------


class TestCalcHfgeou:
    def _make_ht(self, with_time=False):
        ny, nx = 4, 6
        ht_data = np.ones((ny, nx)) * 500.0
        ht_data[0, 0] = 0.0  # land cell

        if with_time:
            times = xr.date_range("2000-01-01", periods=NT, freq="ME")
            ht_data_t = np.broadcast_to(ht_data, (NT, ny, nx)).copy()
            return xr.DataArray(
                ht_data_t,
                dims=["time", "yt_ocean", "xt_ocean"],
                coords={"time": times},
            )
        return xr.DataArray(ht_data, dims=["yt_ocean", "xt_ocean"])

    @pytest.mark.unit
    def test_returns_dataarray(self):
        ht = self._make_ht()
        result = calc_hfgeou(ht)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_ocean_cells_are_zero(self):
        ht = self._make_ht()
        result = calc_hfgeou(ht)
        assert float(result.isel(yt_ocean=1, xt_ocean=0).values) == pytest.approx(0.0)

    @pytest.mark.unit
    def test_land_cells_masked(self):
        ht = self._make_ht()
        result = calc_hfgeou(ht)
        assert np.isnan(float(result.isel(yt_ocean=0, xt_ocean=0).values))

    @pytest.mark.unit
    def test_no_time_dimension(self):
        ht = self._make_ht()
        result = calc_hfgeou(ht)
        assert "time" not in result.dims

    @pytest.mark.unit
    def test_drops_time_dimension_if_present(self):
        ht = self._make_ht(with_time=True)
        result = calc_hfgeou(ht)
        assert "time" not in result.dims

    @pytest.mark.unit
    def test_output_shape_matches_horizontal_grid(self):
        ht = self._make_ht()
        result = calc_hfgeou(ht)
        assert result.shape == ht.shape


# ---------------------------------------------------------------------------
# calc_msftbarot
# ---------------------------------------------------------------------------


def _make_tx_trans(nt=2, nz=4, ny=6, nx=8):
    """Create a 4-D (time, st_ocean, yt_ocean, xu_ocean) transport DataArray."""
    rng = np.random.default_rng(7)
    data = rng.random((nt, nz, ny, nx)) * 1e9
    times = xr.date_range("2000-01-01", periods=nt, freq="ME")
    return xr.DataArray(
        data,
        dims=["time", "st_ocean", "yt_ocean", "xu_ocean"],
        coords={"time": times},
    )


class TestCalcMsftbarot:
    @pytest.mark.unit
    def test_returns_dataarray(self):
        tx = _make_tx_trans()
        result = calc_msftbarot(tx)
        assert isinstance(result, xr.DataArray)

    @pytest.mark.unit
    def test_depth_dim_removed(self):
        """The depth dimension must be collapsed by the depth-sum."""
        tx = _make_tx_trans()
        result = calc_msftbarot(tx)
        assert "st_ocean" not in result.dims

    @pytest.mark.unit
    def test_output_shape(self):
        """Output shape should be (time, yt_ocean, xu_ocean)."""
        nt, nz, ny, nx = 2, 4, 6, 8
        tx = _make_tx_trans(nt=nt, nz=nz, ny=ny, nx=nx)
        result = calc_msftbarot(tx)
        assert result.sizes == {"time": nt, "yt_ocean": ny, "xu_ocean": nx}

    @pytest.mark.unit
    def test_southern_boundary_near_zero(self):
        """The first (southernmost) row is just the depth-sum of tx_trans there."""
        tx = _make_tx_trans()
        result = calc_msftbarot(tx)
        expected_first_row = tx.sum("st_ocean").isel(yt_ocean=0)
        np.testing.assert_allclose(
            result.isel(yt_ocean=0).values,
            expected_first_row.values,
            rtol=1e-12,
        )

    @pytest.mark.unit
    def test_monotonically_accumulates_uniform_eastward_flow(self):
        """For uniform positive (eastward) tx_trans the streamfunction must
        increase monotonically from south to north."""
        nt, nz, ny, nx = 2, 3, 5, 4
        data = np.ones((nt, nz, ny, nx)) * 1e8
        times = xr.date_range("2000-01-01", periods=nt, freq="ME")
        tx = xr.DataArray(
            data,
            dims=["time", "st_ocean", "yt_ocean", "xu_ocean"],
            coords={"time": times},
        )
        result = calc_msftbarot(tx)
        # Each step northward should increase psi by nz * 1e8
        diffs = result.diff("yt_ocean")
        assert (diffs.values > 0).all()

    @pytest.mark.unit
    def test_custom_coordinate_names(self):
        """Function must work with any depth/lat coordinate names (MOM6 style)."""
        nt, nz, ny, nx = 2, 3, 5, 4
        data = np.ones((nt, nz, ny, nx)) * 1e8
        times = xr.date_range("2000-01-01", periods=nt, freq="ME")
        tx = xr.DataArray(
            data,
            dims=["time", "zl", "yh", "xq"],
            coords={"time": times},
        )
        result = calc_msftbarot(tx, depth_coord="zl", lat_coord="yh")
        assert "zl" not in result.dims
        assert result.sizes == {"time": nt, "yh": ny, "xq": nx}

    @pytest.mark.unit
    def test_equivalent_to_depth_sum_then_cumsum(self):
        """Verify the result equals depth-sum then cumsum, element-wise."""
        tx = _make_tx_trans()
        result = calc_msftbarot(tx)
        expected = tx.sum("st_ocean").cumsum("yt_ocean")
        np.testing.assert_allclose(result.values, expected.values, rtol=1e-12)


# ---------------------------------------------------------------------------
# calc_hfds
# ---------------------------------------------------------------------------


class TestCalcHfds:
    """Tests for calc_hfds — surface downward heat flux with frazil fallback."""

    def _base_fields(self):
        """Return three base surface flux DataArrays."""
        times = xr.date_range("2000-01-01", periods=NT, freq="ME")
        shape = (NT, NY, NX)
        dims = ["time", "yt_ocean", "xt_ocean"]
        runoff = xr.DataArray(
            RNG.random(shape) * 10.0, dims=dims, coords={"time": times}
        )
        coupler = xr.DataArray(
            RNG.random(shape) * 50.0, dims=dims, coords={"time": times}
        )
        pme = xr.DataArray(RNG.random(shape) * 5.0, dims=dims, coords={"time": times})
        return runoff, coupler, pme

    @pytest.mark.unit
    def test_uses_frazil_3d_int_z_when_available(self):
        """When frazil_3d_int_z is provided it should be included in the sum."""
        runoff, coupler, pme = self._base_fields()
        frazil_3d = xr.DataArray(
            np.ones((NT, NY, NX)) * 2.0,
            dims=["time", "yt_ocean", "xt_ocean"],
        )
        frazil_2d = xr.DataArray(
            np.ones((NT, NY, NX)) * 99.0,  # should NOT be used
            dims=["time", "yt_ocean", "xt_ocean"],
        )
        result = calc_hfds(
            runoff, coupler, pme, frazil_3d_int_z=frazil_3d, frazil_2d=frazil_2d
        )
        expected = runoff + coupler + pme + frazil_3d
        np.testing.assert_allclose(result.values, expected.values, rtol=1e-10)

    @pytest.mark.unit
    def test_falls_back_to_frazil_2d(self):
        """When frazil_3d_int_z is None, frazil_2d should be used instead."""
        runoff, coupler, pme = self._base_fields()
        frazil_2d = xr.DataArray(
            np.ones((NT, NY, NX)) * 3.0,
            dims=["time", "yt_ocean", "xt_ocean"],
        )
        result = calc_hfds(
            runoff, coupler, pme, frazil_3d_int_z=None, frazil_2d=frazil_2d
        )
        expected = runoff + coupler + pme + frazil_2d
        np.testing.assert_allclose(result.values, expected.values, rtol=1e-10)

    @pytest.mark.unit
    def test_falls_back_to_frazil_2d_emits_warning(self, caplog):
        """Falling back to frazil_2d should log a warning."""
        import logging

        runoff, coupler, pme = self._base_fields()
        frazil_2d = xr.DataArray(
            np.ones((NT, NY, NX)), dims=["time", "yt_ocean", "xt_ocean"]
        )
        with caplog.at_level(
            logging.WARNING, logger="access_moppy.derivations.calc_ocean"
        ):
            calc_hfds(runoff, coupler, pme, frazil_3d_int_z=None, frazil_2d=frazil_2d)
        assert any("frazil_2d" in msg for msg in caplog.messages)

    @pytest.mark.unit
    def test_no_frazil_returns_base_sum(self):
        """When neither frazil term is provided, result equals the three base fluxes."""
        runoff, coupler, pme = self._base_fields()
        result = calc_hfds(runoff, coupler, pme, frazil_3d_int_z=None, frazil_2d=None)
        expected = runoff + coupler + pme
        np.testing.assert_allclose(result.values, expected.values, rtol=1e-10)

    @pytest.mark.unit
    def test_returns_dataarray(self):
        runoff, coupler, pme = self._base_fields()
        result = calc_hfds(runoff, coupler, pme)
        assert isinstance(result, xr.DataArray)
