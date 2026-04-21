"""Tests for access_moppy.derivations.calc_atmos."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations.calc_atmos import (
    calculate_areacella,
    cl_level_to_height,
    cli_level_to_height,
    clw_level_to_height,
)

# ---------------------------------------------------------------------------
# cli_level_to_height / clw_level_to_height / cl_level_to_height
# ---------------------------------------------------------------------------


def _make_level_ds(with_height=True):
    """Return a Dataset mimicking ACCESS atmosphere output on model levels."""
    nlev = 5
    nlat = 4
    nlon = 4
    rng = np.random.default_rng(0)

    data = rng.random((nlev, nlat, nlon))

    if with_height:
        height_data = np.linspace(0, 10000, nlev)
        ds = xr.Dataset(
            {
                "var": (
                    ["model_theta_level_number", "lat", "lon"],
                    data,
                ),
                "theta_level_height": (
                    ["model_theta_level_number"],
                    height_data,
                ),
            },
            coords={
                "model_theta_level_number": np.arange(nlev),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
            },
        )
    else:
        ds = xr.Dataset(
            {"var": (["lev", "lat", "lon"], data)},
            coords={
                "lev": np.arange(nlev),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
            },
        )
    return ds


class TestCliLevelToHeight:
    @pytest.mark.unit
    def test_transforms_level_coord_when_theta_height_present(self):
        ds = _make_level_ds(with_height=True)
        result = cli_level_to_height(ds)
        assert "lev" in result.dims
        assert "model_theta_level_number" not in result.dims

    @pytest.mark.unit
    def test_drops_theta_level_height_variable(self):
        ds = _make_level_ds(with_height=True)
        result = cli_level_to_height(ds)
        assert "theta_level_height" not in result

    @pytest.mark.unit
    def test_drops_model_theta_level_number_variable(self):
        ds = _make_level_ds(with_height=True)
        result = cli_level_to_height(ds)
        assert "model_theta_level_number" not in result.coords

    @pytest.mark.unit
    def test_no_transform_when_theta_height_absent(self):
        ds = _make_level_ds(with_height=False)
        result = cli_level_to_height(ds)
        assert "lev" in result.dims
        assert "model_theta_level_number" not in result.dims

    @pytest.mark.unit
    def test_lev_coord_values_match_theta_height(self):
        ds = _make_level_ds(with_height=True)
        expected_heights = ds["theta_level_height"].values.copy()
        result = cli_level_to_height(ds)
        np.testing.assert_array_equal(result["lev"].values, expected_heights)

    @pytest.mark.unit
    def test_data_values_unchanged(self):
        ds = _make_level_ds(with_height=True)
        original_values = ds["var"].values.copy()
        result = cli_level_to_height(ds)
        np.testing.assert_array_equal(result["var"].values, original_values)


class TestClwLevelToHeight:
    @pytest.mark.unit
    def test_same_behaviour_as_cli(self):
        ds = _make_level_ds(with_height=True)
        result_cli = cli_level_to_height(ds)
        result_clw = clw_level_to_height(ds)
        xr.testing.assert_identical(result_cli, result_clw)


class TestClLevelToHeight:
    @pytest.mark.unit
    def test_same_behaviour_as_cli(self):
        ds = _make_level_ds(with_height=True)
        result_cli = cli_level_to_height(ds)
        result_cl = cl_level_to_height(ds)
        xr.testing.assert_identical(result_cli, result_cl)


# ---------------------------------------------------------------------------
# calculate_areacella
# ---------------------------------------------------------------------------


class TestCalculateAreacella:
    @pytest.mark.unit
    def test_returns_dataset(self):
        result = calculate_areacella()
        assert isinstance(result, xr.Dataset)

    @pytest.mark.unit
    def test_has_areacella_variable(self):
        result = calculate_areacella()
        assert "areacella" in result

    @pytest.mark.unit
    def test_default_grid_size(self):
        result = calculate_areacella()
        assert result["areacella"].shape == (145, 192)

    @pytest.mark.unit
    def test_custom_grid_size(self):
        result = calculate_areacella(nlat=73, nlon=96)
        assert result["areacella"].shape == (73, 96)

    @pytest.mark.unit
    def test_all_values_positive(self):
        result = calculate_areacella()
        assert float(result["areacella"].min()) > 0.0

    @pytest.mark.unit
    def test_total_area_approximately_earth_surface(self):
        """Total grid-cell area should approximate 4π R² (Earth surface area)."""
        earth_radius = 6371000.0
        expected_total = 4 * np.pi * earth_radius**2  # ~5.1e14 m²
        result = calculate_areacella(earth_radius=earth_radius)
        total = float(result["areacella"].sum())
        # Allow 5 % tolerance due to discretisation
        assert total == pytest.approx(expected_total, rel=0.05)

    @pytest.mark.unit
    def test_units_attribute(self):
        result = calculate_areacella()
        assert result["areacella"].attrs.get("units") == "m2"

    @pytest.mark.unit
    def test_has_lat_and_lon_coords(self):
        result = calculate_areacella()
        assert "lat" in result["areacella"].coords
        assert "lon" in result["areacella"].coords

    @pytest.mark.unit
    def test_latitude_bounds(self):
        """Latitude coordinate should span -90 to +90."""
        result = calculate_areacella()
        lats = result["areacella"].coords["lat"].values
        assert float(lats.min()) == pytest.approx(-90.0)
        assert float(lats.max()) == pytest.approx(90.0)

    @pytest.mark.unit
    def test_polar_cells_smaller_than_equatorial(self):
        """Grid cells near the equator should be larger than at the poles."""
        result = calculate_areacella()
        area = result["areacella"]
        equatorial = float(area.isel(lat=area.sizes["lat"] // 2).mean())
        polar_north = float(area.isel(lat=-1).mean())
        assert equatorial > polar_north

    @pytest.mark.unit
    def test_custom_earth_radius(self):
        """Larger radius should produce larger total area."""
        result_small = calculate_areacella(earth_radius=6.0e6)
        result_large = calculate_areacella(earth_radius=7.0e6)
        total_small = float(result_small["areacella"].sum())
        total_large = float(result_large["areacella"].sum())
        assert total_large > total_small


# ---------------------------------------------------------------------------
# level_to_height
# ---------------------------------------------------------------------------


class TestLevelToHeight:
    """Tests for level_to_height()."""

    @pytest.mark.unit
    def test_with_theta_level_height_replaces_dim(self):
        """model_theta_level_number replaced by lev when theta_level_height present."""
        from access_moppy.derivations.calc_atmos import level_to_height

        ds = _make_level_ds(with_height=True)
        result = level_to_height(ds.copy())

        assert "lev" in result.dims
        assert "model_theta_level_number" not in result.dims
        assert "theta_level_height" not in result

    @pytest.mark.unit
    def test_without_theta_level_height_unchanged(self):
        """Dataset returned unmodified when theta_level_height is absent."""
        from access_moppy.derivations.calc_atmos import level_to_height

        ds = _make_level_ds(with_height=False)
        result = level_to_height(ds.copy())

        assert "lev" in result.dims
