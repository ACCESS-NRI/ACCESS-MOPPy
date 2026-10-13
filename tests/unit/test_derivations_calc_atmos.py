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
    def test_transforms_level_coord_when_theta_height_present(self):
        ds = _make_level_ds(with_height=True)
        ds = ds.rename({"var": "cl"})
        result = cl_level_to_height(ds)
        assert "lev" in result.dims
        assert "model_theta_level_number" not in result.dims

    @pytest.mark.unit
    def test_multiplies_cl_by_100(self):
        ds = _make_level_ds(with_height=True)
        ds = ds.rename({"var": "cl"})
        original_values = ds["cl"].values.copy()
        result = cl_level_to_height(ds)
        np.testing.assert_array_almost_equal(result["cl"].values, original_values * 100)
        assert result["cl"].values.min() >= 0
        assert result["cl"].values.max() <= 100

    @pytest.mark.unit
    def test_skips_percentage_conversion_when_cl_absent(self):
        ds = _make_level_ds(with_height=True)
        result = cl_level_to_height(ds)
        assert "var" in result
        np.testing.assert_array_equal(result["var"].values, ds["var"].values)


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
# calculate_areacella on the staggered points
# ---------------------------------------------------------------------------


#: The ACCESS-ESM1.6 N96 grid, read from the model's own output. A measure whose
#: coordinates do not reproduce these exactly cannot be attached to the fields
#: written on that point, which is the whole reason grid_key exists.
ESM1_6_GRID = {
    "default": {
        "shape": (145, 192),
        "lat": (-90.0, -88.75, 90.0),
        "lon": (0.0, 1.875, 358.125),
    },
    "U": {
        "shape": (145, 192),
        "lat": (-90.0, -88.75, 90.0),
        "lon": (0.9375, 2.8125, 359.0625),
    },
    "V": {
        "shape": (144, 192),
        "lat": (-89.375, -88.125, 89.375),
        "lon": (0.0, 1.875, 358.125),
    },
    "other": {
        "shape": (144, 192),
        "lat": (-89.375, -88.125, 89.375),
        "lon": (0.9375, 2.8125, 359.0625),
    },
}


class TestCalculateAreacellaGridKey:
    """ACCESS writes atmosphere fields on four points; areacella must follow."""

    @pytest.mark.unit
    @pytest.mark.parametrize("grid_key", sorted(ESM1_6_GRID))
    def test_shape_matches_the_model_grid(self, grid_key):
        result = calculate_areacella(grid_key=grid_key)
        assert result["areacella"].shape == ESM1_6_GRID[grid_key]["shape"]

    @pytest.mark.unit
    @pytest.mark.parametrize("grid_key", sorted(ESM1_6_GRID))
    def test_coordinates_match_the_model_grid(self, grid_key):
        """The first two and last coordinate values pin both origin and spacing."""
        result = calculate_areacella(grid_key=grid_key)
        expected = ESM1_6_GRID[grid_key]
        lat = result["areacella"].coords["lat"].values
        lon = result["areacella"].coords["lon"].values
        assert (lat[0], lat[1], lat[-1]) == pytest.approx(expected["lat"])
        assert (lon[0], lon[1], lon[-1]) == pytest.approx(expected["lon"])

    @pytest.mark.unit
    @pytest.mark.parametrize("grid_key", sorted(ESM1_6_GRID))
    def test_every_point_tiles_the_sphere(self, grid_key):
        """Theta rows are half cells at the poles, lat_v rows are whole cells;
        either way the rows cover the globe exactly once."""
        earth_radius = 6371000.0
        result = calculate_areacella(grid_key=grid_key, earth_radius=earth_radius)
        total = float(result["areacella"].sum())
        assert total == pytest.approx(4 * np.pi * earth_radius**2, rel=1e-12)

    @pytest.mark.unit
    def test_default_grid_key_is_the_theta_grid(self):
        """The no-argument call must keep producing what it always has."""
        assert np.array_equal(
            calculate_areacella()["areacella"].values,
            calculate_areacella(grid_key="default")["areacella"].values,
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("staggered", "same_as"), [("U", "default"), ("other", "V")]
    )
    def test_longitude_stagger_shifts_coordinates_but_not_areas(
        self, staggered, same_as
    ):
        """Cell area depends on the latitude bounds and nlon only, so a
        half-cell shift in longitude moves the coordinates and nothing else."""
        shifted = calculate_areacella(grid_key=staggered)["areacella"]
        straight = calculate_areacella(grid_key=same_as)["areacella"]
        assert np.array_equal(shifted.values, straight.values)
        assert not np.array_equal(
            shifted.coords["lon"].values, straight.coords["lon"].values
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("grid_key", "point_name"),
        [("default", "theta"), ("U", "u"), ("V", "v"), ("other", "uv")],
    )
    def test_comment_names_the_point_not_the_internal_key(self, grid_key, point_name):
        """The file says which grid it is on -- not saying so is how a measure
        ends up attached to fields it does not describe. It is named the way the
        UM names the point, so the internal key can be renamed without
        rewriting published metadata."""
        comment = calculate_areacella(grid_key=grid_key)["areacella"].attrs["comment"]

        assert f"({point_name} points)" in comment

    @pytest.mark.unit
    def test_unknown_grid_key_is_rejected(self):
        """A typo must not silently fall back to the theta grid: that is the
        defect this parameter exists to prevent."""
        with pytest.raises(ValueError, match="Expected one of"):
            calculate_areacella(grid_key="UV")


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
