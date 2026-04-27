from unittest.mock import patch

import numpy as np
import pytest

from access_moppy.ocean_supergrid import Supergrid
from tests.mocks.mock_data import create_mock_supergrid_dataset


class TestSupergrid:
    """Unit tests for the Supergrid class."""

    @pytest.fixture
    def mock_supergrid_file(self, tmp_path):
        """Create a temporary mock supergrid NetCDF file."""
        supergrid_ds = create_mock_supergrid_dataset(ny=7, nx=9)
        filepath = tmp_path / "mock_supergrid.nc"
        supergrid_ds.to_netcdf(filepath)
        return str(filepath)

    @pytest.fixture
    def supergrid_instance(self, mock_supergrid_file):
        """Create a Supergrid instance with mocked file loading."""
        with patch.object(
            Supergrid, "get_supergrid_path", return_value=mock_supergrid_file
        ):
            sg = Supergrid("100 km")
        return sg

    # ==================== Initialization Tests ====================

    @pytest.mark.unit
    def test_init_loads_supergrid_correctly(self, mock_supergrid_file):
        """Test that __init__ sets resolution and loads supergrid data."""
        with patch.object(
            Supergrid, "get_supergrid_path", return_value=mock_supergrid_file
        ):
            sg = Supergrid("100 km")

        assert sg.nominal_resolution == "100 km"
        assert sg.supergrid is not None
        assert "x" in sg.supergrid
        assert "y" in sg.supergrid

    # ==================== get_supergrid_path Tests ====================

    @pytest.mark.unit
    def test_get_supergrid_path_on_gadi(self):
        """Test that get_supergrid_path returns Gadi path when file exists."""
        gadi_path = "/g/data/xp65/public/apps/access_moppy_data/grids/mom1deg.nc"

        with patch("os.path.exists", return_value=True):
            with patch.object(Supergrid, "load_supergrid"):
                sg = Supergrid.__new__(Supergrid)
                sg.nominal_resolution = "100 km"
                path = sg.get_supergrid_path("100 km")

        assert path == gadi_path

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "resolution,expected_file",
        [
            ("100 km", "mom1deg.nc"),
            ("25 km", "mom025deg.nc"),
            ("10 km", "mom01deg.nc"),
        ],
    )
    def test_get_supergrid_path_resolution_mapping(self, resolution, expected_file):
        """Test that resolutions map to correct filenames."""
        with patch("os.path.exists", return_value=True):
            with patch.object(Supergrid, "load_supergrid"):
                sg = Supergrid.__new__(Supergrid)
                sg.nominal_resolution = resolution
                path = sg.get_supergrid_path(resolution)

        assert expected_file in path

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "resolution,error_match",
        [
            ("50 km", "Unknown or unsupported nominal resolution"),
            (None, "nominal_resolution must be provided"),
        ],
    )
    def test_get_supergrid_path_invalid_resolution(self, resolution, error_match):
        """Test that invalid resolutions raise appropriate errors."""
        with patch.object(Supergrid, "load_supergrid"):
            sg = Supergrid.__new__(Supergrid)
            sg.nominal_resolution = resolution

            with pytest.raises(ValueError, match=error_match):
                sg.get_supergrid_path(resolution)

    # ==================== load_supergrid Tests ====================

    @pytest.mark.unit
    @pytest.mark.parametrize("cell_type", ["hcell", "qcell", "ucell", "vcell"])
    def test_load_supergrid_creates_cell_arrays(self, supergrid_instance, cell_type):
        """Test that load_supergrid creates all cell type arrays with correct structure."""
        sg = supergrid_instance
        sg._compute_grid()

        # Check centres exist
        assert hasattr(sg, f"{cell_type}_centres_x")
        assert hasattr(sg, f"{cell_type}_centres_y")

        # Check corners exist with 4 vertices
        corners_x = getattr(sg, f"{cell_type}_corners_x")
        corners_y = getattr(sg, f"{cell_type}_corners_y")
        assert corners_x.shape[-1] == 4
        assert corners_y.shape[-1] == 4

    @pytest.mark.unit
    def test_load_supergrid_cell_dimensions_relationship(self, supergrid_instance):
        """Test that q-cell has one more point than h-cell in each direction."""
        sg = supergrid_instance
        sg._compute_grid()

        h_shape = sg.hcell_centres_x.shape
        q_shape = sg.qcell_centres_x.shape

        assert q_shape[0] == h_shape[0] + 1
        assert q_shape[1] == h_shape[1] + 1

    # ==================== extract_grid Tests - B-grid ====================

    @pytest.mark.unit
    @pytest.mark.parametrize("grid_type", ["T", "U", "V", "C"])
    def test_extract_grid_b_grid_all_types(self, supergrid_instance, grid_type):
        """Test extract_grid returns correct structure for all B-grid types."""
        grid_info = supergrid_instance.extract_grid(grid_type=grid_type, arakawa="B")

        # All grid types should return these keys
        expected_keys = [
            "latitude",
            "longitude",
            "vertices_latitude",
            "vertices_longitude",
            "i",
            "j",
            "vertices",
        ]
        for key in expected_keys:
            assert key in grid_info

        # Vertices should have 4 corners
        assert grid_info["vertices_latitude"].shape[-1] == 4

    # ==================== extract_grid Tests - C-grid ====================

    @pytest.mark.unit
    @pytest.mark.parametrize("grid_type", ["T", "U", "V", "C"])
    @pytest.mark.parametrize("symmetric", [True, False])
    def test_extract_grid_c_grid_all_types(
        self, supergrid_instance, grid_type, symmetric
    ):
        """Test extract_grid returns correct structure for all C-grid types."""
        grid_info = supergrid_instance.extract_grid(
            grid_type=grid_type, arakawa="C", symmetric=symmetric
        )

        assert "latitude" in grid_info
        assert "longitude" in grid_info
        assert grid_info["vertices_latitude"].shape[-1] == 4

    @pytest.mark.unit
    def test_extract_grid_c_grid_symmetric_vs_asymmetric_dimensions(
        self, supergrid_instance
    ):
        """Test that asymmetric mode has fewer points than symmetric."""
        sg = supergrid_instance

        # U-cell: asymmetric has one fewer column
        u_sym = sg.extract_grid(grid_type="U", arakawa="C", symmetric=True)
        u_asym = sg.extract_grid(grid_type="U", arakawa="C", symmetric=False)
        assert u_asym["longitude"].shape[1] == u_sym["longitude"].shape[1] - 1

        # V-cell: asymmetric has one fewer row
        v_sym = sg.extract_grid(grid_type="V", arakawa="C", symmetric=True)
        v_asym = sg.extract_grid(grid_type="V", arakawa="C", symmetric=False)
        assert v_asym["latitude"].shape[0] == v_sym["latitude"].shape[0] - 1

    # ==================== extract_grid Error Handling ====================

    @pytest.mark.unit
    def test_extract_grid_c_grid_requires_symmetric(self, supergrid_instance):
        """Test that C-grid requires symmetric parameter."""
        with pytest.raises(ValueError, match="Must specify symmetric"):
            supergrid_instance.extract_grid(grid_type="T", arakawa="C", symmetric=None)

    @pytest.mark.unit
    def test_extract_grid_unsupported_arakawa(self, supergrid_instance):
        """Test that unsupported Arakawa grid raises error."""
        with pytest.raises(ValueError, match="arakawa=.* is not supported"):
            supergrid_instance.extract_grid(grid_type="T", arakawa="A")

    @pytest.mark.unit
    @pytest.mark.parametrize("arakawa,symmetric", [("B", None), ("C", True)])
    def test_extract_grid_unsupported_grid_type(
        self, supergrid_instance, arakawa, symmetric
    ):
        """Test that unsupported grid type raises error."""
        with pytest.raises(ValueError, match="is not a supported grid_type"):
            supergrid_instance.extract_grid(
                grid_type="X", arakawa=arakawa, symmetric=symmetric
            )

    # ==================== extract_grid Output Validation ====================

    @pytest.mark.unit
    def test_extract_grid_longitude_normalized(self, supergrid_instance):
        """Test that longitude is normalized to [0, 360) range."""
        grid_info = supergrid_instance.extract_grid(grid_type="T", arakawa="B")

        lon = grid_info["longitude"].values
        assert np.all(lon >= 0)
        assert np.all(lon < 360)

    @pytest.mark.unit
    def test_extract_grid_output_structure(self, supergrid_instance):
        """Test DataArray dimensions and coordinate values."""
        grid_info = supergrid_instance.extract_grid(grid_type="T", arakawa="B")

        # Check dimensions
        assert grid_info["latitude"].dims == ("j", "i")
        assert grid_info["longitude"].dims == ("j", "i")
        assert grid_info["vertices_latitude"].dims == ("j", "i", "vertices")

        # Check vertices shape matches spatial dims
        lat = grid_info["latitude"]
        lat_bnds = grid_info["vertices_latitude"]
        assert lat_bnds.shape[:2] == lat.shape

        # Check coordinate values are sequential integers
        np.testing.assert_array_equal(
            grid_info["i"].values, np.arange(len(grid_info["i"]))
        )
        np.testing.assert_array_equal(
            grid_info["j"].values, np.arange(len(grid_info["j"]))
        )
        np.testing.assert_array_equal(
            grid_info["vertices"].values, np.array([0, 1, 2, 3])
        )
