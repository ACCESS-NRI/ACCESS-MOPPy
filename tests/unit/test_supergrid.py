from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import dask.array as da
import numpy as np
import pytest
import xarray as xr

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
        with patch.object(Supergrid, "get_supergrid_path", return_value=mock_supergrid_file):
            sg = Supergrid("100 km")
        return sg

    # ==================== Initialization Tests ====================

    @pytest.mark.unit
    def test_init_sets_nominal_resolution(self, mock_supergrid_file):
        """Test that __init__ sets the nominal resolution correctly."""
        with patch.object(Supergrid, "get_supergrid_path", return_value=mock_supergrid_file):
            sg = Supergrid("100 km")
        
        assert sg.nominal_resolution == "100 km"

    @pytest.mark.unit
    def test_init_loads_supergrid(self, mock_supergrid_file):
        """Test that __init__ loads supergrid data."""
        with patch.object(Supergrid, "get_supergrid_path", return_value=mock_supergrid_file):
            sg = Supergrid("100 km")
        
        assert sg.supergrid is not None
        assert "x" in sg.supergrid
        assert "y" in sg.supergrid

    # ==================== get_supergrid_path Tests ====================

    @pytest.mark.unit
    def test_get_supergrid_path_on_gadi(self, mock_supergrid_file):
        """Test that get_supergrid_path returns Gadi path when file exists."""
        gadi_path = "/g/data/xp65/public/apps/access_moppy_data/grids/mom1deg.nc"
        
        with patch("os.path.exists", return_value=True):
            with patch.object(Supergrid, "load_supergrid"):
                sg = Supergrid.__new__(Supergrid)
                sg.nominal_resolution = "100 km"
                path = sg.get_supergrid_path("100 km")
        
        assert path == gadi_path

    @pytest.mark.unit
    def test_get_supergrid_path_unsupported_resolution(self):
        """Test that get_supergrid_path raises error for unsupported resolution."""
        with patch.object(Supergrid, "load_supergrid"):
            sg = Supergrid.__new__(Supergrid)
            sg.nominal_resolution = "50 km"
            
            with pytest.raises(ValueError, match="Unknown or unsupported nominal resolution"):
                sg.get_supergrid_path("50 km")

    @pytest.mark.unit
    def test_get_supergrid_path_empty_resolution(self):
        """Test that get_supergrid_path raises error for empty resolution."""
        with patch.object(Supergrid, "load_supergrid"):
            sg = Supergrid.__new__(Supergrid)
            sg.nominal_resolution = None
            
            with pytest.raises(ValueError, match="nominal_resolution must be provided"):
                sg.get_supergrid_path(None)

    @pytest.mark.unit
    def test_get_supergrid_path_resolution_mapping(self, mock_supergrid_file):
        """Test that resolutions map to correct filenames."""
        resolution_to_file = {
            "100 km": "mom1deg.nc",
            "25 km": "mom025deg.nc",
            "10 km": "mom01deg.nc",
        }
        
        for resolution, expected_filename in resolution_to_file.items():
            with patch("os.path.exists", return_value=True):
                with patch.object(Supergrid, "load_supergrid"):
                    sg = Supergrid.__new__(Supergrid)
                    sg.nominal_resolution = resolution
                    path = sg.get_supergrid_path(resolution)
            
            assert expected_filename in path

    # ==================== load_supergrid Tests ====================

    @pytest.mark.unit
    def test_load_supergrid_creates_hcell_arrays(self, supergrid_instance):
        """Test that load_supergrid creates h-cell (tracer) arrays."""
        sg = supergrid_instance
        
        assert hasattr(sg, "hcell_centres_x")
        assert hasattr(sg, "hcell_centres_y")
        assert hasattr(sg, "hcell_corners_x")
        assert hasattr(sg, "hcell_corners_y")
        
        # h-cell corners should have 4 vertices
        assert sg.hcell_corners_x.shape[-1] == 4
        assert sg.hcell_corners_y.shape[-1] == 4

    @pytest.mark.unit
    def test_load_supergrid_creates_qcell_arrays(self, supergrid_instance):
        """Test that load_supergrid creates q-cell (corner) arrays."""
        sg = supergrid_instance
        
        assert hasattr(sg, "qcell_centres_x")
        assert hasattr(sg, "qcell_centres_y")
        assert hasattr(sg, "qcell_corners_x")
        assert hasattr(sg, "qcell_corners_y")
        
        assert sg.qcell_corners_x.shape[-1] == 4
        assert sg.qcell_corners_y.shape[-1] == 4

    @pytest.mark.unit
    def test_load_supergrid_creates_ucell_arrays(self, supergrid_instance):
        """Test that load_supergrid creates u-cell arrays."""
        sg = supergrid_instance
        
        assert hasattr(sg, "ucell_centres_x")
        assert hasattr(sg, "ucell_centres_y")
        assert hasattr(sg, "ucell_corners_x")
        assert hasattr(sg, "ucell_corners_y")
        
        assert sg.ucell_corners_x.shape[-1] == 4
        assert sg.ucell_corners_y.shape[-1] == 4

    @pytest.mark.unit
    def test_load_supergrid_creates_vcell_arrays(self, supergrid_instance):
        """Test that load_supergrid creates v-cell arrays."""
        sg = supergrid_instance
        
        assert hasattr(sg, "vcell_centres_x")
        assert hasattr(sg, "vcell_centres_y")
        assert hasattr(sg, "vcell_corners_x")
        assert hasattr(sg, "vcell_corners_y")
        
        assert sg.vcell_corners_x.shape[-1] == 4
        assert sg.vcell_corners_y.shape[-1] == 4

    @pytest.mark.unit
    def test_load_supergrid_cell_dimensions(self, supergrid_instance):
        """Test that cell arrays have correct relative dimensions."""
        sg = supergrid_instance
        
        # h-cell and q-cell should have related dimensions
        # q-cell has one more point in each direction than h-cell
        h_shape = sg.hcell_centres_x.shape
        q_shape = sg.qcell_centres_x.shape
        
        assert q_shape[0] == h_shape[0] + 1
        assert q_shape[1] == h_shape[1] + 1

    # ==================== extract_grid Tests - B-grid ====================

    @pytest.mark.unit
    def test_extract_grid_b_grid_t_cell(self, supergrid_instance):
        """Test extract_grid for B-grid T-cell (tracer)."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="B")
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info
        assert "vertices_latitude" in grid_info
        assert "vertices_longitude" in grid_info
        assert "i" in grid_info
        assert "j" in grid_info
        assert "vertices" in grid_info

    @pytest.mark.unit
    def test_extract_grid_b_grid_u_cell(self, supergrid_instance):
        """Test extract_grid for B-grid U-cell."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="U", arakawa="B")
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info
        assert grid_info["vertices_latitude"].shape[-1] == 4

    @pytest.mark.unit
    def test_extract_grid_b_grid_v_cell(self, supergrid_instance):
        """Test extract_grid for B-grid V-cell."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="V", arakawa="B")
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    @pytest.mark.unit
    def test_extract_grid_b_grid_c_cell(self, supergrid_instance):
        """Test extract_grid for B-grid C-cell (corner)."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="C", arakawa="B")
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    # ==================== extract_grid Tests - C-grid ====================

    @pytest.mark.unit
    def test_extract_grid_c_grid_t_cell_symmetric(self, supergrid_instance):
        """Test extract_grid for C-grid T-cell with symmetric memory."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="C", symmetric=True)
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    @pytest.mark.unit
    def test_extract_grid_c_grid_t_cell_asymmetric(self, supergrid_instance):
        """Test extract_grid for C-grid T-cell with asymmetric memory."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="C", symmetric=False)
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    @pytest.mark.unit
    def test_extract_grid_c_grid_u_cell_symmetric(self, supergrid_instance):
        """Test extract_grid for C-grid U-cell with symmetric memory."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="U", arakawa="C", symmetric=True)
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    @pytest.mark.unit
    def test_extract_grid_c_grid_u_cell_asymmetric(self, supergrid_instance):
        """Test extract_grid for C-grid U-cell with asymmetric memory."""
        sg = supergrid_instance
        
        grid_info_sym = sg.extract_grid(grid_type="U", arakawa="C", symmetric=True)
        grid_info_asym = sg.extract_grid(grid_type="U", arakawa="C", symmetric=False)
        
        # Asymmetric should have one fewer column
        assert grid_info_asym["longitude"].shape[1] == grid_info_sym["longitude"].shape[1] - 1

    @pytest.mark.unit
    def test_extract_grid_c_grid_v_cell_symmetric(self, supergrid_instance):
        """Test extract_grid for C-grid V-cell with symmetric memory."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="V", arakawa="C", symmetric=True)
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    @pytest.mark.unit
    def test_extract_grid_c_grid_v_cell_asymmetric(self, supergrid_instance):
        """Test extract_grid for C-grid V-cell with asymmetric memory."""
        sg = supergrid_instance
        
        grid_info_sym = sg.extract_grid(grid_type="V", arakawa="C", symmetric=True)
        grid_info_asym = sg.extract_grid(grid_type="V", arakawa="C", symmetric=False)
        
        # Asymmetric should have one fewer row
        assert grid_info_asym["latitude"].shape[0] == grid_info_sym["latitude"].shape[0] - 1

    @pytest.mark.unit
    def test_extract_grid_c_grid_c_cell(self, supergrid_instance):
        """Test extract_grid for C-grid C-cell (corner)."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="C", arakawa="C", symmetric=True)
        
        assert "latitude" in grid_info
        assert "longitude" in grid_info

    # ==================== extract_grid Error Handling ====================

    @pytest.mark.unit
    def test_extract_grid_c_grid_requires_symmetric(self, supergrid_instance):
        """Test that C-grid requires symmetric parameter."""
        sg = supergrid_instance
        
        with pytest.raises(ValueError, match="Must specify symmetric"):
            sg.extract_grid(grid_type="T", arakawa="C", symmetric=None)

    @pytest.mark.unit
    def test_extract_grid_unsupported_arakawa(self, supergrid_instance):
        """Test that unsupported Arakawa grid raises error."""
        sg = supergrid_instance
        
        with pytest.raises(ValueError, match="arakawa=.* is not supported"):
            sg.extract_grid(grid_type="T", arakawa="A")

    @pytest.mark.unit
    def test_extract_grid_unsupported_grid_type_b(self, supergrid_instance):
        """Test that unsupported grid type raises error for B-grid."""
        sg = supergrid_instance
        
        with pytest.raises(ValueError, match="is not a supported grid_type"):
            sg.extract_grid(grid_type="X", arakawa="B")

    @pytest.mark.unit
    def test_extract_grid_unsupported_grid_type_c(self, supergrid_instance):
        """Test that unsupported grid type raises error for C-grid."""
        sg = supergrid_instance
        
        with pytest.raises(ValueError, match="is not a supported grid_type"):
            sg.extract_grid(grid_type="X", arakawa="C", symmetric=True)

    # ==================== extract_grid Output Validation ====================

    @pytest.mark.unit
    def test_extract_grid_longitude_range(self, supergrid_instance):
        """Test that longitude is normalized to [0, 360) range."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="B")
        
        lon = grid_info["longitude"].values
        assert np.all(lon >= 0)
        assert np.all(lon < 360)

    @pytest.mark.unit
    def test_extract_grid_vertices_shape(self, supergrid_instance):
        """Test that vertices arrays have correct shape."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="B")
        
        lat = grid_info["latitude"]
        lat_bnds = grid_info["vertices_latitude"]
        
        # Bounds should have same spatial dims plus vertices dim
        assert lat_bnds.shape[0] == lat.shape[0]
        assert lat_bnds.shape[1] == lat.shape[1]
        assert lat_bnds.shape[2] == 4

    @pytest.mark.unit
    def test_extract_grid_i_j_coords(self, supergrid_instance):
        """Test that i and j coordinates are sequential integers."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="B")
        
        i_coord = grid_info["i"].values
        j_coord = grid_info["j"].values
        
        np.testing.assert_array_equal(i_coord, np.arange(len(i_coord)))
        np.testing.assert_array_equal(j_coord, np.arange(len(j_coord)))

    @pytest.mark.unit
    def test_extract_grid_vertices_coord(self, supergrid_instance):
        """Test that vertices coordinate is [0, 1, 2, 3]."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="B")
        
        vertices = grid_info["vertices"].values
        np.testing.assert_array_equal(vertices, np.array([0, 1, 2, 3]))

    @pytest.mark.unit
    def test_extract_grid_dataarray_dims(self, supergrid_instance):
        """Test that returned DataArrays have correct dimensions."""
        sg = supergrid_instance
        
        grid_info = sg.extract_grid(grid_type="T", arakawa="B")
        
        assert grid_info["latitude"].dims == ("j", "i")
        assert grid_info["longitude"].dims == ("j", "i")
        assert grid_info["vertices_latitude"].dims == ("j", "i", "vertices")
        assert grid_info["vertices_longitude"].dims == ("j", "i", "vertices")