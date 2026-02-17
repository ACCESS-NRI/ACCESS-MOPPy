from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.ocean import (
    Ocean_CMORiser_OM2,
    Ocean_CMORiser_OM3,
)
from tests.mocks.mock_data import (
    create_mock_om2_dataset,
    create_mock_om3_dataset,
)


class TestCMIP6OceanCMORiserOM2:
    """Unit tests for Ocean_CMORiser_OM2 (B-grid)."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary for OM2."""
        vocab = Mock()
        vocab.source_id = "ACCESS-OM2"
        vocab.variable = {"units": "K", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(
            return_value={
                "variable_id": "tos",
                "table_id": "Omon",
                "source_id": "ACCESS-OM2",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            }
        )
        # Mock the methods that return tuples
        vocab._get_axes = Mock(return_value=({}, {}))
        vocab._get_required_bounds_variables = Mock(return_value=({}, {}))
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping for ocean."""
        return {
            "tos": {
                "model_variables": ["surface_temp"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.fixture
    def mock_om2_dataset(self):
        """Create mock OM2 dataset."""
        return create_mock_om2_dataset(nt=12, ny=30, nx=36)

    @pytest.mark.unit
    def test_infer_grid_type_t_grid(
        self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir
    ):
        """Test that T-grid is inferred from xt_ocean/yt_ocean coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = mock_om2_dataset

            grid_type, symmetric = cmoriser.infer_grid_type()

            assert grid_type == "T"
            assert symmetric is None  # MOM5 doesn't use symmetric memory

    @pytest.mark.unit
    def test_infer_grid_type_u_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that U-grid is inferred from xu_ocean/yt_ocean coordinates."""
        ds = xr.Dataset(
            coords={
                "xu_ocean": ("xu_ocean", np.arange(10)),
                "yt_ocean": ("yt_ocean", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.uo",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "U"

    @pytest.mark.unit
    def test_get_dim_rename_om2(self, mock_vocab, mock_mapping, temp_dir):
        """Test dimension renaming for ACCESS-OM2."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            dim_rename = cmoriser._get_dim_rename()

            assert dim_rename["xt_ocean"] == "i"
            assert dim_rename["yt_ocean"] == "j"
            assert dim_rename["xu_ocean"] == "i"
            assert dim_rename["yu_ocean"] == "j"
            assert dim_rename["st_ocean"] == "lev"

    @pytest.mark.unit
    def test_arakawa_grid_type(self, mock_vocab, mock_mapping, temp_dir):
        """Test that ACCESS-OM2 uses B-grid (Arakawa B)."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            assert cmoriser.arakawa == "B"

    @pytest.mark.unit
    def test_time_bnds_dimensions_in_used_coords(
        self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir
    ):
        """Test that time_bnds dimensions are identified as used coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMORiser, "load_dataset", return_value=None):
                cmoriser = Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_om2_dataset

                # Run the processing
                cmoriser.select_and_process_variables()

                # Verify time_bnds dimensions are preserved
                assert "time" in cmoriser.ds.coords
                assert "nv" in cmoriser.ds.coords  # nv is dimension for time_bnds

                # Verify time_bnds has correct dimensions
                assert cmoriser.ds["time_bnds"].dims == ("time", "nv")


class TestCMIP6OceanCMORiserOM3:
    """Unit tests for Ocean_CMORiser_OM3 (C-grid)."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary for OM3."""
        vocab = Mock()
        vocab.source_id = "ACCESS-OM3"
        vocab.variable = {"units": "degC", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(
            return_value={
                "variable_id": "tos",
                "table_id": "Omon",
                "source_id": "ACCESS-OM3",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
            }
        )
        # Mock the methods that return tuples
        vocab._get_axes = Mock(return_value=({}, {}))
        vocab._get_required_bounds_variables = Mock(return_value=({}, {}))
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping."""
        return {
            "tos": {
                "model_variables": ["tos"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.fixture
    def mock_om3_dataset(self):
        """Create mock OM3 dataset."""
        return create_mock_om3_dataset(nt=12, ny=30, nx=36)

    @pytest.mark.unit
    def test_infer_grid_type_t_grid(
        self, mock_vocab, mock_mapping, mock_om3_dataset, temp_dir
    ):
        """Test that T-grid is inferred from xh/yh coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = mock_om3_dataset

            grid_type, symmetric = cmoriser.infer_grid_type()

            assert grid_type == "T"
            assert symmetric is True  # MOM6 uses symmetric memory

    @pytest.mark.unit
    def test_infer_grid_type_u_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that U-grid is inferred from xq/yh coordinates."""
        ds = xr.Dataset(
            coords={
                "xq": ("xq", np.arange(10)),
                "yh": ("yh", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.uo",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "U"

    @pytest.mark.unit
    def test_infer_grid_type_v_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that V-grid is inferred from xh/yq coordinates."""
        ds = xr.Dataset(
            coords={
                "xh": ("xh", np.arange(10)),
                "yq": ("yq", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.vo",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "V"

    @pytest.mark.unit
    def test_infer_grid_type_c_grid(self, mock_vocab, mock_mapping, temp_dir):
        """Test that C-grid (corner) is inferred from xq/yq coordinates."""
        ds = xr.Dataset(
            coords={
                "xq": ("xq", np.arange(10)),
                "yq": ("yq", np.arange(10)),
            }
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.var",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "C"

    @pytest.mark.unit
    def test_get_dim_rename_om3(self, mock_vocab, mock_mapping, temp_dir):
        """Test dimension renaming for ACCESS-OM3."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            dim_rename = cmoriser._get_dim_rename()

            assert dim_rename["xh"] == "i"
            assert dim_rename["yh"] == "j"
            assert dim_rename["xq"] == "i"
            assert dim_rename["yq"] == "j"
            assert dim_rename["zl"] == "lev"

    @pytest.mark.unit
    def test_arakawa_grid_type(self, mock_vocab, mock_mapping, temp_dir):
        """Test that ACCESS-OM3 uses C-grid (Arakawa C)."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            assert cmoriser.arakawa == "C"


class TestOceanDerivations:
    """Unit tests for ocean derivation functions."""

    @pytest.fixture
    def mock_transport_data(self):
        """Create mock transport data for testing umo/vmo calculations."""
        # Create test coordinates
        time = pd.date_range("2000-01-01", periods=3, freq="MS")
        depth = np.arange(0, 50, 10)  # 5 levels
        lat = np.linspace(-60, 60, 5)
        lon = np.linspace(0, 360, 6, endpoint=False)

        # Create test transport data with some realistic patterns
        # Resolved transport: simple zonal flow pattern
        resolved_values = np.random.normal(
            0, 1e6, (len(time), len(depth), len(lat), len(lon))
        )

        # GM transport: typically smaller than resolved
        gm_values = np.random.normal(
            0, 5e5, (len(time), len(depth), len(lat), len(lon))
        )

        # Submeso transport: typically smallest
        submeso_values = np.random.normal(
            0, 2e5, (len(time), len(depth), len(lat), len(lon))
        )

        # Create xarray DataArrays
        coords = {"time": time, "st_ocean": depth, "yt_ocean": lat, "xu_ocean": lon}

        tx_trans = xr.DataArray(
            resolved_values,
            coords=coords,
            dims=["time", "st_ocean", "yt_ocean", "xu_ocean"],
            attrs={"units": "kg/s"},
        )

        tx_trans_gm = xr.DataArray(
            gm_values,
            coords=coords,
            dims=["time", "st_ocean", "yt_ocean", "xu_ocean"],
            attrs={"units": "kg/s"},
        )

        tx_trans_submeso = xr.DataArray(
            submeso_values,
            coords=coords,
            dims=["time", "st_ocean", "yt_ocean", "xu_ocean"],
            attrs={"units": "kg/s"},
        )

        return tx_trans, tx_trans_gm, tx_trans_submeso

    @pytest.mark.unit
    def test_calc_total_mass_transport_resolved_only(self, mock_transport_data):
        """Test total mass transport calculation with only resolved transport."""
        from access_moppy.derivations.calc_ocean import calc_total_mass_transport

        tx_trans, _, _ = mock_transport_data

        result = calc_total_mass_transport(tx_trans)

        # With only resolved transport, result should be identical to input
        xr.testing.assert_allclose(result, tx_trans)
        assert result.attrs["units"] == "kg/s"

    @pytest.mark.unit
    def test_calc_total_mass_transport_with_gm(self, mock_transport_data):
        """Test total mass transport calculation with GM component."""
        from access_moppy.derivations.calc_ocean import calc_total_mass_transport

        tx_trans, tx_trans_gm, _ = mock_transport_data

        result = calc_total_mass_transport(tx_trans, gm_trans=tx_trans_gm)

        # Result should have same shape as input
        assert result.shape == tx_trans.shape
        assert result.dims == tx_trans.dims

        # Result should be different from resolved-only transport
        assert not np.allclose(result.values, tx_trans.values)

    @pytest.mark.unit
    def test_calc_total_mass_transport_all_components(self, mock_transport_data):
        """Test total mass transport with all components."""
        from access_moppy.derivations.calc_ocean import calc_total_mass_transport

        tx_trans, tx_trans_gm, tx_trans_submeso = mock_transport_data

        result = calc_total_mass_transport(
            tx_trans, gm_trans=tx_trans_gm, submeso_trans=tx_trans_submeso
        )

        # Result should have same shape and coordinates
        assert result.shape == tx_trans.shape
        assert result.dims == tx_trans.dims
        assert list(result.coords.keys()) == list(tx_trans.coords.keys())

    @pytest.mark.unit
    def test_calc_umo_corrected(self, mock_transport_data):
        """Test umo corrected calculation."""
        from access_moppy.derivations.calc_ocean import calc_umo_corrected

        tx_trans, tx_trans_gm, tx_trans_submeso = mock_transport_data

        result = calc_umo_corrected(
            tx_trans, tx_trans_gm=tx_trans_gm, tx_trans_submeso=tx_trans_submeso
        )

        # Check output properties
        assert result.shape == tx_trans.shape
        assert result.dims == tx_trans.dims
        assert "time" in result.dims
        assert "st_ocean" in result.dims

        # Should be different from resolved-only
        assert not np.allclose(result.values, tx_trans.values)

    @pytest.mark.unit
    def test_calc_vmo_corrected(self, mock_transport_data):
        """Test vmo corrected calculation."""
        from access_moppy.derivations.calc_ocean import calc_vmo_corrected

        # Use same mock data but imagine it's ty_trans instead of tx_trans
        ty_trans, ty_trans_gm, ty_trans_submeso = mock_transport_data

        # Change coordinate names to match meridional transport
        ty_trans = ty_trans.rename({"xu_ocean": "xt_ocean", "yt_ocean": "yu_ocean"})
        ty_trans_gm = ty_trans_gm.rename(
            {"xu_ocean": "xt_ocean", "yt_ocean": "yu_ocean"}
        )
        ty_trans_submeso = ty_trans_submeso.rename(
            {"xu_ocean": "xt_ocean", "yt_ocean": "yu_ocean"}
        )

        result = calc_vmo_corrected(
            ty_trans, ty_trans_gm=ty_trans_gm, ty_trans_submeso=ty_trans_submeso
        )

        # Check output properties
        assert result.shape == ty_trans.shape
        assert result.dims == ty_trans.dims
        assert "time" in result.dims
        assert "st_ocean" in result.dims

    @pytest.mark.unit
    def test_vertical_difference_boundary_condition(self, mock_transport_data):
        """Test that vertical difference correctly handles surface boundary conditions."""
        from access_moppy.derivations.calc_ocean import calc_total_mass_transport

        tx_trans, tx_trans_gm, _ = mock_transport_data

        # Create a simple case where GM transport is constant with depth
        # The vertical difference should then be zero everywhere except surface
        const_gm = xr.ones_like(tx_trans_gm) * 1e5

        result = calc_total_mass_transport(tx_trans, gm_trans=const_gm)

        # The GM contribution should be zero everywhere except first level
        gm_contribution = result - tx_trans

        # For constant GM transport, expect first level = const_gm, rest = 0
        # First level should equal const_gm (1e5)
        assert np.allclose(gm_contribution.isel(st_ocean=0).values, 1e5)

        # Deeper levels should be zero (diff of constant is 0)
        for i in range(1, len(gm_contribution.st_ocean)):
            assert np.allclose(gm_contribution.isel(st_ocean=i).values, 0.0, atol=1e-10)
