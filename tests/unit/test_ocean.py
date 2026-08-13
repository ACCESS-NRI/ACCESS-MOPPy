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


@pytest.mark.unit
@pytest.mark.parametrize("cmoriser_class", [Ocean_CMORiser_OM2, Ocean_CMORiser_OM3])
def test_ocean_chunk_settings_reach_base_cmoriser(cmoriser_class, temp_dir):
    vocab = Mock()
    vocab._get_nominal_resolution.return_value = "1deg"

    with patch("access_moppy.ocean.Supergrid"):
        cmoriser = cmoriser_class(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            compound_name="Omon.tos",
            vocab=vocab,
            variable_mapping={"tos": {}},
            enable_chunking=True,
            chunk_size_mb=8,
            max_chunk_size_mb=64,
        )

    assert cmoriser.enable_chunking is True
    assert cmoriser.chunker.target_chunk_size_mb == 8
    assert cmoriser.chunker.max_chunk_size_mb == 64


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
            assert dim_rename["sw_ocean"] == "lev"

    @pytest.mark.unit
    def test_sw_ocean_variable_gets_lev_dim_and_cmor_order(self, mock_vocab, temp_dir):
        """w-point variables (wo, wmo) must end up on `lev` in T, Z, Y, X order.

        Left unrenamed, `sw_ocean` is not matched by the preferred dimension
        order and gets appended last, yielding (time, j, i, sw_ocean).
        """
        mock_vocab.variable = {
            "units": "m s-1",
            "type": "real",
            "dimensions": "longitude latitude olevel time",
        }
        mapping = {
            "wo": {
                "model_variables": ["wt"],
                "calculation": {"type": "direct"},
            }
        }
        ds = xr.Dataset(
            data_vars={
                "wt": (
                    ["time", "sw_ocean", "yt_ocean", "xt_ocean"],
                    np.zeros((2, 3, 4, 5), dtype=np.float32),
                )
            },
            coords={
                "time": ("time", pd.date_range("1850-01-01", periods=2, freq="MS")),
                "sw_ocean": ("sw_ocean", np.array([10.0, 20.0, 30.0])),
                "yt_ocean": ("yt_ocean", np.linspace(-80.0, 80.0, 4)),
                "xt_ocean": ("xt_ocean", np.linspace(0.5, 359.5, 5)),
            },
        )

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.wo",
                vocab=mock_vocab,
                variable_mapping=mapping,
            )

        with patch.object(cmoriser, "load_dataset", return_value=None):
            cmoriser.ds = ds
            cmoriser.select_and_process_variables()

        assert cmoriser.ds["wo"].dims == ("time", "lev", "j", "i")
        assert "sw_ocean" not in cmoriser.ds.dims
        assert "sw_ocean" not in cmoriser.ds.coords

    @pytest.mark.unit
    def test_get_dim_rename_accepts_access_esm1_6(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """ACCESS-ESM1-6 uses the OM2/MOM5 ocean grid conventions."""
        mock_vocab.source_id = "ACCESS-ESM1-6"

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
    def test_infer_grid_type_unknown_coords_raises_with_context(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """infer_grid_type raises ValueError with expected/found coord sets when no match."""
        ds = xr.Dataset(coords={"unknown_x": ("unknown_x", np.arange(5))})

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            with pytest.raises(ValueError, match="MOM5/OM2") as exc_info:
                cmoriser.infer_grid_type()

            msg = str(exc_info.value)
            assert "xt_ocean" in msg
            assert "Found coordinates" in msg

    @pytest.mark.unit
    def test_select_and_process_empty_required_vars_direct_raises(
        self, mock_vocab, temp_dir
    ):
        """direct calc type with empty model_variables raises ValueError."""
        mapping = {
            "tos": {
                "model_variables": [],
                "calculation": {"type": "direct"},
            }
        }

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mapping,
            )

        with patch.object(cmoriser, "load_dataset", return_value=None):
            cmoriser.ds = xr.Dataset()
            with pytest.raises(
                ValueError, match="requires at least one model_variable"
            ):
                cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_om2_get_dim_rename_unsupported_source_id_lists_supported(
        self, mock_mapping, temp_dir
    ):
        """OM2 _get_dim_rename raises ValueError listing supported source_ids."""
        vocab = Mock()
        vocab.source_id = "UNKNOWN-MODEL"
        vocab._get_nominal_resolution = Mock(return_value="1deg")

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=vocab,
                variable_mapping=mock_mapping,
            )

            with pytest.raises(
                ValueError, match="Unsupported source_id 'UNKNOWN-MODEL'"
            ) as exc_info:
                cmoriser._get_dim_rename()

            msg = str(exc_info.value)
            assert "Ocean_CMORiser_OM2" in msg
            assert "ACCESS-OM2" in msg

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

    @pytest.mark.unit
    def test_infer_grid_type_unknown_coords_raises_with_context(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """infer_grid_type raises ValueError with expected/found coord sets when no match."""
        ds = xr.Dataset(coords={"unknown_x": ("unknown_x", np.arange(5))})

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            with pytest.raises(ValueError, match="MOM6/OM3") as exc_info:
                cmoriser.infer_grid_type()

            msg = str(exc_info.value)
            assert "xh" in msg
            assert "Found coordinates" in msg

    @pytest.mark.unit
    def test_select_and_process_empty_required_vars_direct_raises(
        self, mock_vocab, temp_dir
    ):
        """direct calc type with empty model_variables raises ValueError."""
        mapping = {
            "tos": {
                "model_variables": [],
                "calculation": {"type": "direct"},
            }
        }

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=mock_vocab,
                variable_mapping=mapping,
            )

        with patch.object(cmoriser, "load_dataset", return_value=None):
            cmoriser.ds = xr.Dataset()
            with pytest.raises(
                ValueError, match="requires at least one model_variable"
            ):
                cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_om3_get_dim_rename_unsupported_source_id_mentions_required(
        self, mock_mapping, temp_dir
    ):
        """OM3 _get_dim_rename raises ValueError naming the required source_id prefix."""
        vocab = Mock()
        vocab.source_id = "OTHER-MODEL"
        vocab._get_nominal_resolution = Mock(return_value="1deg")

        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                vocab=vocab,
                variable_mapping=mock_mapping,
            )

            with pytest.raises(
                ValueError, match="Unsupported source_id 'OTHER-MODEL'"
            ) as exc_info:
                cmoriser._get_dim_rename()

            msg = str(exc_info.value)
            assert "Ocean_CMORiser_OM3" in msg
            assert "ACCESS-OM3" in msg
            assert "ACCESS-CM" in msg


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


def _make_grid_info(ny=4, nx=5):
    """Minimal grid_info dict matching supergrid.extract_grid output."""
    return {
        "i": np.arange(nx),
        "j": np.arange(ny),
        "vertices": np.arange(4),
        "latitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
        "longitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
        "vertices_latitude": xr.DataArray(
            np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
        ),
        "vertices_longitude": xr.DataArray(
            np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
        ),
    }


def _scalar_ds(nt=3, with_orphaned_dims=False):
    """Dataset simulating zostoga state after drop_intermediates().

    pot_temp and dzt have been removed but their dimension coordinates
    (lev, i, j) remain as orphans when with_orphaned_dims=True.
    """
    data_vars = {
        "zostoga": (["time"], np.ones(nt, dtype=np.float32)),
        "time_bnds": (["time", "nv"], np.zeros((nt, 2))),
    }
    coords = {
        "time": (
            "time",
            np.arange(nt, dtype=float),
            {"calendar": "proleptic_gregorian", "units": "days since 1850-01-01"},
        ),
        "nv": ("nv", [1.0, 2.0]),
    }
    if with_orphaned_dims:
        # Simulate lev/i/j left behind after pot_temp and dzt were dropped.
        coords["lev"] = ("lev", np.arange(5, dtype=float))
        coords["i"] = ("i", np.arange(5))
        coords["j"] = ("j", np.arange(4))
    return xr.Dataset(data_vars, coords=coords)


def _spatial_ds(nt=3, ny=4, nx=5):
    """Dataset simulating tos state (spatial variable, dims time/j/i)."""
    return xr.Dataset(
        {
            "tos": (
                ["time", "j", "i"],
                np.ones((nt, ny, nx), dtype=np.float32),
            ),
            "time_bnds": (["time", "nv"], np.zeros((nt, 2))),
        },
        coords={
            "time": (
                "time",
                np.arange(nt, dtype=float),
                {"calendar": "proleptic_gregorian", "units": "days since 1850-01-01"},
            ),
            "nv": ("nv", [1.0, 2.0]),
            "i": ("i", np.arange(nx)),
            "j": ("j", np.arange(ny)),
        },
    )


def _make_cmoriser(vocab, mapping, compound_name, temp_dir, ds, grid_info=None):
    """Build an Ocean_CMORiser_OM2 with ds and grid_info pre-populated."""
    if grid_info is None:
        grid_info = _make_grid_info()
    with patch("access_moppy.ocean.Supergrid"):
        cmoriser = Ocean_CMORiser_OM2(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            compound_name=compound_name,
            vocab=vocab,
            variable_mapping=mapping,
        )
    cmoriser.ds = ds
    cmoriser.grid_type = "T"
    cmoriser.symmetric = None
    cmoriser.supergrid = Mock()
    cmoriser.supergrid.extract_grid.return_value = grid_info
    return cmoriser


# ---------------------------------------------------------------------------
# TestUpdateAttributes
# ---------------------------------------------------------------------------


class TestUpdateAttributes:
    """Tests for the update_attributes() changes in Ocean_CMORiser."""

    @pytest.fixture
    def mock_vocab(self):
        vocab = Mock()
        vocab.source_id = "ACCESS-OM2"
        vocab.variable = {"units": "m", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={})
        vocab._get_axes = Mock(return_value=({}, {}))
        vocab._get_required_bounds_variables = Mock(return_value=({}, {}))
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "long_name": "time",
                "axis": "T",
            }
        }
        return vocab

    @pytest.fixture
    def scalar_mapping(self):
        return {
            "zostoga": {
                "model_variables": ["pot_temp", "dzt"],
                "calculation": {
                    "type": "formula",
                    "operation": "calc_zostoga",
                    "args": [],
                },
            }
        }

    @pytest.fixture
    def spatial_mapping(self):
        return {
            "tos": {
                "model_variables": ["surface_temp"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.mark.unit
    def test_bnds_is_pure_dimension_not_coordinate(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """nv→bnds rename must leave bnds as a dimension only, not a coord variable."""
        cmoriser = _make_cmoriser(
            mock_vocab, scalar_mapping, "Omon.zostoga", temp_dir, _scalar_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "bnds" not in cmoriser.ds.coords
        assert "bnds" in cmoriser.ds.dims

    @pytest.mark.unit
    def test_calculated_bnds_index_coordinate_dropped(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """calculate_missing_bounds_variables attaches a [0, 1] index coordinate to
        the bnds dimension; update_attributes must drop it so bnds stays a pure
        dimension even when there is no nv dimension to rename."""
        # Mirror the output of calculate_time_bounds: time_bnds on a bnds dimension
        # that carries a [0, 1] index coordinate, and no nv dimension at all.
        ds = xr.Dataset(
            {
                "zostoga": (["time"], np.ones(3, dtype=np.float32)),
                "time_bnds": (["time", "bnds"], np.zeros((3, 2))),
            },
            coords={
                "time": (
                    "time",
                    np.arange(3, dtype=float),
                    {
                        "calendar": "proleptic_gregorian",
                        "units": "days since 1850-01-01",
                    },
                ),
                "bnds": ("bnds", np.array([0, 1])),
            },
        )
        cmoriser = _make_cmoriser(
            mock_vocab, scalar_mapping, "Omon.zostoga", temp_dir, ds
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "bnds" not in cmoriser.ds.coords
        assert "bnds" in cmoriser.ds.dims

    @pytest.mark.unit
    def test_missing_table_type_preserves_source_dtype(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """No 'type' in the CMOR table entry (as in CMIP7 tables): keep the
        source dtype (float32) instead of falling back to float64."""
        del mock_vocab.variable["type"]
        ds = _scalar_ds()
        assert ds["zostoga"].dtype == np.float32

        cmoriser = _make_cmoriser(
            mock_vocab, scalar_mapping, "Omon.zostoga", temp_dir, ds
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["zostoga"].dtype == np.float32

    @pytest.mark.unit
    def test_explicit_double_type_upcasts_and_recasts_fill_value(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """When the table does specify 'double', the data is upcast as
        before, and _FillValue/missing_value are re-cast to float64 too so
        they stay bit-consistent with the now-float64 data."""
        mock_vocab.variable["type"] = "double"
        ds = _scalar_ds()
        ds["zostoga"].attrs["_FillValue"] = np.float32(1e20)
        ds["zostoga"].attrs["missing_value"] = np.float32(1e20)

        cmoriser = _make_cmoriser(
            mock_vocab, scalar_mapping, "Omon.zostoga", temp_dir, ds
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["zostoga"].dtype == np.float64
        assert isinstance(cmoriser.ds["zostoga"].attrs["_FillValue"], np.float64)
        assert isinstance(cmoriser.ds["zostoga"].attrs["missing_value"], np.float64)
        assert cmoriser.ds["zostoga"].attrs["_FillValue"] == np.float64(
            np.float32(1e20)
        )

    @pytest.mark.unit
    def test_scalar_variable_no_spatial_coords_added(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """latitude, longitude and vertices must NOT be added for a scalar variable."""
        cmoriser = _make_cmoriser(
            mock_vocab, scalar_mapping, "Omon.zostoga", temp_dir, _scalar_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        for var in ("latitude", "longitude", "vertices_latitude", "vertices_longitude"):
            assert (
                var not in cmoriser.ds
            ), f"'{var}' should not be present for scalar variable"

    @pytest.mark.unit
    def test_scalar_variable_orphaned_dims_dropped(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """lev/i/j orphaned after drop_intermediates must be removed."""
        cmoriser = _make_cmoriser(
            mock_vocab,
            scalar_mapping,
            "Omon.zostoga",
            temp_dir,
            _scalar_ds(with_orphaned_dims=True),
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert set(cmoriser.ds.dims) == {"time", "bnds"}

    @pytest.mark.unit
    def test_spatial_variable_grid_coords_still_added(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """Regression: latitude/longitude/vertices must still be added for spatial vars."""
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        for var in ("latitude", "longitude", "vertices_latitude", "vertices_longitude"):
            assert var in cmoriser.ds, f"'{var}' missing for spatial variable"

    @pytest.mark.unit
    def test_spatial_variable_coordinates_point_to_lat_lon(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """Data variable's coordinates attr must reference the supergrid lat/lon.

        The WCRP ATTR004 'coordinates as-variable' check fails when the attribute
        names variables not present in the file.
        """
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["tos"].attrs.get("coordinates") == "latitude longitude"

    @pytest.mark.unit
    def test_vertices_is_pure_dimension_not_coordinate(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """`vertices` must be a bare dimension, not an int coordinate variable
        (matches the published reference; avoids CF §2.2/§3.3 findings)."""
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "vertices" not in cmoriser.ds.coords
        assert "vertices" in cmoriser.ds.dims

    @pytest.mark.unit
    def test_time_bnds_has_no_attributes(self, mock_vocab, spatial_mapping, temp_dir):
        """time_bnds must be attribute-free (CF §7.1: bounds inherit from parent)."""
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time_bnds"].attrs == {}

    @pytest.mark.unit
    def test_vertices_bounds_have_no_standard_name(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """vertices_latitude/longitude must keep units but not standard_name
        (CF §7.1; matches the published reference)."""
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        for v in ("vertices_latitude", "vertices_longitude"):
            assert "standard_name" not in cmoriser.ds[v].attrs
            assert cmoriser.ds[v].attrs.get("units")

    @pytest.mark.unit
    def test_lev_gets_cf_axis(self, mock_vocab, spatial_mapping, temp_dir):
        """The vertical coordinate must get a CF `axis='Z'` (WCRP ATTR001)."""
        ds = _spatial_ds().assign_coords(lev=("lev", np.array([5.0, 15.0])))
        cmoriser = _make_cmoriser(mock_vocab, spatial_mapping, "Omon.tos", temp_dir, ds)
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["lev"].attrs.get("axis") == "Z"

    @pytest.mark.unit
    def test_stale_model_coordinates_overwritten(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """A stale 'geolon_t geolat_t' inherited from the model file is replaced."""
        ds = _spatial_ds()
        ds["tos"].attrs["coordinates"] = "geolon_t geolat_t"
        cmoriser = _make_cmoriser(mock_vocab, spatial_mapping, "Omon.tos", temp_dir, ds)
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        coords_attr = cmoriser.ds["tos"].attrs.get("coordinates")
        assert coords_attr == "latitude longitude"
        assert "geolon_t" not in coords_attr

    @pytest.mark.unit
    def test_scalar_variable_coordinates_not_set(
        self, mock_vocab, scalar_mapping, temp_dir
    ):
        """A scalar variable (no i/j) must not gain a curvilinear coordinates attr."""
        cmoriser = _make_cmoriser(
            mock_vocab, scalar_mapping, "Omon.zostoga", temp_dir, _scalar_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["zostoga"].attrs.get("coordinates") != "latitude longitude"

    @pytest.mark.unit
    def test_time_coordinate_gets_cf_attributes(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """time must carry standard_name and axis from the CMOR table (wcrp_cmip6)."""
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time"].attrs["standard_name"] == "time"
        assert cmoriser.ds["time"].attrs["axis"] == "T"

    @pytest.mark.unit
    def test_missing_scalar_axis_with_value_is_synthesized(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """A CMOR dimension defined purely by a fixed value (e.g. mlotst's
        `deltasigt` threshold) must be added as a scalar coordinate when the
        dataset doesn't already carry it, since the model output has no
        reason to."""
        mock_vocab.axes["deltasigt"] = {
            "out_name": "deltasigt",
            "standard_name": "sea_water_sigma_t_difference",
            "long_name": "sigma_t criterion that determines layer thickness",
            "units": "kg m-3",
            "type": "double",
            "value": "0.03",
        }
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "deltasigt" in cmoriser.ds.coords
        coord = cmoriser.ds["deltasigt"]
        assert coord.ndim == 0
        assert coord.dtype == np.float64
        assert float(coord.values) == pytest.approx(0.03)
        assert coord.attrs["standard_name"] == "sea_water_sigma_t_difference"
        assert coord.attrs["units"] == "kg m-3"

    @pytest.mark.unit
    def test_axis_already_present_is_not_overwritten(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """A scalar-with-value axis that the dataset already carries (e.g. from
        the model output) must be left alone, not clobbered by a synthesized
        one."""
        mock_vocab.axes["deltasigt"] = {
            "out_name": "deltasigt",
            "standard_name": "sea_water_sigma_t_difference",
            "units": "kg m-3",
            "type": "double",
            "value": "0.03",
        }
        ds = _spatial_ds()
        ds = ds.assign_coords(deltasigt=xr.DataArray(0.05, attrs={"units": "kg m-3"}))
        cmoriser = _make_cmoriser(mock_vocab, spatial_mapping, "Omon.tos", temp_dir, ds)
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert float(cmoriser.ds["deltasigt"].values) == pytest.approx(0.05)

    @pytest.mark.unit
    def test_time_bnds_upcast_to_match_time_coordinate(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """A float32 time_bnds carried through from the model file must be
        upcast to double, matching the CMOR-declared "double" type applied to
        the time coordinate itself (observed real-world mismatch: time
        written as double, time_bnds as float, in the same output file)."""
        ds = _spatial_ds()
        ds["time_bnds"] = ds["time_bnds"].astype(np.float32)
        cmoriser = _make_cmoriser(mock_vocab, spatial_mapping, "Omon.tos", temp_dir, ds)
        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time_bnds"].dtype == np.float64

    @pytest.mark.unit
    def test_apply_time_coord_attrs_noop_without_time(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """No-op when the dataset has no time coordinate (e.g. fx variables)."""
        ds = xr.Dataset(
            {"tos": (["j", "i"], np.ones((4, 5), dtype=np.float32))},
            coords={"i": ("i", np.arange(5)), "j": ("j", np.arange(4))},
        )
        cmoriser = _make_cmoriser(mock_vocab, spatial_mapping, "Ofx.tos", temp_dir, ds)

        cmoriser._apply_time_coordinate_attributes()

        assert "time" not in cmoriser.ds

    @pytest.mark.unit
    def test_apply_time_coord_attrs_noop_without_time_axis_in_vocab(
        self, mock_vocab, spatial_mapping, temp_dir
    ):
        """No-op when vocab.axes declares no time axis; existing attrs untouched."""
        cmoriser = _make_cmoriser(
            mock_vocab, spatial_mapping, "Omon.tos", temp_dir, _spatial_ds()
        )
        cmoriser.vocab.axes = {"lat": {"out_name": "lat"}}  # no time entry

        cmoriser._apply_time_coordinate_attributes()

        assert "standard_name" not in cmoriser.ds["time"].attrs
        assert "axis" not in cmoriser.ds["time"].attrs


# ---------------------------------------------------------------------------
# TestAlignMainVarDimsWithVocab
# ---------------------------------------------------------------------------


def _make_align_cmoriser(
    vocab_dims, compound_name, temp_dir, ds, cmor_name="masscello"
):
    """Build an Ocean_CMORiser_OM2 with a minimal vocab for dim-alignment tests."""
    vocab = Mock()
    vocab.source_id = "ACCESS-ESM1-5"
    vocab.variable = {"dimensions": vocab_dims, "units": "kg m-2", "type": "real"}
    vocab._get_nominal_resolution = Mock(return_value="1deg")
    vocab._get_axes = Mock(return_value=({}, {}))
    vocab._get_required_bounds_variables = Mock(return_value=({}, {}))
    mapping = {
        cmor_name: {"model_variables": ["src"], "calculation": {"type": "direct"}}
    }
    with patch("access_moppy.ocean.Supergrid"):
        cmoriser = Ocean_CMORiser_OM2(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            compound_name=compound_name,
            vocab=vocab,
            variable_mapping=mapping,
        )
    cmoriser.ds = ds
    return cmoriser


class TestExpectedDimNames:
    """Tests for Ocean_CMORiser._expected_dim_names (CMIP6 vs CMIP7 format)."""

    @pytest.mark.unit
    def test_cmip6_string_form(self, temp_dir):
        """CMIP6 stores dimensions as a space-separated string."""
        cmoriser = _make_align_cmoriser(
            vocab_dims="longitude latitude olevel time",
            compound_name="Omon.masscello",
            temp_dir=temp_dir,
            ds=xr.Dataset(),
        )
        assert cmoriser._expected_dim_names() == {
            "longitude",
            "latitude",
            "olevel",
            "time",
        }

    @pytest.mark.unit
    def test_cmip7_list_form(self, temp_dir):
        """CMIP7 stores dimensions as a list."""
        cmoriser = _make_align_cmoriser(
            vocab_dims=["longitude", "latitude", "olevel"],
            compound_name="Ofx.masscello",
            temp_dir=temp_dir,
            ds=xr.Dataset(),
        )
        assert cmoriser._expected_dim_names() == {"longitude", "latitude", "olevel"}

    @pytest.mark.unit
    def test_missing_dimensions_key_returns_empty(self, temp_dir):
        """Defensive: a vocab variable entry without 'dimensions' must not crash."""
        cmoriser = _make_align_cmoriser(
            vocab_dims="time",
            compound_name="Omon.masscello",
            temp_dir=temp_dir,
            ds=xr.Dataset(),
        )
        # Remove the dimensions key entirely
        cmoriser.vocab.variable = {"units": "K"}
        assert cmoriser._expected_dim_names() == set()


class TestAlignMainVarDimsWithVocab:
    """Tests for Ocean_CMORiser._align_main_var_dims_with_vocab.

    Verifies the core invariant of the fix: the time axis on the main CMOR
    variable is dropped if and only if it exists on the data but is not part
    of what the active CMOR table requests.
    """

    @staticmethod
    def _ds_with_time(name="masscello"):
        return xr.Dataset(
            {
                name: (
                    ["time", "st_ocean", "yt_ocean", "xt_ocean"],
                    np.ones((3, 2, 4, 5), dtype=np.float32),
                ),
            },
            coords={
                "time": ("time", np.arange(3, dtype=float)),
                "st_ocean": ("st_ocean", np.arange(2, dtype=float)),
                "yt_ocean": ("yt_ocean", np.arange(4, dtype=float)),
                "xt_ocean": ("xt_ocean", np.arange(5, dtype=float)),
            },
        )

    @staticmethod
    def _ds_without_time(name="masscello"):
        return xr.Dataset(
            {
                name: (
                    ["st_ocean", "yt_ocean", "xt_ocean"],
                    np.ones((2, 4, 5), dtype=np.float32),
                ),
            },
            coords={
                "st_ocean": ("st_ocean", np.arange(2, dtype=float)),
                "yt_ocean": ("yt_ocean", np.arange(4, dtype=float)),
                "xt_ocean": ("xt_ocean", np.arange(5, dtype=float)),
            },
        )

    @pytest.mark.unit
    def test_drops_time_when_vocab_does_not_request_it(self, temp_dir):
        """Ofx-style: data has time, vocab doesn't → time is dropped from main var."""
        cmoriser = _make_align_cmoriser(
            vocab_dims="longitude latitude olevel",  # no time → Ofx
            compound_name="Ofx.masscello",
            temp_dir=temp_dir,
            ds=self._ds_with_time(),
        )
        cmoriser._align_main_var_dims_with_vocab()

        assert "time" not in cmoriser.ds["masscello"].dims
        assert cmoriser.ds["masscello"].dims == ("st_ocean", "yt_ocean", "xt_ocean")

    @pytest.mark.unit
    def test_keeps_time_when_vocab_requests_it(self, temp_dir):
        """Omon-style: data has time and vocab requests it → unchanged."""
        cmoriser = _make_align_cmoriser(
            vocab_dims="longitude latitude olevel time",  # includes time → Omon
            compound_name="Omon.masscello",
            temp_dir=temp_dir,
            ds=self._ds_with_time(),
        )
        original_dims = cmoriser.ds["masscello"].dims

        cmoriser._align_main_var_dims_with_vocab()

        assert cmoriser.ds["masscello"].dims == original_dims
        assert "time" in cmoriser.ds["masscello"].dims

    @pytest.mark.unit
    def test_noop_when_main_var_has_no_time(self, temp_dir):
        """Data already lacks time → no change, regardless of vocab."""
        cmoriser = _make_align_cmoriser(
            vocab_dims="longitude latitude olevel",
            compound_name="Ofx.masscello",
            temp_dir=temp_dir,
            ds=self._ds_without_time(),
        )
        original_dims = cmoriser.ds["masscello"].dims

        cmoriser._align_main_var_dims_with_vocab()

        assert cmoriser.ds["masscello"].dims == original_dims

    @pytest.mark.unit
    def test_noop_when_cmor_name_not_in_ds(self, temp_dir):
        """Defensive: missing main variable on ds → no error, no change."""
        cmoriser = _make_align_cmoriser(
            vocab_dims="longitude latitude olevel",
            compound_name="Ofx.masscello",
            temp_dir=temp_dir,
            ds=xr.Dataset(),
        )
        cmoriser._align_main_var_dims_with_vocab()  # must not raise

        assert "masscello" not in cmoriser.ds

    @pytest.mark.unit
    def test_does_not_touch_other_data_vars(self, temp_dir):
        """Scope guarantee: only the main CMOR variable is altered."""
        ds = self._ds_with_time()
        # Add an unrelated time-bearing variable that the helper must not touch.
        ds["other"] = (("time", "st_ocean"), np.zeros((3, 2), dtype=np.float32))
        cmoriser = _make_align_cmoriser(
            vocab_dims="longitude latitude olevel",
            compound_name="Ofx.masscello",
            temp_dir=temp_dir,
            ds=ds,
        )
        cmoriser._align_main_var_dims_with_vocab()

        assert "time" not in cmoriser.ds["masscello"].dims
        # The unrelated variable keeps its time axis
        assert "time" in cmoriser.ds["other"].dims

    @pytest.mark.unit
    def test_cmip7_list_dims_form_also_drops_time(self, temp_dir):
        """CMIP7-style list dimensions field is honoured by the helper."""
        cmoriser = _make_align_cmoriser(
            vocab_dims=["longitude", "latitude", "olevel"],
            compound_name="Ofx.masscello",
            temp_dir=temp_dir,
            ds=self._ds_with_time(),
        )
        cmoriser._align_main_var_dims_with_vocab()

        assert "time" not in cmoriser.ds["masscello"].dims
