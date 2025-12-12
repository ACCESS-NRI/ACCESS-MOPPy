from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import dask.array as da
import numpy as np
import pytest
import xarray as xr
import pandas as pd

from access_moppy.base import CMIP6_CMORiser
from access_moppy.ocean import (
    CMIP6_Ocean_CMORiser,
    CMIP6_Ocean_CMORiser_OM2,
    CMIP6_Ocean_CMORiser_OM3,
)
from tests.mocks.mock_data import (
    create_mock_om2_dataset,
    create_mock_om3_dataset,
)

class TestCMIP6OceanCMORiserOM2:
    """Unit tests for CMIP6_Ocean_CMORiser_OM2 (B-grid)."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary for OM2."""
        vocab = Mock()
        vocab.source_id = "ACCESS-OM2"
        vocab.variable = {"units": "K", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={
            "variable_id": "tos",
            "table_id": "Omon",
            "source_id": "ACCESS-OM2",
            "experiment_id": "historical",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
        })
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
    def test_infer_grid_type_t_grid(self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir):
        """Test that T-grid is inferred from xt_ocean/yt_ocean coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
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
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.uo",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "U"

    @pytest.mark.unit
    def test_get_dim_rename_om2(self, mock_vocab, mock_mapping, temp_dir):
        """Test dimension renaming for ACCESS-OM2."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
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
            cmoriser = CMIP6_Ocean_CMORiser_OM2(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            assert cmoriser.arakawa == "B"
    
    @pytest.mark.unit
    def test_time_bnds_loaded_and_preserved(self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir):
        """Test that time_bnds is loaded with other variables and preserved in output."""
        with patch("access_moppy.ocean.Supergrid"):
            # Mock load_dataset to avoid file I/O
            with patch.object(CMIP6_CMORiser, 'load_dataset', return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_om2_dataset
                
                # Run the processing
                cmoriser.select_and_process_variables()
                
                # Verify time_bnds is in the output dataset
                assert "time_bnds" in cmoriser.ds.data_vars
                
                # Verify only cmor_name and time_bnds are kept as data variables
                assert set(cmoriser.ds.data_vars) == {"tos", "time_bnds"}

    @pytest.mark.unit
    def test_time_bnds_dimensions_in_used_coords(self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir):
        """Test that time_bnds dimensions are identified as used coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, 'load_dataset', return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
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

    @pytest.mark.unit
    def test_time_bnds_missing_handled_gracefully(self, mock_vocab, mock_mapping, temp_dir):
        """Test that processing continues gracefully if time_bnds is missing."""
        # Create dataset without time_bnds
        ds_no_bnds = xr.Dataset(
            data_vars={
                "surface_temp": (
                    ["time", "yt_ocean", "xt_ocean"],
                    np.random.rand(12, 30, 36),
                ),
            },
            coords={
                "time": pd.date_range("2000-01-01", periods=12, freq="M"),
                "yt_ocean": np.arange(30),
                "xt_ocean": np.arange(36),
            },
        )
        
        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, 'load_dataset', return_value=None):
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds_no_bnds
                
                # Should not raise error even without time_bnds
                cmoriser.select_and_process_variables()
                
                # Verify processing completed
                assert "tos" in cmoriser.ds.data_vars
                # time_bnds should not be present since it wasn't in input
                assert "time_bnds" not in cmoriser.ds.data_vars

    @pytest.mark.unit  
    def test_required_vars_includes_time_bnds(self, mock_vocab, mock_mapping, mock_om2_dataset, temp_dir):
        """Test that time_bnds is included in required_vars during loading."""
        with patch("access_moppy.ocean.Supergrid"):
            with patch.object(CMIP6_CMORiser, 'load_dataset') as mock_load:
                cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="Omon.tos",
                    cmip6_vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_om2_dataset
                
                # Run processing
                cmoriser.select_and_process_variables()
                
                # Verify load_dataset was called with time_bnds in required_vars
                mock_load.assert_called_once()
                call_args = mock_load.call_args
                required_vars = call_args.kwargs.get('required_vars') or call_args[0][0]
                assert "time_bnds" in required_vars
                assert "surface_temp" in required_vars  # model variable


class TestCMIP6OceanCMORiserOM3:
    """Unit tests for CMIP6_Ocean_CMORiser_OM3 (C-grid)."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary for OM3."""
        vocab = Mock()
        vocab.source_id = "ACCESS-OM3"
        vocab.variable = {"units": "degC", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={
            "variable_id": "tos",
            "table_id": "Omon",
            "source_id": "ACCESS-OM3",
            "experiment_id": "historical",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
        })
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
    def test_infer_grid_type_t_grid(self, mock_vocab, mock_mapping, mock_om3_dataset, temp_dir):
        """Test that T-grid is inferred from xh/yh coordinates."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
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
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.uo",
                cmip6_vocab=mock_vocab,
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
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.vo",
                cmip6_vocab=mock_vocab,
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
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.var",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            grid_type, _ = cmoriser.infer_grid_type()

            assert grid_type == "C"

    @pytest.mark.unit
    def test_get_dim_rename_om3(self, mock_vocab, mock_mapping, temp_dir):
        """Test dimension renaming for ACCESS-OM3."""
        with patch("access_moppy.ocean.Supergrid"):
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
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
            cmoriser = CMIP6_Ocean_CMORiser_OM3(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="Omon.tos",
                cmip6_vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )

            assert cmoriser.arakawa == "C"