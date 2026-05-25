from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.sea_ice import SeaIce_CMORiser


class TestSeaIceCMORiser:
    """Unit tests for sea-ice CMORisation."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock vocabulary for sea-ice tests."""
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1.6"
        vocab.variable = {"units": "1", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab._get_axes = Mock(return_value=({}, {}))
        vocab._get_required_bounds_variables = Mock(return_value=({}, {}))
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping for sea-ice tests."""
        return {
            "siconc": {
                "model_variables": ["ice_conc"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.fixture
    def mock_seaice_dataset(self):
        """Create a mock sea-ice dataset with time not in first position."""
        time = pd.date_range("2000-01-01", periods=3, freq="ME")
        nj = np.arange(2)
        ni = np.arange(4)

        return xr.Dataset(
            data_vars={
                "ice_conc": (
                    ["nj", "time", "ni"],
                    np.random.random((2, 3, 4)),
                    {"coordinates": "ULON ULAT", "units": "1"},
                )
            },
            coords={
                "time": ("time", time),
                "nj": ("nj", nj),
                "ni": ("ni", ni),
            },
        )

    @pytest.mark.unit
    def test_select_and_process_variables_moves_time_to_first_dimension(
        self, mock_vocab, mock_mapping, mock_seaice_dataset, temp_dir
    ):
        """Ensure the processed sea-ice variable has time as the leading dimension."""
        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_seaice_dataset

                cmoriser.select_and_process_variables()

                assert cmoriser.ds[cmoriser.cmor_name].dims == ("time", "j", "i")

    @pytest.mark.unit
    def test_select_and_process_missing_model_var_in_formula_raises_key_error(
        self, mock_vocab, temp_dir
    ):
        """Formula calc with a missing model variable raises KeyError with context."""
        import pandas as pd

        mapping = {
            "siconc": {
                "model_variables": ["missing_var"],
                "calculation": {"type": "formula", "operation": "missing_var * 0.01"},
            }
        }
        time = pd.date_range("2000-01-01", periods=2, freq="ME")
        ds = xr.Dataset(
            {"other_var": (["time"], [1.0, 2.0])},
            coords={"time": time},
        )

        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mapping,
                )
                cmoriser.ds = ds

                with pytest.raises(KeyError, match="missing_var"):
                    cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_select_and_process_empty_required_vars_direct_raises(
        self, mock_vocab, temp_dir
    ):
        """direct calc type with empty model_variables raises ValueError."""
        mapping = {
            "siconc": {
                "model_variables": [],
                "calculation": {"type": "direct"},
            }
        }

        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mapping,
                )
                cmoriser.ds = xr.Dataset()

                with pytest.raises(
                    ValueError, match="requires at least one model_variable"
                ):
                    cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_infer_grid_type_lists_data_var_coord_attrs(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """infer_grid_type error must list each data_var with its 'coordinates' attr."""
        ds = xr.Dataset(
            data_vars={
                "ice_conc": (
                    ["nj", "ni"],
                    np.zeros((2, 2)),
                    {"coordinates": "XLON XLAT"},
                ),
                "ice_thick": (["nj", "ni"], np.zeros((2, 2))),
            }
        )

        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            with pytest.raises(
                ValueError, match="Could not infer grid type"
            ) as exc_info:
                cmoriser.infer_grid_type()

            msg = str(exc_info.value)
            assert "ice_conc" in msg
            assert "XLON XLAT" in msg
            assert "ice_thick" in msg
            assert "<missing>" in msg

    @pytest.mark.unit
    def test_get_dim_rename_unsupported_source_id_mentions_access_prefix(
        self, mock_mapping, temp_dir
    ):
        """_get_dim_rename raises ValueError naming the required ACCESS- prefix."""
        vocab = Mock()
        vocab.source_id = "OTHER-MODEL"
        vocab._get_nominal_resolution = Mock(return_value="1deg")

        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=vocab,
                variable_mapping=mock_mapping,
            )

            with pytest.raises(
                ValueError, match="Unsupported source_id 'OTHER-MODEL'"
            ) as exc_info:
                cmoriser._get_dim_rename()

            msg = str(exc_info.value)
            assert "SeaIce_CMORiser" in msg
            assert "ACCESS-" in msg

    @pytest.mark.unit
    def test_update_attributes_sets_time_cf_attributes(self, temp_dir):
        """Sea-ice time must gain both standard_name and axis from the CMOR table."""
        ny, nx, nt = 2, 4, 3
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1.6"
        vocab.variable = {"units": "1", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={})
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "long_name": "time",
                "axis": "T",
            }
        }
        mapping = {
            "siconc": {
                "model_variables": ["ice_conc"],
                "calculation": {"type": "direct"},
            }
        }
        ds = xr.Dataset(
            {"siconc": (["time", "j", "i"], np.ones((nt, ny, nx), dtype=np.float32))},
            coords={
                "time": ("time", pd.date_range("2000-01-01", periods=nt, freq="ME")),
                "i": ("i", np.arange(nx)),
                "j": ("j", np.arange(ny)),
            },
        )
        grid_info = {
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
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=vocab,
                variable_mapping=mapping,
            )
        cmoriser.ds = ds
        cmoriser.grid_type = "T"
        cmoriser.symmetric = None
        cmoriser.supergrid = Mock()
        cmoriser.supergrid.extract_grid.return_value = grid_info

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time"].attrs["standard_name"] == "time"
        assert cmoriser.ds["time"].attrs["axis"] == "T"
