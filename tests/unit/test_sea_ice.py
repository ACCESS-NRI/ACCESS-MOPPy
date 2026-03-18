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
