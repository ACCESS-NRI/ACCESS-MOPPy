"""
Unit tests for the CMIP6_CMORiser base class.

These tests focus on the core functionality of the CMIP6_CMORiser class
without requiring complex dependencies or data files.
"""

from pathlib import Path
from unittest.mock import Mock

import pytest

from access_moppy.base import CMIP6_CMORiser


class TestCMIP6CMORiser:
    """Unit tests for CMIP6_CMORiser base class."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock CMIP6 vocabulary object."""
        vocab = Mock()
        vocab.get_table = Mock(return_value={"tas": {"units": "K"}})
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping."""
        return {
            "CF standard Name": "air_temperature",
            "units": "K",
            "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            "positive": None,
        }

    @pytest.mark.unit
    def test_init_with_valid_params(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with valid parameters."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        assert cmoriser.input_paths == ["test.nc"]
        assert cmoriser.output_path == str(temp_dir)
        assert cmoriser.cmor_name == "tas"
        assert cmoriser.vocab == mock_vocab
        assert cmoriser.mapping == mock_mapping

    @pytest.mark.unit
    def test_init_with_multiple_input_paths(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with multiple input files."""
        input_files = ["test1.nc", "test2.nc", "test3.nc"]
        cmoriser = CMIP6_CMORiser(
            input_paths=input_files,
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        assert cmoriser.input_paths == input_files

    @pytest.mark.unit
    def test_init_with_single_input_path_string(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """Test initialization with single input path as string."""
        cmoriser = CMIP6_CMORiser(
            input_paths="single_file.nc",
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        assert cmoriser.input_paths == ["single_file.nc"]

    @pytest.mark.unit
    def test_init_with_drs_root(self, mock_vocab, mock_mapping, temp_dir):
        """Test initialization with DRS root path."""
        drs_root = temp_dir / "drs"
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
            drs_root=str(drs_root),
        )

        assert cmoriser.drs_root == Path(drs_root)

    @pytest.mark.unit
    def test_version_date_format(self, mock_vocab, mock_mapping, temp_dir):
        """Test that version date is set correctly."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        # Check that version_date is a string in YYYYMMDD format
        assert isinstance(cmoriser.version_date, str)
        assert len(cmoriser.version_date) == 8
        assert cmoriser.version_date.isdigit()

    @pytest.mark.unit
    def test_type_mapping_attribute(self, mock_vocab, mock_mapping, temp_dir):
        """Test that type_mapping is available as class attribute."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        # type_mapping should be available from utilities
        assert hasattr(cmoriser, "type_mapping")
        assert cmoriser.type_mapping is not None

    @pytest.mark.unit
    def test_dataset_proxy_methods(self, mock_vocab, mock_mapping, temp_dir):
        """Test that the CMORiser can proxy dataset operations."""
        # Create a mock dataset
        mock_dataset = Mock()
        mock_dataset.test_attr = "test_value"
        mock_dataset.__getitem__ = Mock(return_value="dataset_item")
        mock_dataset.__setitem__ = Mock()
        mock_dataset.__repr__ = Mock(return_value="<Dataset representation>")

        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        # Set the dataset
        cmoriser.ds = mock_dataset

        # Test __getitem__ proxy
        result = cmoriser["test_key"]
        assert result == "dataset_item"
        mock_dataset.__getitem__.assert_called_with("test_key")

        # Test __getattr__ proxy
        assert cmoriser.test_attr == "test_value"

        # Test __setitem__ proxy
        cmoriser["new_key"] = "new_value"
        mock_dataset.__setitem__.assert_called_with("new_key", "new_value")

        # Test __repr__ proxy
        repr_result = repr(cmoriser)
        assert repr_result == "<Dataset representation>"

    @pytest.mark.unit
    def test_dataset_none_initially(self, mock_vocab, mock_mapping, temp_dir):
        """Test that dataset is None initially."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        assert cmoriser.ds is None

    @pytest.mark.unit
    def test_getattr_fallback(self, mock_vocab, mock_mapping, temp_dir):
        """Test __getattr__ behavior when dataset is None."""
        cmoriser = CMIP6_CMORiser(
            input_paths=["test.nc"],
            output_path=str(temp_dir),
            cmor_name="tas",
            cmip6_vocab=mock_vocab,
            variable_mapping=mock_mapping,
        )

        # When ds is None, getattr should raise AttributeError
        with pytest.raises(AttributeError):
            _ = cmoriser.nonexistent_attribute
