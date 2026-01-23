#!/usr/bin/env python
"""
Unit tests for missing value standardization functionality.

Tests the CMIP6Vocabulary missing value methods and their integration
with the CMORiser base class.
"""

from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.vocabulary_processors import CMIP6Vocabulary


class TestCMIP6VocabularyMissingValues:
    """Test missing value standardization in CMIP6Vocabulary."""

    @pytest.fixture
    def mock_vocab_data(self):
        """Create mock vocabulary data for testing."""
        return {
            "experiment_id": {
                "piControl": {
                    "experiment": "pre-industrial control",
                    "activity_id": ["CMIP"],
                }
            },
            "source_id": {
                "ACCESS-ESM1.6": {
                    "label": "ACCESS-ESM1.6",
                    "institution_id": ["CSIRO"],
                    "license_info": {"id": "CC BY 4.0"},
                    "release_year": "2021",
                    "model_component": {
                        "atmos": {"description": "UM atmosphere model"}
                    },
                }
            },
        }

    @pytest.fixture
    def mock_table_data(self):
        """Create mock CMIP6 table data for testing."""
        return {
            "Header": {
                "missing_value": "1e20",
                "int_missing_value": "-999",
                "table_id": "Amon",
            },
            "variable_entry": {
                "tas": {
                    "frequency": "mon",
                    "modeling_realm": "atmos",
                    "units": "K",
                    "type": "real",
                    "dimensions": "longitude latitude time",
                },
                "pr": {
                    "frequency": "mon",
                    "modeling_realm": "atmos",
                    "units": "kg m-2 s-1",
                    "type": "real",
                    "missing_value": "9.96921e+36",  # Variable-specific missing value
                    "dimensions": "longitude latitude time",
                },
            },
        }

    @pytest.fixture
    def vocabulary_instance(self, mock_vocab_data, mock_table_data):
        """Create a CMIP6Vocabulary instance with mocked data."""
        with (
            patch.object(
                CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
            ),
            patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
        ):
            vocab = CMIP6Vocabulary(
                compound_name="Amon.tas",
                experiment_id="piControl",
                source_id="ACCESS-ESM1.6",
                variant_label="r1i1p1f1",
                grid_label="gn",
            )
            return vocab

    def test_get_cmip_missing_value_default(self, vocabulary_instance):
        """Test getting default missing value from table header."""
        missing_value = vocabulary_instance.get_cmip_missing_value()
        assert missing_value == 1e20

    def test_get_cmip_missing_value_variable_specific(
        self, mock_vocab_data, mock_table_data
    ):
        """Test getting variable-specific missing value."""
        with (
            patch.object(
                CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
            ),
            patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
        ):
            vocab = CMIP6Vocabulary(
                compound_name="Amon.pr",
                experiment_id="piControl",
                source_id="ACCESS-ESM1.6",
                variant_label="r1i1p1f1",
                grid_label="gn",
            )

            missing_value = vocab.get_cmip_missing_value()
            assert missing_value == 9.96921e36

    def test_get_cmip_fill_value_equals_missing_value(self, vocabulary_instance):
        """Test that _FillValue equals missing_value for CMIP6."""
        missing_value = vocabulary_instance.get_cmip_missing_value()
        fill_value = vocabulary_instance.get_cmip_fill_value()
        assert fill_value == missing_value

    def test_standardize_missing_values_nan_replacement(self, vocabulary_instance):
        """Test that NaN values are replaced with CMIP6 missing value."""
        # Create test data with NaN values
        data = np.array([[1.0, 2.0, np.nan], [4.0, np.nan, 6.0]])
        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            attrs={"units": "K", "missing_value": -1e20, "_FillValue": -1e20},
        )

        # Standardize missing values
        result = vocabulary_instance.standardize_missing_values(da)

        # Check that NaN values are replaced
        expected_missing = 1e20
        assert result.attrs["missing_value"] == expected_missing
        assert result.attrs["_FillValue"] == expected_missing

        # Check data values
        expected_data = np.array(
            [[1.0, 2.0, expected_missing], [4.0, expected_missing, 6.0]]
        )
        np.testing.assert_array_equal(result.values, expected_data)

    def test_standardize_missing_values_convert_existing(self, vocabulary_instance):
        """Test conversion of existing missing values to CMIP6 standard."""
        # Create test data with non-standard missing values
        old_missing = -999.0
        data = np.array([[1.0, 2.0, old_missing], [4.0, old_missing, 6.0]])
        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            attrs={
                "units": "K",
                "missing_value": old_missing,
                "_FillValue": old_missing,
            },
        )

        # Standardize missing values with conversion
        result = vocabulary_instance.standardize_missing_values(
            da, convert_existing=True
        )

        # Check that old missing values are converted
        expected_missing = 1e20
        assert result.attrs["missing_value"] == expected_missing
        assert result.attrs["_FillValue"] == expected_missing

        # Check data values - old missing values should be replaced
        expected_data = np.array(
            [[1.0, 2.0, expected_missing], [4.0, expected_missing, 6.0]]
        )
        np.testing.assert_array_equal(result.values, expected_data)

    def test_standardize_missing_values_preserve_valid_data(self, vocabulary_instance):
        """Test that valid data values are preserved during standardization."""
        # Create test data with various valid values
        data = np.array([[1.5, 273.15, 0.0], [-10.5, 350.2, 1000.0]])
        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            attrs={"units": "K", "missing_value": 1e30, "_FillValue": 1e30},
        )

        # Standardize missing values
        result = vocabulary_instance.standardize_missing_values(da)

        # Valid data should be unchanged
        np.testing.assert_array_equal(result.values, data)

        # Only attributes should change
        assert result.attrs["missing_value"] == 1e20
        assert result.attrs["_FillValue"] == 1e20

    def test_standardize_missing_values_no_conversion(self, vocabulary_instance):
        """Test standardization without converting existing missing values."""
        # Create test data with old missing values and NaN
        old_missing = -999.0
        data = np.array([[1.0, np.nan, old_missing], [4.0, old_missing, 6.0]])
        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            attrs={
                "units": "K",
                "missing_value": old_missing,
                "_FillValue": old_missing,
            },
        )

        # Standardize without conversion
        result = vocabulary_instance.standardize_missing_values(
            da, convert_existing=False
        )

        # Only NaN should be converted, old missing values preserved
        expected_missing = 1e20
        expected_data = np.array(
            [[1.0, expected_missing, old_missing], [4.0, old_missing, 6.0]]
        )
        np.testing.assert_array_equal(result.values, expected_data)

        # Attributes should still be updated
        assert result.attrs["missing_value"] == expected_missing
        assert result.attrs["_FillValue"] == expected_missing


class TestSafeArithmeticOperations:
    """Test safe arithmetic operations for derived variables."""

    def test_safe_operations_preserve_missing_values(self):
        """Test that safe operations properly handle missing values from multiple sources."""
        # This would require importing the actual safe operation functions
        # For now, this is a placeholder for integration testing
        pass

    def test_derived_variable_missing_value_consistency(self):
        """Test that derived variables have consistent missing values."""
        # This would test the full pipeline from derivation to standardization
        # For now, this is a placeholder for integration testing
        pass


if __name__ == "__main__":
    pytest.main([__file__])
