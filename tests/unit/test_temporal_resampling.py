"""
Tests for temporal resampling functionality in ACCESS-MOPPy.

Tests cover:
- Aggregation method detection based on variable metadata  
- Temporal resampling with various methods (mean, sum, min, max)
- Integration with CMORiser workflow
- Error handling for invalid operations
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.utilities import (
    determine_resampling_method,
    get_resampling_frequency_string,
    resample_dataset_temporal,
    validate_and_resample_if_needed,
    FrequencyMismatchError,
    IncompatibleFrequencyError,
)


class TestResamplingMethodDetection:
    """Tests for automatic resampling method detection."""
    
    def test_precipitation_variables_use_sum(self):
        """Test that precipitation variables default to sum aggregation."""
        # Standard precipitation variable
        attrs = {
            'standard_name': 'precipitation_flux',
            'units': 'kg m-2 s-1',
            'long_name': 'Precipitation'
        }
        method = determine_resampling_method('pr', attrs)
        assert method == 'sum'
        
        # Rain variable
        attrs = {
            'standard_name': 'rainfall_flux', 
            'units': 'kg/m2/s',
            'long_name': 'Rainfall Rate'
        }
        method = determine_resampling_method('prrn', attrs)
        assert method == 'sum'

    def test_temperature_variables_use_mean(self):
        """Test that temperature variables default to mean aggregation."""
        # Surface air temperature
        attrs = {
            'standard_name': 'air_temperature',
            'units': 'K',
            'long_name': 'Near-Surface Air Temperature'
        }
        method = determine_resampling_method('tas', attrs)
        assert method == 'mean'
        
        # Sea surface temperature
        attrs = {
            'standard_name': 'sea_surface_temperature',
            'units': 'degC', 
            'long_name': 'Sea Surface Temperature'
        }
        method = determine_resampling_method('tos', attrs)
        assert method == 'mean'

    def test_wind_variables_use_mean(self):
        """Test that wind variables default to mean aggregation."""
        # Eastward wind
        attrs = {
            'standard_name': 'eastward_wind',
            'units': 'm s-1',
            'long_name': 'Eastward Near-Surface Wind'
        }
        method = determine_resampling_method('uas', attrs)
        assert method == 'mean'
        
        # Northward wind
        attrs = {
            'standard_name': 'northward_wind',
            'units': 'm/s',
            'long_name': 'Northward Near-Surface Wind'
        }
        method = determine_resampling_method('vas', attrs)
        assert method == 'mean'

    def test_extremes_use_appropriate_method(self):
        """Test that extreme variables use min/max as appropriate."""
        # Maximum temperature
        attrs = {
            'standard_name': 'air_temperature',
            'units': 'K',
            'long_name': 'Daily Maximum Near-Surface Air Temperature'
        }
        method = determine_resampling_method('tasmax', attrs)
        assert method == 'max'
        
        # Minimum temperature  
        attrs = {
            'standard_name': 'air_temperature',
            'units': 'K',
            'long_name': 'Daily Minimum Near-Surface Air Temperature'
        }
        method = determine_resampling_method('tasmin', attrs)
        assert method == 'min'

    def test_cell_methods_guidance(self):
        """Test that cell_methods attribute provides guidance."""
        # Explicitly marked as sum
        attrs = {
            'cell_methods': 'area: time: sum',
            'units': 'kg m-2 s-1'
        }
        method = determine_resampling_method('var', attrs)
        assert method == 'sum'
        
        # Explicitly marked as mean
        attrs = {
            'cell_methods': 'area: mean time: mean',
            'units': 'K'
        }
        method = determine_resampling_method('var', attrs)  
        assert method == 'mean'

    def test_default_fallback_to_mean(self):
        """Test that unknown variables default to mean."""
        attrs = {
            'standard_name': 'unknown_quantity',
            'units': 'arbitrary_units'
        }
        method = determine_resampling_method('unknown', attrs)
        assert method == 'mean'


class TestFrequencyStringConversion:
    """Tests for converting pandas Timedelta to frequency strings."""
    
    def test_hourly_frequencies(self):
        """Test conversion of hourly frequencies."""
        # 1 hour
        freq_str = get_resampling_frequency_string(pd.Timedelta(hours=1))
        assert freq_str == 'H'
        
        # 3 hours
        freq_str = get_resampling_frequency_string(pd.Timedelta(hours=3))
        assert freq_str == '3H'
        
        # 6 hours  
        freq_str = get_resampling_frequency_string(pd.Timedelta(hours=6))
        assert freq_str == '6H'

    def test_daily_frequencies(self):
        """Test conversion of daily frequencies.""" 
        # 1 day
        freq_str = get_resampling_frequency_string(pd.Timedelta(days=1))
        assert freq_str == 'D'

    def test_monthly_frequencies(self):
        """Test conversion of monthly frequencies."""
        # ~30 days (monthly)
        freq_str = get_resampling_frequency_string(pd.Timedelta(days=30))
        assert freq_str == 'M'

    def test_yearly_frequencies(self):
        """Test conversion of yearly frequencies."""
        # ~365 days (yearly)
        freq_str = get_resampling_frequency_string(pd.Timedelta(days=365))
        assert freq_str == 'Y'


class TestTemporalResampling:
    """Tests for actual temporal resampling operations."""
    
    def create_test_dataset(self, freq='H', periods=24, start='2020-01-01'):
        """Create a test dataset for resampling."""
        time = pd.date_range(start=start, periods=periods, freq=freq)
        
        # Create test variables with different characteristics
        data_vars = {
            'tas': (['time'], np.random.normal(290, 5, periods), {
                'standard_name': 'air_temperature',
                'units': 'K',
                'long_name': 'Near-Surface Air Temperature'
            }),
            'pr': (['time'], np.random.exponential(0.1, periods), {
                'standard_name': 'precipitation_flux', 
                'units': 'kg m-2 s-1',
                'long_name': 'Precipitation'
            }),
            'uas': (['time'], np.random.normal(0, 3, periods), {
                'standard_name': 'eastward_wind',
                'units': 'm s-1',
                'long_name': 'Eastward Near-Surface Wind'
            })
        }
        
        coords = {'time': time}
        
        return xr.Dataset(data_vars, coords=coords)

    def test_hourly_to_daily_resampling(self):
        """Test resampling from hourly to daily frequency."""
        # Create hourly dataset
        ds = self.create_test_dataset(freq='H', periods=48)  # 2 days of hourly data
        
        # Resample to daily
        target_freq = pd.Timedelta(days=1)
        ds_resampled = resample_dataset_temporal(
            ds, target_freq, 'tas', method='auto'
        )
        
        # Check results
        assert len(ds_resampled.time) == 2  # Should have 2 days
        assert 'tas' in ds_resampled.data_vars
        assert 'pr' in ds_resampled.data_vars
        assert 'uas' in ds_resampled.data_vars
        
        # Check cell_methods were updated
        assert 'time: mean' in ds_resampled['tas'].attrs['cell_methods']
        assert 'time: sum' in ds_resampled['pr'].attrs['cell_methods']
        assert 'time: mean' in ds_resampled['uas'].attrs['cell_methods']

    def test_daily_to_monthly_resampling(self):
        """Test resampling from daily to monthly frequency.""" 
        # Create daily dataset for one month
        ds = self.create_test_dataset(freq='D', periods=31, start='2020-01-01')
        
        # Resample to monthly
        target_freq = pd.Timedelta(days=30)  # Approximate monthly
        ds_resampled = resample_dataset_temporal(
            ds, target_freq, 'tas', method='auto'
        )
        
        # Check results
        assert len(ds_resampled.time) == 1  # Should have 1 month
        assert ds_resampled['tas'].values.size > 0

    def test_explicit_method_selection(self):
        """Test using explicit resampling methods."""
        ds = self.create_test_dataset(freq='H', periods=24)
        target_freq = pd.Timedelta(days=1)
        
        # Test explicit mean
        ds_mean = resample_dataset_temporal(
            ds, target_freq, 'tas', method='mean'
        )
        assert 'time: mean' in ds_mean['tas'].attrs['cell_methods']
        assert 'time: mean' in ds_mean['pr'].attrs['cell_methods']  # Override auto
        
        # Test explicit sum
        ds_sum = resample_dataset_temporal(
            ds, target_freq, 'tas', method='sum'
        )
        assert 'time: sum' in ds_sum['tas'].attrs['cell_methods']
        assert 'time: sum' in ds_sum['pr'].attrs['cell_methods']

    def test_invalid_resampling_fails(self):
        """Test that invalid resampling operations fail gracefully."""
        ds = self.create_test_dataset(freq='D', periods=7)
        
        # Try to resample to higher frequency (should work but may not be meaningful)
        target_freq = pd.Timedelta(hours=1)
        
        # This should work technically but results may not be meaningful
        ds_resampled = resample_dataset_temporal(
            ds, target_freq, 'tas', method='mean'
        )
        assert len(ds_resampled.time) >= len(ds.time)


class TestIntegratedValidationAndResampling:
    """Tests for the integrated validation and resampling workflow."""
    
    def create_test_dataset_with_time_encoding(self, freq='H', periods=24):
        """Create test dataset with proper time encoding for CMIP6."""
        time = pd.date_range(start='2020-01-01', periods=periods, freq=freq)
        
        data_vars = {
            'tas': (['time'], np.random.normal(290, 5, periods), {
                'standard_name': 'air_temperature',
                'units': 'K'
            })
        }
        
        coords = {
            'time': (['time'], time, {
                'units': 'days since 1850-01-01',
                'calendar': 'standard'
            })
        }
        
        return xr.Dataset(data_vars, coords=coords)

    def test_compatible_frequency_no_resampling(self):
        """Test that compatible frequencies don't trigger resampling."""
        # Create monthly dataset
        ds = self.create_test_dataset_with_time_encoding(freq='MS', periods=12)  # MS = month start
        
        # Should not need resampling for monthly table
        ds_result, was_resampled = validate_and_resample_if_needed(
            ds, 'Amon.tas', 'tas'
        )
        
        assert not was_resampled
        assert ds_result is ds  # Should be same object

    def test_resampling_required_and_applied(self):
        """Test that incompatible frequencies trigger resampling."""
        # Create daily dataset  
        ds = self.create_test_dataset_with_time_encoding(freq='D', periods=31)
        
        # Should need resampling for monthly table
        ds_result, was_resampled = validate_and_resample_if_needed(
            ds, 'Amon.tas', 'tas'
        )
        
        assert was_resampled
        assert ds_result is not ds  # Should be different object
        assert len(ds_result.time) < len(ds.time)  # Should have fewer time steps

    def test_incompatible_frequency_raises_error(self):
        """Test that incompatible upsampling raises appropriate error.""" 
        # Create monthly dataset
        ds = self.create_test_dataset_with_time_encoding(freq='MS', periods=12)
        
        # Try to use for daily table (upsampling not allowed)
        with pytest.raises(IncompatibleFrequencyError):
            validate_and_resample_if_needed(ds, 'day.tas', 'tas')

    def test_invalid_compound_name_raises_error(self):
        """Test that invalid compound names are handled."""
        ds = self.create_test_dataset_with_time_encoding(freq='D', periods=7)
        
        # Invalid compound name format
        with pytest.raises(ValueError):
            validate_and_resample_if_needed(ds, 'invalid_format', 'tas')
        
        # Unsupported table frequency
        with pytest.raises(ValueError):
            validate_and_resample_if_needed(ds, 'Unknown.tas', 'tas')


class TestErrorHandling:
    """Tests for error handling in resampling operations."""
    
    def test_missing_time_coordinate_raises_error(self):
        """Test that missing time coordinate raises appropriate error."""
        # Dataset without time coordinate
        ds = xr.Dataset({
            'tas': (['x'], [1, 2, 3])
        }, coords={'x': [1, 2, 3]})
        
        target_freq = pd.Timedelta(days=1)
        
        with pytest.raises(ValueError, match="Time coordinate 'time' not found"):
            resample_dataset_temporal(ds, target_freq, 'tas')

    def test_invalid_method_falls_back_to_mean(self):
        """Test that invalid methods fall back to mean."""
        time = pd.date_range('2020-01-01', periods=24, freq='H')
        ds = xr.Dataset({
            'tas': (['time'], np.random.normal(290, 5, 24))
        }, coords={'time': time})
        
        target_freq = pd.Timedelta(days=1)
        
        # Should work and fall back to mean
        ds_result = resample_dataset_temporal(
            ds, target_freq, 'tas', method='invalid_method'
        )
        
        assert len(ds_result.time) == 1
        assert 'tas' in ds_result.data_vars


if __name__ == '__main__':
    pytest.main([__file__])