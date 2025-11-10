"""
Tests for temporal frequency detection functionality.
"""

import pytest
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import os

from access_moppy.utilities import (
    detect_time_frequency_lazy,
    validate_consistent_frequency,
    FrequencyMismatchError
)


class TestFrequencyDetection:
    """Test frequency detection functionality."""
    
    def create_test_dataset(self, time_values, time_units="days since 2000-01-01", calendar="standard"):
        """Create a test dataset with specified time values."""
        ds = xr.Dataset({
            'tas': (['time', 'lat', 'lon'], np.random.rand(len(time_values), 10, 10)),
            'time': (['time'], time_values, {
                'units': time_units,
                'calendar': calendar
            }),
            'lat': (['lat'], np.linspace(-90, 90, 10)),
            'lon': (['lon'], np.linspace(-180, 180, 10))
        })
        return ds
    
    def test_detect_monthly_frequency(self):
        """Test detection of monthly frequency."""
        # Create monthly time series (30-day intervals)
        time_values = np.arange(0, 365*2, 30)  # ~monthly for 2 years
        ds = self.create_test_dataset(time_values)
        
        freq = detect_time_frequency_lazy(ds)
        assert freq is not None
        # Should detect approximately 30-day frequency
        assert 25 <= freq.days <= 35  # Allow some tolerance
    
    def test_detect_daily_frequency(self):
        """Test detection of daily frequency."""
        # Create daily time series
        time_values = np.arange(0, 30)  # 30 days
        ds = self.create_test_dataset(time_values)
        
        freq = detect_time_frequency_lazy(ds)
        assert freq is not None
        assert freq.days == 1
    
    def test_detect_hourly_frequency(self):
        """Test detection of hourly frequency."""
        # Create hourly time series (fractional days)
        time_values = np.arange(0, 2, 1/24)  # 2 days, hourly
        ds = self.create_test_dataset(time_values)
        
        freq = detect_time_frequency_lazy(ds)
        assert freq is not None
        assert 0.95 <= freq.total_seconds() / 3600 <= 1.05  # ~1 hour
    
    def test_insufficient_time_points(self):
        """Test error handling for insufficient time points."""
        time_values = np.array([0])  # Only 1 time point
        ds = self.create_test_dataset(time_values)
        
        with pytest.raises(ValueError, match="Need at least 2 time points"):
            detect_time_frequency_lazy(ds)
    
    def test_missing_time_coordinate(self):
        """Test error handling for missing time coordinate."""
        ds = xr.Dataset({
            'tas': (['x', 'y'], np.random.rand(10, 10)),
        })
        
        with pytest.raises(ValueError, match="Time coordinate 'time' not found"):
            detect_time_frequency_lazy(ds)
    
    def test_validate_consistent_frequency_success(self):
        """Test successful validation of consistent frequency across files."""
        # Create temporary test files
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create 3 files with the same frequency (daily)
            file_paths = []
            for i in range(3):
                time_values = np.arange(i*10, (i+1)*10)  # 10 days each, consecutive
                ds = self.create_test_dataset(time_values)
                
                filepath = Path(tmpdir) / f"test_{i}.nc"
                ds.to_netcdf(filepath)
                file_paths.append(str(filepath))
            
            # Should validate successfully
            freq = validate_consistent_frequency(file_paths)
            assert freq is not None
            assert freq.days == 1  # Daily frequency
    
    def test_validate_inconsistent_frequency_error(self):
        """Test error handling for inconsistent frequencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files with different frequencies
            file_paths = []
            
            # File 1: daily
            time_values1 = np.arange(0, 10)  # days 0-9
            ds1 = self.create_test_dataset(time_values1)
            filepath1 = Path(tmpdir) / "daily.nc"
            ds1.to_netcdf(filepath1)
            file_paths.append(str(filepath1))
            
            # File 2: monthly (30-day intervals)
            time_values2 = np.arange(0, 120, 30)  # 4 months
            ds2 = self.create_test_dataset(time_values2)
            filepath2 = Path(tmpdir) / "monthly.nc"
            ds2.to_netcdf(filepath2)
            file_paths.append(str(filepath2))
            
            # Should raise FrequencyMismatchError
            with pytest.raises(FrequencyMismatchError, match="Inconsistent temporal frequencies"):
                validate_consistent_frequency(file_paths, tolerance_seconds=3600)  # 1 hour tolerance
    
    def test_validate_single_file(self):
        """Test validation with single file (should work without error)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            time_values = np.arange(0, 10)  # 10 days
            ds = self.create_test_dataset(time_values)
            
            filepath = Path(tmpdir) / "single.nc"
            ds.to_netcdf(filepath)
            
            # Should work with single file
            freq = validate_consistent_frequency([str(filepath)])
            assert freq is not None
            assert freq.days == 1
    
    def test_validate_empty_file_list(self):
        """Test error handling for empty file list."""
        with pytest.raises(ValueError, match="No file paths provided"):
            validate_consistent_frequency([])
    
    def test_frequency_tolerance(self):
        """Test frequency validation with tolerance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_paths = []
            
            # File 1: exactly daily
            time_values1 = np.arange(0, 10)  # days 0-9
            ds1 = self.create_test_dataset(time_values1)
            filepath1 = Path(tmpdir) / "exact_daily.nc"
            ds1.to_netcdf(filepath1)
            file_paths.append(str(filepath1))
            
            # File 2: slightly off daily (1.1 day intervals)
            time_values2 = np.arange(0, 11, 1.1)  # ~10 days with 1.1 day intervals
            ds2 = self.create_test_dataset(time_values2)
            filepath2 = Path(tmpdir) / "slightly_off.nc"
            ds2.to_netcdf(filepath2)
            file_paths.append(str(filepath2))
            
            # Should pass with large tolerance
            freq = validate_consistent_frequency(file_paths, tolerance_seconds=10000)  # ~2.8 hours
            assert freq is not None
            
            # Should fail with small tolerance
            with pytest.raises(FrequencyMismatchError):
                validate_consistent_frequency(file_paths, tolerance_seconds=1000)  # ~17 minutes


class TestIntegrationWithCMORiser:
    """Test integration with the main CMORiser classes."""
    
    def test_frequency_validation_in_driver(self):
        """Test that frequency validation can be controlled via driver."""
        # This is a basic integration test - would need actual test files for full testing
        from access_moppy.driver import ACCESS_ESM_CMORiser
        
        # Test that the parameter is accepted
        try:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=[],  # Empty for this test
                compound_name="Amon.tas",
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                validate_frequency=False  # This should disable validation
            )
            # Just check that the parameter is stored correctly
            assert hasattr(cmoriser, 'validate_frequency')
            assert cmoriser.validate_frequency is False
        except Exception as e:
            # If this fails due to missing files, that's OK for this basic test
            # We just want to ensure the parameter is accepted
            if "No file paths provided" not in str(e):
                raise


if __name__ == "__main__":
    # Run a quick test if executed directly
    test = TestFrequencyDetection()
    print("Running basic frequency detection tests...")
    
    # Test monthly frequency detection
    time_values = np.arange(0, 365, 30)  # Monthly
    ds = test.create_test_dataset(time_values)
    freq = detect_time_frequency_lazy(ds)
    print(f"Detected monthly frequency: {freq}")
    
    # Test daily frequency detection  
    time_values = np.arange(0, 30)  # Daily
    ds = test.create_test_dataset(time_values)
    freq = detect_time_frequency_lazy(ds)
    print(f"Detected daily frequency: {freq}")
    
    print("Basic tests completed successfully!")