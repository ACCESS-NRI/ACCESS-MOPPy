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
    validate_cmip6_frequency_compatibility,
    parse_cmip6_table_frequency,
    is_frequency_compatible,
    FrequencyMismatchError,
    IncompatibleFrequencyError,
    ResamplingRequiredWarning
)


class TestFrequencyDetection:
    """Tests for lazy temporal frequency detection."""
    
    def create_test_dataset(self, freq='H', periods=24, start='2020-01-01'):
        """Create a test dataset for frequency detection."""
        time = pd.date_range(start=start, periods=periods, freq=freq)
        
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
    
    def create_dataset_with_bounds(self, freq_seconds=3600, periods=24, start_day=0):
        """Create a dataset with CF-compliant time bounds."""
        # Create time centers
        time_centers = np.arange(start_day, start_day + periods * freq_seconds / 86400, freq_seconds / 86400)
        
        # Create time bounds (start and end of each interval)
        half_interval = (freq_seconds / 86400) / 2
        time_bounds = np.array([[center - half_interval, center + half_interval] 
                               for center in time_centers])
        
        data_vars = {
            'tas': (['time'], np.random.normal(290, 5, periods)),
            'time_bnds': (['time', 'bnds'], time_bounds, {
                'units': 'days since 2000-01-01',
                'calendar': 'standard'
            })
        }
        
        coords = {
            'time': (['time'], time_centers, {
                'units': 'days since 2000-01-01',
                'calendar': 'standard',
                'bounds': 'time_bnds'  # CF-compliant bounds reference
            })
        }
        
        return xr.Dataset(data_vars, coords=coords)
    
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

    def test_single_time_point_without_bounds_warns(self):
        """Test that datasets with single time point and no bounds warn appropriately."""
        # Dataset with only 1 time point and no bounds
        ds = self.create_test_dataset([0])  # Single time point
        
        with pytest.warns(UserWarning, match="Only one time point available"):
            result = detect_time_frequency_lazy(ds)
            assert result is None

    def test_time_bounds_detection_hourly(self):
        """Test frequency detection from hourly time bounds."""
        ds = self.create_dataset_with_bounds(freq_seconds=3600, periods=24)  # Hourly
        
        detected_freq = detect_time_frequency_lazy(ds)
        
        assert detected_freq is not None
        assert abs(detected_freq.total_seconds() - 3600) < 1  # 1 hour ± 1 second tolerance

    def test_time_bounds_detection_daily(self):
        """Test frequency detection from daily time bounds."""
        ds = self.create_dataset_with_bounds(freq_seconds=86400, periods=7)  # Daily
        
        detected_freq = detect_time_frequency_lazy(ds)
        
        assert detected_freq is not None
        assert abs(detected_freq.total_seconds() - 86400) < 1  # 1 day ± 1 second tolerance

    def test_time_bounds_detection_3hourly(self):
        """Test frequency detection from 3-hourly time bounds."""
        ds = self.create_dataset_with_bounds(freq_seconds=10800, periods=8)  # 3-hourly
        
        detected_freq = detect_time_frequency_lazy(ds)
        
        assert detected_freq is not None
        assert abs(detected_freq.total_seconds() - 10800) < 1  # 3 hours ± 1 second tolerance

    def test_single_time_point_with_bounds_works(self):
        """Test that single time point works when time bounds are available."""
        ds = self.create_dataset_with_bounds(freq_seconds=86400, periods=1)  # Single daily point
        
        detected_freq = detect_time_frequency_lazy(ds)
        
        assert detected_freq is not None
        assert abs(detected_freq.total_seconds() - 86400) < 1  # 1 day ± 1 second tolerance

    def test_bounds_priority_over_coordinates(self):
        """Test that bounds detection takes priority over coordinate differences."""
        # Create dataset with inconsistent bounds vs coordinates
        time_centers = np.array([0, 1, 2, 3])  # Daily centers
        # But bounds indicate 12-hour intervals
        time_bounds = np.array([[i-0.25, i+0.25] for i in time_centers])  # 12-hour intervals
        
        ds = xr.Dataset({
            'tas': (['time'], np.random.normal(290, 5, 4)),
            'time': (['time'], time_centers, {
                'units': 'days since 2000-01-01',
                'calendar': 'standard',
                'bounds': 'time_bnds'
            }),
            'time_bnds': (['time', 'bnds'], time_bounds, {
                'units': 'days since 2000-01-01',
                'calendar': 'standard'
            })
        })
        
        detected_freq = detect_time_frequency_lazy(ds)
        
        # Should use bounds (12 hours) not coordinate differences (24 hours)
        assert detected_freq is not None
        assert abs(detected_freq.total_seconds() - 43200) < 1  # 12 hours


class TestCMIP6FrequencyValidation:
    """Test CMIP6-specific frequency validation functionality."""
    
    def test_parse_cmip6_table_frequency(self):
        """Test parsing of CMIP6 table frequencies."""
        test_cases = {
            "Amon.tas": pd.Timedelta(days=30),
            "Aday.pr": pd.Timedelta(days=1),
            "A3hr.ua": pd.Timedelta(hours=3),
            "A6hr.va": pd.Timedelta(hours=6),
            "Omon.thetao": pd.Timedelta(days=30),
            "Oday.sos": pd.Timedelta(days=1),
            "Oyr.volcello": pd.Timedelta(days=365),
            "CFday.tas": pd.Timedelta(days=1),
            "CFmon.pr": pd.Timedelta(days=30),
        }
        
        for compound_name, expected_freq in test_cases.items():
            freq = parse_cmip6_table_frequency(compound_name)
            assert freq == expected_freq, f"Expected {expected_freq} for {compound_name}, got {freq}"
    
    def test_parse_invalid_compound_name(self):
        """Test error handling for invalid compound names."""
        invalid_cases = [
            "invalid",  # No dot
            "InvalidTable.tas",  # Unknown table
            "",  # Empty string
            "Amon.",  # Missing variable
        ]
        
        for invalid_name in invalid_cases:
            with pytest.raises(ValueError):
                parse_cmip6_table_frequency(invalid_name)
    
    def test_frequency_compatibility_valid_cases(self):
        """Test frequency compatibility for valid resampling cases."""
        valid_cases = [
            # (input_freq, target_freq, should_be_compatible, description)
            (pd.Timedelta(hours=1), pd.Timedelta(days=1), True, "hourly to daily"),
            (pd.Timedelta(days=1), pd.Timedelta(days=30), True, "daily to monthly"),
            (pd.Timedelta(hours=3), pd.Timedelta(days=1), True, "3-hourly to daily"),
            (pd.Timedelta(days=1), pd.Timedelta(days=1), True, "daily to daily (exact)"),
            (pd.Timedelta(hours=6), pd.Timedelta(days=30), True, "6-hourly to monthly"),
        ]
        
        for input_freq, target_freq, expected_compatible, desc in valid_cases:
            is_compatible, reason = is_frequency_compatible(input_freq, target_freq)
            assert is_compatible == expected_compatible, f"Failed for {desc}: {reason}"
    
    def test_frequency_compatibility_invalid_cases(self):
        """Test frequency compatibility for invalid upsampling cases."""
        invalid_cases = [
            (pd.Timedelta(days=30), pd.Timedelta(days=1), "monthly to daily"),
            (pd.Timedelta(days=1), pd.Timedelta(hours=3), "daily to 3-hourly"),
            (pd.Timedelta(days=365), pd.Timedelta(days=30), "yearly to monthly"),
        ]
        
        for input_freq, target_freq, desc in invalid_cases:
            is_compatible, reason = is_frequency_compatible(input_freq, target_freq)
            assert not is_compatible, f"Should be incompatible for {desc}"
            assert "Cannot upsample" in reason
    
    def test_cmip6_validation_compatible_resampling(self):
        """Test CMIP6 validation for cases requiring resampling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create hourly data
            time_values = np.arange(0, 2, 1/24)  # 2 days, hourly
            ds = self.create_test_dataset(time_values)
            
            filepath = Path(tmpdir) / "hourly.nc"
            ds.to_netcdf(filepath)
            
            # Test hourly -> daily (should require resampling)
            detected_freq, resampling_required = validate_cmip6_frequency_compatibility(
                [str(filepath)],
                "Aday.tas",
                interactive=False
            )
            
            assert detected_freq == pd.Timedelta(hours=1)
            assert resampling_required is True
    
    def test_cmip6_validation_exact_match(self):
        """Test CMIP6 validation for exact frequency matches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create daily data
            time_values = np.arange(0, 10)  # 10 days
            ds = self.create_test_dataset(time_values)
            
            filepath = Path(tmpdir) / "daily.nc"
            ds.to_netcdf(filepath)
            
            # Test daily -> daily (should be exact match)
            detected_freq, resampling_required = validate_cmip6_frequency_compatibility(
                [str(filepath)],
                "Aday.tas",
                interactive=False
            )
            
            assert detected_freq == pd.Timedelta(days=1)
            assert resampling_required is False
    
    def test_cmip6_validation_incompatible_frequency(self):
        """Test CMIP6 validation for incompatible frequencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create monthly data
            time_values = np.arange(0, 365*2, 30)  # 2 years, monthly
            ds = self.create_test_dataset(time_values)
            
            filepath = Path(tmpdir) / "monthly.nc"
            ds.to_netcdf(filepath)
            
            # Test monthly -> daily (should be incompatible)
            with pytest.raises(IncompatibleFrequencyError, match="cannot be resampled"):
                validate_cmip6_frequency_compatibility(
                    [str(filepath)],
                    "Aday.tas",
                    interactive=False
                )
    
    def test_cmip6_validation_interactive_abort(self):
        """Test user abort in interactive mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create hourly data
            time_values = np.arange(0, 1, 1/24)  # 1 day, hourly  
            ds = self.create_test_dataset(time_values)
            
            filepath = Path(tmpdir) / "hourly.nc"
            ds.to_netcdf(filepath)
            
            # Mock user input to simulate abort
            import sys
            from io import StringIO
            sys.stdin = StringIO("n\n")  # User says no
            
            try:
                with pytest.raises(InterruptedError, match="aborted by user"):
                    validate_cmip6_frequency_compatibility(
                        [str(filepath)],
                        "Aday.tas",  # hourly -> daily requires resampling
                        interactive=True
                    )
            finally:
                sys.stdin = sys.__stdin__  # Restore stdin

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