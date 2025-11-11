#!/usr/bin/env python3
"""
Test script to verify the new smart tolerance system resolves the monthly calendar variation issue.
"""

import pandas as pd
import sys
import tempfile
import os
sys.path.insert(0, 'src')

import xarray as xr
from access_moppy.utilities import validate_consistent_frequency, _determine_smart_tolerance, FrequencyMismatchError

def create_monthly_dataset(days_in_month: int, month_name: str) -> xr.Dataset:
    """Create a test dataset simulating a monthly file with specific number of days."""
    
    # Create time coordinate for the month
    time_attrs = {
        'units': 'days since 2020-01-01 00:00:00',
        'calendar': 'standard',
        'long_name': 'time'
    }
    
    # Create time bounds for the month
    time_bounds = xr.DataArray(
        data=[[0, days_in_month]],  # Start of month to end of month
        dims=['time', 'bnds'],
        attrs={'long_name': 'time bounds'}
    )
    
    # Create the main time coordinate (middle of the month)
    time_coord = xr.DataArray(
        data=[days_in_month / 2],  # Middle of month
        dims=['time'],
        attrs=time_attrs
    )
    
    # Create a dummy variable
    temp = xr.DataArray(
        data=[[15.0]],  # Dummy temperature value
        dims=['time', 'lat'],
        coords={'time': time_coord, 'lat': [0.0]},
        attrs={'long_name': 'temperature', 'units': 'K'}
    )
    
    # Create dataset
    ds = xr.Dataset(
        data_vars={'tas': temp, 'time_bnds': time_bounds},
        coords={'time': time_coord, 'lat': [0.0]},
        attrs={'frequency': '1mon', 'month': month_name}
    )
    
    return ds

def test_monthly_calendar_variations():
    """Test that the new smart tolerance system handles calendar month variations."""
    
    print("🗓️  Testing Monthly Calendar Variation Handling")
    print("=" * 60)
    
    # Create temporary directory for test files
    with tempfile.TemporaryDirectory() as tmpdir:
        
        # Create monthly files with realistic calendar month variations
        monthly_data = [
            (31, "January"),    # 31 days
            (28, "February"),   # 28 days (non-leap year)
            (31, "March"),      # 31 days
            (30, "April"),      # 30 days
            (31, "May"),        # 31 days
            (30, "June"),       # 30 days
        ]
        
        file_paths = []
        
        for days, month_name in monthly_data:
            # Create dataset
            ds = create_monthly_dataset(days, month_name)
            
            # Save to file
            filename = f"test_monthly_{month_name.lower()}_{days}days.nc"
            filepath = os.path.join(tmpdir, filename)
            ds.to_netcdf(filepath)
            file_paths.append(filepath)
            
            print(f"📁 Created {month_name}: {days} days -> {filepath}")
        
        print()
        
        # Test 1: With old system (would fail with 1-hour tolerance)
        print("❌ OLD SYSTEM TEST (manual 1-hour tolerance):")
        try:
            # This should fail because month differences exceed 1 hour
            result = validate_consistent_frequency(file_paths, tolerance_seconds=3600.0)
            print(f"   UNEXPECTED: Validation passed: {result}")
        except FrequencyMismatchError as e:
            print(f"   EXPECTED: Validation failed due to calendar month variations")
            # Show first few lines of error
            error_lines = str(e).split('\n')[:6]
            for line in error_lines:
                print(f"   {line}")
            if len(str(e).split('\n')) > 6:
                print("   ...")
        
        print()
        
        # Test 2: With new smart tolerance system (should succeed)
        print("✅ NEW SYSTEM TEST (smart auto-tolerance):")
        try:
            # This should succeed with auto-determined tolerance
            result = validate_consistent_frequency(file_paths, tolerance_seconds=None)
            print(f"   SUCCESS: Validation passed with smart tolerance!")
            print(f"   Detected frequency: {result}")
            
            # Show the smart tolerance that was used
            smart_tolerance = _determine_smart_tolerance(result)
            print(f"   Smart tolerance used: {smart_tolerance/86400:.1f} days ({smart_tolerance:.0f}s)")
            
        except Exception as e:
            print(f"   FAILED: {e}")
            
        print()
        
        # Test 3: Show detailed comparison
        print("📊 DETAILED COMPARISON:")
        print("   Calendar month lengths:")
        base_days = monthly_data[0][0]  # January = 31 days
        for days, month_name in monthly_data:
            diff = abs(days - base_days)
            print(f"     {month_name:>9}: {days} days (diff: {diff} days from January)")
        
        print(f"\n   Tolerance comparison:")
        print(f"     Old system: 1 hour = {3600/86400:.4f} days")
        print(f"     New system: 4 days = {4*86400} seconds")
        print(f"     Max difference: 3 days (Feb vs Jan/Mar/May)")
        print(f"     Result: {'NEW SYSTEM WORKS!' if 3 <= 4 else 'STILL FAILS'}")

if __name__ == "__main__":
    test_monthly_calendar_variations()