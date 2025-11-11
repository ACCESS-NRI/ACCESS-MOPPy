#!/usr/bin/env python3
"""
Test script to simulate the exact error case you encountered with real calendar month variations.
"""

import pandas as pd
import sys
import tempfile
import os
sys.path.insert(0, 'src')

import xarray as xr
import numpy as np
from access_moppy.utilities import validate_consistent_frequency, FrequencyMismatchError

def create_realistic_monthly_file(start_date: str, days_in_month: int, filename_suffix: str) -> xr.Dataset:
    """Create a realistic monthly file that mimics actual ACCESS model output."""
    
    # Create realistic time bounds for the month
    start_time = pd.to_datetime(start_date)
    end_time = start_time + pd.Timedelta(days=days_in_month)
    
    # Time coordinate is the middle of the month
    mid_time = start_time + pd.Timedelta(days=days_in_month/2)
    
    # Convert to days since reference
    ref_date = pd.to_datetime('2000-01-01')
    start_days = (start_time - ref_date).total_seconds() / 86400
    end_days = (end_time - ref_date).total_seconds() / 86400  
    mid_days = (mid_time - ref_date).total_seconds() / 86400
    
    # Create time bounds (this is what drives frequency detection)
    time_bnds = xr.DataArray(
        data=[[start_days, end_days]],
        dims=['time', 'bnds'],
        attrs={'long_name': 'time bounds'}
    )
    
    # Create time coordinate
    time_coord = xr.DataArray(
        data=[mid_days],
        dims=['time'],
        attrs={
            'units': 'days since 2000-01-01 00:00:00',
            'calendar': 'standard',
            'bounds': 'time_bnds'
        }
    )
    
    # Create dummy atmospheric data
    tas = xr.DataArray(
        data=[[[[288.15]]]],  # Temperature in Kelvin
        dims=['time', 'plev', 'lat', 'lon'],
        coords={
            'time': time_coord,
            'plev': [85000.0],
            'lat': [-37.5], 
            'lon': [144.5]
        },
        attrs={
            'long_name': 'Air Temperature',
            'units': 'K',
            'standard_name': 'air_temperature'
        }
    )
    
    # Create dataset without ACCESS frequency metadata (to force bounds detection)
    ds = xr.Dataset(
        data_vars={
            'tas': tas,
            'time_bnds': time_bnds
        },
        coords={
            'time': time_coord,
            'plev': [85000.0],
            'lat': [-37.5],
            'lon': [144.5]
        },
        attrs={
            'title': f'ACCESS Model Output - {filename_suffix}',
            'institution': 'ACCESS-NRI',
            'source': 'ACCESS-ESM1-6'
            # Deliberately NO frequency metadata to force bounds detection
        }
    )
    
    return ds

def simulate_your_exact_error():
    """Simulate the exact error case you encountered."""
    
    print("🚫 Simulating Your Exact Error Case")
    print("=" * 60)
    
    # These are the realistic monthly periods from your error
    monthly_files = [
        ('2096-10-01', 31, 'aiihca.pa-096110_mon.nc'),  # October - reference file
        ('2096-06-01', 30, 'aiihca.pa-096106_mon.nc'),  # June
        ('2096-02-01', 28, 'aiihca.pa-096102_mon.nc'),  # February (non-leap)
        ('2096-11-01', 30, 'aiihca.pa-096111_mon.nc'),  # November  
        ('2096-09-01', 30, 'aiihca.pa-096109_mon.nc'),  # September
        ('2096-04-01', 30, 'aiihca.pa-096104_mon.nc'),  # April
    ]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        file_paths = []
        
        print("📁 Creating test files with realistic calendar month lengths:")
        for start_date, days, filename in monthly_files:
            ds = create_realistic_monthly_file(start_date, days, filename)
            filepath = os.path.join(tmpdir, filename)
            ds.to_netcdf(filepath)
            file_paths.append(filepath)
            
            month_name = pd.to_datetime(start_date).strftime('%B')
            print(f"   {filename}: {month_name} ({days} days)")
        
        print()
        
        # Reproduce your exact error with old system
        print("❌ REPRODUCING YOUR ERROR (1-hour tolerance):")
        try:
            result = validate_consistent_frequency(file_paths, tolerance_seconds=3600.0)
            print(f"   UNEXPECTED SUCCESS: {result}")
        except FrequencyMismatchError as e:
            print("   EXPECTED ERROR REPRODUCED:")
            
            # Show the error in the same format as your original
            error_lines = str(e).split('\n')
            for line in error_lines[:10]:  # Show first 10 lines
                print(f"   {line}")
            if len(error_lines) > 10:
                print("   ...")
        
        print()
        
        # Show the fix
        print("✅ FIXED WITH SMART TOLERANCE:")
        try:
            result = validate_consistent_frequency(file_paths, tolerance_seconds=None)
            print(f"   SUCCESS: All files validated!")
            print(f"   Detected frequency: {result}")
            
        except Exception as e:
            print(f"   UNEXPECTED FAILURE: {e}")
        
        print()
        
        # Show the numbers
        print("📊 THE NUMBERS:")
        differences = [
            ("June (30d) vs October (31d)", 1 * 86400),
            ("February (28d) vs October (31d)", 3 * 86400), 
            ("November (30d) vs October (31d)", 1 * 86400),
            ("September (30d) vs October (31d)", 1 * 86400),
            ("April (30d) vs October (31d)", 1 * 86400),
        ]
        
        print("   Frequency differences from reference file:")
        for desc, diff_seconds in differences:
            diff_days = diff_seconds / 86400
            print(f"     {desc}: {diff_seconds:.0f}s ({diff_days:.0f} days)")
        
        print(f"\n   Tolerance comparison:")
        print(f"     Old tolerance: 3,600s (1 hour)")
        print(f"     New tolerance: 345,600s (4 days)")
        print(f"     Maximum difference: 259,200s (3 days)")
        print(f"     ✓ 3 days < 4 days → PROBLEM SOLVED!")

if __name__ == "__main__":
    simulate_your_exact_error()