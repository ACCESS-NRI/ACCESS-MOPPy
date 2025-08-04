#!/usr/bin/env python
"""
Generate test data files for ACCESS-MOPPeR testing.
This script creates small NetCDF files that mimic ACCESS model output.
"""

import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from tests.mocks.mock_data import (
    create_mock_atmosphere_dataset,
    create_mock_ocean_dataset,
    save_mock_dataset,
)


def create_test_files(output_dir, small=True):
    """Create test NetCDF files."""
    output_dir = Path(output_dir)

    # Create directory structure
    atmos_dir = output_dir / "atmosphere"
    ocean_dir = output_dir / "ocean"
    atmos_dir.mkdir(parents=True, exist_ok=True)
    ocean_dir.mkdir(parents=True, exist_ok=True)

    if small:
        # Small files for fast testing
        n_time, n_lat, n_lon = 12, 10, 20
        ocean_lat, ocean_lon = 20, 40
    else:
        # Larger files for realistic testing
        n_time, n_lat, n_lon = 120, 145, 192
        ocean_lat, ocean_lon = 300, 360

    print(f"Creating test files in {output_dir}")
    print(f"Dimensions: time={n_time}, lat={n_lat}, lon={n_lon}")

    # Create atmosphere test files
    print("Creating atmosphere test files...")
    atmos_variables = ["temp", "precip", "psl"]
    atmos_ds = create_mock_atmosphere_dataset(
        variables=atmos_variables, n_time=n_time, n_lat=n_lat, n_lon=n_lon
    )

    # Save atmosphere file
    atmos_file = atmos_dir / "test_atmosphere.nc"
    save_mock_dataset(atmos_ds, atmos_file)
    print(f"  Saved: {atmos_file}")

    # Create ocean test files
    print("Creating ocean test files...")
    ocean_variables = ["temp", "salt"]
    ocean_ds = create_mock_ocean_dataset(
        variables=ocean_variables, n_time=n_time, n_lat=ocean_lat, n_lon=ocean_lon
    )

    # Save ocean file
    ocean_file = ocean_dir / "test_ocean.nc"
    save_mock_dataset(ocean_ds, ocean_file)
    print(f"  Saved: {ocean_file}")

    # Create individual variable files (like your existing structure)
    print("Creating individual variable files...")
    for var in atmos_variables:
        var_ds = create_mock_atmosphere_dataset(
            variables=[var], n_time=n_time, n_lat=n_lat, n_lon=n_lon
        )
        var_file = atmos_dir / f"test_{var}.nc"
        save_mock_dataset(var_ds, var_file)
        print(f"  Saved: {var_file}")

    print("Test data generation complete!")


def main():
    parser = argparse.ArgumentParser(description="Generate test data for ACCESS-MOPPeR")
    parser.add_argument("output_dir", help="Output directory for test files")
    parser.add_argument(
        "--large", action="store_true", help="Create larger, more realistic test files"
    )

    args = parser.parse_args()

    create_test_files(args.output_dir, small=not args.large)


if __name__ == "__main__":
    main()
