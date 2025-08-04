import importlib.resources as resources
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

# Use your existing DATA_DIR
DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def parent_experiment_config():
    """Parent experiment configuration - same as your existing one."""
    return {
        "parent_experiment_id": "piControl",
        "parent_activity_id": "CMIP",
        "parent_source_id": "ACCESS-ESM1-5",
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01 00:00:00",
        "parent_mip_era": "CMIP6",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 54786.0,
        "branch_method": "standard",
    }


@pytest.fixture
def mock_netcdf_dataset():
    """Create a mock xarray dataset that mimics ACCESS model output."""
    time = pd.date_range("2000-01-01", periods=12, freq="M")
    lat = np.linspace(-90, 90, 10)
    lon = np.linspace(0, 360, 20)

    # Create realistic test data
    temp_data = 273.15 + 15 + 10 * np.random.random((12, 10, 20))
    precip_data = np.abs(5e-5 * np.random.random((12, 10, 20)))

    ds = xr.Dataset(
        {
            "temp": (
                ["time", "lat", "lon"],
                temp_data,
                {
                    "units": "K",
                    "standard_name": "air_temperature",
                    "long_name": "Near-Surface Air Temperature",
                },
            ),
            "precip": (
                ["time", "lat", "lon"],
                precip_data,
                {
                    "units": "kg m-2 s-1",
                    "standard_name": "precipitation_flux",
                    "long_name": "Precipitation",
                },
            ),
        },
        coords={
            "time": ("time", time),
            "lat": ("lat", lat, {"units": "degrees_north"}),
            "lon": ("lon", lon, {"units": "degrees_east"}),
        },
    )

    return ds


@pytest.fixture
def mock_config():
    """Standard configuration for testing."""
    return {
        "experiment_id": "historical",
        "source_id": "ACCESS-ESM1-5",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
        "activity_id": "CMIP",
    }


@pytest.fixture
def batch_config():
    """Sample batch configuration for testing."""
    return {
        "variables": ["Amon.pr", "Amon.tas"],
        "experiment_id": "historical",
        "source_id": "ACCESS-ESM1-5",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
        "activity_id": "CMIP",
        "input_folder": "/test/input",
        "output_folder": "/test/output",
        "file_patterns": {
            "Amon.pr": "/output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc",
            "Amon.tas": "/output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc",
        },
        "cpus_per_node": 4,
        "mem": "16GB",
        "walltime": "01:00:00",
        "queue": "normal",
    }


def load_filtered_variables(mappings):
    """Load variables from mapping files - keeping your existing function."""
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
    return df.index.tolist()
