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


def load_filtered_variables(model_id="ACCESS-ESM1.6", component=None, table_name=None):
    """
    Load variables from model-specific mapping files.

    Args:
        model_id: Model identifier (e.g., 'ACCESS-ESM1.6')
        component: Component to load variables from ('atmosphere', 'land', 'ocean')
        table_name: CMIP6 table name for filtering (e.g., 'Amon', 'Lmon')

    Returns:
        List of available variable names
    """
    import json

    mapping_file = f"{model_id}_mappings.json"

    try:
        with (
            resources.files("access_moppy.mappings").joinpath(mapping_file).open() as f
        ):
            all_mappings = json.load(f)

        variables = []

        # If specific component requested, only get variables from that component
        if component:
            if component in all_mappings:
                variables.extend(list(all_mappings[component].keys()))
        else:
            # Get variables from all components if no specific component requested
            for comp in ["atmosphere", "land", "ocean"]:
                if comp in all_mappings:
                    variables.extend(list(all_mappings[comp].keys()))

        # Map CMIP6 tables to typical components for filtering
        table_to_component = {
            "Amon": "atmosphere",
            "Aday": "atmosphere",
            "A3hr": "atmosphere",
            "A6hr": "atmosphere",
            "Lmon": "land",
            "Lday": "land",
            "Omon": "ocean",
            "Oday": "ocean",
            "Oyr": "ocean",
            "SImon": "ocean",
        }

        # Special handling for Emon table which includes variables from multiple components
        if table_name == "Emon":
            # Return variables from both atmosphere and land components
            variables = []
            for comp in ["atmosphere", "land"]:
                if comp in all_mappings:
                    variables.extend(list(all_mappings[comp].keys()))
            variables = list(set(variables))  # Remove duplicates
        elif table_name and table_name in table_to_component:
            # If table_name specified, filter by appropriate component
            component = table_to_component[table_name]
            if component in all_mappings:
                variables = list(all_mappings[component].keys())
            else:
                variables = []

        # Filter to only variables that work with test data
        if table_name:
            variables = _filter_variables_by_test_data(variables, table_name)

        return variables

    except Exception as e:
        # Return empty list if mapping file not found or error occurs
        print(f"Warning: Could not load variables from {mapping_file}: {e}")
        return []


def _filter_variables_by_test_data(variables, table_name):
    """
    Filter variables to only those that can be processed with available test data.

    This is a conservative approach for integration tests - only include variables
    that we know work with the standard test data files.
    """
    # Known working variables for each table based on test data availability
    # These are variables that have been confirmed to work with the aiihca.pa-101909_mon.nc test file
    test_data_compatible_vars = {
        "Amon": [
            "rldscs",  # Surface Downwelling Longwave Radiation assuming Clear Sky
            "rlutcs",  # TOA Outgoing Longwave Radiation assuming Clear Sky
            "tas",  # Near-Surface Air Temperature
            "pr",  # Precipitation
            "uas",  # Eastward Near-Surface Wind
            "vas",  # Northward Near-Surface Wind
            "psl",  # Sea Level Pressure
            "ps",  # Surface Air Pressure
            "huss",  # Near-Surface Specific Humidity
            "hurs",  # Near-Surface Relative Humidity
            "rsds",  # Surface Downwelling Shortwave Radiation
            "rlds",  # Surface Downwelling Longwave Radiation
            "rsus",  # Surface Upwelling Shortwave Radiation
            "rlus",  # Surface Upwelling Longwave Radiation
            "hfls",  # Surface Upward Latent Heat Flux
            "hfss",  # Surface Upward Sensible Heat Flux
            "evspsbl",  # Evaporation including Sublimation and Transpiration
            "clt",  # Total Cloud Cover Percentage
            "rsdt",  # TOA Incident Shortwave Radiation
            "rsut",  # TOA Outgoing Shortwave Radiation
            "rlut",  # TOA Outgoing Longwave Radiation
        ],
        "Lmon": [
            # For land variables, we need different test data files
            # For now, return a minimal set for basic testing
            "mrso",  # Total Soil Moisture Content (if soil data available)
        ],
        "Emon": [
            # Subset of atmosphere and land variables that work
            "tas",
            "pr",
            "uas",
            "vas",
            "psl",
            "huss",
        ],
    }

    # Get the compatible variables for this table
    compatible = test_data_compatible_vars.get(table_name, [])

    # Return intersection of requested variables and compatible variables
    filtered_vars = [var for var in variables if var in compatible]

    # If no compatible variables found, return a minimal safe set
    if not filtered_vars and table_name == "Amon":
        # At minimum, return variables we know work from the test output
        filtered_vars = ["rldscs", "rlutcs"]

    return filtered_vars
