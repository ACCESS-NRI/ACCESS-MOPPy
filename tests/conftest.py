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


def pytest_addoption(parser):
    """Register test-suite command line options."""
    parser.addoption(
        "--validation-tool",
        action="store",
        default="prepare",
        choices=("prepare", "wcrp"),
        help=(
            "Validation backend for CMOR integration tests: "
            "'prepare' (default) or 'wcrp' for compliance-checker + cc-plugin-wcrp."
        ),
    )


@pytest.fixture(scope="session")
def compliance_validation_tool(pytestconfig) -> str:
    """Return the selected validation backend for integration tests."""
    return pytestconfig.getoption("validation_tool")


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
def mock_config_om2():
    """Standard configuration for testing."""
    return {
        "experiment_id": "historical",
        "source_id": "ACCESS-OM2",
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
            for comp in ["atmosphere", "land", "ocean", "sea_ice"]:
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
            "Ofx": "ocean",
            "Oday": "ocean",
            "Oyr": "ocean",
            "SImon": "sea_ice",
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
    # These are variables that have been confirmed to work with the aiihca.pa-298810_mon.nc test file
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
            "cli",  # Mass Fraction of cloud ice in air
            "rluscs",
            "rsdscs",
            "rsuscs",
            "rsutcs",
            "rtmt",
            "cl",
            "clivi",
            "clw",
            "hur",
            "hus",
            "prc",
            "prsn",
            "prw",
            "ta",
            "tasmax",
            "tasmin",
            "tauu",
            "tauv",
            "ts",
            "ua",
            "va",
            "wap",
            "zg",
            "sfcWind",
        ],
        "AERmon": ["od550aer", "pfull", "phalf", "ua", "va"],
        "Lmon": [
            "mrso",
            "mrsos",
            "cLeaf",
            "cLitter",
            "cRoot",
            "cProduct",
            "baresoilFrac",
            "c3PftFrac",
            "c4PftFrac",
            "cSoilFast",
            "cSoilMedium",
            "cSoilSlow",
            "cropFrac",
            "grassFrac",
            "npp",
            "nbp",
            "ra",
            "rh",
            "residualFrac",
            "shrubFrac",
            "treeFrac",
            "lai",
        ],
        "Emon": [
            "cLand",
            "cSoil",
            "cropFracC3",
            "fBNF",
            "fDeforestToProduct",
            "fNdep",
            "fNgas",
            "fNleach",
            "fNloss",
            "fNnetmin",
            "fNup",
            "fProductDecomp",
            "grassFracC3",
            "grassFracC4",
            "mrsfl",
            "mrsll",
            "mrsol",
            "nep",
            "nLand",
            "nLitter",
            "nMineral",
            "nProduct",
            "nSoil",
            "nVeg",
            "orog",
            "treeFracBdlDcd",
            "treeFracBdlEvg",
            "treeFracNdlDcd",
            "treeFracNdlEvg",
            "vegFrac",
            "vegHeight",
            "wetlandFrac",
        ],
        "fx": [
            "areacella",  # Cell area on native grid
            "orog",  # Surface orography
        ],
        "Omon": [
            # Ocean variables that are commonly available and suitable for testing
            "evs",  # Water Evaporation Flux from Sea Water
            "thetao",  # Sea Water Potential Temperature
            "so",  # Sea Water Salinity
            "uo",  # Sea Water X Velocity
            "vo",  # Sea Water Y Velocity
            "zos",  # Sea Surface Height Above Geoid
            "mlotst",  # Ocean Mixed Layer Thickness Defined by Sigma T
            "thkcello",  # Cell Thickness
            "volcello",  # Ocean Grid-Cell Volume
            "areacello",  # Ocean Grid-Cell Area
            "sftof",  # Sea Area Fraction
            "wfo",  # Water Flux into Sea Water
            "pbo",  # Sea Water Pressure at Sea Floor
            "tob",  # Sea Water Potential Temperature at Sea Floor
            "sob",  # Sea Water Salinity at Sea Floor
            "tos",  # Sea Surface Temperature
            "sos",  # Sea Surface Salinity
            "bigthetao",  # Sea Water Conservative Temperature
            "agessc",  # Sea Water Age Since Surface Contact
            "ficeberg2d",  # Iceberg Calving Flux
            "bigthetaoga",  # Sea Water Conservative Temperature on Ocean Grid at Sea Surface
            "hfbasinpmadv",  # Heat Flux at Basin Level
            "hfevapds",  # Heat Flux due to Evaporation
            "hfrainds",  # Heat Flux due to Rain
            "htovgyre",  # Heat Transport by Gyre
            "htovovrt",  # Heat Transport by Overturning
            "masscello",  # Ocean Mass
            "mfo",  # Ocean Mass Flux
            "mlotst",  # Ocean Mixed Layer Thickness
            "msftmrho",  # Ocean Surface Temperature
            "msftmz",  # Ocean Surface Salinity
            "msftyrho",  # Ocean Surface Density
            "pbo",  # Sea Water Pressure at Sea Floor
            "sltovgyre",  # Salt Transport by Gyre
            "sltovovrt",  # Salt Transport by Overturning
            "so",  # Sea Water Salinity
            "sob",  # Sea Water Salinity at Sea Floor
            "soga",  # Sea Water Salinity on Ocean Grid at Sea Surface
            "sos",  # Sea Surface Salinity
            "sosga",  # Sea Surface Salinity on Ocean Grid at Sea Surface
            "tauuo",  # Zonal Wind Stress
            "tauvo",  # Meridional Wind Stress
            "thetaoga",  # Sea Water Conservative Temperature on Ocean Grid at Sea Surface
            "tob",  # Sea Water Potential Temperature at Sea Floor
            "umo",  # Sea Water X Velocity on Ocean Grid
            "uo",  # Sea Water X Velocity
            "vmo",  # Sea Water Y Velocity on Ocean Grid
            "vo",  # Sea Water Y Velocity
            "volo",  # Ocean Grid-Cell Volume
            "wo",  # Sea Water Vertical Velocity
        ],
        "Ofx": [
            # Only resource-backed variables that don't require external ocean data
            "areacello",  # Ocean Grid-Cell Area (uses bundled fx.areacello_ACCESS-ESM.nc)
            "sftof",  # Sea Area Fraction (uses bundled land_ocean_mask_ACCESS-ESM.nc)
            "hfgeou",  # Upward Geothermal Heat Flux (uses bundled ocean-2d-ht.nc)
            "deptho",  # Sea Floor Depth (uses bundled ocean-2d-ht.nc)
        ],
        "CFmon": [
            "hur",
            "hus",
            "ta",
        ],
        # Daily frequency (day table)
        "day": [
            "clt",
            "hfls",
            "hfss",
            "hurs",
            "hursmax",
            "hursmin",
            "huss",
            "mrro",
            "mrsos",
            "mrso",
            "prc",
            "prsn",
            "psl",
            "rlds",
            "rlus",
            "rlut",
            "rsds",
            "rsus",
            "sfcWind",
            "sfcWindmax",
            "tas",
            "tasmax",
            "tasmin",
            "uas",
            "vas",
        ],
        "Eday": [
            "lai",
            "mrsfl",
            "mrsll",
            "mrsol",
        ],
        "3hr": [
            "clt",
            "huss",
            "hfls",
            "hfss",
            "pr",
            "prsn",
            "ps",
            "rlds",
            "rlus",
            "rsus",
            "rsds",
            "tas",
        ],
        "6hrPlev": [
            "hurs",
            "psl",
            "sfcWind",
            "tas",
            "uas",
            "vas",
        ],
        "6hrPlevPt": [
            "hus",
            "ps",
            "ta",
            "ua",
            "va",
        ],
        "SImon": [
            "siage",
            "siconc",
            "sidconcdyn",
            "sidconcth",
            "siareaacrossline",
            "siarean",
            "siareas",
            "siconca",
            "sidivvel",
            "sidmassdyn",
            "sidmassth",
            "sidmassmeltlat",
            "sidmassevapsubl",
            "sidmassgrowthsi",
            "sidmassgrowthbot",
            "sidmassgrowthwat",
            "sidmassmeltbot",
            "sidmassmelttop",
            "sisndmasssi",
            "sifb",
            "sidmasstranx",
            "sidmasstrany",
            "siextentn",
            "siextents",
            "siflfwbot",
            "simass",
            "simassacrossline",
            "sisnconc",
            "sisnhc",
            "sisnthick",
            "sispeed",
            "sistrxdtop",
            "sistrxubot",
            "sistrydtop",
            "sistryubot",
            "sitempbot",
            "sitemptop",
            "sisndmassmelt",
            "sisndmasssnf",
            "sisnmassn",
            "sisnmasss",
            "sithick",
            "siv",
            "siu",
            "sivol",
            "sivoln",
            "sivols",
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
