"""Generate mock datasets for testing."""

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def create_mock_atmosphere_dataset(
    n_time=12, n_lat=145, n_lon=192, variables=None, start_date="2000-01-01", freq="M"
):
    """Create mock atmospheric dataset mimicking ACCESS output."""
    if variables is None:
        variables = ["temp", "precip"]

    time = pd.date_range(start_date, periods=n_time, freq=freq)
    lat = np.linspace(-90, 90, n_lat)
    lon = np.linspace(0, 360, n_lon)

    data_vars = {}

    for var in variables:
        if var == "temp":
            # Realistic temperature data in Kelvin
            data = (
                273.15
                + 15
                + 20 * np.cos(np.radians(lat[None, :, None]))
                + 5 * np.random.random((n_time, n_lat, n_lon))
            )
            attrs = {
                "units": "K",
                "standard_name": "air_temperature",
                "long_name": "Near-Surface Air Temperature",
            }
        elif var == "precip":
            # Realistic precipitation data
            data = np.abs(np.random.exponential(2e-5, (n_time, n_lat, n_lon)))
            attrs = {
                "units": "kg m-2 s-1",
                "standard_name": "precipitation_flux",
                "long_name": "Precipitation",
            }
        elif var == "psl":
            # Sea level pressure
            data = 101325 + 2000 * np.random.random((n_time, n_lat, n_lon))
            attrs = {
                "units": "Pa",
                "standard_name": "air_pressure_at_mean_sea_level",
                "long_name": "Sea Level Pressure",
            }
        else:
            # Generic variable
            data = np.random.random((n_time, n_lat, n_lon))
            attrs = {"units": "1", "long_name": f"Test variable {var}"}

        data_vars[var] = (["time", "lat", "lon"], data, attrs)

    ds = xr.Dataset(
        data_vars,
        coords={
            "time": (
                "time",
                time,
                {"units": "days since 1850-01-01", "calendar": "proleptic_gregorian"},
            ),
            "lat": (
                "lat",
                lat,
                {"units": "degrees_north", "standard_name": "latitude"},
            ),
            "lon": (
                "lon",
                lon,
                {"units": "degrees_east", "standard_name": "longitude"},
            ),
        },
    )

    # Add global attributes
    ds.attrs.update(
        {
            "title": "ACCESS-ESM1-5 atmospheric data",
            "institution": "CSIRO",
            "source": "ACCESS-ESM1-5",
            "history": "Mock data for testing",
            "Conventions": "CF-1.7",
        }
    )

    return ds


def create_mock_ocean_dataset(
    n_time=12, n_lat=300, n_lon=360, variables=None, start_date="2000-01-01", freq="M"
):
    """Create mock ocean dataset mimicking ACCESS output."""
    if variables is None:
        variables = ["temp", "salt"]

    time = pd.date_range(start_date, periods=n_time, freq=freq)
    lat = np.linspace(-90, 90, n_lat)
    lon = np.linspace(0, 360, n_lon)

    data_vars = {}

    for var in variables:
        if var == "temp":
            # Ocean temperature (warmer at equator, cooler at poles)
            data = (
                15
                + 15 * np.cos(np.radians(lat[None, :, None]))
                + 2 * np.random.random((n_time, n_lat, n_lon))
            )
            attrs = {
                "units": "degrees_C",
                "standard_name": "sea_water_temperature",
                "long_name": "Sea Water Temperature",
            }
        elif var == "salt":
            # Ocean salinity
            data = 35 + 2 * np.random.random((n_time, n_lat, n_lon))
            attrs = {
                "units": "psu",
                "standard_name": "sea_water_salinity",
                "long_name": "Sea Water Salinity",
            }
        else:
            data = np.random.random((n_time, n_lat, n_lon))
            attrs = {"units": "1", "long_name": f"Test ocean variable {var}"}

        data_vars[var] = (["time", "lat", "lon"], data, attrs)

    ds = xr.Dataset(
        data_vars,
        coords={
            "time": (
                "time",
                time,
                {"units": "days since 1850-01-01", "calendar": "proleptic_gregorian"},
            ),
            "lat": (
                "lat",
                lat,
                {"units": "degrees_north", "standard_name": "latitude"},
            ),
            "lon": (
                "lon",
                lon,
                {"units": "degrees_east", "standard_name": "longitude"},
            ),
        },
    )

    # Add global attributes
    ds.attrs.update(
        {
            "title": "ACCESS-ESM1-5 ocean data",
            "institution": "CSIRO",
            "source": "ACCESS-ESM1-5",
            "history": "Mock data for testing",
            "Conventions": "CF-1.7",
        }
    )

    return ds


def create_chunked_dataset(chunks=None, **kwargs):
    """Create a chunked dataset for testing dask operations."""
    if chunks is None:
        chunks = {"time": 6, "lat": 50, "lon": 100}

    ds = create_mock_atmosphere_dataset(**kwargs)
    return ds.chunk(chunks)


def save_mock_dataset(dataset, file_path):
    """Save mock dataset to NetCDF file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    dataset.to_netcdf(file_path)
    return file_path
