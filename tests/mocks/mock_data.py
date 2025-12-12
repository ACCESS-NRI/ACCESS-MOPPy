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

    # time_bnds
    dtime = (time[1] - time[0]) / 2
    time_bnds = np.zeros((n_time, 2), dtype="datetime64[ns]")
    time_bnds[:, 0] = time - dtime
    time_bnds[:, 1] = time + dtime

    # lat_bnds
    dlat = lat[1] - lat[0]
    lat_bnds = np.zeros((n_lat, 2))
    lat_bnds[:, 0] = lat - dlat / 2
    lat_bnds[:, 1] = lat + dlat / 2
    lat_bnds[0, 0] = -90
    lat_bnds[-1, 1] = 90

    # lon_bnds
    dlon = lon[1] - lon[0]
    lon_bnds = np.zeros((n_lon, 2))
    lon_bnds[:, 0] = lon - dlon / 2
    lon_bnds[:, 1] = lon + dlon / 2
    lon_bnds[0, 0] = 0
    lon_bnds[-1, 1] = 360

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
            raw_var = "fld_s03i236"
        elif var == "precip":
            # Realistic precipitation data
            data = np.abs(np.random.exponential(2e-5, (n_time, n_lat, n_lon)))
            attrs = {
                "units": "kg m-2 s-1",
                "standard_name": "precipitation_flux",
                "long_name": "Precipitation",
            }
            raw_var = "fld_s05i216"
        elif var == "psl":
            # Sea level pressure
            data = 101325 + 2000 * np.random.random((n_time, n_lat, n_lon))
            attrs = {
                "units": "Pa",
                "standard_name": "air_pressure_at_mean_sea_level",
                "long_name": "Sea Level Pressure",
            }
            raw_var = "fld_s16i222"
        else:
            # Generic variable
            data = np.random.random((n_time, n_lat, n_lon))
            attrs = {"units": "1", "long_name": f"Test variable {var}"}

        data_vars[raw_var] = (["time", "lat", "lon"], data, attrs)

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

    # add bounds
    ds["time_bnds"] = (("time", "bnds"), time_bnds)
    ds["lat_bnds"] = (("lat", "bnds"), lat_bnds)
    ds["lon_bnds"] = (("lon", "bnds"), lon_bnds)

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


def create_mock_2d_ocean_dataset(
    nt=12, ny=300, nx=360, variables=["surface_temp"], start_date="2000-01-01", freq="M"
):
    """
    Create a mock xarray Dataset mimicking ACCESS-ESM ocean surface temperature output.

    Returns a dataset with 12 monthly time steps, matching the structure from ncdump.
    """
    # Dimensions
    nt, ny, nx = 12, 300, 360

    # Coordinates
    xt_ocean = np.linspace(0.5, 359.5, nx)
    yt_ocean = np.linspace(-89.5, 89.5, ny)
    nv = np.array([1.0, 2.0])

    # Time: 12 months starting from year 1444
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    base_days = (1444 - 1) * 365

    # time = []
    time_bnds = np.zeros((12, 2))
    cumulative = base_days

    for i in range(12):
        time_bnds[i, 0] = cumulative
        time_bnds[i, 1] = cumulative + days_per_month[i]
        cumulative += days_per_month[i]

    time = pd.date_range(start=start_date, periods=nt, freq=freq)
    average_T1 = time_bnds[:, 0]
    average_T2 = time_bnds[:, 1]
    average_DT = average_T2 - average_T1

    # Surface temperature data (K)
    np.random.seed(42)
    data_var = np.random.uniform(273.0, 303.0, (nt, ny, nx)).astype(np.float32)

    # Create dataset
    ds = xr.Dataset(
        data_vars={
            variables[0]: (
                ["time", "yt_ocean", "xt_ocean"],
                data_var,
                {
                    "long_name": "Conservative temperature",
                    "units": "K",
                    "valid_range": np.array([-10.0, 500.0], dtype=np.float32),
                    "missing_value": np.float32(-1e20),
                    "_FillValue": np.float32(-1e20),
                    "cell_methods": "time: mean",
                    "standard_name": "sea_surface_conservative_temperature",
                },
            ),
            "average_T1": (
                ["time"],
                average_T1,
                {
                    "long_name": "Start time for average period",
                    "units": "days since 0001-01-01 00:00:00",
                },
            ),
            "average_T2": (
                ["time"],
                average_T2,
                {
                    "long_name": "End time for average period",
                    "units": "days since 0001-01-01 00:00:00",
                },
            ),
            "average_DT": (
                ["time"],
                average_DT,
                {"long_name": "Length of average period", "units": "days"},
            ),
            "time_bnds": (
                ["time", "nv"],
                time_bnds,
                {"long_name": "time axis boundaries", "units": "days"},
            ),
        },
        coords={
            "xt_ocean": (
                "xt_ocean",
                xt_ocean,
                {
                    "long_name": "tcell longitude",
                    "units": "degrees_E",
                    "cartesian_axis": "X",
                },
            ),
            "yt_ocean": (
                "yt_ocean",
                yt_ocean,
                {
                    "long_name": "tcell latitude",
                    "units": "degrees_N",
                    "cartesian_axis": "Y",
                },
            ),
            "time": (
                "time",
                time,
                {
                    "long_name": "time",
                    "units": "days since 0001-01-01 00:00:00",
                    "calendar": "PROLEPTIC_GREGORIAN",
                    "bounds": "time_bnds",
                },
            ),
            "nv": ("nv", nv, {"long_name": "vertex number"}),
        },
        attrs={
            "filename": "ocean-2d-surface_temp-1monthly-mean-ym_2000_01.nc",
            "title": "ACCESS-ESM_CMIP6",
            "grid_type": "mosaic",
            "grid_tile": "1",
        },
    )

    return ds


def create_mock_3d_ocean_dataset(
    nt=12,
    nz=50,
    ny=300,
    nx=360,
    variables=["pot_temp"],
    start_date="2000-01-01",
    freq="M",
):
    """
    Create a mock xarray Dataset mimicking ACCESS-ESM 3D ocean temperature output.

    Returns a dataset with 12 monthly time steps and 50 depth levels, matching
    the structure of 3D ocean model output.
    """
    # Dimensions
    nt, nz, ny, nx = 12, 50, 300, 360

    # Coordinates
    xt_ocean = np.linspace(0.5, 359.5, nx)
    yt_ocean = np.linspace(-89.5, 89.5, ny)

    # Depth levels (st_ocean) - typical ACCESS ocean levels in meters
    st_ocean = np.array(
        [
            2.5,
            7.5,
            12.5,
            17.5,
            22.5,
            30,
            40,
            50,
            62.5,
            77.5,
            95,
            115,
            137.5,
            162.5,
            192.5,
            230,
            275,
            330,
            395,
            475,
            575,
            700,
            850,
            1030,
            1250,
            1520,
            1850,
            2250,
            2750,
            3250,
            3750,
            4250,
            4750,
            5250,
            5750,
            6250,
            6750,
            7250,
            7750,
            8250,
            8750,
            9250,
            9750,
            10250,
            10750,
            11250,
            11750,
            12250,
            12750,
            13250,
        ]
    )[:nz]

    nv = np.array([1.0, 2.0])

    # Time setup (same as 2D)
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    base_days = (1444 - 1) * 365

    time_bnds = np.zeros((12, 2))
    cumulative = base_days

    for i in range(12):
        time_bnds[i, 0] = cumulative
        time_bnds[i, 1] = cumulative + days_per_month[i]
        cumulative += days_per_month[i]

    time = pd.date_range(start=start_date, periods=nt, freq=freq)
    average_T1 = time_bnds[:, 0]
    average_T2 = time_bnds[:, 1]
    average_DT = average_T2 - average_T1

    # 3D ocean temperature data (K) - varies with depth
    np.random.seed(42)
    data_var = np.zeros((nt, nz, ny, nx), dtype=np.float32)

    # Temperature decreases with depth (simplified profile)
    for k in range(nz):
        depth_factor = 1 - (st_ocean[k] / st_ocean.max()) * 0.5  # Warmer at surface
        data_var[:, k, :, :] = np.random.uniform(
            273.0 + depth_factor * 20, 303.0 * depth_factor, (nt, ny, nx)
        ).astype(np.float32)

    # Create dataset
    ds = xr.Dataset(
        data_vars={
            variables[0]: (
                ["time", "st_ocean", "yt_ocean", "xt_ocean"],
                data_var,
                {
                    "long_name": "Sea Water Potential Temperature",
                    "units": "K",
                    "valid_range": np.array([-10.0, 500.0], dtype=np.float32),
                    "missing_value": np.float32(-1e20),
                    "_FillValue": np.float32(-1e20),
                    "cell_methods": "time: mean",
                    "standard_name": "sea_water_potential_temperature",
                },
            ),
            "average_T1": (
                ["time"],
                average_T1,
                {
                    "long_name": "Start time for average period",
                    "units": "days since 0001-01-01 00:00:00",
                },
            ),
            "average_T2": (
                ["time"],
                average_T2,
                {
                    "long_name": "End time for average period",
                    "units": "days since 0001-01-01 00:00:00",
                },
            ),
            "average_DT": (
                ["time"],
                average_DT,
                {"long_name": "Length of average period", "units": "days"},
            ),
            "time_bnds": (
                ["time", "nv"],
                time_bnds,
                {"long_name": "time axis boundaries", "units": "days"},
            ),
        },
        coords={
            "xt_ocean": (
                "xt_ocean",
                xt_ocean,
                {
                    "long_name": "tcell longitude",
                    "units": "degrees_E",
                    "cartesian_axis": "X",
                },
            ),
            "yt_ocean": (
                "yt_ocean",
                yt_ocean,
                {
                    "long_name": "tcell latitude",
                    "units": "degrees_N",
                    "cartesian_axis": "Y",
                },
            ),
            "st_ocean": (
                "st_ocean",
                st_ocean,
                {
                    "long_name": "tcell depth",
                    "units": "meters",
                    "cartesian_axis": "Z",
                    "positive": "down",
                    "edges": "st_edges_ocean",
                },
            ),
            "time": (
                "time",
                time,
                {
                    "long_name": "time",
                    "units": "days since 0001-01-01 00:00:00",
                    "calendar": "PROLEPTIC_GREGORIAN",
                    "bounds": "time_bnds",
                },
            ),
            "nv": ("nv", nv, {"long_name": "vertex number"}),
        },
        attrs={
            "filename": "ocean-3d-thetao-1monthly-mean-ym_2000_01.nc",
            "title": "ACCESS-ESM_CMIP6",
            "grid_type": "mosaic",
            "grid_tile": "1",
        },
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
