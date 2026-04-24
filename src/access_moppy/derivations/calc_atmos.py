#!/usr/bin/env python

import numpy as np
import xarray as xr

#
# Utilities
# ----------------------------------------------------------------------


def level_to_height(ds):
    """
    Transform model level indices to height coordinates.

    Converts from level dimension to height dimension by using stored height values
    and updating dimension coordinates accordingly.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset with model level coordinates

    Returns
    -------
    xarray.Dataset
        Dataset with height coordinate dimension
    """
    # Handle level coordinate transformation
    if "theta_level_height" in ds:
        ds = (
            ds.assign_coords({"lev": ds["theta_level_height"]})
            .swap_dims({"model_theta_level_number": "lev"})
            .drop_vars(
                ["theta_level_height", "model_theta_level_number"], errors="ignore"
            )
        )
    return ds


def cli_level_to_height(ds):
    # Handle level coordinate transformation
    if "theta_level_height" in ds:
        ds = (
            ds.assign_coords({"lev": ds["theta_level_height"]})
            .swap_dims({"model_theta_level_number": "lev"})
            .drop_vars(
                ["theta_level_height", "model_theta_level_number"], errors="ignore"
            )
        )
    return ds


def clw_level_to_height(ds):
    return cli_level_to_height(ds)


def cl_level_to_height(ds):
    ds = cli_level_to_height(ds)
    if "cl" in ds:
        ds["cl"] = ds["cl"] * 100
    return ds


def calculate_areacella(nlat=145, nlon=192, earth_radius=6371000.0):
    """
    Calculate atmospheric grid cell area (areacella) for ACCESS-ESM1.5 and ACCESS-ESM1.6.

    This function computes the area of each grid cell on a regular latitude-longitude
    grid using spherical geometry. The calculation is optimized for xarray and dask.

    Parameters
    ----------
    nlat : int, default 145
        Number of latitude points (ACCESS-ESM1.5/1.6: 145)
    nlon : int, default 192
        Number of longitude points (ACCESS-ESM1.5/1.6: 192)
    earth_radius : float, default 6371000.0
        Earth radius in meters

    Returns
    -------
    areacella : xarray.Dataset
        Grid cell areas in m² with dimensions (lat, lon) as a Dataset

    Notes
    -----
    This function is specifically designed for ACCESS-ESM1.5 and ACCESS-ESM1.6
    which use a regular lat-lon grid with nlat=145 and nlon=192.

    The area calculation uses the formula:
    area = 2π * R² * Δ(sin(lat)) / nlon

    where R is Earth's radius and Δ(sin(lat)) is the difference in sine
    of latitude bounds for each grid cell.
    """

    # Create latitude coordinates from -90 to +90
    lat = xr.DataArray(
        np.linspace(-90, 90, nlat),
        dims=["lat"],
        attrs={
            "units": "degrees_north",
            "standard_name": "latitude",
            "long_name": "latitude",
        },
    )

    # Create longitude coordinates from 0 to 360 (excluding 360)
    lon = xr.DataArray(
        np.linspace(0, 360, nlon, endpoint=False),
        dims=["lon"],
        attrs={
            "units": "degrees_east",
            "standard_name": "longitude",
            "long_name": "longitude",
        },
    )

    # Calculate latitude bounds for area computation
    # Use dask-compatible operations
    lat_vals = lat.values
    lat_bnds = np.zeros((nlat, 2))

    # Set boundary conditions
    lat_bnds[0, 0] = -90.0  # South pole
    lat_bnds[-1, 1] = 90.0  # North pole

    # Calculate mid-points between latitude centers for interior bounds
    lat_bnds[1:, 0] = (lat_vals[:-1] + lat_vals[1:]) * 0.5
    lat_bnds[:-1, 1] = lat_bnds[1:, 0]

    # Convert to radians for area calculation
    lat_bnds_rad = np.radians(lat_bnds)

    # Calculate area using spherical geometry formula
    # area = 2π * R² * Δ(sin(lat)) / nlon
    delta_sin_lat = np.diff(np.sin(lat_bnds_rad), axis=1).squeeze()
    area_1d = 2 * np.pi * earth_radius**2 * delta_sin_lat / nlon

    # Create xarray DataArray and broadcast to full 2D grid
    areacella = xr.DataArray(
        area_1d,
        coords={"lat": lat},
        dims=["lat"],
        attrs={
            "units": "m2",
            "standard_name": "cell_area",
            "long_name": "Grid-Cell Area for Atmospheric Grid Variables",
        },
    )

    # Broadcast to 2D grid (lat, lon) - this creates a lazy dask array
    areacella_2d = areacella.broadcast_like(
        xr.DataArray(
            np.ones((nlat, nlon)), coords={"lat": lat, "lon": lon}, dims=["lat", "lon"]
        )
    )

    # Ensure proper attributes are maintained
    areacella_2d.attrs.update(
        {
            "units": "m2",
            "standard_name": "cell_area",
            "long_name": "Grid-Cell Area for Atmospheric Grid Variables",
            "comment": f"Calculated for {nlat}x{nlon} regular grid with Earth radius {earth_radius} m",
        }
    )

    # Return as Dataset for use in internal calculations
    return xr.Dataset({"areacella": areacella_2d})
