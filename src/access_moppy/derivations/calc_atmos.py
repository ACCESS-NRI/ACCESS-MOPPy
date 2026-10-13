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


#: What each stagger point is called in the output file's ``comment``. The keys
#: are the internal grid keys; the values are the names the UM uses for the
#: points, so a reader of the file is not shown our configuration vocabulary
#: (and renaming a key later does not rewrite published metadata).
GRID_KEY_POINT_NAMES = {
    "default": "theta",
    "U": "u",
    "V": "v",
    "other": "uv",
}

#: The atmosphere stagger points, using the same keys as
#: :func:`access_moppy.utilities.resolve_atmosphere_grid_key`.
ATMOS_GRID_KEYS = tuple(GRID_KEY_POINT_NAMES)


def calculate_areacella(grid_key="default", nlat=145, nlon=192, earth_radius=6371000.0):
    """
    Calculate atmospheric grid cell area (areacella) for ACCESS-ESM1.5 and ACCESS-ESM1.6.

    This function computes the area of each grid cell on a regular latitude-longitude
    grid using spherical geometry. The calculation is optimized for xarray and dask.

    Parameters
    ----------
    grid_key : {"default", "U", "V", "other"}, default "default"
        The stagger point to build the grid for, named with the same keys as
        :func:`access_moppy.utilities.resolve_atmosphere_grid_key`:

        - ``"default"`` -- theta (mass) points, ``lat``/``lon``
        - ``"U"``       -- ``lat``/``lon_u``   (staggered in longitude)
        - ``"V"``       -- ``lat_v``/``lon``   (staggered in latitude)
        - ``"other"``   -- ``lat_v``/``lon_u`` (staggered in both)

        ACCESS writes atmosphere fields on all four points and CMIP7 registers
        a separate grid label for each, so a cell measure is only usable by the
        fields written on the same point.
    nlat : int, default 145
        Number of *theta* latitude points (ACCESS-ESM1.5/1.6: 145).  The
        staggered ``lat_v`` rows are derived from these and there is one fewer
        of them, so ``"V"`` and ``"other"`` return ``nlat - 1`` rows.
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

    The theta rows run pole to pole, so the first and last are half cells
    centred on the poles; the ``lat_v`` rows sit half a cell north of them, so
    all of them are full cells.  Either way the rows tile the sphere exactly
    and the areas sum to 4πR².
    """

    if grid_key not in ATMOS_GRID_KEYS:
        raise ValueError(
            f"Unknown atmosphere grid_key {grid_key!r}. "
            f"Expected one of {list(ATMOS_GRID_KEYS)}."
        )

    theta_lat = np.linspace(-90, 90, nlat)
    theta_lon = np.linspace(0, 360, nlon, endpoint=False)

    # lat_v sits midway between the theta rows; lon_u half a cell east of the
    # theta columns. The longitude offset leaves the areas untouched but not the
    # coordinates, and downstream tools match a measure to its data on both.
    lat_vals = (
        (theta_lat[:-1] + theta_lat[1:]) * 0.5
        if grid_key in ("V", "other")
        else theta_lat
    )
    lon_vals = (
        theta_lon + (360.0 / nlon) * 0.5 if grid_key in ("U", "other") else theta_lon
    )
    nrows = lat_vals.size

    # Create latitude coordinates from -90 to +90
    lat = xr.DataArray(
        lat_vals,
        dims=["lat"],
        attrs={
            "units": "degrees_north",
            "standard_name": "latitude",
            "long_name": "latitude",
        },
    )

    # Create longitude coordinates from 0 to 360 (excluding 360)
    lon = xr.DataArray(
        lon_vals,
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
    lat_bnds = np.zeros((nrows, 2))

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
            np.ones((nrows, nlon)), coords={"lat": lat, "lon": lon}, dims=["lat", "lon"]
        )
    )

    # Ensure proper attributes are maintained
    areacella_2d.attrs.update(
        {
            "units": "m2",
            "standard_name": "cell_area",
            "long_name": "Grid-Cell Area for Atmospheric Grid Variables",
            "comment": f"Calculated for {nrows}x{nlon} regular grid ({GRID_KEY_POINT_NAMES[grid_key]} points) with Earth radius {earth_radius} m",
        }
    )

    # Return as Dataset for use in internal calculations
    return xr.Dataset({"areacella": areacella_2d})
