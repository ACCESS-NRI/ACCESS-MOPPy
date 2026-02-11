#!/usr/bin/env python
# Copyright 2023 ARC Centre of Excellence for Climate Extremes
# author: Paola Petrelli <paola.petrelli@utas.edu.au>
# author: Sam Green <sam.green@unsw.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is the ACCESS Model Output Post Processor, derived from the APP4
# originally written for CMIP5 by Peter Uhe and dapted for CMIP6 by Chloe Mackallah
# ( https://doi.org/10.5281/zenodo.7703469 )
#
# last updated 08/10/2024
#
# This file contains a collection of functions to calculate atmospheric
# derived variables from ACCESS model output.
# Initial functions' definitions were based on APP4 modified to work with Xarray.
#
# To propose new calculations and/or update to existing ones see documentation:
#
# and open a new issue on github.


# import logging

# import click
# import dask
import numpy as np
import xarray as xr

# from metpy.calc import height_to_geopotential
# from mopdb.utils import MopException
# from moppy.calc_utils import get_plev, rename_coord

# Global Variables
# ----------------------------------------------------------------------

ice_density = 900  # kg/m3
snow_density = 300  # kg/m3

rd = 287.1
cp = 1003.5
p_0 = 100000.0
g_0 = 9.8067  # gravity constant
R_e = 6.378e06


# @click.pass_context
# def height_gpheight(ctx, hslv, pmod=None, levnum=None):
#    """Returns geopotential height based on model levels height from
#    sea level, using metpy.height_to_geopotential() function
#
#    See: https://unidata.github.io/MetPy/latest/api/generated/metpy.calc.height_to_geopotential.html
#    If pmod and levnum are passed returns geopotential height interpolated on pressure levels.
#
#    Parameters
#    ----------
#    ctx : click context
#        Includes obj dict with 'cmor' settings, exp attributes
#    hslv : xarray.DataArray
#        Height of model levels from sea level
#    pmod : Xarray DataArray
#        Air pressure on model levels dims(lev, lat, lon), default None
#    levnum : int
#        Number of the pressure levels to load. NB these need to be
#        defined in the '_coordinates.yaml' file as 'plev#'. Default None
#
#    Returns
#    -------
#    gpheight : xarray.DataArray
#        Geopotential height on model or pressure levels
#
#    """
#
#    var_log = logging.getLogger(ctx.obj["var_log"])
#    geopot = height_to_geopotential(hslv)
#    gpheight_vals = geopot / g_0
#    gpheight = xr.zeros_like(hslv)
#    gpheight[:] = gpheight_vals
#    if pmod is not None:
#        if levnum is None:
#            var_log.error("Pressure levels need to be defined using levnum")
#            raise MopException("Pressure levels need to be defined using levnum")
#        else:
#            # check time axis gpheighe is same or interpolate
#            gpheight, override = rename_coord(pmod, gpheight, 0)
#            var_log.debug(f"override: {override}")
#            if override is True:
#                gpheight = gpheight.reindex_like(pmod, method="nearest")
#            gpheight = plevinterp(gpheight, pmod, levnum)
#
#    return gpheight
#
#
# @click.pass_context
# def plevinterp(ctx, var, pmod, levnum):
#    """Interpolating var from model levels to pressure levels
#
#    Based on function from Dale Roberts (currently ANU)
#
#    Parameters
#    ----------
#    ctx : click context
#        Includes obj dict with 'cmor' settings, exp attributes
#    var : Xarray DataArray
#        The variable to interpolate dims(time, lev, lat, lon)
#    pmod : Xarray DataArray
#        Air pressure on model levels dims(lev, lat, lon)
#    levnum : str
#        Inidcates the pressure levels to load. NB these need to be
#        defined in the '_coordinates.yaml' file as 'plev#'
#
#    Returns
#    -------
#    interp : Xarray DataArray
#        The variable interpolated on pressure levels
#
#    """
#
#    var_log = logging.getLogger(ctx.obj["var_log"])
#    # avoid dask warning
#    dask.config.set(**{"array.slicing.split_large_chunks": True})
#    plev = get_plev(levnum)
#    lev = var.dims[1]
#    # if pmod is pressure on rho_level_0 and variable is on rho_level
#    # change name and remove last level
#    pmodlev = pmod.dims[1]
#    if pmodlev == lev + "_0":
#        pmod = pmod.isel({pmodlev: slice(0, -1)})
#        pmod = pmod.rename({pmodlev: lev})
#    # we can assume lon_0/lat_0 are same as lon/lat for this purpose
#    # if pressure and variable have different coordinates change name
#    pmod, override = rename_coord(var, pmod, 2)
#    pmod, override = rename_coord(var, pmod, 3, override=override)
#    var_log.debug(f"override: {override}")
#    if override is True:
#        pmod = pmod.reindex_like(var, method="nearest")
#    var_log.debug(f"pmod and var coordinates: {pmod.dims}, {var.dims}")
#    var = var.chunk({lev: -1})
#    pmod = pmod.chunk({lev: -1})
#    # temporarily making pressure values negative so they are in ascending
#    # order as required by numpy.interp final result it's same and
#    # we re-assign original plev to interp anyway
#    interp = xr.apply_ufunc(
#        np.interp,
#        -1 * plev,
#        -1 * pmod,
#        var,
#        kwargs={"left": np.nan, "right": np.nan},
#        input_core_dims=[["plev"], [lev], [lev]],
#        output_core_dims=[["plev"]],
#        exclude_dims=set((lev,)),
#        vectorize=True,
#        dask="parallelized",
#        output_dtypes=["float32"],
#        keep_attrs=True,
#    )
#    interp["plev"] = plev
#    interp["plev"] = interp["plev"].assign_attrs(
#        {"units": "Pa", "axis": "Z", "standard_name": "air_pressure", "positive": ""}
#    )
#    dims = list(var.dims)
#    dims[1] = "plev"
#    interp = interp.transpose(*dims)
#    return interp
#
#
# Utilities
# ----------------------------------------------------------------------


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
    return cli_level_to_height(ds)


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
