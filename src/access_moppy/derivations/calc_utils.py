#!/usr/bin/env python
# Copyright 2024 ARC Centre of Excellence for Climate Extremes
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
# last updated 10/10/2024
#
# This file contains a collection of utilities to help calculate derived variables
# from ACCESS model output.
# Initial functions' definitions were based on APP4 modified to work with Xarray.


import json
import logging

import click
import numpy as np
import xarray as xr

# Global Variables
# ----------------------------------------------------------------------

ice_density = 900  # kg/m3
snow_density = 300  # kg/m3

rd = 287.1
cp = 1003.5
p_0 = 100000.0
g_0 = 9.8067  # gravity constant
R_e = 6.378e06
# ----------------------------------------------------------------------


@click.pass_context
def time_resample(ctx, var, rfrq, tdim, sample="down", stats="mean"):
    """
    Resamples the input variable to the specified frequency using
    specified statistic.

    Resample is used with the options:
    origin = 'start_day'
    closed = 'right'
    This puts the time label to the start of the interval and
    offset is applied to get a centered time label.
    The `rfrq` valid lables are described here:
    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#period-aliases

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    var : xarray.DataArray
        Variable to resample.
    rfrq : str
        Resample frequency see above for valid inputs.
    tdim: str
        The name of the time dimension
    sample : str
        The type of resampling to perform. Valid inputs are 'up' for
        upsampling or 'down' for downsampling. (default down)
    stats : str
        The reducing function to follow resample: mean, min, max, sum.
        (default mean)

    Returns
    -------
    vout : xarray.DataArray or xarray.Dataset
        The resampled variable.

    Raises
    ------
    ValueError
        If the input variable is not a valid Xarray object.
    ValueError
        If the sample parameter is not 'up' or 'down'.

    """
    var_log = logging.getLogger(ctx.obj["var_log"])
    if not isinstance(var, xr.DataArray):
        raise ValueError("'var' must be a valid Xarray DataArray")
    valid_stats = ["mean", "min", "max", "sum"]
    if stats not in valid_stats:
        var_log.error(f"Resample unrecognised stats {stats}")
        raise ValueError(f"{stats} not in valid list: {valid_stats}.")
    offset = {
        "30m": [15, "min"],
        "h": [30, "min"],
        "3h": [90, "min"],
        "6h": [3, "h"],
        "12h": [6, "h"],
        "D": [12, "h"],
        "7D": [84, "h"],
        "10D": [5, "D"],
        "M": [15, "D"],
        "Y": [6, "M"],
        "10Y": [5, "Y"],
    }
    if sample == "down":
        try:
            vout = var.resample({tdim: rfrq}, origin="start_day", closed="right")
            method = getattr(vout, stats)
            vout = method()
            half, tunit = offset[rfrq][:]
            vout = vout.assign_coords(
                {tdim: xr.CFTimeIndex(vout[tdim].values).shift(half, tunit)}
            )
        except Exception as e:
            var_log.error(f"Resample error: {e}")
            raise ValueError(f"{e}")
    elif sample == "up":
        try:
            vout = var.resample({tdim: rfrq}).interpolate("linear")
        except Exception as e:
            var_log.error(f"Resample error: {e}")
            raise ValueError(f"{e}")
    else:
        var_log.error("Resample can only be up or down")
        raise ValueError("Sample is expected to be up or down")
    return vout


def add_axis(var, name, value):
    """Returns the same variable with an extra singleton axis added

    Parameters
    ----------
    var : Xarray DataArray
        Variable to modify
    name : str
        cmor name for axis
    value : float
        value of the new singleton dimension

    Returns
    -------
    var : Xarray DataArray
        Same variable with added axis at start

    """
    var = var.expand_dims(dim={name: [float(value)]})
    return var


def drop_axis(var, dims, errors="raise"):
    """Returns variable with specified dimensions dropped (lazy operation).

    This function performs lazy dimension dropping that preserves dask arrays.
    For time dimensions, it selects the first time step and then drops the coordinate.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to modify (supports both eager and lazy/dask arrays)
    dims : str or list of str
        Dimension name(s) to drop
    errors : str, optional
        How to handle missing dimensions ('raise' or 'ignore'), default 'raise'

    Returns
    -------
    var : xarray.DataArray
        Variable with specified dimensions dropped. Preserves lazy computation
        if input is lazy.

    Notes
    -----
    - Uses lazy xarray operations - no computation until .compute() is called
    - Fully compatible with dask arrays and preserves chunking
    - For time dimensions: selects first time step then drops time coordinate
    - For other dimensions: uses isel(dim=0) then drops the coordinate
    """
    if isinstance(dims, str):
        dims = [dims]

    result = var
    for dim in dims:
        if dim in result.dims:
            # Select first index along this dimension and drop the coordinate
            result = result.isel({dim: 0}, drop=True)

    return result


def drop_time_axis(var):
    """Returns variable with time dimension dropped by selecting first time step (lazy operation).

    Convenience function specifically for dropping time dimensions, which is a common
    operation for time-independent variables like cell thickness, bathymetry, etc.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to modify (supports both eager and lazy/dask arrays)

    Returns
    -------
    var : xarray.DataArray
        Variable with time dimension dropped. Preserves lazy computation if input is lazy.

    Notes
    -----
    - Uses lazy xarray operations - no computation until .compute() is called
    - Selects first time step and drops time coordinate
    - Safe to use even if time dimension doesn't exist
    """
    if "time" in var.dims:
        return var.isel(time=0, drop=True)
    return var


def squeeze_axis(var, dims=None):
    """Returns variable with singleton dimensions removed (lazy operation).

    This function performs lazy dimension squeezing that preserves dask arrays.
    No computation is triggered until .compute() is called.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to modify (supports both eager and lazy/dask arrays)
    dims : str, list of str, or None, optional
        Dimension name(s) to squeeze. If None, squeeze all singleton dims

    Returns
    -------
    var : xarray.DataArray
        Variable with singleton dimensions squeezed. Preserves lazy computation
        if input is lazy.

    Notes
    -----
    - Uses lazy xarray operations - no computation until .compute() is called
    - Fully compatible with dask arrays and preserves chunking
    - When dims=None, automatically detects and squeezes all singleton dimensions
    """
    # squeeze is a lazy operation that preserves dask arrays
    return var.squeeze(dim=dims)


def sum_vars(varlist):
    """Returns sum of all variables in list
    Parameters
    ----------
    varlist : list(xarray.DataArray)
        Variables to sum

    Returns
    -------
    varout : xarray.DataArray
        Sum of input variables

    """
    # first check that dimensions are same for all variables
    varout = varlist[0]
    for v in varlist[1:]:
        varout = varout + v
    return varout


def rename_coord(var1, var2, ndim, override=False):
    """If coordinates in ndim position are different, renames var2
    coordinates as var1.

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes

    :meta private:
    """
    coord1 = var1.dims[ndim]
    coord2 = var2.dims[ndim]
    if coord1 != coord2:
        var2 = var2.rename({coord2: coord1})
        if "bounds" in var1[coord1].attrs.keys():
            var2[coord1].attrs["bounds"] = var1[coord1].attrs["bounds"]
        override = True
    return var2, override


@click.pass_context
def get_ancil_var(ctx, ancil, varname):
    """Opens the ancillary file and get varname

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes

    Returns
    -------
    ctx : click context obj
        Dictionary including 'cmor' settings and attributes for experiment
        Automatically passed
    var : Xarray DataArray
        selected variable from ancil file

    :meta private:
    """
    f = xr.open_dataset(f"{ctx.obj['ancil_path']}/" + f"{ctx.obj[ancil]}")
    var = f[varname]

    return var


@click.pass_context
def get_plev(ctx, levnum):
    """Read pressure levels from .._coordinate.json file

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes

    Returns
    -------
    ctx : click context obj
        Dictionary including 'cmor' settings and attributes for experiment
        Automatically passed
    levnum : str
        Indicates pressure levels to load, corresponds to plev#levnum axis

    :meta private:
    """
    fpath = f"{ctx.obj['tpath']}/{ctx.obj['_AXIS_ENTRY_FILE']}"
    with open(fpath, "r") as jfile:
        data = json.load(jfile)
    axis_dict = data["axis_entry"]
    plev = np.array(axis_dict[f"plev{levnum}"]["requested"])
    plev = plev.astype(float)
    return plev


def calculate_monthly_minimum(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly minimum values from higher frequency data (lazy computation).

    This function aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly minimum values using lazy xarray operations that preserve Dask arrays.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with time dimension. Should have frequency higher than monthly.
        Supports both eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly minimum values with updated cell_methods attribute.
        Preserves lazy computation if input is lazy.

    Raises
    ------
    ValueError
        If the time dimension is not found in the input data array.

    Examples
    --------
    >>> # Calculate monthly minimum from daily temperature data
    >>> daily_tas = xr.DataArray(...)  # Daily temperature data
    >>> monthly_min_tas = calculate_monthly_minimum(daily_tas)

    Notes
    -----
    - Uses lazy xarray/Dask operations - no computation until .compute() is called
    - Input data should have temporal frequency higher than monthly (daily, 3hr, 6hr, etc.)
    - The function uses xarray's resample method with 'M' frequency (end of month)
    - Cell methods attribute is updated to reflect the temporal aggregation
    - Time coordinate is preserved with monthly timestamps
    """
    if time_dim not in da.dims:
        raise ValueError(
            f"Time dimension '{time_dim}' not found in data array dimensions: {list(da.dims)}"
        )

    # Check if we have a time coordinate
    if time_dim not in da.coords:
        raise ValueError(
            f"Time coordinate '{time_dim}' not found in data array coordinates"
        )

    # Perform monthly resampling using minimum (lazy operation)
    try:
        monthly_min = da.resample({time_dim: "ME"}).min(keep_attrs=preserve_attrs)

        if preserve_attrs:
            # Update cell_methods to reflect the temporal aggregation
            cell_methods = da.attrs.get("cell_methods", "")
            new_cell_method = f"{time_dim}: minimum"

            if cell_methods:
                monthly_min.attrs["cell_methods"] = f"{cell_methods} {new_cell_method}"
            else:
                monthly_min.attrs["cell_methods"] = new_cell_method

        return monthly_min

    except Exception as e:
        raise RuntimeError(f"Failed to calculate monthly minimum: {e}")


def calculate_monthly_maximum(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly maximum values from higher frequency data (lazy computation).

    This function aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly maximum values using lazy xarray operations that preserve Dask arrays.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with time dimension. Should have frequency higher than monthly.
        Supports both eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly maximum values with updated cell_methods attribute.
        Preserves lazy computation if input is lazy.

    Raises
    ------
    ValueError
        If the time dimension is not found in the input data array.

    Examples
    --------
    >>> # Calculate monthly maximum from daily temperature data
    >>> daily_tasmax = xr.DataArray(...)  # Daily maximum temperature data
    >>> monthly_max_tasmax = calculate_monthly_maximum(daily_tasmax)

    Notes
    -----
    - Uses lazy xarray/Dask operations - no computation until .compute() is called
    - Input data should have temporal frequency higher than monthly (daily, 3hr, 6hr, etc.)
    - The function uses xarray's resample method with 'M' frequency (end of month)
    - Cell methods attribute is updated to reflect the temporal aggregation
    - Time coordinate is preserved with monthly timestamps
    """
    if time_dim not in da.dims:
        raise ValueError(
            f"Time dimension '{time_dim}' not found in data array dimensions: {list(da.dims)}"
        )

    # Check if we have a time coordinate
    if time_dim not in da.coords:
        raise ValueError(
            f"Time coordinate '{time_dim}' not found in data array coordinates"
        )

    # Perform monthly resampling using maximum (lazy operation)
    try:
        monthly_max = da.resample({time_dim: "ME"}).max(keep_attrs=preserve_attrs)

        if preserve_attrs:
            # Update cell_methods to reflect the temporal aggregation
            cell_methods = da.attrs.get("cell_methods", "")
            new_cell_method = f"{time_dim}: maximum"

            if cell_methods:
                monthly_max.attrs["cell_methods"] = f"{cell_methods} {new_cell_method}"
            else:
                monthly_max.attrs["cell_methods"] = new_cell_method

        return monthly_max

    except Exception as e:
        raise RuntimeError(f"Failed to calculate monthly maximum: {e}")
