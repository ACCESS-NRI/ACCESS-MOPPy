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
from mopdb.utils import MopException

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
        raise MopException("'var' must be a valid Xarray DataArray")
    valid_stats = ["mean", "min", "max", "sum"]
    if stats not in valid_stats:
        var_log.error(f"Resample unrecognised stats {stats}")
        raise MopException(f"{stats} not in valid list: {valid_stats}.")
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
            raise MopException(f"{e}")
    elif sample == "up":
        try:
            vout = var.resample({tdim: rfrq}).interpolate("linear")
        except Exception as e:
            var_log.error(f"Resample error: {e}")
            raise MopException(f"{e}")
    else:
        var_log.error("Resample can only be up or down")
        raise MopException("Sample is expected to be up or down")
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
    var = var.expand_dims(dim={name: float(value)})
    return var


@click.pass_context
def sum_vars(ctx, varlist):
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


@click.pass_context
def rename_coord(ctx, var1, var2, ndim, override=False):
    """If coordinates in ndim position are different, renames var2
    coordinates as var1.

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes

    :meta private:
    """
    var_log = logging.getLogger(ctx.obj["var_log"])
    coord1 = var1.dims[ndim]
    coord2 = var2.dims[ndim]
    if coord1 != coord2:
        var_log.debug(f"{var1.name}, {var2.name}: {coord1}, {coord2}")
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
