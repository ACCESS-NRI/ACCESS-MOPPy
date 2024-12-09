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


import click
import xarray as xr
import os
import json 
import yaml
import numpy as np
import dask
import logging

from importlib.resources import files as import_files
from pathlib import Path

from mopdb.utils import read_yaml, MopException

# Global Variables
#----------------------------------------------------------------------

ice_density = 900 #kg/m3
snow_density = 300 #kg/m3

rd = 287.1
cp = 1003.5
p_0 = 100000.0
g_0 = 9.8067   # gravity constant
R_e = 6.378E+06
#----------------------------------------------------------------------


@click.pass_context
def time_resample(ctx, var, rfrq, tdim, orig_tshot, sample='down', stats='mean'):
    """
    Resamples the input variable to the specified frequency using
    specified statistic.

    Resample is used with the options:
    origin = 'start_day'
    closed = 'right'
    This puts the time label to the start of the interval and
    offset is applied to get a centered time label.
    The `rfrq` valid labels are described here:
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
    orig_tshot: str
        original timeshot of input variable
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
    var_log = logging.getLogger(ctx.obj['var_log'])
    if not isinstance(var, xr.DataArray):
        raise MopException("'var' must be a valid Xarray DataArray")
    valid_stats = ["mean", "min", "max", "sum"]
    if stats not in valid_stats:
        var_log.error(f"Resample unrecognised stats {stats}")
        raise MopException(f"{stats} not in valid list: {valid_stats}.")
    offset = {'30m': [15, 'min'], 'h': [30, 'min'], '3h': [90, 'min'],
              '6h': [3, 'h'], '12h': [6, 'h'], 'D': [12, 'h'],
              '7D': [84, 'h'], '10D': [5, 'D'], 'M': [15, 'D'],
              'Y': [6, 'M'], '10Y': [5, 'Y']}
    if sample == "down":
        try:
            vout = var.resample({tdim: rfrq}, origin="start_day",
                                closed="right")
            method = getattr(vout, stats)
            vout = method()
            # apply negative offset if original timeshot is point
            if orig_tshot != 'point':
                half, tunit = offset[rfrq][:]
                vout = vout.assign_coords({tdim:
                    xr.CFTimeIndex(vout[tdim].values).shift(half, tunit)})
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
    var_log = logging.getLogger(ctx.obj['var_log'])
    coord1 = var1.dims[ndim]
    coord2 = var2.dims[ndim]
    if coord1 != coord2:
        var_log.debug(f"{var1.name}, {var2.name}: {coord1}, {coord2}")
        var2 = var2.rename({coord2: coord1})
        if 'bounds' in var1[coord1].attrs.keys():
            var2[coord1].attrs['bounds'] = var1[coord1].attrs['bounds']
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
    f = xr.open_dataset(f"{ctx.obj['ancil_path']}/" +
            f"{ctx.obj[ancil]}")
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
    with open(fpath, 'r') as jfile:
        data = json.load(jfile)
    axis_dict = data['axis_entry']
    plev = np.array(axis_dict[f"plev{levnum}"]['requested'])
    plev = plev.astype(float)
    return plev


@click.pass_context
def K_degC(ctx, var, inverse=False):
    """Converts temperature from/to K to/from degC.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    var : Xarray DataArray 
        temperature array

    Returns
    -------
    vout : Xarray DataArray 
        temperature array in degrees Celsius or Kelvin if inverse is True

    """    
    var_log = logging.getLogger(ctx.obj['var_log'])
    if not inverse and 'K' in var.units:
        var_log.info("temp in K, converting to degC")
        vout = var - 273.15
    elif inverse and 'C' in var.units:
        var_log.info("temp in degC, converting to K")
        vout = var + 273.15
    return vout

@click.pass_context
def get_coords(ctx, coords):
    """Get lat/lon and their boundaries from ancil file

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    coords : list
        List of coordinates retrieved from variable encoding 
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    # open ancil grid file to read vertices
    #PP be careful this is currently hardcoded which is not ok!
    ancil_dir = ctx.obj.get('ancils_path', '')
    ancil_file = ancil_dir + "/" + ctx.obj.get(f"grid_{ctx.obj['realm']}", '')
    if (ancil_file == '' or not Path(ancil_file).exists() or 
        f"grid_{ctx.obj['realm']}" not in ctx.obj.keys()):
        var_log.error(f"Ancil file {ancil_file} not set or inexistent")
        raise MopException(f"Ancil file {ancil_file} not set or inexistent")
    var_log.debug(f"getting lat/lon and bnds from ancil file: {ancil_file}")
    ds = xr.open_dataset(ancil_file)
    var_log.debug(f"ancil ds: {ds}")
    # read lat/lon and vertices mapping
    cfile = import_files('mopdata').joinpath('latlon_vertices.yaml')
    with open(cfile, 'r') as yfile:
        data = yaml.safe_load(yfile)
    ll_dict = data[ctx.obj['realm']]
    #ensure longitudes are in the 0-360 range.
    # first two coordinates should be lon,lat
    for c in coords[:2]:
         var_log.debug(f"ancil coord: {c}")
         coord = ds[ll_dict[c][0]]
         var_log.debug(f"bnds name: {ll_dict[c]}")
         bnds = ds[ll_dict[c][1]]
         # num of vertices should be last dimension 
         if bnds.shape[-1] > bnds.shape[0]:
             bnds = bnds.transpose(*(list(bnds.dims[1:]) + [bnds.dims[0]]))
         if 'lon' in c.lower():
             lon = np.mod(coord, 360)
             lon_bnds = np.mod(bnds, 360)
         elif 'lat' in c.lower():
             lat = coord
             lat_bnds = bnds
    return lat, lat_bnds, lon, lon_bnds
