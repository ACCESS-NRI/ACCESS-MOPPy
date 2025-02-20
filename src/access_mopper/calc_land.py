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
# This file contains a collection of functions to calculate land derived variables
# from ACCESS model output.
# Initial functions' definitions were based on APP4 modified to work with Xarray.
#
# To propose new calculations and/or update to existing ones see documentation:
#
# and open a new issue on github.

import click
import xarray as xr
import os
import json 
import numpy as np
import dask
import logging
from importlib.resources import files as import_files

from mopdb.utils import read_yaml, MopException
from mopper.calc_utils import get_ancil_var

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
def extract_tilefrac(ctx, tilefrac, tilenum, landfrac=None, lev=None):
    """Calculates the land fraction of a specific type: crops, grass,
    etc.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    tilefrac : Xarray DataArray
        variable 
    tilenum : Int or [Int]
        the number indicating the tile
    landfrac : Xarray DataArray
        Land fraction variable if None (default) is read from ancil file
    lev: str
        name of pseudo level to add to output array (default is None)

    Returns
    -------
    vout : Xarray DataArray
        land fraction of object

    Raises
    ------
    Exception
        tile number must be an integer or list

    """    
    var_log = logging.getLogger(ctx.obj['var_log'])
    pseudo_level = tilefrac.dims[1]
    tilefrac = tilefrac.rename({pseudo_level: 'pseudo_level'})
    vout = tilefrac.sel(pseudo_level=tilenum)
    if isinstance(tilenum, int):
        vout = tilefrac.sel(pseudo_level=tilenum)
    elif isinstance(tilenum, list):
        vout = tilefrac.sel(pseudo_level=tilenum).sum(dim='pseudo_level')
    else:
        raise Exception('E: tile number must be an integer or list')
    if landfrac is None: 
        landfrac = get_ancil_var('land_frac', 'fld_s03i395')
    vout = vout * landfrac
    if lev:
        fname = import_files('mopdata').joinpath('landtype.yaml')
        data = read_yaml(fname)
        type_dict = data['mod_mapping']
        var_log.debug(f"extract_tile with lev {lev}, type_dict: {type_dict}")
        vout = vout.expand_dims(dim={lev: [type_dict[lev]]})
    return vout.fillna(0)


@click.pass_context
def landuse_frac(ctx, var, landfrac=None, nwd=0, tiles='cmip6'):    
    """Defines new tile fractions variables where 
    original model tiles are re-organised in 4 super-categories

    0 - psl Primary and secondary land (includes forest, grasslands,
      and bare ground) (1,2,3,4,5,6,7,11,14) or (6,7,11,14?) if nwd is true.
      Possibly excluding barren soil is an error?
    1 - pst Pastureland (includes managed pastureland and rangeland) (2) or (7) if nwd
    2 - crp Cropland  (9) or (7) if nwd
    3 - Urban settlement (15) or (14) if nwd is true?? 

    Tiles in CABLE:
    1. Evergreen Needleleaf
    2. Evergreen Broadleaf
    3. Deciduous Needleleaf
    4. Deciduous Broadleaf 
    5. Shrub
    6. C3 Grassland
    7. C4 Grassland
    8. Tundra
    9. C3 Cropland
    10. C4 Cropland
    11. Wetland
    12. empty
    13. empty
    14. Barren
    15. Urban
    16. Lakes
    17. Ice

    NB this is currently hardcoded for above definitions, but potentially
    output could depend on different categories and land model used.

    Parameters
    ----------
    var : Xarray DataArray
        Tile variable 
    landfrac : Xarray DataArray
        Land fraction variable if None (default) is read from ancil file
    nwd : int
        Indicates if only non-woody categories (1) or all (0 - default)
        should be used
    tiles : str
        Tiles definition to use for landUse dimension, default is cmip

    Returns
    -------
    vout : Xarray DataArray
        Input tile variable redifined over 4 super-categories 

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    pseudo_level = var.dims[1]
    #nwd (non-woody vegetation only) - tiles 6,7,9,11 only
    vout = xr.zeros_like(var[:, :4, :, :])
    vout = vout.rename({pseudo_level: 'landUse'})
    fname = import_files('mopdata').joinpath('land_tiles.yaml')
    data = read_yaml(fname)
    var_log.debug(f"model land tiles: {data}") 
    vout['landUse'] = data['cmip6']
    vout['landUse'].attrs['units'] = ""
    var_log.debug(f"landUse: {vout['landUse']}, {vout['landUse'].attrs}") 
    # Define the tile indices for primary .. based on 'nwd' value
    if nwd == 0:
        tile_indices = [1, 2, 3, 4, 5, 6, 7, 11, 14]
    elif nwd == 1:
        tile_indices = [6, 7, 11, 14]  # 
    for t in tile_indices:
        vout.loc[dict(landUse='primary_and_secondary_land')] += var.sel({pseudo_level: t})
    # Pastureland not included in CABLE
    # Crop tile 9
    vout.loc[dict(landUse='crops')] = var.sel({pseudo_level: 9})
    # Urban tile updated based on 'nwd' in app4 not sure why
    #if nwd == 0:
    vout.loc[dict(landUse='urban')] = var.sel({pseudo_level: 15})
    if landfrac is None:
        landfrac = get_ancil_var('land_frac', 'fld_s03i395')
    vout = vout * landfrac
    # if nwdFracLut we want typenwd as an extra dimension as axis=0
    if nwd == 1:
        vout = vout.expand_dims(typenwd=['herbaceous_vegetation'])
    return vout


def average_tile(var, tilefrac=None, lfrac=1, landfrac=None, lev=None):
    """Returns variable averaged over grid-cell, counting only
    specific tile/s and land fraction when suitable.

    For example: nLitter is nitrogen mass in litter and should be
    calculated only over land fraction and each tile type will have
    different amounts of litter.
    average = sum_over_tiles(N amount on tile * tilefrac) * landfrac  

    Parameters
    ----------
    var : Xarray DataArray
        Variable to process defined opver tiles
    tilefrac : Xarray DataArray, optional 
        Variable defining tiles' fractions (default is None)
        if None, read from ancil file 
    lfrac : int, optional
         Controls if landfrac is considered (1) or not (0) (deafault 1)
    landfrac : Xarray DataArray
        Variable defining land fraction (default is None)
        If None, read from ancil file 
    lev: str
        Name of pseudo level to add to output array (default is None)

    Returns
    -------
    vout : Xarray DataArray
        averaged input variable

    """    
    pseudo_level = var.dims[1]
    if tilefrac is None:
        tilefrac = get_ancil_var('land_tile', 'fld_s03i317')
    vout = var * tilefrac
    vout = vout.sum(dim=pseudo_level)
    if lfrac == 1:
        if landfrac is None:
            landfrac = get_ancil_var('land_frac', 'fld_s03i395')
        vout = vout * landfrac
    if lev:
        fname = import_files('mopdata').joinpath('landtype.yaml')
        data = read_yaml(fname)
        type_dict = data['mod_mapping']
        vout = vout.expand_dims(dim={lev: type_dict[lev]})
    return vout


@click.pass_context
def calc_topsoil(ctx, soilvar):
    """Returns the variable over the first 10cm of soil.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    soilvar : Xarray DataArray
        Soil moisture over soil levels 

    Returns
    -------
    topsoil : Xarray DataArray
        Variable defined on top 10cm of soil

    """    
    var_log = logging.getLogger(ctx.obj['var_log'])
    depth = soilvar.depth
    # find index of bottom depth level including the first 10cm of soil
    maxlev = np.nanargmin(depth.where(depth >= 0.1).values)
    var_log.debug(f"Max level of soil used is {maxlev}")
    # calculate the fraction of maxlev which falls in first 10cm
    fraction = (0.1 - depth[maxlev -1])/(depth[maxlev] - depth[maxlev-1])
    topsoil = soilvar.isel(depth=slice(0,maxlev)).sum(dim='depth')
    topsoil = topsoil + fraction*soilvar.isel(depth=maxlev)
    return topsoil


@click.pass_context
def calc_landcover(ctx, var, model):
    """Returns land cover fraction variable

    Parameters
    ----------
    ctx : click context obj
        Dictionary including 'cmor' settings and attributes for experiment
    var : list(xarray.DataArray)
        List of input variables to sum
    model: str
        Name of land surface model to retrieve land tiles definitions

    Returns
    -------
    vout : xarray.DataArray
        Land cover faction variable

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    fname = import_files('mopdata').joinpath('land_tiles.yaml')
    data = read_yaml(fname)
    vegtype = data[model]
    var_log.debug(f"vegtype used from {model}: {vegtype}")
    pseudo_level = var[0].dims[1]
    vout = (var[0]*var[1]).fillna(0)
    vout = vout.rename({pseudo_level: 'vegtype'})
    vout['vegtype'] = vegtype
    vout['vegtype'].attrs['units'] = ""
    return vout

# Utilities
#----------------------------------------------------------------------
