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

from importlib.resources import files as import_files

import numpy as np
import yaml


def extract_tilefrac(tilefrac, tilenum, landfrac=None):
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

    Returns
    -------
    vout : Xarray DataArray
        land fraction of object

    Raises
    ------
    Exception
        tile number must be an integer or list

    """
    pseudo_level = tilefrac.dims[1]
    tilefrac = tilefrac.rename({pseudo_level: "pseudo_level"})
    vout = tilefrac.sel(pseudo_level=tilenum)
    if isinstance(tilenum, int):
        vout = tilefrac.sel(pseudo_level=tilenum)
    elif isinstance(tilenum, list):
        vout = tilefrac.sel(pseudo_level=tilenum).sum(dim="pseudo_level")
    else:
        raise Exception("E: tile number must be an integer or list")
    if landfrac is None:
        # landfrac = get_ancil_var("land_frac", "fld_s03i395")
        raise Exception("E: landfrac not defined")
    vout = vout * landfrac
    return vout.fillna(0)


def calc_topsoil(soilvar):
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
    depth = soilvar.depth
    # find index of bottom depth level including the first 10cm of soil
    maxlev = np.nanargmin(depth.where(depth >= 0.1).values)
    # calculate the fraction of maxlev which falls in first 10cm
    fraction = (0.1 - depth[maxlev - 1]) / (depth[maxlev] - depth[maxlev - 1])
    topsoil = soilvar.isel(depth=slice(0, maxlev)).sum(dim="depth")
    topsoil = topsoil + fraction * soilvar.isel(depth=maxlev)
    return topsoil


def calc_landcover(var, model):
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
    fname = import_files("mopdata").joinpath("land_tiles.yaml")
    # data = read_yaml(fname)
    with fname.open(mode="r") as yfile:
        data = yaml.safe_load(yfile)
    vegtype = data[model]
    pseudo_level = var[0].dims[1]
    vout = (var[0] * var[1]).fillna(0)
    vout = vout.rename({pseudo_level: "vegtype"})
    vout["vegtype"] = vegtype
    vout["vegtype"].attrs["units"] = ""
    return vout


def average_tile(var, tilefrac, landfrac=1.0):
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
        Variable defining tiles' fractions
    landfrac : Xarray DataArray
        Variable defining land fraction (default is 1)

    Returns
    -------
    vout : Xarray DataArray
        averaged input variable

    """
    pseudo_level = var.dims[1]
    vout = var * tilefrac
    vout = vout.sum(dim=pseudo_level)
    vout = vout * landfrac
    return vout
