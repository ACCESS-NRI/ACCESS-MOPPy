#!/usr/bin/env python
# Copyright 2024 ARC Centre of Excellence for Climate Extremes
# Authors: Paola Petrelli <paola.petrelli@utas.edu.au>, Sam Green <sam.green@unsw.edu.au>
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
# This file contains functions to calculate land-derived variables
# from ACCESS model output, adapted from APP4 for use with Xarray.
# For updates or new calculations, see documentation and open a new issue on GitHub.

import numpy as np


def extract_tilefrac(tilefrac, tilenum, landfrac=None):
    """
    Calculates the land fraction of a specific type (e.g., crops, grass).

    Parameters
    ----------
    tilefrac : xarray.DataArray
        Tile fraction variable.
    tilenum : int or list of int
        Tile number(s) to extract.
    landfrac : xarray.DataArray, optional
        Land fraction variable. If None, raises Exception.

    Returns
    -------
    xarray.DataArray
        Land fraction of specified tile(s).

    Raises
    ------
    Exception
        If tilenum is not int or list, or landfrac is None.
    """
    pseudo_level = tilefrac.dims[1]
    tilefrac = tilefrac.rename({pseudo_level: "pseudo_level"})
    if isinstance(tilenum, int):
        vout = tilefrac.sel(pseudo_level=tilenum)
    elif isinstance(tilenum, list):
        vout = tilefrac.sel(pseudo_level=tilenum).sum(dim="pseudo_level")
    else:
        raise Exception("E: tile number must be an integer or list")
    if landfrac is None:
        raise Exception("E: landfrac not defined")
    vout = vout * landfrac
    return vout.fillna(0)


def calc_topsoil(soilvar):
    """
    Returns the variable over the first 10cm of soil.

    Parameters
    ----------
    soilvar : xarray.DataArray
        Soil variable over soil levels.

    Returns
    -------
    xarray.DataArray
        Variable defined on top 10cm of soil.
    """
    depth = soilvar.depth
    maxlev = np.nanargmin(depth.where(depth >= 0.1).values)
    fraction = (0.1 - depth[maxlev - 1]) / (depth[maxlev] - depth[maxlev - 1])
    topsoil = soilvar.isel(depth=slice(0, maxlev)).sum(dim="depth")
    topsoil = topsoil + fraction * soilvar.isel(depth=maxlev)
    return topsoil


def calc_landcover(var, model):
    """
    Returns land cover fraction variable.

    Parameters
    ----------
    var : list of xarray.DataArray
        List of input variables to sum.
    model : str
        Name of land surface model to retrieve land tiles definitions.

    Returns
    -------
    xarray.DataArray
        Land cover fraction variable.
    """
    land_tiles = {
        "cmip6": ["primary_and_secondary_land", "pastures", "crops", "urban"],
        "cable": [
            "Evergreen_Needleleaf",
            "Evergreen_Broadleaf",
            "Deciduous_Needleleaf",
            "Deciduous_Broadleaf",
            "Shrub",
            "C3_grass",
            "C4_grass",
            "Tundra",
            "C3_crop",
            "C4_crop",
            "Wetland",
            "",
            "",
            "Barren",
            "Urban",
            "Lakes",
            "Ice",
        ],
    }

    vegtype = land_tiles[model]
    pseudo_level = var[0].dims[1]
    vout = (var[0] * var[1]).fillna(0)
    vout = vout.rename({pseudo_level: "vegtype"})
    vout["vegtype"] = vegtype
    vout["vegtype"].attrs["units"] = ""
    return vout


def average_tile(var, tilefrac, landfrac=1.0):
    """
    Returns variable averaged over grid-cell, counting only
    specific tile(s) and land fraction when suitable.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to process defined over tiles.
    tilefrac : xarray.DataArray
        Variable defining tiles' fractions.
    landfrac : xarray.DataArray or float, optional
        Land fraction (default is 1.0).

    Returns
    -------
    xarray.DataArray
        Averaged input variable.
    """
    pseudo_level = var.dims[1]
    vout = var * tilefrac
    vout = vout.sum(dim=pseudo_level)
    vout = vout * landfrac
    return vout
