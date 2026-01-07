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
    Calculates the land fraction of a specific tile type as a percentage.

    This function extracts the fractional coverage of specific land tile types
    (e.g., crops, grass, forests) and converts the result to percentage values.
    The calculation accounts for the overall land fraction to provide accurate
    tile coverage relative to the total grid cell area.

    Parameters
    ----------
    tilefrac : xarray.DataArray
        Tile fraction variable containing fractional coverage for each tile type.
        Must have a pseudo-level dimension representing different tile types.
    tilenum : int or list of int
        Tile number(s) to extract:
        - int: Extract single tile type
        - list: Extract and sum multiple tile types
    landfrac : xarray.DataArray, optional
        Land fraction variable (fractional, 0-1) representing the proportion
        of each grid cell that is land. Required for proper calculation.

    Returns
    -------
    xarray.DataArray
        Land fraction of specified tile type(s) as percentage (0-100%).
        - Units: % (percentage)
        - Missing values filled with 0
        - Represents tile coverage relative to total grid cell area

    Raises
    ------
    Exception
        If tilenum is not int or list, or if landfrac is None.

    Examples
    --------
    Extract crop fraction as percentage:
    
    >>> crop_percent = extract_tilefrac(tilefrac, 9, landfrac)
    
    Extract combined grass types (C3 + C4) as percentage:
    
    >>> grass_percent = extract_tilefrac(tilefrac, [6, 7], landfrac)

    Notes
    -----
    - Output is converted to percentage (0-100%) for CMIP compliance
    - Multiple tile types are summed before percentage calculation
    - Result represents actual land coverage accounting for land/ocean fraction
    - Missing values are filled with zeros for consistent output
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
    # Convert to percentage
    vout = vout * landfrac * 100.0
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
    Calculate land cover fraction variable as percentage with vegetation type labels.

    This function computes land cover fractions by combining tile fractions with
    land fractions, converts the result to percentage values, and assigns
    meaningful vegetation type names based on the specified land surface model.

    Parameters
    ----------
    var : list of xarray.DataArray
        List containing exactly 2 input variables:
        - var[0]: Tile fraction variable (fractional, 0-1)
        - var[1]: Land fraction variable (fractional, 0-1)
        Both must have compatible dimensions for multiplication.
    model : str
        Name of land surface model to retrieve vegetation type definitions:
        - "cable": CABLE land surface model (17 vegetation types)
        - "cmip6": CMIP6 standard land categories (4 categories)

    Returns
    -------
    xarray.DataArray
        Land cover fraction variable as percentage (0-100%).
        - Units: % (percentage)
        - Coordinates: Includes 'vegtype' dimension with descriptive names
        - Missing values filled with 0
        - Represents land cover relative to total grid cell area

    Examples
    --------
    Calculate CABLE vegetation fractions as percentage:
    
    >>> landcover_pct = calc_landcover([tilefrac, landfrac], "cable")
    
    Calculate CMIP6 land categories as percentage:
    
    >>> landcover_pct = calc_landcover([tilefrac, landfrac], "cmip6")

    Notes
    -----
    - Output is converted to percentage (0-100%) for CMIP compliance
    - Vegetation type coordinate provides human-readable category names
    - CABLE model includes 17 vegetation types (forests, grasses, crops, etc.)
    - CMIP6 model includes 4 broad categories (primary/secondary land, pastures, crops, urban)
    - Result represents actual land coverage accounting for land/ocean fraction
    - Missing values are filled with zeros for consistent output
    
    Vegetation Types by Model:
    - CABLE: Evergreen/Deciduous Forests, Shrub, C3/C4 Grass, Crops, Tundra, etc.
    - CMIP6: Primary/Secondary Land, Pastures, Crops, Urban
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
    # convert to percentage
    vout = (var[0] * var[1]).fillna(0) * 100.0
    vout = vout.rename({pseudo_level: "vegtype"})
    vout["vegtype"] = vegtype
    vout["vegtype"].attrs["units"] = ""
    return vout


def weighted_tile_sum(var, tilefrac, landfrac=1.0):
    """
    Returns variable weighted by tile fractions and summed over tiles.
    
    This function performs tile-weighted integration by multiplying each tile
    value by its fractional coverage, summing across all tiles, and scaling
    by land fraction to get the grid-cell integrated value.

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
        Tile-weighted and land-fraction scaled variable.
    """
    pseudo_level = var.dims[1]
    vout = var * tilefrac
    vout = vout.sum(dim=pseudo_level)
    vout = vout * landfrac
    return vout


def calc_cland_with_wood_products(carbon_pools_sum, wood_pools_sum, tilefrac, landfrac):
    """
    Calculate total land carbon including wood products with correct weighting.
    
    Parameters:
    - carbon_pools_sum: Sum of variables 851-860 (to be weighted by tilefrac)
    - wood_pools_sum: Sum of variables 898-900 (no tilefrac weighting)
    - tilefrac, landfrac: Weighting variables
    """
    # Determine pseudo-level dimension from carbon pools
    pseudo_level = carbon_pools_sum.dims[1]

    # Carbon pools: multiply by tilefrac then sum over tiles
    carbon_weighted = carbon_pools_sum * tilefrac
    carbon_sum = carbon_weighted.sum(dim=pseudo_level)
    
    # Wood products: sum over tiles only (no tilefrac multiplication)
    wood_sum = wood_pools_sum.sum(dim=pseudo_level)
    
    # Combine and apply land fraction, convert to kg m-2 (divide by 1000)
    total = ((carbon_sum + wood_sum) / 1000.0) * landfrac
    return total


def calc_carbon_pool_kg_m2(var, tilefrac, landfrac):
    """
    Calculate individual carbon pool variable with unit conversion to kg m-2.
    
    Parameters:
    - var: Carbon pool variable (to be weighted by tilefrac and converted)
    - tilefrac, landfrac: Weighting variables
    """
    pseudo_level = var.dims[1]
    
    # Weight by tilefrac then sum over tiles
    weighted = var * tilefrac
    summed = weighted.sum(dim=pseudo_level)
    
    # Apply land fraction and convert to kg m-2 (divide by 1000)
    result = (summed / 1000.0) * landfrac
    return result
