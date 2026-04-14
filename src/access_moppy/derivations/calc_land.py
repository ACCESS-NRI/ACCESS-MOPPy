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


def extract_tilefrac(tilefrac, tilenum, landfrac=None, lev=None):
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
    lev : str, optional
        Name of vegetation type key from mod_mapping dictionary to add as a
        dimension to output array. Used for CMOR character-type variables.
        Examples: "typebare", "typecrop", "typetree", etc.

    Returns
    -------
    xarray.DataArray
        Land fraction of specified tile type(s) as percentage (0-100%).
        - Units: % (percentage)
        - Missing values filled with 0
        - Represents tile coverage relative to total grid cell area
        - If lev is specified, includes additional dimension with vegetation type label

    Raises
    ------
    Exception
        If tilenum is not int or list, or if landfrac is None.

    Examples
    --------
    Extract crop fraction as percentage:

    >>> crop_percent = extract_tilefrac(tilefrac, 9, landfrac)

    Extract combined grass types with vegetation type dimension:

    >>> grass_percent = extract_tilefrac(tilefrac, [6, 7], landfrac, lev="typenatgr")

    Notes
    -----
    - Output is converted to percentage (0-100%) for CMIP compliance
    - Multiple tile types are summed before percentage calculation
    - Result represents actual land coverage accounting for land/ocean fraction
    - Missing values are filled with zeros for consistent output
    - When lev is specified, creates dimension for CMOR character-type output
    """
    # Vegetation type mapping for CMOR character variables
    # mod_mapping = {
    #    "typebare": "bare_ground",
    #    "typeburnt": "burnt_vegetation",
    #    "typec3pft": "c3_plant_functional_types",
    #    "typec3crop": "crops_of_c3_plant_functional_types",
    #    "typec3natg": "natural_grasses_of_c3_plant_functional_types",
    #    "typec3pastures": "pastures_of_c3_plant_functional_types",
    #    "typec4pft": "c4_plant_functional_types",
    #    "typec4crop": "crops_of_c4_plant_functional_types",
    #    "typec4natg": "natural_grasses_of_c4_plant_functional_types",
    #    "typec4pastures": "pastures_of_c4_plant_functional_types",
    #    "typecloud": "cloud",
    #    "typecrop": "crops",
    #    "typefis": "floating_ice_shelf",
    #    "typegis": "grounded_ice_sheet",
    #    "typeland": "land",
    #    "typeli": "land_ice",
    #    "typemp": "sea_ice_melt_pond",
    #    "typenatgr": "natural_grasses",
    #    "typenwd": "herbaceous_vegetation",
    #    "typepasture": "pastures",
    #    "typepdec": "primary_deciduous_trees",
    #    "typepever": "primary_evergreen_trees",
    #    "typeresidual": "residual",
    #    "typesdec": "secondary_deciduous_trees",
    #    "typesea": "sea",
    #    "typesever": "secondary_evergreen_trees",
    #    "typeshrub": "shrubs",
    #    "typesi": "sea_ice",
    #    "typesirdg": "sea_ice_ridges",
    #    "typetree": "trees",
    #    "typeveg": "vegetation",
    #    "typewetla": "wetland",
    # }

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

    # TODO: Revisit adding vegetation type dimension
    # Add vegetation type dimension if requested
    #    if lev:
    #        if lev not in mod_mapping:
    #            raise Exception(f"E: vegetation type '{lev}' not found in mod_mapping")
    #
    #        # Create character coordinate for the type dimension
    #        type_string = mod_mapping[lev]
    #        strlen = len(type_string)
    #
    #        # Convert string to character array for NetCDF
    #        char_data = np.array([c.encode("utf-8") for c in type_string], dtype="S1")
    #
    #        # Import xarray locally
    #        import xarray as xr
    #
    #        # Create 2D character array: typebare(typebare=1, strlen=N)
    #        char_2d = char_data.reshape(1, -1)
    #
    #        # Add both the type dimension and strlen dimension to the data variable
    #        vout = vout.expand_dims(dim={lev: 1, "strlen": strlen})
    #
    #        # Create character coordinate as a proper 2D character array
    #        type_coord = xr.DataArray(
    #            char_2d,
    #            dims=[lev, "strlen"],
    #            coords={
    #                lev: [0],  # Single index for the type dimension
    #                "strlen": np.arange(strlen),
    #            },
    #            attrs={"long_name": "surface type", "standard_name": "area_type"},
    #        )
    #
    #        # Assign the character coordinate to the type dimension
    #        vout = vout.assign_coords({lev: type_coord})

    return vout.fillna(0)


def calc_topsoil(soilvar):
    """
    Returns the variable over the first 10cm of soil using lazy operations.

    Parameters
    ----------
    soilvar : xarray.DataArray
        Soil variable over soil levels with soil_model_level_number dimension.

    Returns
    -------
    xarray.DataArray
        Variable defined on top 10cm of soil.
    """
    import xarray as xr

    # Soil depth mapping from model level numbers to depth values (meters)
    depths = {
        1: 0.0109999999403954,  # ~0.011m
        2: 0.0509999990463257,  # ~0.051m
        3: 0.157000005245209,  # ~0.157m
        4: 0.438499987125397,  # ~0.439m
        5: 1.18550002574921,  # ~1.186m
        6: 2.87199997901917,  # ~2.872m
    }

    target_depth = 0.1  # 10cm

    # Get soil level dimension info without triggering computation
    soil_dim = "soil_model_level_number"

    # Create depth coordinate for the levels present in this variable
    if soil_dim in soilvar.coords:
        # Use existing coordinate values
        levels = soilvar[soil_dim]
    else:
        # Create coordinate from dimension size
        level_size = soilvar.sizes[soil_dim]
        levels = xr.DataArray(
            list(range(1, level_size + 1)),
            dims=[soil_dim],
            coords={soil_dim: list(range(level_size))},
        )

    # Create lazy depth array mapped to the actual levels
    depth_values = levels.copy()
    for level_num, depth_val in depths.items():
        # Use xarray's where for lazy assignment
        depth_values = depth_values.where(levels != level_num, depth_val)

    # Find levels within 10cm using lazy operations
    within_target = depth_values <= target_depth

    # Sum all levels completely within target depth
    # Use where to mask levels outside target, then sum
    masked_data = soilvar.where(within_target, 0.0)
    topsoil = masked_data.sum(dim=soil_dim, keep_attrs=True)

    # Add fractional contribution from next level if needed
    # Find the first level that exceeds target depth
    exceeds_target = depth_values > target_depth

    if exceeds_target.any():
        # Get the minimum depth that exceeds target (next level after those within target)
        next_level_depth = depth_values.where(exceeds_target).min()
        next_level_mask = depth_values == next_level_depth

        # Calculate previous depth (maximum depth within target)
        if within_target.any():
            prev_depth = depth_values.where(within_target).max()
        else:
            prev_depth = xr.zeros_like(next_level_depth)

        # Calculate fraction lazily
        depth_range = next_level_depth - prev_depth
        fraction = (target_depth - prev_depth) / depth_range

        # Add fractional contribution
        next_level_contrib = soilvar.where(next_level_mask, 0.0).sum(
            dim=soil_dim, keep_attrs=True
        )
        topsoil = topsoil + fraction * next_level_contrib

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


def weighted_tile_sum(var, tilefrac, landfrac=1.0, pseudo_level="pseudo_level_0"):
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
    # TODO: Might be good to avoid hardcoding pseudo_level name
    pseudo_level = "pseudo_level_0"

    # Carbon pools: multiply by tilefrac then sum over tiles
    carbon_weighted = carbon_pools_sum * tilefrac
    carbon_sum = carbon_weighted.sum(dim=pseudo_level)

    # Wood products: sum over tiles only (no tilefrac multiplication)
    wood_sum = wood_pools_sum.sum(dim=pseudo_level)

    # Combine and apply land fraction, convert to kg m-2 (divide by 1000)
    total = ((carbon_sum + wood_sum) / 1000.0) * landfrac

    return total


def calc_mass_pool_kg_m2(var, tilefrac, landfrac):
    """
    Calculate mass pool variable (carbon, nitrogen, etc.) with unit conversion to kg m-2.

    This function provides a generalized calculation for any mass pool variable
    that requires tile weighting, spatial integration, and unit conversion.

    Parameters
    ----------
    var : xarray.DataArray
        Mass pool variable (in g m-2) to be weighted by tilefrac and converted.
        Must have a pseudo-level dimension representing tiles.
    tilefrac : xarray.DataArray
        Variable defining tiles' fractions (fractional, 0-1).
    landfrac : xarray.DataArray
        Land fraction (fractional, 0-1).

    Returns
    -------
    xarray.DataArray
        Mass pool variable in kg m-2, weighted by tile fractions and land fraction.
    """
    pseudo_level = "pseudo_level_0"

    # Weight by tilefrac then sum over tiles
    weighted = var * tilefrac
    summed = weighted.sum(dim=pseudo_level)

    # Apply land fraction and convert to kg m-2 (divide by 1000)
    result = (summed / 1000.0) * landfrac
    return result


def calc_carbon_pool_kg_m2(var, tilefrac, landfrac):
    """
    Calculate individual carbon pool variable with unit conversion to kg m-2.

    This function is an alias for calc_mass_pool_kg_m2 to maintain backward
    compatibility with existing carbon pool calculations.

    Parameters
    ----------
    var : xarray.DataArray
        Carbon pool variable (to be weighted by tilefrac and converted).
    tilefrac : xarray.DataArray
        Variable defining tiles' fractions.
    landfrac : xarray.DataArray
        Land fraction variable.

    Returns
    -------
    xarray.DataArray
        Carbon pool variable in kg m-2.
    """
    return calc_mass_pool_kg_m2(var, tilefrac, landfrac)


# Alias for nitrogen pools - same calculation as carbon pools
calc_nitrogen_pool_kg_m2 = calc_mass_pool_kg_m2


def calc_mrsfl(var1, var2):
    """
    Calculate frozen water content of soil layer with depth coordinate transformation.

    This function multiplies soil moisture (fld_s08i223) by frozen fraction (fld_s08i230)
    and transforms the soil_model_level_number coordinate to depth values.

    Parameters
    ----------
    var1 : xarray.DataArray
        fld_s08i223 (soil moisture) with soil_model_level_number dimension.
    var2 : xarray.DataArray
        fld_s08i230 (frozen fraction) with soil_model_level_number dimension.

    Returns
    -------
    xarray.DataArray
        Frozen water content with depth coordinate instead of soil_model_level_number.
    """

    # Soil depth mapping from model level numbers to depth values (meters)
    depths = {
        1: 0.0109999999403954,
        2: 0.0509999990463257,
        3: 0.157000005245209,
        4: 0.438499987125397,
        5: 1.18550002574921,
        6: 2.87199997901917,
    }

    # Perform the multiplication
    result = var1 * var2

    # Transform soil levels to depths if soil_model_level_number dimension exists
    if "soil_model_level_number" in result.dims:
        # Get soil level coordinate/dimension
        if "soil_model_level_number" in result.coords:
            soil_levels = result["soil_model_level_number"]
            level_values = soil_levels.values
        else:
            # If it's just a dimension, assume sequential levels 1,2,3,etc.
            level_size = result.sizes["soil_model_level_number"]
            level_values = list(range(1, level_size + 1))

        # Create depth values array (lazy operation)
        depth_values = [depths.get(int(level), float("nan")) for level in level_values]

        # Transform the result to use depth coordinate
        result = (
            result.assign_coords({"depth": ("soil_model_level_number", depth_values)})
            .swap_dims({"soil_model_level_number": "depth"})
            .drop_vars(["soil_model_level_number"], errors="ignore")
        )

    return result


def calc_mrsll(var1, var2):
    """
    Calculate liquid water content of soil layer with depth coordinate transformation.

    This function multiplies soil moisture (fld_s08i223) by liquid fraction (fld_s08i229)
    and transforms the soil_model_level_number coordinate to depth values.

    Parameters
    ----------
    var1 : xarray.DataArray
        fld_s08i223 (soil moisture) with soil_model_level_number dimension.
    var2 : xarray.DataArray
        fld_s08i229 (liquid fraction) with soil_model_level_number dimension.

    Returns
    -------
    xarray.DataArray
        Liquid water content with depth coordinate instead of soil_model_level_number.
    """

    # Soil depth mapping from model level numbers to depth values (meters)
    depths = {
        1: 0.0109999999403954,
        2: 0.0509999990463257,
        3: 0.157000005245209,
        4: 0.438499987125397,
        5: 1.18550002574921,
        6: 2.87199997901917,
    }

    # Perform the multiplication
    result = var1 * var2

    # Transform soil levels to depths if soil_model_level_number dimension exists
    if "soil_model_level_number" in result.dims:
        # Get soil level coordinate/dimension
        if "soil_model_level_number" in result.coords:
            soil_levels = result["soil_model_level_number"]
            level_values = soil_levels.values
        else:
            # If it's just a dimension, assume sequential levels 1,2,3,etc.
            level_size = result.sizes["soil_model_level_number"]
            level_values = list(range(1, level_size + 1))

        # Create depth values array (lazy operation)
        depth_values = [depths.get(int(level), float("nan")) for level in level_values]

        # Transform the result to use depth coordinate
        result = (
            result.assign_coords({"depth": ("soil_model_level_number", depth_values)})
            .swap_dims({"soil_model_level_number": "depth"})
            .drop_vars(["soil_model_level_number"], errors="ignore")
        )

    return result


def calc_mrsol(var1):
    """
    Calculate mass content of water in soil layer with depth coordinate transformation.

    This function takes soil moisture (fld_s08i223) and transforms the
    soil_model_level_number coordinate to depth values.

    Parameters
    ----------
    var1 : xarray.DataArray
        fld_s08i223 (soil moisture) with soil_model_level_number dimension.

    Returns
    -------
    xarray.DataArray
        Soil moisture with depth coordinate instead of soil_model_level_number.
    """

    # Soil depth mapping from model level numbers to depth values (meters)
    depths = {
        1: 0.0109999999403954,
        2: 0.0509999990463257,
        3: 0.157000005245209,
        4: 0.438499987125397,
        5: 1.18550002574921,
        6: 2.87199997901917,
    }

    # Use the variable directly
    result = var1

    # Transform soil levels to depths if soil_model_level_number dimension exists
    if "soil_model_level_number" in result.dims:
        # Get soil level coordinate/dimension
        if "soil_model_level_number" in result.coords:
            soil_levels = result["soil_model_level_number"]
            level_values = soil_levels.values
        else:
            # If it's just a dimension, assume sequential levels 1,2,3,etc.
            level_size = result.sizes["soil_model_level_number"]
            level_values = list(range(1, level_size + 1))

        # Create depth values array (lazy operation)
        depth_values = [depths.get(int(level), float("nan")) for level in level_values]

        # Transform the result to use depth coordinate
        result = (
            result.assign_coords({"depth": ("soil_model_level_number", depth_values)})
            .swap_dims({"soil_model_level_number": "depth"})
            .drop_vars(["soil_model_level_number"], errors="ignore")
        )

    return result


def calc_tsl(var1):
    """
    This function takes soil temperature (fld_s08i225) and transforms the
    soil_model_level_number coordinate to depth values.

    Parameters
    ----------
    var1 : xarray.DataArray
        fld_s08i225 (soil temperature) with soil_model_level_number dimension.

    Returns
    -------
    xarray.DataArray
        Soil temperature with depth coordinate instead of soil_model_level_number.
    """

    # Soil depth mapping from model level numbers to depth values (meters)
    depths = {
        1: 0.0109999999403954,
        2: 0.0509999990463257,
        3: 0.157000005245209,
        4: 0.438499987125397,
        5: 1.18550002574921,
        6: 2.87199997901917,
    }

    # Use the variable directly
    result = var1

    # Transform soil levels to depths if soil_model_level_number dimension exists
    if "soil_model_level_number" in result.dims:
        # Get soil level coordinate/dimension
        if "soil_model_level_number" in result.coords:
            soil_levels = result["soil_model_level_number"]
            level_values = soil_levels.values
        else:
            # If it's just a dimension, assume sequential levels 1,2,3,etc.
            level_size = result.sizes["soil_model_level_number"]
            level_values = list(range(1, level_size + 1))

        # Create depth values array (lazy operation)
        depth_values = [depths.get(int(level), float("nan")) for level in level_values]

        # Transform the result to use depth coordinate
        result = (
            result.assign_coords({"depth": ("soil_model_level_number", depth_values)})
            .swap_dims({"soil_model_level_number": "depth"})
            .drop_vars(["soil_model_level_number"], errors="ignore")
        )

    return result
