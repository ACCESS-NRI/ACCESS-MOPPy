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
# import numpy as np
# import xarray as xr
# from metpy.calc import height_to_geopotential
# from mopdb.utils import MopException
# from mopper.calc_utils import get_plev, rename_coord

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
## Aerosol Calculations
## ----------------------------------------------------------------------
#
#
# def optical_depth(var, lwave):
#    """
#    Calculates the optical depth.
#    First selects all variables at selected wavelength then sums them.
#
#    Parameters
#    ----------
#    var: DataArray
#        variable from Xarray dataset
#    lwave: int
#        level corresponding to desidered wavelength
#
#    Returns
#    -------
#    vout: DataArray
#        Optical depth
#
#    """
#    pseudo_level = var[0].dims[1]
#    var_list = [v.sel(pseudo_level=lwave) for v in var]
#    vout = sum_vars(var_list)
#    vout = vout.rename({pseudo_level: "pseudo_level"})
#    return vout
#
#
# @click.pass_context
# def calc_depositions(ctx, var, weight=None):
#    """Returns aerosol depositions
#
#    At the moment is assuming sea salt will need more work to be
#    adapted for other depositions.
#    Original variables are mol s-1 output is kg m-2 s-1, so we
#    multiply by molecular weight.
#    Sea salt is assumed to be NaCl: 0.05844 kg.mol-1
#    NB we are using only surface level as: "Dry deposition occurs
#    when aerosol bumps into something at surface level, so it doesn't
#    make sense for there to be data in the levels above"
#    (personal communication from M. Woodhouse)
#
#    Parameters
#    ----------
#    ctx : click context
#        Includes obj dict with 'cmor' settings, exp attributes
#    var : list(xarray.DataArray)
#        List of input variables to sum
#    weight: float
#        Weight of 1 mole, default is None and it uses NaCl weight (to be updated)
#
#    Returns
#    -------
#    varout : xarray.DataArray
#        Areosol depositions
#
#    """
#
#    # var_log = logging.getLogger(ctx.obj['var_log'])
#    varlist = []
#    for v in var:
#        v0 = v.sel(model_theta_level_number=1).squeeze(dim="model_theta_level_number")
#        varlist.append(v0)
#    if weight is None:
#        weight = 0.05844
#    deps = sum_vars(varlist) * weight
#    return deps


# Utilities
# ----------------------------------------------------------------------


def level_to_height(var, levs=None):
    """Returns model level variable with level height instead of
    number as dimension

    Parameters
    ----------
    var : Xarray DataArray
        Variable defined on model levels number
    levs : tuple(str,str)
        slice of levels to apply (optional, default is None)

    Returns
    -------
    vout : Xarray DataArray
        Same variable defined on model levels height

    """
    zdim = var.dims[1]
    zdim_height = zdim.replace("number", "height").replace("model_", "")
    var = var.swap_dims({zdim: zdim_height})
    if levs is not None:
        var = var.isel({zdim_height: slice(int(levs[0]), int(levs[1]))})
    return var
