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
# This file contains a collection of functions to calculate seaice derived variables
# from ACCESS model output.
# Initial functions' definitions were based on APP4 modified to work with Xarray.
#
# To propose new calculations and/or update to existing ones see documentation:
#
# and open a new issue on github.


from importlib.resources import files as import_files

import click
import numpy as np
import xarray as xr
from mopdb.utils import MopException, read_yaml

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


class IceTransportCalculations:
    """
    Functions to calculate mass transports.

    Parameters
    ----------

    Returns
    -------
    transports : Xarray DataArray
        mass transport array

    :meta private:
    """

    @click.pass_context
    def __init__(self, ctx):
        fname = import_files("mopdata").joinpath("transport_lines.yaml")
        self.yaml_data = read_yaml(fname)["lines"]

        self.gridfile = xr.open_dataset(
            f"{ctx.obj['ancils_path']}/" + f"{ctx.obj['grid_ice']}"
        )
        self.lines = self.yaml_data["sea_lines"]
        self.ice_lines = self.yaml_data["ice_lines"]

    def __del__(self):
        self.gridfile.close()

    def get_grid_cell_length(self, xy):
        """
         Select the hun or hue variable from the opened gridfile depending on whether
         x or y are passed in.


         Parameters
         ----------
         xy : string
             axis name

         Returns
         -------
         L : Xarray dataset
             hun or hue variable

        :meta private:
        """
        if xy == "y":
            L = self.gridfile.hun / 100  # grid cell length in m (from cm)
        elif xy == "x":
            L = self.gridfile.hue / 100  # grid cell length in m (from cm)
        else:
            raise Exception("""Need to supply value either 'x' or 'y'
                            for ice Transports""")

        return L

    def transAcrossLine(self, var, i_start, i_end, j_start, j_end):
        """Calculates the mass trasport across a line either
         i_start=i_end and the line goes from j_start to j_end or
         j_start=j_end and the line goes from i_start to i_end.
         var is either the x or y mass transport depending on the line.


         Parameters
         ----------
         var : DataArray
             variable extracted from Xarray dataset
         i_start: int
             xt_ocean axis position
         i_end: int
             xt_ocean axis position
         j_start: int
             yu_ocean axis position
         j_end: int
             yu_ocean axis position


         Returns
         -------
         transports : DataArray

        :meta private:
        """
        # PP is it possible to generalise this? as I'm sure I have to do the same in main
        # code to work out correct coordinates
        if "yt_ocean" in var:
            y_ocean = "yt_ocean"
            x_ocean = "xu_ocean"
        else:
            y_ocean = "yu_ocean"
            x_ocean = "xt_ocean"

        # could we try to make this a sel lat lon values?
        if i_start == i_end or j_start == j_end:
            try:
                # sum each axis apart from time (3d)
                # trans = var.isel(yu_ocean=slice(271, 271+1), xt_ocean=slice(292, 300+1))
                trans = var[..., j_start : j_end + 1, i_start : i_end + 1].sum(
                    dim=["st_ocean", f"{y_ocean}", f"{x_ocean}"]
                )  # 4D
            except Exception as e:
                trans = var[..., j_start : j_end + 1, i_start : i_end + 1].sum(
                    dim=[f"{y_ocean}", f"{x_ocean}"]
                )  # 3D

            return trans
        else:
            raise Exception("""ERROR: Transport across a line needs to
                be calculated for a single value of i or j""")

    def lineTransports(self, tx_trans, ty_trans):
        """
         Calculates the mass transports across the ocn straits.


         Parameters
         ----------
         tx_trans : DataArray
             variable extracted from Xarray dataset
         ty_trans: DataArray
             variable extracted from Xarray dataset


         Returns
         -------
         trans : Datarray

        :meta private:
        """
        # PP these are all hardcoded need to change this to be dependent on grid!!!
        # initialise array
        transports = np.zeros([len(tx_trans.time), len(self.lines)])

        # 0 barents opening
        transports[:, 0] = self.transAcrossLine(ty_trans, 292, 300, 271, 271)
        transports[:, 0] += self.transAcrossLine(tx_trans, 300, 300, 260, 271)

        # 1 bering strait
        transports[:, 1] = self.transAcrossLine(ty_trans, 110, 111, 246, 246)

        # 2 canadian archipelago
        transports[:, 2] = self.transAcrossLine(ty_trans, 206, 212, 285, 285)
        transports[:, 2] += self.transAcrossLine(tx_trans, 235, 235, 287, 288)

        # 3 denmark strait
        transports[:, 3] = self.transAcrossLine(tx_trans, 249, 249, 248, 251)
        transports[:, 3] += self.transAcrossLine(ty_trans, 250, 255, 247, 247)

        # 4 drake passage
        transports[:, 4] = self.transAcrossLine(tx_trans, 212, 212, 32, 49)

        # 5 english channel
        # Is unresolved by the access model

        # 6 pacific equatorial undercurrent
        # specified down to 350m not the whole depth
        tx_trans_ma = tx_trans.where(tx_trans[:, 0:25, :] >= 0)
        transports[:, 6] = self.transAcrossLine(tx_trans_ma, 124, 124, 128, 145)

        # 7 faroe scotland channel
        transports[:, 7] = self.transAcrossLine(ty_trans, 273, 274, 238, 238)
        transports[:, 7] += self.transAcrossLine(tx_trans, 274, 274, 232, 238)

        # 8 florida bahamas strait
        transports[:, 8] = self.transAcrossLine(ty_trans, 200, 205, 192, 192)

        # 9 fram strait
        transports[:, 9] = self.transAcrossLine(tx_trans, 267, 267, 279, 279)
        transports[:, 9] += self.transAcrossLine(ty_trans, 268, 284, 278, 278)

        # 10 iceland faroe channel
        transports[:, 10] = self.transAcrossLine(ty_trans, 266, 268, 243, 243)
        transports[:, 10] += self.transAcrossLine(tx_trans, 268, 268, 240, 243)
        transports[:, 10] += self.transAcrossLine(ty_trans, 269, 272, 239, 239)
        transports[:, 10] += self.transAcrossLine(tx_trans, 272, 272, 239, 239)

        # 11 indonesian throughflow
        transports[:, 11] = self.transAcrossLine(tx_trans, 31, 31, 117, 127)
        transports[:, 11] += self.transAcrossLine(ty_trans, 35, 36, 110, 110)
        transports[:, 11] += self.transAcrossLine(ty_trans, 43, 44, 110, 110)
        transports[:, 11] += self.transAcrossLine(tx_trans, 46, 46, 111, 112)
        transports[:, 11] += self.transAcrossLine(ty_trans, 47, 57, 113, 113)

        # 12 mozambique channel
        transports[:, 12] = self.transAcrossLine(ty_trans, 320, 323, 91, 91)

        # 13 taiwan luzon straits
        transports[:, 13] = self.transAcrossLine(ty_trans, 38, 39, 190, 190)
        transports[:, 13] += self.transAcrossLine(tx_trans, 40, 40, 184, 188)

        # 14 windward passage
        transports[:, 14] = self.transAcrossLine(ty_trans, 205, 206, 185, 185)

        return transports

    def iceTransport(self, ice_thickness, vel, xy):
        """
        Calculate ice mass transport.


        Parameters
        ----------
        ice_thickness : DataArray
            variable extracted from Xarray dataset
        vel: DataArray
            variable extracted from Xarray dataset
        xy: str
            variable extracted from Xarray dataset


        Returns
        -------
        ice_mass : DataArray


        :meta private:
        """
        L = self.gridfile(xy)
        ice_mass = ice_density * ice_thickness * vel * L

        return ice_mass

    def snowTransport(self, snow_thickness, vel, xy):
        """
        Calculate snow mass transport.


        Parameters
        ----------
        snow_thickness : DataArray
            variable extracted from Xarray dataset
        vel: DataArray
            variable extracted from Xarray dataset
        xy: str
            variable extracted from Xarray dataset


        Returns
        -------
        snow_mass : DataArray

        :meta private:
        """
        L = self.gridfile(xy)
        snow_mass = snow_density * snow_thickness * vel * L

        return snow_mass

    def iceareaTransport(self, ice_fraction, vel, xy):
        """
        Calculate ice area transport.


        Parameters
        ----------
        ice_fraction : DataArray
            variable extracted from Xarray dataset
        vel: DataArray
            variable extracted from Xarray dataset
        xy: str
            variable extracted from Xarray dataset


        Returns
        -------
        ice_area : DataArray

        :meta private:
        """
        L = self.gridfile(xy)
        ice_area = ice_fraction * vel * L

        return ice_area

    def fill_transports(self, tx_trans, ty_trans):
        """
        Calculates the mass transports across the ice straits.


        Parameters
        ----------
        tx_trans : DataArray
            variable extracted from Xarray dataset
        ty_trans: DataArray
            variable extracted from Xarray dataset


        Returns
        -------
        transports : DataArray

        :meta private:
        """
        transports = np.zeros([len(tx_trans.time), len(self.lines)])

        # PP these are all hardcoded need to change this to be dependent on grid!!!
        # 0 fram strait
        transports[:, 0] = self.transAcrossLine(tx_trans, 267, 267, 279, 279)
        transports[:, 0] += self.transAcrossLine(ty_trans, 268, 284, 278, 278)

        # 1 canadian archipelago
        transports[:, 1] = self.transAcrossLine(ty_trans, 206, 212, 285, 285)
        transports[:, 1] += self.transAcrossLine(tx_trans, 235, 235, 287, 288)

        # 2 barents opening
        transports[:, 2] = self.transAcrossLine(ty_trans, 292, 300, 271, 271)
        transports[:, 2] += self.transAcrossLine(tx_trans, 300, 300, 260, 271)

        # 3 bering strait
        transports[:, 3] = self.transAcrossLine(ty_trans, 110, 111, 246, 246)

        return transports

    def icelineTransports(self, ice_thickness, velx, vely):
        """
        Calculates the ice mass transport across the straits


        Parameters
        ----------
        ice_thickness : DataArray
            variable extracted from Xarray dataset
        velx : DataArray
            variable extracted from Xarray dataset
        vely: DataArray
            variable extracted from Xarray dataset


        Returns
        -------
        transports : DataArray

        :meta private:
        """

        tx_trans = self.iceTransport(ice_thickness, velx, "x").fillna(0)
        ty_trans = self.iceTransport(ice_thickness, vely, "y").fillna(0)
        transports = self.fill_transports(tx_trans, ty_trans)

        return transports

    def snowlineTransports(self, snow_thickness, velx, vely):
        """
        Calculates the Snow mass transport across the straits


        Parameters
        ----------
        snow_thickness : DataArray
            variable extracted from Xarray dataset
        velx : DataArray
            variable extracted from Xarray dataset
        vely: DataArray
            variable extracted from Xarray dataset


        Returns
        -------
        transports : DataArray

        :meta private:
        """
        tx_trans = self.snowTransport(snow_thickness, velx, "x").fillna(0)
        ty_trans = self.snowTransport(snow_thickness, vely, "y").fillna(0)
        transports = self.fill_transports(tx_trans, ty_trans)

        return transports

    def icearealineTransports(self, ice_fraction, velx, vely):
        """
        Calculates the ice are transport across the straits


        Parameters
        ----------
        ice_fraction : DataArray
            variable extracted from Xarray dataset
        velx : DataArray
            variable extracted from Xarray dataset
        vely: DataArray
            variable extracted from Xarray dataset


        Returns
        -------
        transports : DataArray

        :meta private:
        """
        tx_trans = self.iceareaTransport(ice_fraction, velx, "x").fillna(0)
        ty_trans = self.iceareaTransport(ice_fraction, vely, "y").fillna(0)
        transports = self.fill_transports(tx_trans, ty_trans)

        return transports

    def msftbarot(self, psiu, tx_trans):
        """
        Calculates the drake trans


        Parameters
        ----------
        psiu : DataArray
            variable extracted from Xarray dataset
        tx_trans : DataArray
            variable extracted from Xarray dataset


        Returns
        -------
        psiu : DataArray

        :meta private:
        """
        # PP these are all hardcoded need to change this to be dependent on grid!!!
        drake_trans = self.transAcrossLine(tx_trans, 212, 212, 32, 49)
        # loop over times
        for i, trans in enumerate(drake_trans):
            # offset psiu by the drake passage transport at that time
            psiu[i, :] = psiu[i, :] + trans
        return psiu


class SeaIceCalculations:
    """
    Functions to calculate mass transports.

    Parameters
    ----------

    Returns
    -------
    transports : Xarray DataArray
        mass transport array

    :meta private:
    """

    @click.pass_context
    def __init__(self, ctx):
        fname = import_files("mopdata").joinpath("transport_lines.yaml")
        self.yaml_data = read_yaml(fname)["lines"]

        self.gridfile = xr.open_dataset(
            f"{ctx.obj['ancil_path']}/" + f"{ctx.obj['grid_ice']}"
        )
        self.lines = self.yaml_data["sea_lines"]
        self.ice_lines = self.yaml_data["ice_lines"]

    def __del__(self):
        self.gridfile.close()


def calc_hemi_seaice(invar, carea, hemi, extent=False):
    """Calculate seaice properties (volume, area and extent) over
    hemisphere.

    Parameters
    ----------
    invar : Xarray DataArray
        Variable to process, either fraction (aice) or volume (hi)
    carea : Xarray DataArray
        Grid cell area
    hemi : str
            Assigning the hemisphere to calculate, either 'north' or'south'.
    extent : bool
        True if calculaitng extent: tarea as extent and var (aice) as filter.

    Returns
    -------
    vout : xarray DataArray
        Sum of property over selected hemisphere

    """
    vlat = invar.dims[1]
    # if calculating extent sum carea and aice is used as filter
    # with volume and area invar is multiplied by carea first
    if extent:
        var = tarea.where(invar <= 1.0 and invar >= 0.15, drop=True)
    else:
        var = invar * tarea
    if hemi == "north":
        var = var.sel(vlat >= 0.0)
    elif hemi == "south":
        var = var.sel(vlat < 0.0)
    else:
        mop_log.error(f"invalid hemisphere: {hemi}")
        raise MopException(f"invalid hemisphere: {hemi}")
    vout = var.sum()
    return vout


def maskSeaIce(var, aice):
    """Mask seaice.

    Parameters
    ----------
    var : Xarray dataset
        seaice variable
    aice : Xarray dataset
        seaice fraction

    Returns
    -------
    vout : Xarray dataset
        masked seaice variable

    """
    vout = var.where(aice != 0)
    return vout


def sithick(hi, aice):
    """Calculate seaice thickness.

    Parameters
    ----------
    hi : Xarray dataset
        seaice thickness
    aice : Xarray dataset
        seaice fraction

    Returns
    -------
    vout : Xarray dataset
        seaice thickness

    :meta private:
    """
    aice = aice.where(aice > 1e-3, drop=True)
    vout = hi / aice
    return vout


def sisnconc(sisnthick):
    """Calculate seas ice?

    Parameters
    ----------
    sisnthick : Xarray dataset

    Returns
    -------
    vout : Xarray dataset

    :meta private:
    """
    vout = 1 - np.exp(-0.2 * 330 * sisnthick)
    return vout
