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
#
# last updated 07/07/2023

'''
This script is a collection of functions that perform special calulations.

Everything here is from app_functions.py but has been modfied to work with Xarray data
more eficiently and optimized inn general; i.e. reduced number of For and If statements.

'''

import click
import xarray as xr
import os
import yaml
import json 
import numpy as np
#import cf
from scipy.interpolate import interp1d

# Global Variables
#----------------------------------------------------------------------
ancillary_path = os.environ.get('ANCILLARY_FILES', '')+'/'

ice_density = 900 #kg/m3
snow_density = 300 #kg/m3

rd = 287.0
cp = 1003.5
p_0 = 100000.0

R_e = 6.378E+06
#----------------------------------------------------------------------

# 
#----------------------------------------------------------------------
def read_yaml(fname):
    """
    """
    with open(fname, 'r') as yfile:
        data = yaml.safe_load(yfile)
    return data
#----------------------------------------------------------------------


# Modify data frequency
#----------------------------------------------------------------------
def time_resample(var, trange, tdim, sample='down', stats='mean'):
    """
    Resamples the input variable to the specified frequency.
    Resample is used with the options:
    origin =  'start_day'
    closed = 'right'
    This put the time label to the start of the interval and offset is applied
    to get a centered time label.

    Parameters
    ----------
    var : xarray.DataArray or xarray.Dataset
        Variable from Xarray dataset.
    range : str
        The frequency to which the data should be resampled. Valid inputs are 'mon' (monthly), 'day' (daily), or 'year' (yearly).
    tdim: str
        The name of the time dimension
    sample : str
        The type of resampling to perform. Valid inputs are 'up' for upsampling or 'down' for downsampling. (default down)
    stats : str
        The reducing function to follow resample: mean, min, max, sum. (default mean)

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

    if not isinstance(var, (xr.DataArray, xr.Dataset)):
        raise ValueError("The 'var' parameter must be a valid Xarray DataArray or Dataset.")


    valid_stats = ['mean', 'min', 'max', 'sum']
    if stats not in valid_stats:
        raise ValueError(f"The 'stats' parameter must be one of {valid_stats}.")

    offset = {'30m': [15, 'T'], 'H': [30, 'T'], '3H': [90, 'T'], '6H': [3, 'H'],
              '12H': [6, 'H'], 'D': [12, 'H'], '7D': [84, 'H'], '10D': [5, 'D'],
              'M': [15, 'D'], 'Y': [6, 'M'], '10Y': [5, 'Y']}

    if sample == 'down':
        try:
            vout = var.resample({tdim: trange}, origin='start_day',
                                closed='right')
            method = getattr(vout, stats)
            vout = method()
            half, tunit = offset[trange][:]
            vout = vout.assign_coords({tdim: xr.CFTimeIndex(vout[tdim].values).shift(half, tunit)})
    
        except Exception as e:
            print(f'{e}')

    elif sample == 'up':
        try:
            vout = var.resample({tdim: trange}).interpolate("linear")
    
        except Exception as e:
            print(f'{e}')

    else:
        raise Exception('sample is expected to be up or down')

    return vout
#----------------------------------------------------------------------


# Sea Ice calculations
#----------------------------------------------------------------------
class IceTransportCalculations():
    """
    Functions to calculate mass transports.

    Parameters
    ----------

    Returns
    -------
    transports : Xarray dataset
        mass transport array
    """

    def __init__(self, ancillary_path):
        self.yaml_data = read_yaml('transport_lines.yaml')['lines']

        self.gridfile = xr.open_dataset(f"{ancillary_path}/{self.yaml_data['gridfile']}")
        self.lines = self.yaml_data['sea_lines']
        self.ice_lines = self.yaml_data['ice_lines']

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

        """
        if xy == 'y':
            L = self.gridfile.hun / 100 #grid cell length in m (from cm)
        elif xy == 'x':
            L = self.gridfile.hue / 100 #grid cell length in m (from cm)
        else:
            raise Exception('need to supply value either \'x\' or \'y\' for ice Transports')
        
        return L
    
    def transAcrossLine(self, var, i_start, i_end, j_start, j_end):
        """
        Calculate the mass trasport across a line either i_start=i_end and 
        the line goes from j_start to j_end or j_start=j_end and the line goes 
        from i_start to i_end var is either the x or y mass transport depending 
        on the line


        Parameters
        ----------
        var : array
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
        transports : array

        """
        if 'yt_ocean' in var:
            y_ocean = 'yt_ocean'
            x_ocean = 'xu_ocean'
        else:
            y_ocean = 'yu_ocean'
            x_ocean = 'xt_ocean'

        if i_start==i_end or j_start==j_end:
            try:
                #sum each axis apart from time (3d)
                #trans = var.isel(yu_ocean=slice(271, 271+1), xt_ocean=slice(292, 300+1))
                trans = var[..., j_start:j_end+1, i_start:i_end+1].sum(dim=['st_ocean', f'{y_ocean}', f'{x_ocean}']) #4D
            except:
                trans = var[..., j_start:j_end+1, i_start:i_end+1].sum(dim=[f'{y_ocean}', f'{x_ocean}']) #3D
            
            return trans
        else: 
            raise Exception('ERROR: Transport across a line needs to be calculated for a single value of i or j')


    def lineTransports(self, tx_trans, ty_trans):
        """
        Calculates the mass transports across the ocn straits.


        Parameters
        ----------
        tx_trans : array
            variable extracted from Xarray dataset
        ty_trans: array
            variable extracted from Xarray dataset


        Returns
        -------
        trans : array

        """
        #initialise array
        transports = np.zeros([len(tx_trans.time),len(self.lines)])
        
        #0 barents opening
        transports[:,0] = self.transAcrossLine(ty_trans,292,300,271,271)
        transports[:,0] += self.transAcrossLine(tx_trans,300,300,260,271)

        #1 bering strait
        transports[:,1] = self.transAcrossLine(ty_trans,110,111,246,246)

        #2 canadian archipelago
        transports[:,2] = self.transAcrossLine(ty_trans,206,212,285,285)
        transports[:,2] += self.transAcrossLine(tx_trans,235,235,287,288)

        #3 denmark strait
        transports[:,3] = self.transAcrossLine(tx_trans,249,249,248,251)
        transports[:,3] += self.transAcrossLine(ty_trans,250,255,247,247)

        #4 drake passage
        transports[:,4] = self.transAcrossLine(tx_trans,212,212,32,49)

        #5 english channel 
        # Is unresolved by the access model

        #6 pacific equatorial undercurrent
        #specified down to 350m not the whole depth
        tx_trans_ma = tx_trans.where(tx_trans[:, 0:25, :] >= 0)
        transports[:,6] = self.transAcrossLine(tx_trans_ma,124,124,128,145)
        
        #7 faroe scotland channel    
        transports[:,7] = self.transAcrossLine(ty_trans,273,274,238,238)
        transports[:,7] += self.transAcrossLine(tx_trans,274,274,232,238)

        #8 florida bahamas strait
        transports[:,8] = self.transAcrossLine(ty_trans,200,205,192,192)

        #9 fram strait
        transports[:,9] = self.transAcrossLine(tx_trans,267,267,279,279)
        transports[:,9] += self.transAcrossLine(ty_trans,268,284,278,278)

        #10 iceland faroe channel
        transports[:,10] = self.transAcrossLine(ty_trans,266,268,243,243)
        transports[:,10] += self.transAcrossLine(tx_trans,268,268,240,243)
        transports[:,10] += self.transAcrossLine(ty_trans,269,272,239,239)
        transports[:,10] += self.transAcrossLine(tx_trans,272,272,239,239)

        #11 indonesian throughflow
        transports[:,11] = self.transAcrossLine(tx_trans,31,31,117,127)
        transports[:,11] += self.transAcrossLine(ty_trans,35,36,110,110)
        transports[:,11] += self.transAcrossLine(ty_trans,43,44,110,110)
        transports[:,11] += self.transAcrossLine(tx_trans,46,46,111,112)
        transports[:,11] += self.transAcrossLine(ty_trans,47,57,113,113)

        #12 mozambique channel    
        transports[:,12] = self.transAcrossLine(ty_trans,320,323,91,91)

        #13 taiwan luzon straits
        transports[:,13] = self.transAcrossLine(ty_trans,38,39,190,190)
        transports[:,13] += self.transAcrossLine(tx_trans,40,40,184,188)

        #14 windward passage
        transports[:,14] = self.transAcrossLine(ty_trans,205,206,185,185)
        
        return transports
    
    def iceTransport(self, ice_thickness, vel, xy):
        """
        Calculate ice mass transport.


        Parameters
        ----------
        ice_thickness : array
            variable extracted from Xarray dataset
        vel: array
            variable extracted from Xarray dataset
        xy: str
            variable extracted from Xarray dataset


        Returns
        -------
        ice_mass : array

        """
        L = self.gridfile(xy)
        ice_mass = ice_density * ice_thickness * vel * L

        return ice_mass

    def snowTransport(self, snow_thickness, vel, xy):
        """
        Calculate snow mass transport.


        Parameters
        ----------
        snow_thickness : array
            variable extracted from Xarray dataset
        vel: array
            variable extracted from Xarray dataset
        xy: str
            variable extracted from Xarray dataset


        Returns
        -------
        snow_mass : array

        """
        L = self.gridfile(xy)
        snow_mass = snow_density * snow_thickness * vel * L

        return snow_mass

    def iceareaTransport(self, ice_fraction, vel, xy):
        """
        Calculate ice area transport.


        Parameters
        ----------
        ice_fraction : array
            variable extracted from Xarray dataset
        vel: array
            variable extracted from Xarray dataset
        xy: str
            variable extracted from Xarray dataset


        Returns
        -------
        ice_area : array

        """
        L = self.gridfile(xy)
        ice_area = ice_fraction * vel * L

        return ice_area
    
    def fill_transports(self, tx_trans, ty_trans):
        """
        Calculates the mass transports across the ice straits.


        Parameters
        ----------
        tx_trans : array
            variable extracted from Xarray dataset
        ty_trans: array
            variable extracted from Xarray dataset


        Returns
        -------
        transports : array

        """
        transports = np.zeros([len(tx_trans.time),len(self.lines)])

        #0 fram strait
        transports[:,0] = self.transAcrossLine(tx_trans,267,267,279,279)
        transports[:,0] += self.transAcrossLine(ty_trans,268,284,278,278)

        #1 canadian archipelago
        transports[:,1] = self.transAcrossLine(ty_trans,206,212,285,285)
        transports[:,1] += self.transAcrossLine(tx_trans,235,235,287,288)

        #2 barents opening
        transports[:,2] = self.transAcrossLine(ty_trans,292,300,271,271)
        transports[:,2] += self.transAcrossLine(tx_trans,300,300,260,271)

        #3 bering strait
        transports[:,3] = self.transAcrossLine(ty_trans,110,111,246,246)

        return transports
    

    def icelineTransports(self, ice_thickness, velx, vely):
        """
        Calculates the ice mass transport across the straits


        Parameters
        ----------
        ice_thickness : array
            variable extracted from Xarray dataset
        velx : array
            variable extracted from Xarray dataset
        vely: array
            variable extracted from Xarray dataset


        Returns
        -------
        transports : array

        """
        
        tx_trans = self.iceTransport(ice_thickness,velx,'x').filled(0)
        ty_trans = self.iceTransport(ice_thickness,vely,'y').filled(0)
        transports = self.fill_transports(tx_trans, ty_trans)

        return transports


    def snowlineTransports(self, snow_thickness, velx, vely):
        """
        Calculates the Snow mass transport across the straits


        Parameters
        ----------
        snow_thickness : array
            variable extracted from Xarray dataset
        velx : array
            variable extracted from Xarray dataset
        vely: array
            variable extracted from Xarray dataset


        Returns
        -------
        transports : array

        """
        tx_trans = self.snowTransport(snow_thickness,velx,'x').filled(0)
        ty_trans = self.snowTransport(snow_thickness,vely,'y').filled(0)
        transports = self.fill_transports(tx_trans, ty_trans)

        return transports


    def icearealineTransports(self, ice_fraction, velx, vely):
        """
        Calculates the ice are transport across the straits


        Parameters
        ----------
        ice_fraction : array
            variable extracted from Xarray dataset
        velx : array
            variable extracted from Xarray dataset
        vely: array
            variable extracted from Xarray dataset


        Returns
        -------
        transports : array

        """
        tx_trans = self.iceareaTransport(ice_fraction,velx,'x').filled(0)
        ty_trans = self.iceareaTransport(ice_fraction,vely,'y').filled(0)
        transports = self.fill_transports(tx_trans, ty_trans)

        return transports
    

    def msftbarot(self, psiu, tx_trans):
        """
        Calculates the drake trans


        Parameters
        ----------
        psiu : array
            variable extracted from Xarray dataset
        tx_trans : array
            variable extracted from Xarray dataset


        Returns
        -------
        psiu : array

        """
        drake_trans=self.transAcrossLine(tx_trans,212,212,32,49)
        #loop over times
        for i,trans in enumerate(drake_trans):
            #offset psiu by the drake passage transport at that time
            psiu[i,:] = psiu[i,:]+trans
        return psiu


class SeaIceCalculations():
    """
    Functions to calculate mass transports.

    Parameters
    ----------

    Returns
    -------
    transports : Xarray dataset
        mass transport array
    """

    def __init__(self, ancillary_path):
        self.yaml_data = read_yaml('transport_lines.yaml')['lines']

        self.gridfile = xr.open_dataset(f"{ancillary_path}/{self.yaml_data['gridfile']}")
        self.lines = self.yaml_data['sea_lines']
        self.ice_lines = self.yaml_data['ice_lines']

    def __del__(self):
        self.gridfile.close()


class HemiSeaIce:    
    def __init__(self, var, tarea, lat):
        """Assign var, tarea, and lat to instance variables for later use.

        Parameters
        ----------
        var : Xarray dataset
            seaice variable
        tarea : Xarray dataset
            _description_
        lat : Xarray dataset
            _description_
        """     
        self.var = xr.where(var[0].mask, 0, var[0])
        self.tarea = tarea
        self.lat = lat
    
    def hemi_calc(self, hemi, var, nhlat, shlat):
        """Calculate hemisphere seaice fraction.

        Parameters
        ----------
        hemi : str
            hemisphere to calculate seaice fraction for

        Returns
        -------
        vout : Xarray dataset
            
        """

        if hemi.find('north') != -1:
            v = var.where(nhlat, drop=True)
        elif hemi.find('south') != -1:
            v = var.where(shlat, drop=True)
        vout = v.sum()

        return vout
        
    def calc_hemi_seaice_area_vol(self, hemi):
        """Calculate the hemi seaice area volume.

        Parameters
        ----------
        hemi : str
            Assigning the hemisphere to calculate, either 'north' or'south'.

        Returns
        -------
        vout : float
            seaice area volume
        """        
        nhlati = self.lat.where(self.lat >= 0., drop=True)
        shlati = self.lat.where(self.lat < 0., drop=True)
        var = self.var * self.tarea

        vout = self.hemi_calc(hemi, var, nhlati, shlati)

        return vout.item()

    def calc_hemi_seaice_extent(self, hemi):
        """Calculate the hemi seaice extents.

        Parameters
        ----------
        hemi : str
            Assigning the hemisphere to calculate, either 'north' or'south'.

        Returns
        -------
        vout : float
            seaice extents
        """
        nhlatiext = self.lat.where((self.var >= 0.15) & (self.var <= 1.) & (self.lat >= 0.), drop=True)
        shlatiext = self.lat.where((self.var >= 0.15) & (self.var <= 1.) & (self.lat < 0.), drop=True)

        vout = self.hemi_calc(hemi, self.tarea, nhlatiext, shlatiext)

        return vout.item()


def maskSeaIce(var, sic):
    """Mask seaice.

    Parameters
    ----------
    var : Xarray dataset
        seaice variable
    sic : Xarray dataset
        seaice fraction

    Returns
    -------
    vout : Xarray dataset
        masked seaice variable
    """
    vout = var.where(sic != 0)
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

    """
    vout = 1 - np.exp(-0.2 * 330 * sisnthick)
    vout = xr.where(np.isnan(vout), 0.0, vout)
    return vout

#----------------------------------------------------------------------


# Ocean Calculations
#----------------------------------------------------------------------
def optical_depth(lbplev, var):
    """
    Calculates the optical depth. First saves all variables at the 
    correct level into an array and then sums the contents together.

    Parameters
    ----------
    lbplev: int 
    var: array
        variable from Xarray dataset

    Returns
    -------
    vout: float
        Optical depth

    """
    # Note sure 'pseudo_level_0' is the correct name. 
    vars = [v.isel(pseudo_level_0=lbplev) for v in var]
    vout = sum(vars)

    return vout

def ocean_floor(var):
    """Not sure.. 

    Parameters
    ----------
    var : Xarray dataset
        pot_temp variable

    Returns
    -------
    vout : Xarray dataset
        ocean floor temperature?
    """
    lv = (~var.isnull()).sum(dim='st_ocean') - 1
    vout = var.take(lv, dim='st_ocean').squeeze()
    return vout

def calc_global_ave_ocean(var, rho_dzt, area_t):
    """Calculate global ocean mass transport.

    Parameters
    ----------
    var : Xarray dataset
        ocean variable
    rho_dzt : Xarray dataset
        density transport
    area_t : Xarray dataset
        area transport

    Returns
    -------
    vnew : Xarray dataset
        global ocean mass transport
    """
    mass = rho_dzt * area_t
    
    try:
        vnew = var.weighted(mass).mean(dim=('st_ocean', 'yt_ocean', 'xt_ocean'), skipna=True)
    except:
        vnew = var.weighted(mass[:, 0, :, :]).mean(dim=('x', 'y'), skipna=True)
    
    return vnew

#----------------------------------------------------------------------


# Unknown Calculations
#----------------------------------------------------------------------

def areacella(nlat):
    """
    Don't know

    Parameters
    ----------
    nlat: int 

    Returns
    -------
    vals: array
        Variable from xarray dataset

    """
    if nlat == 145:
        f = xr.open_dataset(f'{ancillary_path}esm_areacella.nc')
    elif nlat == 144:
        f = xr.open_dataset(f'{ancillary_path}cm2_areacella.nc')
    vals = f.areacella
    #f.close()
    return vals


def topsoil(var):
    """Calculate top soil moisture.

    Parameters
    ----------
    var : Xarray dataset
        fld_s08i223 variable

    Returns
    -------
    soil : Xarray dataset
        top soil moisture
    """
    soil = var.isel(depth=slice(3)).sum(dim=['depth']) * 0.012987
    return soil
#----------------------------------------------------------------------


# Soil Calculations
#----------------------------------------------------------------------
def topsoil_tsl(var):
    """Calculate top soil?

    Parameters
    ----------
    var : Xarray dataset

    Returns
    -------
    soil : Xarray dataset
        top soil
    """
    soil_tsl = var.isel(depth=slice(2)).sum(dim=['depth']) / 2.0
    return soil_tsl

#----------------------------------------------------------------------


# Pressure level Calculations
#----------------------------------------------------------------------
def plev19(levnum):
    """Read in pressure levels.

    Returns
    -------
    plev : numpy array
    plevb: numpy array
    """
    yaml_data = read_yaml('press_levs.yaml')[levels]

    plev = np.flip(np.array(yaml_data[levnum]))
    plevmin = np.array(yaml_data[levnum+'min'])
    plevmax = np.array(yaml_data[levnum+'max'])
    plevb = np.column_stack((plevmin,plevmax))

    return plev, plevb


@click.pass_context
def get_plev(ctx, levnum):
    """Read pressure levels from .._coordinate.json file

    Returns
    -------
    plev : numpy array
    """
    fpath = f"{ctx.obj['tables_path']}/{ctx.obj['_AXIS_ENTRY_FILE']}"
    with open(fpath, 'r') as jfile:
        data = json.load(jfile)
    axis_dict = data['axis_entry']

    plev = np.array(axis_dict[levnum]['requested'])
    plev = plev.astype(float)

    return plev


def pointwise_interp(pres, var, plev):
    """
    """
    vint = interp1d(pres, var, kind="linear",
        fill_value="extrapolate")
    return vint(plev)


@click.pass_context
def plevinterp(ctx, var, pmod, levnum):
    """Interpolating var from model levels to pressure levels

    _extended_summary_

    Parameters
    ----------
    var : Xarray DataArray 
        The variable to interpolate
    pmod : Xarray DataArray
        Air pressure on model levels
    levnum : str
        Name of the pressure levels to load. NB these need to be
        defined in the '_coordinates.yaml' file

    Returns
    -------
    interp : Xarray DataArray
        The variable interpolated on pressure levels
    """

    var_log = ctx.obj['var_log']
    plev = get_plev(levnum)
    lev = var.dims[1]
    var_log.debug(lev)
    var_log.debug(var)
    var_log.debug(pmod)
    interp = xr.apply_ufunc(
        pointwise_interp,
        pmod,
        var,
        plev,
        input_core_dims=[ [lev],[lev], ["plev"]],
        output_core_dims=[ ["plev"] ],
        exclude_dims=set((lev,)),
        vectorize=True,
        dask="parallelized",
        output_dtypes=['float32'],
        keep_attrs=True
    )
    interp['plev'] = plev
    interp['plev'] = interp['plev'].assign_attrs({'units': "Pa",
        'axis': "Z", 'standard_name': "air_pressure",
        'positive': "down"})
    dims = list(var.dims)
    dims[1] = 'plev'
    interp = interp.transpose(*dims)
    return interp


def plevinterp2(var, pmod, heavy=None):
    """Interpolating var from model levels to plev19

    _extended_summary_

    Parameters
    ----------
    var : Xarray DataArray 
    pmod : Xarray DataArray
    heavy : Xarray DataArray

    Returns
    -------
    vout : Xarray dataset
    """    
    plev, bounds = plev19()

    if heavy is not None:
        t, z, x, y = var.shape
        th, zh, xh, yh = heavy.shape
        if xh != x:
            print('heavyside not on same grid as variable; interpolating...')
            hout = heavy.interp(lat_v=heavy.lat_v, method='linear',
                                kwargs={'fill_value': 'extrapolate'})
        else:
            hout = heavy

        hout = np.where(hout > 0.5, 1, 0)

    interp_var = var.interp_like(pmod, method='linear', kwargs={'fill_value': 'extrapolate'})
    vout = interp_var.interp(plev=plev)
    if heavy is not None:
        vout = vout/hout
    return vout

#----------------------------------------------------------------------


# Temperature Calculations
#----------------------------------------------------------------------
def tos_degC(var):
    """Covert temperature from K to degC.

    Parameters
    ----------
    var : Xarray dataset

    Returns
    -------
    vout : Xarray dataset
    """    

    if var.units == 'K':
        print('temp in K, converting to degC')
        vout = var - 273.15
    return vout

def tos_3hr(var, landfrac):
    """notes

    Parameters
    ----------
    var : Xarray dataset

    Returns
    -------
    vout : Xarray dataset
    """    

    v = tos_degC(var)

    vout = xr.zeros_like(var)
    t = len(var.time)

    for i in range(t):
         vout[i,:,:] = var[i,:,:].where(landfrac[i,:,:] != 1)
    return vout
#----------------------------------------------------------------------


# Land Calculations
#----------------------------------------------------------------------
def landFrac(var, landfrac):
    """Retrieve the land fraction variable.

    Parameters
    ----------
    var : Xarray dataset
    nlat : str
        Latitude dimension size

    Returns
    -------
    vout : Xarray dataset
        land fraction array
    """    

    try:
        vout = landfrac
    except:
        if var.lat.shape[0] == 145:
            f = xr.open_dataset(f'{ancillary_path}esm_landfrac.nc')
        elif var.lat.shape[0] == 144:
            f = xr.open_dataset(f'{ancillary_path}cm2_landfrac.nc')
        else:
            print('nlat needs to be 145 or 144.')
        vout = f.fld_s03i395 

    return vout

def tileFracExtract(tileFrac, landfrac, tilenum):
    """Calculations the land fraction of a specific type.
        i.e. crops, grass, wetland, etc.

    Parameters
    ----------
    tileFrac : Xarray dataset
    landfrac : Xarray dataset
    tilenum : Int

    Returns
    -------
    vout : Xarray dataset
        land fraction of object

    Raises
    ------
    Exception
        tile number must be an integer or list
    """    
    
    vout = xr.zeros_like(tileFrac[:, 0, :, :])

    if isinstance(tilenum, int):
        vout += tileFrac.loc[dict(pseudo_level_1=tilenum)]
    elif isinstance(tilenum, list):
        for t in tilenum:
            vout += tileFrac.loc[dict(pseudo_level_1=tilenum)]
    else:
        raise Exception('E: tile number must be an integer or list')
    
    vout = vout * landFrac(vout, landfrac)

    return vout

def fracLut(var, landfrac, nwd):    
    #nwd (non-woody vegetation only) - tiles 6,7,9,11 only
    vout = xr.zeros_like(var[:, :4, :, :])

    # Define the tile indices based on 'nwd' value
    if nwd == 0:
        tile_indices = [1, 2, 3, 4, 5, 6, 7, 11, 14]
    elif nwd == 1:
        tile_indices = [6, 7, 11]

    # Iterate over the tile indices and update 'vout'
    # .loc allows you to modify the original array in-place, based on label and index respectively. So when you use .loc, the changes are applied to the original vout1 DataArray.
    # The sel() operation doesn't work in-place, it returns a new DataArray that is a subset of the original DataArray.
    for t in tile_indices:
        vout.loc[dict(pseudo_level_1=1)] += var.loc[dict(pseudo_level_1=t)]

    # Crop tile 9
    vout.loc[dict(pseudo_level_1=3)] = var.loc[dict(pseudo_level_1=9)]

    # Update urban tile based on 'nwd'
    if nwd == 0:
        vout.loc[dict(pseudo_level_1=4)] = var.loc[dict(pseudo_level_1=15)]

    landfrac = landFrac(vout, landfrac)
    vout.loc[dict(pseudo_level_1=1)] = vout.loc[dict(pseudo_level_1=1)] * landfrac
    vout.loc[dict(pseudo_level_1=2)] = vout.loc[dict(pseudo_level_1=2)] * landfrac
    vout.loc[dict(pseudo_level_1=3)] = vout.loc[dict(pseudo_level_1=3)] * landfrac
    vout.loc[dict(pseudo_level_1=4)] = vout.loc[dict(pseudo_level_1=4)] * landfrac

    return vout

def tileFraci317():
    """Opens up the base cm2_tilefrac.nc file from the ancillary_path and
        saves the tile_farc variable (fld_s03i317) as an xarray dataset.

    Returns
    -------
    vals : Xarray dataset
        tile_frac variable
    """    
    f = xr.open_dataset(f'{ancillary_path}cm2_tilefrac.nc')
    vals = f.fld_s03i317
    return vals

def tileAve(var, tileFrac, landfrac, lfrac=1):
    """tileAve _summary_

    Parameters
    ----------
    var : _type_
        _description_
    tileFrac : _type_
        _description_
    landfrac : _type_
        _description_
    lfrac : int, optional
        _description_, by default 1

    Returns
    -------
    vout : Xarray dataset
        _description_
    """    
    vout = xr.zeros_like(tileFrac[:, :, :, :])
    
    if tileFrac == '317':
        tileFrac=tileFraci317()
        #loop over pft tiles and sum
        for k in var.time.values:
            for i in var.pseudo_level_1.values:
                vout.loc[dict(pseudo_level_1=i, time=k)] += var.loc[dict(pseudo_level_1=i, time=k)] * tileFrac.loc[dict(pseudo_level_1=i, time=tileFrac.time.values[0])]
    else:
        #loop over pft tiles and sum
        for i in var.pseudo_level_1.values:
            vout.loc[dict(pseudo_level_1=i)] += var.loc[dict(pseudo_level_1=i)] * tileFrac.loc[dict(pseudo_level_1=i)]
            
    if lfrac == 1:
        vout = vout * landfrac

    return vout
#----------------------------------------------------------------------


# More Calculations
#----------------------------------------------------------------------
