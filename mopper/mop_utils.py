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
# last updated 07/07/2023
'''
Changes to script

17/03/23:
SG - Updated print statements and exceptions to work with python3.
SG- Added spaces and formatted script to read better.

20/03/23:
SG - Changed cdms2 to Xarray.

21/03/23:
PP - Changed cdtime to datetime. NB this is likely a bad way of doing this, but I need to remove cdtime to do further testing
PP - datetime assumes Gregorian calendar
'''

import numpy as np
import glob
import re
import os,sys
import stat
import xarray as xr
import cmor
import calendar
import click
import logging
import cftime
import itertools
import copy
from functools import partial

from mopper.calculations import *


def config_log(debug, path):
    """Configure log file for main process and errors from variable processes"""
    # start a logger first otherwise settings also apply to root logger
    logger = logging.getLogger('mop_log')
    # set the level for the logger, has to be logging.LEVEL not a string
    # until we do so applog doesn't have a level and inherits the root logger level:WARNING
    stream_level = logging.WARNING
    if debug is True:
        level = logging.DEBUG
    else:
        level = logging.INFO
    # set main logger level
    logger.setLevel(level)
    # disable any root handlers
    #for handler in logging.root.handlers[:]:
    #    logging.root.removeHandler(handler)
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")

    # add a handler to send WARNING level messages to console
    clog = logging.StreamHandler()
    clog.setLevel(stream_level)
    logger.addHandler(clog)

    # add a handler to send INFO level messages to file
    # the messagges will be appended to the same file
    logname = f"{path}/mopper_log.txt"
    flog = logging.FileHandler(logname)
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except OSError:
        pass
    flog.setLevel(level)
    flog.setFormatter(formatter)
    logger.addHandler(flog)
    # return the logger object
    return logger


def config_varlog(debug, logname):
    """Configure varlog file: use this for specific var information"""
    logger = logging.getLogger('var_log')
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    if debug is True:
        level = logging.DEBUG
    else:
        level = logging.INFO
    # set main logger level
    logger.setLevel(level)
    flog = logging.FileHandler(logname)
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except OSError:
        pass
    flog.setLevel(level)
    flog.setFormatter(formatter)
    logger.addHandler(flog)
    return logger


def _preselect(ds, varlist):
    varsel = [v for v in varlist if v in ds.variables]
    coords = ds[varsel].coords
    bnds = ['bnds', 'bounds', 'edges']
    pot_bnds = [f"{x[0]}_{x[1]}" for x in itertools.product(coords, bnds)]
    varsel.extend( [v for v in ds.variables if v in pot_bnds] )
    return ds[varsel]



@click.pass_context
def get_files(ctx, var_log):
    """Returns all files in time range
    First identifies all files with pattern/s defined for invars
    Then retrieve time dimension and if multiple time axis are present
    Finally filter only files in time range based on file timestamp (faster)
    If this fails or multiple time axis are present reads first and
    last timestep from each file
    """
    # Returns file list for each input var and list of vars for each file pattern
    all_files, path_vars = find_all_files(var_log)

    # PP FUNCTION END return all_files, extra_files
    var_log.debug(f"access files from: {os.path.basename(all_files[0][0])}" +
                 f"to {os.path.basename(all_files[0][-1])}")
    ds = xr.open_dataset(all_files[0][0], decode_times=False)
    time_dim, multiple_times = get_time_dim(ds, var_log)
    del ds
    try:
        inrange_files = []
        for i,paths in enumerate(all_files):
            if multiple_times is True:
                inrange_files.append( check_in_range(paths, time_dim, var_log) )
            else:
                inrange_files.append( check_timestamp(paths, var_log) )
    except:
        for i,paths in enumerate(all_files):
            inrange_files.append( check_in_range(paths, time_dim, var_log) )

    for i,paths in enumerate(inrange_files):
        if paths == []:
            mop_log.error(f"no data in requested time range for: {ctx.obj['filename']}")
            var_log.error(f"no data in requested time range for: {ctx.obj['filename']}")
    return inrange_files, path_vars, time_dim


@click.pass_context
def find_all_files(ctx, var_log):
    """Find all the ACCESS file names which match the pattern/s associated with invars.
    Sort the filenames, assuming that the sorted filenames will
    be in chronological order because there is usually some sort of date
    and/or time information in the filename.
    Check that all variables needed are in file, otherwise add extra file pattern
    """
    var_log.debug(f"input file structure: {ctx.obj['infile']}")
    patterns = ctx.obj['infile'].split()
    #set normal set of files
    files = []
    for i,p in enumerate(patterns):
        files.append(glob.glob(p))
        files[i].sort()
        if len(files[i]) == 0:
            var_log.warning(f"""Could not find files for pattern {p}.
                Make sure path correct and project storage flag included""")
    # if there is more than one variable: make sure all vars are in
    # one of the file pattern and couple them
    missing = copy.deepcopy(ctx.obj['vin'])
    i = 0
    path_vars = {}
    while len(missing) > 0 and i < len(patterns):
        path_vars[i] = []
        f = files[i][0]
        missing, found = check_vars_in_file(missing, f, var_log)
        if len(found) > 0:
            for v in found:
                path_vars[i].append(v)
        i+=1
    # if we couldn't find a variable check other files in same directory
    if len(missing) > 0:
        var_log.error(f"Input vars: {missing} not in files {ctx.obj['infile']}")
    return files, path_vars 


@click.pass_context
def check_vars_in_file(ctx, invars, fname, var_log):
    """Check that all variables needed for calculation are in file
    else return extra filenames
    """
    ds = xr.open_dataset(fname, decode_times=False)
    tofind = [v for v in invars if v not in ds.variables]
    found = [v for v in invars if v not in tofind]
    return tofind, found


@click.pass_context
def get_time_dim(ctx, ds, var_log):
    """Find time info: time axis, reference time and set tstart and tend
       also return mutlitple_times True if more than one time axis
    """
    time_dim = None
    multiple_times = False
    varname = [ctx.obj['vin'][0]]
    #    
    var_log.debug(f" check time var dims: {ds[varname].dims}")
    for var_dim in ds[varname].dims:
        axis = ds[var_dim].attrs.get('axis', '')
        if 'time' in var_dim or axis == 'T':
            time_dim = var_dim
            #units = ds[var_dim].units
            var_log.debug(f"first attempt to tdim: {time_dim}")
    
    var_log.debug(f"time var is: {time_dim}")
    # check if files contain more than 1 time dim
    tdims = [ x for x in ds.dims if 'time' in x or 
              ds[x].attrs.get('axis', '')  == 'T']
    if len(tdims) > 1:
        multiple_times = True
    del ds 
    return time_dim, multiple_times


@click.pass_context
def check_timestamp(ctx, all_files, var_log):
    """This function tries to guess the time coverage of a file based on its timestamp
       and return the files in range. At the moment it does a lot of checks based on the realm and real examples
       eventually it would make sense to make sure all files generated are consistent in naming
    """
    inrange_files = []
    realm = ctx.obj['realm']
    var_log.info("checking files timestamp ...")
    tstart = ctx.obj['sel_start']
    tend = ctx.obj['sel_end']
    var_log.debug(f"tstart, tend: {tstart}, {tend}")
    #if we are using a time invariant parameter, just use a file with vin
    if 'fx' in ctx.obj['frequency']:
        inrange_files = [all_files[0]]
    else:
        for infile in all_files:
            inf = infile.replace('.','_')
            inf = inf.replace('-','_')
            dummy = inf.split("_")
            if realm == 'ocean':
                tstamp = dummy[-1]
            elif realm == 'ice':
                tstamp = ''.join(dummy[-3:-2])
            else:
                tstamp = dummy[-3]
            # usually atm files are xxx.code_date_frequency.nc
            # sometimes there's no separator between code and date
            # 1 make all separator _ so xxx_code_date_freq_nc
            # then analyse date to check if is only date or codedate
            # check if timestamp as the date time separator T
            hhmm = ''
            if 'T' in tstamp:
                tstamp, hhmm = tstamp.split('T')
            # if tstamp start with number assume is date
            if not tstamp[0].isdigit():
                tstamp = re.sub("\\D", "", tstamp)
                tlen = len(tstamp)
                if tlen >= 8:
                    tstamp = tstamp[-8:]
                elif 6 <= tlen < 8:
                    tstamp = tstamp[-6:]
                elif 4 <= tlen < 6:
                    tstamp = tstamp[-4:]
            tlen = len(tstamp)
            if tlen != 8:
                if tlen in [3, 5, 7] :
                    #assume year is yyy
                    tstamp += '0'
                if len(tstamp) == 4:
                    tstart = tstart[:4]
                    tend = tend[:4]
                elif len(tstamp) == 6:
                    tstart = tstart[:6]
                    tend = tend[:6]
            else:
            # if hhmm were present add them back to tstamp otherwise as 0000 
            #tstamp = tstamp + hhmm.ljust(4,'0')
                tstamp = tstamp + hhmm
                if len(tstamp) == 8:
                    tstart = tstart[:8]
                    tend = tend[:8]
            var_log.debug(f"tstamp for {inf}: {tstamp}")
            var_log.debug(f"tstart, tend {tstart}, {tend}")
            if tstart <= tstamp <= tend:
                inrange_files.append(infile)
    return inrange_files

 
@click.pass_context
def check_in_range(ctx, all_files, tdim, var_log):
    """Return a list of files in time range
       Open each file and check based on time axis
       Use this function only if check_timestamp fails
    """
    inrange_files = []
    var_log.info("loading files...")
    var_log.debug(f"time dimension: {tdim}")
    tstart = ctx.obj['tstart'].replace('T','')
    tend = ctx.obj['tend'].replace('T','')
    if 'fx' in ctx.obj['table']:
        inrange_files = [all_files[0]]
    else:
        for input_file in all_files:
            try:
                ds = xr.open_dataset(input_file, use_cftime=True)
                # get first and last values as date string
                tmin = ds[tdim][0].dt.strftime('%4Y%m%d%H%M')
                tmax = ds[tdim][-1].dt.strftime('%4Y%m%d%H%M')
                var_log.debug(f"tmax from time dim: {tmax}")
                var_log.debug(f"tend from opts: {tend}")
                if not(tmin > tend or tmax < tstart):
                    inrange_files.append(input_file)
                del ds
            except Exception as e:
                var_log.error(f"Cannot open file: {e}")
    var_log.debug(f"Number of files in time range: {len(inrange_files)}")
    var_log.info("Found all the files...")
    return inrange_files


@click.pass_context
def load_data(ctx, inrange_files, path_vars, time_dim, var_log):
    """Returns a dictionary of input var: xarray dataset
    """
    # preprocessing to select only variables we need to avoid
    # concatenation issues with multiple coordinates
    input_ds = {}
    for i, paths in enumerate(inrange_files):
        preselect = partial(_preselect, varlist=path_vars[i])
        dsin = xr.open_mfdataset(paths, preprocess=preselect,
            parallel=True, use_cftime=True) 
        if time_dim is not None and 'fx' not in ctx.obj['frequency']:
            dsin = dsin.sel({time_dim: slice(ctx.obj['tstart'],
                                             ctx.obj['tend'])})
        for v in path_vars[i]:
            input_ds[v] = dsin
    return input_ds
 

@click.pass_context
def get_cmorname(ctx, axis_name, axis, var_log, z_len=None):
    """Get time cmor name based on timeshot option
    """
    var_log.debug(f'axis_name, axis.name: {axis_name}, {axis.name}')
    ctx.obj['axes_modifier'] = []
    if axis_name == 't':
        timeshot = ctx.obj['timeshot']
        if any(x in timeshot for x in ['mean', 'min', 'max', 'sum']):
            cmor_name = 'time'
        elif 'point' in timeshot:
            cmor_name = 'time1'
        elif 'clim' in timeshot:
            cmor_name = 'time2'
        else:
            #assume timeshot is mean
            var_log.warning("timeshot unknown or incorrectly specified")
            cmor_name = 'time'
    elif axis_name == 'lat':
        #PP this needs fixing!!!
        # PP this only modifies standard_name if "latitude-longitude system defined with respect to a rotated North Pole"
        if 'gridlat' in ctx.obj['axes_modifier']:
            cmor_name = 'gridlatitude',
        else:
            cmor_name = 'latitude'
    elif axis_name == 'lon':
        #PP this needs fixing!!!
        if 'gridlon' in ctx.obj['axes_modifier']:
            cmor_name = 'gridlongitude',
        else:
            cmor_name = 'longitude'
    elif axis_name == 'z':
        #PP pressure levels derived from plevinterp
        if 'plevinterp' in ctx.obj['calculation'] :
            levnum = re.findall(r'\d+', ctx.obj['variable_id'])[-1]
            cmor_name = f"plev{levnum}"
        elif 'depth100' in ctx.obj['axes_modifier']:
            cmor_name = 'depth100m'
        elif (axis.name == 'st_ocean') or (axis.name == 'sw_ocean'):
            cmor_name = 'depth_coord'
        #ocean pressure levels
        elif axis.name == 'potrho':
            cmor_name = 'rho'
        elif 'theta_level_height' in axis.name or 'rho_level_height' in axis.name:
            cmor_name = 'hybrid_height2'
        elif axis.name == 'level_number':
            cmor_name = 'hybrid_height'
        elif 'rho_level_number' in axis.name:
            cmor_name = 'hybrid_height_half'
        #atmospheric pressure levels:
        elif axis.name == 'lev' or \
            any(x in axis.name for x in ['_p_level', 'pressure']):
            cmor_name = f"plev{str(z_len)}"
        elif 'soil' in axis.name or axis.name == 'depth':
            cmor_name = 'sdepth'
            if 'topsoil' in ctx.obj['axes_modifier']:
                #top layer of soil only
                cmor_name = 'sdepth1'
    var_log.debug(f"Cmor name for axis {axis.name}: {cmor_name}")
    return cmor_name


#PP this should eventually just be generated directly by defining the dimension using the same terms 
# in related calculation 
@click.pass_context
def pseudo_axis(axis, var_log):
    """coordinates with axis_identifier other than X,Y,Z,T
    PP not sure if axis can be used to remove axes_mod
    """
    cmor_name = None
    p_vals = None
    p_len = None
    #PP still need to work on this to eleiminate axes-modifier!
    if 'dropLev' in ctx.obj['axes_modifier']:
        var_log.info("variable on tiles, setting pseudo levels...")
        #z_len=len(dim_values)
        for mod in ctx.obj['axes_modifier']:
            if 'type' in mod:
                cmor_name = mod
            if cmor_name is None:
                var_log.error('could not determine land type, check '
                    + 'variable dimensions and calculations')
            #PP check if we can just return list from det_landtype
        p_vals = list( det_landtype(cmor_name) )
    if 'landUse' in ctx.obj['axes_modifier']:
        p_vals = getlandUse()
        p_len = len(landUse)
        cmor_name = 'landUse'
    if 'vegtype' in ctx.obj['axes_modifier']:
        p_vals = cableTiles()
        p_len = len(cabletiles)
        cmor_name = 'vegtype'
    return cmor_name, p_vals, p_len


#PP this should eventually just be generated directly by defining the dimension using the same terms 
# in calculation for meridional overturning
def create_axis(name, table, var_log):
    """
    """
    var_log.info("creating {name} axis...")
    func_dict = {'oline': getTransportLines(),
                 'siline': geticeTransportLines(),
                 'basin': np.array(['atlantic_arctic_ocean','indian_pacific_ocean','global_ocean'])}
    result = func_dict[name]
    cmor.set_table(table)
    axis_id = cmor.axis(table_entry=name,
                        units='',
                        length=len(result),
                        coord_vals=result)
    var_log.info(f"setup of {name} axis complete")
    return axis_id


def hybrid_axis(lev, z_ax_id, z_ids, var_log):
    """Setting up additional hybrid axis information
    """
    hybrid_dict = {'hybrid_height': 'b',
                   'hybrid_height_half': 'b_half'}
    orog_vals = getOrog()
    zfactor_b_id = cmor.zfactor(zaxis_id=z_ax_id,
        zfactor_name=hybrid_dict[lev],
        axis_ids=z_ids,
        units='1',
        type='d',
        zfactor_values=b_vals,
        zfactor_bounds=b_bounds)
    zfactor_orog_id = cmor.zfactor(zaxis_id=z_ax_id,
            zfactor_name='orog',
            axis_ids=z_ids,
            units='m',
            type='f',
            zfactor_values=orog_vals)
    return zfactor_b_id, zfactor_orog_id


@click.pass_context
def ij_axis(ctx, ax, ax_name, table, var_log):
    """
    """
    cmor.set_table(table)
    ax_id = cmor.axis(table_entry=ax_name,
        units='1',
        coord_vals=ax.values)
    return ax_id


@click.pass_context
def ll_axis(ctx, ax, ax_name, ds, table, bounds_list, var_log):
    """
    """
    cmor.set_table(table)
    cmor_aName = get_cmorname(ax_name, ax, var_log)
    try:
        ax_units = ax.units
    except:
        ax_units = 'degrees'
    a_bnds = None
    if cmor_aName in bounds_list:
        a_bnds = get_bounds(ds, ax, cmor_aName, var_log)
        a_vals = ax.values
        var_log.debug(f"a_bnds: {a_bnds.shape}")
        var_log.debug(f"a_vals: {a_vals.shape}")
        if 'longitude' in cmor_aName:
            a_vals = np.mod(a_vals, 360)
            a_bnds = np.mod(a_bnds, 360)
        ax_id = cmor.axis(table_entry=cmor_aName,
            units=ax_units,
            length=len(ax),
            coord_vals=a_vals,
            cell_bounds=a_bnds,
            interval=None)
    return ax_id

@click.pass_context
def define_grid(ctx, j_id, i_id, lat, lat_bnds, lon, lon_bnds,
                var_log):
    """If we are on a non-cartesian grid, Define the spatial grid
    """
    grid_id=None
    var_log.info("setting up grid")
    # open ancil grid file to read vertices
    #Set grid id and append to axis and z ids
    grid_id = cmor.grid(axis_ids=np.array([j_id,i_id]),
            latitude=lat,
            longitude=lon[:],
            latitude_vertices=lat_bnds[:],
            longitude_vertices=lon_bnds[:])
    var_log.info("setup of lat,lon grid complete")
    return grid_id


@click.pass_context
def get_coords(ctx, ovar, coords, var_log):
    """Get latitude and longitude values plus their boundaries from ancil file
    """
    var_log.debug("getting lat/lon and bnds from ancil file ...")
    # open ancil grid file to read vertices
    #PP be careful this is currently hardcoded which is not ok!
    ds = xr.open_dataset(f"{ctx.obj['ancils_path']}/cice_grid_20101208.nc")
    bnds_dict = {'ULON': 'lonu_bonds', 'ULAT': 'latu_bonds'}
    #ensure longitudes are in the 0-360 range.
    for c in coords:
         if 'lon' in c.lower():
             lon_vals = np.mod(ovar[c].values, 360)
             bnds = ds[bnds_dict[c]]
             var_log.debug(f"vert: {bnds}")
             # num of vertices should be last dimension 
             if bnds.shape[-1] > bnds.shape[0]:
                 bnds = bnds.transpose(*(list(bnds.dims[1:]) + [bnds.dims[0]]))
             lon_bnds = np.mod(bnds.values, 360)
         elif 'lat' in c.lower():
             lat_vals = ovar[c].values
             bnds = ds[bnds_dict[c]]
             var_log.debug(f"vert: {bnds}")
             # num of vertices should be last dimension 
             if bnds.shape[-1] > bnds.shape[0]:
                 bnds = bnds.transpose(*(list(bnds.dims[1:]) + [bnds.dims[0]]))
             lat_bnds = bnds.values
    return lat_vals, lat_bnds, lon_vals, lon_bnds


@click.pass_context
def get_axis_dim(ctx, var, coords, var_log):
    """
    """
    t_ax = None
    z_ax = None
    lat_ax = None
    lon_ax = None
    j_ax = None
    i_ax = None
    p_ax = None
    # add special extra axis: basin, oline, siline
    e_ax = None
    # make sure axis are correctly defined
    for dim in var.dims:
        try:
            axis = var[dim]
            var_log.debug(f"axis found: {axis}")
        except:
            var_log.warning(f"No coordinate variable associated with the dimension {dim}")
            axis = None
        # need to file to give a value then???
        if axis is not None:
            attrs = axis.attrs
            axis_name = attrs.get('axis', None)
            axis_name = attrs.get('cartesian_axis', axis_name)
            if axis_name == 'T' or 'time' in dim:
                t_ax = axis
                #t_ax.attrs['axis'] = 'T'
            elif 'lat' in dim.lower():
                lat_ax = axis
                #lat_ax.attrs['axis'] = 'Y'
            elif 'lon' in dim.lower():
                lon_ax = axis
                #lon_ax.attrs['axis'] = 'X'
            elif axis_name == 'Y' or 'nj' in dim:
                j_ax = axis
                #j_ax.attrs['axis'] = 'Y'
            elif axis_name == 'X' or 'ni' in dim.lower():
                i_ax = axis
                #i_ax.attrs['axis'] = 'X'
            elif axis_name == 'Z' or any(x in dim for x in ['lev', 'heigth', 'depth']):
                z_ax = axis
                z_ax.attrs['axis'] = 'Z'
            elif 'pseudo' in axis_name:
                p_ax = axis
            elif dim in ['basin', 'oline', 'siline']:
                e_ax = dim
            else:
                var_log.info(f"Unknown axis: {axis_name}")
    return t_ax, z_ax, lat_ax, lon_ax, j_ax, i_ax, p_ax, e_ax


def check_time_bnds(bnds_val, frequency, var_log):
    """Checks if dimension boundaries from file are wrong"""
    approx_interval = bnds_val[:,1] - bnds_val[:,0]
    var_log.debug(f"{bnds_val}")
    var_log.debug(f"Time bnds approx interval: {approx_interval}")
    frq2int = {'dec': 3650.0, 'yr': 365.0, 'mon': 30.0,
                'day': 1.0, '6hr': 0.25, '3hr': 0.125,
                '1hr': 0.041667, '10min': 0.006944, 'fx': 0.0}
    interval = frq2int[frequency]
    # add a small buffer to interval value
    inrange = all(interval*0.99 < x < interval*1.01 for x in approx_interval)
    var_log.debug(f"{inrange}")
    return inrange


@click.pass_context
def require_bounds(ctx, var_log):
    """Returns list of coordinates that require bounds.
    Reads the requirement directly from .._coordinate.json file
    """
    fpath = f"{ctx.obj['tables_path']}/{ctx.obj['_AXIS_ENTRY_FILE']}"
    with open(fpath, 'r') as jfile:
        data = json.load(jfile)
    axis_dict = data['axis_entry']
    bnds_list = [k for k,v in axis_dict.items() 
        if (v['must_have_bounds'] == 'yes')] 
    var_log.debug(f"{bnds_list}")
    return bnds_list


@click.pass_context
def bnds_change(ctx, axis, var_log):
    """Returns True if calculation/resample changes bnds of specified
       dimension.
    """
    dim = axis.name
    calculation = ctx.obj['calculation']
    changed_bnds = False
    if 'time' in dim and ctx.obj['resample'] != '':
        changed_bnds = True
    if calculation != '':
        if f"sum(dim={dim})" in calculation:
            changed_bnds = True
        elif "level_to_height(var[0],levs=" in calculation and 'height' in dim:
            changed_bnds = True
    return changed_bnds


@click.pass_context
def get_bounds(ctx, ds, axis, cmor_name, var_log, ax_val=None):
    """Returns bounds for input dimension, if bounds are not available
       uses edges or tries to calculate them.
       If variable goes through calculation potentially bounds are different from
       input file and forces re-calculating them
    """
    dim = axis.name
 #PP if lontigtude an dlatidude 2-dim we should get bnds from ancil files, which we actuakly do ind efine grid, so should we set that up that way?
    var_log.info(f"Getting bounds for axis: {dim}")
    changed_bnds = bnds_change(axis, var_log) 
    var_log.debug(f"Bounds has changed: {changed_bnds}")
    #The default bounds assume that the grid cells are centred on
    #each grid point specified by the coordinate variable.
    keys = [k for k in axis.attrs]
    calc = False
    frq = ctx.obj['frequency']
    if 'subhr' in frq:
        frq =  ctx.obj['subhr'] + frq.split('subhr')[1]
    if 'bounds' in keys and not changed_bnds:
        dim_val_bnds = ds[axis.bounds].values
        var_log.info(f"Using dimension bounds: {axis.bounds}")
    elif 'edges' in keys and not changed_bnds:
        dim_val_bnds = ds[axis.edges].values
        var_log.info(f"Using dimension edges as bounds: {axis.edges}")
    else:
        var_log.info(f"No bounds for {dim}")
        calc = True
    if 'time' in cmor_name and calc is False:
        dim_val_bnds = cftime.date2num(dim_val_bnds,
            units=ctx.obj['reference_date'],
            calendar=ctx.obj['attrs']['calendar'])
        inrange = check_time_bnds(dim_val_bnds, frq, var_log)
        if not inrange:
            calc = True
            var_log.info(f"Inherited bounds for {dim} are incorrect")
    if calc is True:
        var_log.info(f"Calculating bounds for {dim}")
        if ax_val is None:
            ax_val = axis.values
        try:
            #PP using roll this way without specifying axis assume axis is 1D
            min_val = (ax_val + np.roll(ax_val, 1))/2
            min_val[0] = 1.5*ax_val[0] - 0.5*ax_val[1]
            max_val = np.roll(min_val, -1)
            max_val[-1] = 1.5*ax_val[-1] - 0.5*ax_val[-2]
            dim_val_bnds = np.column_stack((min_val, max_val))
        except Exception as e:
            var_log.warning(f"dodgy bounds for dimension: {dim}")
            var_log.error(f"error: {e}")
        if 'time' in cmor_name:
            inrange = check_time_bnds(dim_val_bnds, frq, var_log)
            if inrange is False:
                var_log.error(f"Boundaries for {cmor_name} are "
                    + "wrong even after calculation")
                #PP should probably raise error here!
    # Take into account type of axis
    # as we are often concatenating along time axis and bnds are
    # considered variables they will also be concatenated along time axis
    # and we need only 1st timestep
    #not sure yet if I need special treatment for if cmor_name == 'time2':
    if dim_val_bnds.ndim == 3:
            dim_val_bnds = dim_val_bnds[0,:,:].squeeze() 
            var_log.debug(f"dimbnds.shape: {dim_val_bnds.shape}")
    #force the bounds back to the poles if necessary
    if cmor_name == 'latitude' and changed_bnds:
        if dim_val_bnds[0,0] < -90.0:
            dim_val_bnds[0,0] = -90.0
            var_log.info("setting minimum latitude bound to -90")
        if dim_val_bnds[-1,-1] > 90.0:
            dim_val_bnds[-1,-1] = 90.0
            var_log.info("setting maximum latitude bound to 90")
    elif cmor_name == 'depth':
        if 'OM2' in ctx.obj['access_version'] and dim == 'sw_ocean':
            dim_val_bnds[-1] = axis[-1]
    elif 'height' in cmor_name and dim_val_bnds[0,0] < 0:
        dim_val_bnds[0,0] = 0.0
        var_log.info(f"setting minimum {cmor_name} bound to 0")
    return dim_val_bnds


@click.pass_context
def get_attrs(ctx, infiles, var1, var_log):
    """
    """
    # open only first file so we can access encoding
    ds = xr.open_dataset(infiles[0][0])
    var_attrs = ds[var1].attrs 
    in_units = ctx.obj['in_units']
    if in_units in [None, '']:
        in_units = var_attrs.get('units', "1")
    in_missing = var_attrs.get('_FillValue', 9.96921e+36)
    in_missing = var_attrs.get('missing_value', in_missing)
    in_missing = float(in_missing)
    if all(x not in var_attrs.keys() for x in ['_FillValue', 'missing_value']):
        var_log.info("trying fillValue as missing value")
        
    # work out if there is a vertical direction associated with the variable
    #(for example radiation variables).
    # search for positive attribute keyword in standard name/positive attrs
    positive = None
    if ctx.obj['positive'] in ['up', 'down']:
        positive = ctx.obj['positive']
    else:
        standard_name = var_attrs.get('standard_name', 'None')
    #P might not need this as positive gets ignore if not defined in cmor table
     # however might be good to spot potential misses
        if any(x in standard_name.lower() for x in 
            ['up', 'outgoing', 'out_of']):
            positive = 'up'
        elif any(x in standard_name.lower() for x in
            ['down', 'incoming', 'into']):
            positive = 'down'
    coords = ds[var1].encoding.get('coordinates','')
    coords = coords.split()
    return in_units, in_missing, positive, coords


@click.pass_context
def axm_t_integral(ctx, invar, dsin, variable_id, var_log):
    """I couldn't find anywhere in mappings where this is used
    so I'm keeping it exactly as it is it's not worth it to adapt it
    still some cdms2 options and we're now passing all files at one time but this code assumes more than one file
    """
    try:
        run = np.float32(ctx.obj['calculation'])
    except:
        run = np.float32(0.0)
    #for input_file in inrange_files:
    #If the data is a climatology, store the values in a running sum

    t = invar[time_dim]
    # need to look ofr xarray correspondent of daysInMonth (cdms2)
    tbox = daysInMonth(t)
    varout = np.float32(var[:,0]*tbox[:]*24).cumsum(0) + run
    run = varout[-1]
    #if we have a second variable, just add this to the output (not included in the integration)
    if len(ctx.obj['vin']) == 2:
        varout += dsin[ctx.obj['vin'][1]][:]
    cmor.write(variable_id, (varout), ntimes_passed=np.shape(varout)[0])
    return


@click.pass_context
def axm_timeshot(ctx, dsin, variable_id, var_log):
    """
    #PP not sure where this is used
        #Set var to be sum of variables in 'vin' (can modify to use calculation if needed)
    """
    var = None
    for v in ctx.obj['vin']:
        try:        
            var += (dsin[v])
            var_log.info("added extra variable")
        #PP I'm not sure this makes sense, if sum on next variable fails then I restart from that variable??
        except:        
            var = dsin[v][:]
    try: 
        vals_wsum, clim_days = monthClim(var,t,vals_wsum,clim_days)
    except:
        #first time
        tmp = var[0,:].shape
        out_shape = (12,) + tmp
        vals_wsum = np.ma.zeros(out_shape,dtype=np.float32)
        var_log.info(f"first time, data shape: {np.shape(vals_wsum)}")
        clim_days = np.zeros([12],dtype=int)#sum of number of days in the month
        vals_wsum,clim_days = monthClim(var,t,vals_wsum,clim_days)
    #calculate the climatological average for each month from the running sum (vals_wsum)
    #and the total number of days for each month (clim_days)
    for j in range(12):
        var_log.info(f"month: {j+1}, sum of days: {clim_days[j]}")
        #average vals_wsum using the total number of days summed for each month
        vals_wsum[j,:] = vals_wsum[j,:] / clim_days[j]
    cmor.write(variable_id, (vals_wsum), ntimes_passed=12)


@click.pass_context
def calc_monsecs(ctx, dsin, tdim, in_missing, mop_log):
    """ #PP again not sure where this is used
    """
    monsecs = calendar.monthrange(dsin[tdim].dt.year,dsin[tdim].dt.month)[1] * 86400
    if ctx.obj['calculation'] == '':
        array = dsin[ctx.obj['vin'][0]]
    else:
        mop_log.info("calculating...")
        array = calculateVals(dsin, ctx.obj['vin'], ctx.obj['calculation'])
    array = array / monsecs
    #convert mask to missing values
    try: 
        array = array.fillna(in_missing)
    except:
        #if values aren't in a masked array
        pass 
    return array


@click.pass_context
def extract_var(ctx, input_ds, tdim, in_missing, mop_log, var_log):
    """
    This function pulls the required variables from the Xarray dataset.
    If a calculation isn't needed then it just returns the variables to be saved.
    If a calculation is needed then it evaluates the calculation and returns the result.
    Finally it re-select time rnage in case resmaple or other operations have introduced
    extra timesteps

    input_ds - dict
       dictionary of input datasets for each variable
    """
    failed = False
    # Save the variables
    if ctx.obj['calculation'] == '':
        varname = ctx.obj['vin'][0]
        array = input_ds[varname][varname][:]
        var_log.debug(f"{array}")
    else:
        var = []
        var_log.info("Adding variables to var list")
        for v in ctx.obj['vin']:
            try:
                var.append(input_ds[v][v][:])
            except Exception as e:
                failed = True
                var_log.error(f"Error appending variable, {v}: {e}")

        var_log.info("Finished adding variables to var list")

        # Now try to perform the required calculation
        array = eval(ctx.obj['calculation'])
        try:
            array = eval(ctx.obj['calculation'])
            var_log.debug(f"Variable after calculation: {array}")
        except Exception as e:
            failed = True
            mop_log.info(f"error evaluating calculation, {ctx.obj['filename']}")
            var_log.error(f"error evaluating calculation, {ctx.obj['calculation']}: {e}")
    #Call to resample operation is deifned based on timeshot
    if ctx.obj['resample'] != '':
        array = time_resample(array, ctx.obj['resample'], tdim,
            stats=ctx.obj['timeshot'])
        var_log.debug(f"Variable after resample: {array}")

    # STill need to check if this is needed, it probably is need for integer values but the others?
    if array.dtype.kind == 'i':
        try:
            in_missing = int(in_missing)
        except:
            in_missing = int(-999)
    else:
        array = array.fillna(in_missing)
        var_log.debug(f"Variable after fillna: {array}")
    # Some ops (e.g., resample) might introduce extra tstep: select time range 
    if tdim is not None and 'fx' not in ctx.obj['frequency']:
        var_log.debug(f"{ctx.obj['tstart']}, {ctx.obj['tend']}")
        array = array.sel({tdim: slice(ctx.obj['tstart'], ctx.obj['tend'])})
        var_log.debug(f"{array[tdim][0].values}, {array[tdim][-1].values}")
    return array, failed
