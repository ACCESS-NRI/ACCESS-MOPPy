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
# last updated 15/05/2024

import numpy as np
import glob
import re
import os,sys
import stat
import yaml
import xarray as xr
import cmor
import calendar
import click
import logging
import cftime
import itertools
import copy
from functools import partial
from pathlib import Path

from mopper.calculations import *
from mopdb.utils import read_yaml
from importlib.resources import files as import_files


def config_log(debug, path, stream_level=logging.WARNING):
    """Configure log file for main process and errors from variable processes"""
    # start a logger first otherwise settings also apply to root logger
    logger = logging.getLogger('mop_log')
    # set the level for the logger, has to be logging.LEVEL not a string
    # until we do so applog doesn't have a level and inherits the root logger level:WARNING
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


def config_varlog(debug, logname, pid):
    """Configure varlog file: use this for specific var information"""
    logger = logging.getLogger(f'{pid}_log')
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
    # Stop propagation
    logger.propagate = False
    return logger


def _preselect(ds, varlist):
    varsel = [v for v in varlist if v in ds.variables]
    bnds = []
    for c in ds[varsel].coords:
        bounds = ds[c].attrs.get('bounds', None)
        if bounds is None:
            bounds = ds[c].attrs.get('edges', None)
        if bounds is not None:
            bnds.extend([b for b in bounds.split() if b in ds.variables])
    # check all bnds are in file
    varsel.extend(bnds)
    # remove attributes for boundaries
    for v in bnds:
        ds[v].attrs = {}
    return ds[varsel]


@click.pass_context
def get_files(ctx):
    """Returns all files in time range
    First identifies all files with pattern/s defined for invars
    Then retrieve time dimension and if multiple time axis are present
    Finally filter only files in time range based on file timestamp (faster)
    If this fails or multiple time axis are present reads first and
    last timestep from each file
    """
    # Returns file list for each input var and list of vars for each file pattern
    var_log = logging.getLogger(ctx.obj['var_log'])
    all_files, path_vars = find_all_files()

    # PP FUNCTION END return all_files, extra_files
    var_log.debug(f"access files from: {os.path.basename(all_files[0][0])}" +
                 f"to {os.path.basename(all_files[0][-1])}")
    ds = xr.open_dataset(all_files[0][0], decode_times=False)
    time_dim, units, multiple_times = get_time_dim(ds)
    del ds
    try:
        inrange_files = []
        for i,paths in enumerate(all_files):
            if multiple_times is True:
                inrange_files.append( check_in_range(paths, time_dim) )
            else:
                inrange_files.append( check_timestamp(paths) )
    except:
        for i,paths in enumerate(all_files):
            inrange_files.append( check_in_range(paths, time_dim) )

    for i,paths in enumerate(inrange_files):
        if paths == []:
            var_log.error(f"No data in requested time range for: {ctx.obj['filename']}")
    return inrange_files, path_vars, time_dim, units


@click.pass_context
def find_all_files(ctx):
    """Find all the ACCESS file names which match the pattern/s associated with invars.
    Sort the filenames, assuming that the sorted filenames will
    be in chronological order because there is usually some sort of date
    and/or time information in the filename.
    Check that all variables needed are in file, otherwise add extra file pattern
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"Input file structure: {ctx.obj['infile']}")
    patterns = ctx.obj['infile'].split()
    var_log.debug(f"Input file patterns: {patterns}")
    #set normal set of files
    files = []
    for i,p in enumerate(patterns):
        path, match = p.split("**/")
        pattern_paths = [x for x in  Path(path).rglob(match)]
        if len(pattern_paths) == 0:
            var_log.warning(f"""Could not find files for pattern {p}.
                Make sure path correct and project storage flag included""")
        pattern_paths.sort( key=lambda x:x.name)
        files.append(pattern_paths)
        #files.append( [str(x) for x in Path(path).rglob(match)])
        #files[i].sort()
    # if there is more than one variable: make sure all vars are in
    # one of the file pattern and couple them
    missing = copy.deepcopy(ctx.obj['vin'])
    i = 0
    path_vars = {}
    while len(missing) > 0 and i < len(patterns):
        path_vars[i] = []
        f = files[i][0]
        missing, found = check_vars_in_file(missing, f)
        if len(found) > 0:
            for v in found:
                path_vars[i].append(v)
        i+=1
    # if we couldn't find a variable check other files in same directory
    if len(missing) > 0:
        var_log.error(f"Input vars: {missing} not in files {ctx.obj['infile']}")
    return files, path_vars 


@click.pass_context
def check_vars_in_file(ctx, invars, fname):
    """Check that all variables needed for calculation are in file
    else return extra filenames
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    ds = xr.open_dataset(fname, decode_times=False)
    tofind = [v for v in invars if v not in ds.variables]
    found = [v for v in invars if v not in tofind]
    return tofind, found


@click.pass_context
def get_time_dim(ctx, ds):
    """Find time info: time axis, reference time and set tstart and tend
       also return mutlitple_times True if more than one time axis
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    time_dim = None
    multiple_times = False
    varname = [ctx.obj['vin'][0]]
    #    
    var_log.debug(f" check time var dims: {ds[varname].dims}")
    for var_dim in ds[varname].dims:
        axis = ds[var_dim].attrs.get('axis', '')
        if 'time' in var_dim or axis == 'T':
            time_dim = var_dim
            units = ds[var_dim].units
            var_log.debug(f"first attempt to tdim: {time_dim}")
    
    var_log.debug(f"time var is: {time_dim}")
    # check if files contain more than 1 time dim
    tdims = [ x for x in ds.dims if 'time' in x or 
              ds[x].attrs.get('axis', '')  == 'T']
    if len(tdims) > 1:
        multiple_times = True
    del ds 
    return time_dim, units, multiple_times


@click.pass_context
def check_timestamp(ctx, all_files):
    """This function tries to guess the time coverage of a file based on its timestamp
       and return the files in range. At the moment it does a lot of checks based on the realm and real examples
       eventually it would make sense to make sure all files generated are consistent in naming
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
def check_in_range(ctx, all_files, tdim):
    """Return a list of files in time range
       Open each file and check based on time axis
       Use this function only if check_timestamp fails
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
def load_data(ctx, inrange_files, path_vars, time_dim):
    """Returns a dictionary of input var: xarray dataset
    """
    # preprocessing to select only variables we need to avoid
    # concatenation issues with multiple coordinates
    # temporarily opening file without decoding times, fixing
    # faulty time bounds units and decoding times
    # this is to prevent issues with ocean files
    var_log = logging.getLogger(ctx.obj['var_log'])
    input_ds = {}
    for i, paths in enumerate(inrange_files):
        preselect = partial(_preselect, varlist=path_vars[i])
        dsin = xr.open_mfdataset(paths, preprocess=preselect,
            parallel=True, decode_times=False)
        dsin = xr.decode_cf(dsin, use_cftime=True)
        if time_dim is not None and 'fx' not in ctx.obj['frequency']:
            dsin = dsin.sel({time_dim: slice(ctx.obj['tstart'],
                                             ctx.obj['tend'])})
        for v in path_vars[i]:
            var_log.debug(f"Load data, var and path: {v}, {path_vars[i]}")
            input_ds[v] = dsin
    return input_ds
 

@click.pass_context
def get_cmorname(ctx, axis_name, axis, z_len=None):
    """Get time cmor name based on timeshot option
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
        cmor_name = 'latitude'
    elif axis_name == 'lon':
        cmor_name = 'longitude'
    elif axis_name == 'glat':
        cmor_name = 'gridlatitude'
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
def pseudo_axis(ctx, axis):
    """coordinates with axis_identifier other than X,Y,Z,T
    PP not sure if axis can be used to remove axes_mod
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
@click.pass_context
def create_axis(ctx, axis, table):
    """
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    # maybe we can just create these axis as they're meant in calculations 
    var_log.info(f"creating {axis.name} axis...")
    #func_dict = {'oline': getTransportLines(),
    #             'siline': geticeTransportLines(),
    #             'basin': np.array(['atlantic_arctic_ocean','indian_pacific_ocean','global_ocean'])}
    #result = func_dict[name]
    axval = axis.values.astype(str)
    cmor.set_table(table)
    axis_id = cmor.axis(table_entry=axis.name,
                        units='',
                        length=axval.size,
                        coord_vals=axval)
    var_log.info(f"setup of {axis.name} axis complete")
    return axis_id


def hybrid_axis(lev, z_ax_id, z_ids):
    """Setting up additional hybrid axis information
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
def ij_axis(ctx, ax, ax_name, table):
    """
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    cmor.set_table(table)
    ax_id = cmor.axis(table_entry=ax_name,
        units='1',
        coord_vals=ax.values)
    return ax_id


@click.pass_context
def ll_axis(ctx, ax, ax_name, ds, table, bounds_list):
    """
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"in ll_axis")
    cmor.set_table(table)
    cmor_aName = get_cmorname(ax_name, ax)
    try:
        ax_units = ax.units
    except:
        ax_units = 'degrees'
    a_bnds = None
    var_log.debug(f"got cmor name: {cmor_aName}")
    if cmor_aName in bounds_list:
        a_bnds = get_bounds(ds, ax, cmor_aName)
        a_vals = ax.values
        var_log.debug(f"a_bnds: {a_bnds.shape}")
        var_log.debug(f"a_vals: {a_vals.shape}")
        #if 'longitude' in cmor_aName:
        #    var_log.debug(f"longitude: {cmor_aName}")
        #    a_vals = np.mod(a_vals, 360)
        #    a_bnds = np.mod(a_bnds, 360)
        ax_id = cmor.axis(table_entry=cmor_aName,
            units=ax_units,
            length=len(ax),
            coord_vals=a_vals,
            cell_bounds=a_bnds,
            interval=None)
    return ax_id

@click.pass_context
def define_grid(ctx, j_id, i_id, lat, lat_bnds, lon, lon_bnds):
    """If we are on a non-cartesian grid, Define the spatial grid
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    grid_id=None
    var_log.info("setting up grid")
    #Set grid id and append to axis and z ids
    grid_id = cmor.grid(axis_ids=np.array([j_id,i_id]),
            latitude=lat,
            longitude=lon[:],
            latitude_vertices=lat_bnds[:],
            longitude_vertices=lon_bnds[:])
    var_log.info("setup of lat,lon grid complete")
    return grid_id

@click.pass_context
def get_coords(ctx, ovar, coords):
    """Get lat/lon and their boundaries from ancil file
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    # open ancil grid file to read vertices
    #PP be careful this is currently hardcoded which is not ok!
    ancil_file = ctx.obj[f"grid_{ctx.obj['realm']}"]
    var_log.debug(f"getting lat/lon and bnds from ancil file: {ancil_file}")
    ds = xr.open_dataset(f"{ctx.obj['ancils_path']}/{ancil_file}")
    var_log.debug(f"ancil ds: {ds}")
    # read lat/lon and vertices mapping
    cfile = import_files('mopdata').joinpath('latlon_vertices.yaml')
    with open(cfile, 'r') as yfile:
        data = yaml.safe_load(yfile)
    ll_dict = data[ctx.obj['realm']]
    #ensure longitudes are in the 0-360 range.
    for c in coords:
         var_log.debug(f"ancil coord: {c}")
         coord = ds[ll_dict[c][0]]
         var_log.debug(f"bnds name: {ll_dict[c]}")
         bnds = ds[ll_dict[c][1]]
         # num of vertices should be last dimension 
         if bnds.shape[-1] > bnds.shape[0]:
             bnds = bnds.transpose(*(list(bnds.dims[1:]) + [bnds.dims[0]]))
         if 'lon' in c.lower():
             lon_vals = np.mod(coord.values, 360)
             lon_bnds = np.mod(bnds.values, 360)
         elif 'lat' in c.lower():
             lat_vals = coord.values
             lat_bnds = bnds.values
    return lat_vals, lat_bnds, lon_vals, lon_bnds


@click.pass_context
def get_axis_dim(ctx, var):
    """
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    axes = {'t_ax': None, 'z_ax': None, 'glat_ax': None,
            'lat_ax': None, 'lon_ax': None, 'j_ax': None,
            'i_ax': None, 'p_ax': None, 'e_ax': None}
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
            var_log.debug(f"trying axis attrs: {axis_name}")
            axis_name = attrs.get('cartesian_axis', axis_name)
            var_log.debug(f"trying cart axis attrs: {axis_name}")
            if axis_name == 'T' or 'time' in dim.lower():
                axes['t_ax'] = axis
            elif axis_name and 'Y' in axis_name:
                if dim.lower() == 'gridlat':
                    axes['glat_ax'] = axis
                elif 'lat' in dim.lower():
                    axes['lat_ax'] = axis
                elif any(x in dim.lower() for x in ['nj', 'yu_ocean', 'yt_ocean']):
                    axes['j_ax'] = axis
            # have to add this because a simulation didn't have the dimenision variables
            elif any(x in dim.lower() for x in ['nj', 'yu_ocean', 'yt_ocean']):
                axes['j_ax'] = axis
            elif axis_name and 'X' in axis_name:
                if 'glon' in dim.lower():
                    axes['glon_ax'] = axis
                elif 'lon' in dim.lower():
                    axes['lon_ax'] = axis
                elif any(x in dim.lower() for x in ['ni', 'xu_ocean', 'xt_ocean']):
                    axes['i_ax'] = axis
            # have to add this because a simulation didn't have the dimenision variables
            elif any(x in dim.lower() for x in ['ni', 'xu_ocean', 'xt_ocean']):
                axes['i_ax'] = axis
            elif axis_name == 'Z' or any(x in dim for x in ['lev', 'heigth', 'depth']):
                axes['z_ax'] = axis
                #z_ax.attrs['axis'] = 'Z'
            elif axis_name and 'pseudo' in axis_name:
                axes['p_ax'] = axis
            elif dim in ['basin', 'oline', 'siline']:
                axes['e_ax'] = axis 
            else:
                var_log.info(f"Unknown axis: {axis_name}")
    return axes


@click.pass_context
def check_time_bnds(ctx, bnds, frequency):
    """Checks if dimension boundaries from file are wrong"""
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"Time bnds 1,0: {bnds[:,1], bnds[:,0]}")
    diff = bnds[:,1] - bnds[:,0]
    #approx_int = [np.timedelta64(x, 'D').astype(float) for x in diff]
    approx_int = [x.astype(float) for x in diff]
    var_log.debug(f"Time bnds approx interval: {approx_int}")
    frq2int = {'dec': 3650.0, 'yr': 365.0, 'mon': 30.0,
                'day': 1.0, '6hr': 0.25, '3hr': 0.125,
                '1hr': 0.041667, '10min': 0.006944, 'fx': 0.0}
    interval = frq2int[frequency]
    # add a small buffer to interval value
    var_log.debug(f"interval: {interval}")
    inrange = all(interval*0.9 < x < interval*1.1 for x in approx_int)
    var_log.debug(f"{inrange}")
    return inrange


@click.pass_context
def require_bounds(ctx):
    """Returns list of coordinates that require bounds.
    Reads the requirement directly from .._coordinate.json file
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    fpath = f"{ctx.obj['tpath']}/{ctx.obj['_AXIS_ENTRY_FILE']}"
    with open(fpath, 'r') as jfile:
        data = json.load(jfile)
    axis_dict = data['axis_entry']
    bnds_list = [k for k,v in axis_dict.items() 
        if (v['must_have_bounds'] == 'yes')] 
    var_log.debug(f"{bnds_list}")
    return bnds_list


@click.pass_context
def bnds_change(ctx, axis):
    """Returns True if calculation/resample changes bnds of specified
       dimension.
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
def get_bounds(ctx, ds, axis, cmor_name, ax_val=None):
    """Returns bounds for input dimension, if bounds are not available
       uses edges or tries to calculate them.
       If variable goes through calculation potentially bounds are different from
       input file and forces re-calculating them
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f'in getting bounds: {axis}')
    dim = axis.name
    var_log.info(f"Getting bounds for axis: {dim}")
    changed_bnds = bnds_change(axis) 
    var_log.debug(f"Bounds has changed: {changed_bnds}")
    #The default bounds assume that the grid cells are centred on
    #each grid point specified by the coordinate variable.
    keys = [k for k in axis.attrs]
    calc = False
    frq = ctx.obj['frequency']
    if 'subhr' in frq:
        frq =  ctx.obj['subhr'] + frq.split('subhr')[1]
    if 'bounds' in keys and not changed_bnds:
        calc, dim_bnds_val = get_bounds_values(ds, axis.bounds)
        var_log.info(f"Using dimension bounds: {axis.bounds}")
    elif 'edges' in keys and not changed_bnds:
        calc, dim_bnds_val = get_bounds_values(ds, axis.edges)
        var_log.info(f"Using dimension edges as bounds: {axis.edges}")
    else:
        var_log.info(f"No bounds for {dim}")
        calc = True
    if 'time' in cmor_name and calc is False:
        # in most cases if time_bounds decoded we need to re-convert them
        if 'cftime' in str(type(dim_bnds_val[0,1])):
            dim_bnds_val = cftime.date2num(dim_bnds_val,
                units=ctx.obj['reference_date'],
                calendar=ctx.obj['attrs']['calendar'])
        inrange = check_time_bnds(dim_bnds_val, frq)
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
            dim_bnds_val = np.column_stack((min_val, max_val))
            var_log.debug(f"{axis.name} bnds: {dim_bnds_val}")
        except Exception as e:
            var_log.warning(f"dodgy bounds for dimension: {dim}")
            var_log.error(f"error: {e}")
        if 'time' in cmor_name:
            inrange = check_time_bnds(dim_bnds_val, frq)
            if inrange is False:
                var_log.error(f"Boundaries for {cmor_name} are "
                    + "wrong even after calculation")
                #PP should probably raise error here!
    # Take into account type of axis
    # as we are often concatenating along time axis and bnds are
    # considered variables they will also be concatenated along time axis
    # and we need only 1st timestep
    #not sure yet if I need special treatment for if cmor_name == 'time2':
    if dim_bnds_val.ndim == 3:
            dim_bnds_val = dim_bnds_val[0,:,:].squeeze() 
            var_log.debug(f"dimbnds.shape: {dim_bnds_val.shape}")
    #force the bounds back to the poles if necessary
    if cmor_name == 'latitude' and calc:
        if dim_bnds_val[0,0] < -90.0:
            dim_bnds_val[0,0] = -90.0
            var_log.info("setting minimum latitude bound to -90")
        if dim_bnds_val[-1,-1] > 90.0:
            dim_bnds_val[-1,-1] = 90.0
            var_log.info("setting maximum latitude bound to 90")
    elif cmor_name == 'depth':
        if 'OM2' in ctx.obj['access_version'] and dim == 'sw_ocean':
            dim_bnds_val[-1] = axis[-1]
    elif 'height' in cmor_name and dim_bnds_val[0,0] < 0:
        dim_bnds_val[0,0] = 0.0
        var_log.info(f"setting minimum {cmor_name} bound to 0")
    return dim_bnds_val

@click.pass_context
def get_bounds_values(ctx, ds, bname):
    """Return values of axis bounds, if they're not in file
       tries to get them from ancillary grid file instead.
    """
    calc = False
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"Getting bounds values for {bname}")
    if bname in ds.variables:
        bnds_val = ds[bname].values
    elif ancil_file != "":     
        ancil_file =  ctx.obj[f"grid_{ctx.obj['realm']}"]
        fname = f"{ctx.obj['ancils_path']}/{ancil_file}"
        ancil = xr.open_dataset(fname)
        if bname in ancil.variables:
            bnds_val = ancil[bname].values
        else:
            var_log.info(f"Can't locate {bname} in data or ancil file")
            bnds_val = None
            calc = True
    return calc, bnds_val

@click.pass_context
def get_attrs(ctx, infiles, var1):
    """
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
def extract_var(ctx, input_ds, tdim, in_missing):
    """
    This function pulls the required variables from the Xarray dataset.
    If a calculation isn't needed then it just returns the variables to be saved.
    If a calculation is needed then it evaluates the calculation and returns the result.
    Finally it re-select time rnage in case resmaple or other operations have introduced
    extra timesteps

    input_ds - dict
       dictionary of input datasets for each variable
    """
    mop_log = logging.getLogger('mop_log')
    var_log = logging.getLogger(ctx.obj['var_log'])
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
        try:
            array = eval(ctx.obj['calculation'])
            var_log.debug(f"Variable after calculation: {array}")
        except Exception as e:
            failed = True
            mop_log.info(f"error evaluating calculation, {ctx.obj['filename']}")
            var_log.error(f"error evaluating calculation, {ctx.obj['calculation']}: {e}")
    #Call to resample operation is defined based on timeshot
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


@click.pass_context
def define_attrs(ctx):
    """Returns all global attributes to be set up by CMOR after
    checking if there are notes to be added for a specific field.

    Notes are read from src/data/notes.yaml
    NB for calculation is checking only if name of function used is
    listed in notes file, this is indicated by precending any function
    in file with a ~. For other fields it checks equality.
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    attrs = ctx.obj['attrs']
    notes = attrs.get('notes', '')
    # open file containing notes
    fname = import_files('mopdata').joinpath('notes.yaml')
    data = read_yaml(fname)['notes']
    # check all fields and if any of their keys (e.g. a specific variable)
    # match the field value for the file being processed
    # if keys has ~ as first char: check key in fval
    # e.g. calculation: ~plevinterp checks for "plevinterp" in "ctx.obj['calculation']
    # instead of "_plevinterp" == "ctx.obj['calculation']
    for field in data.keys():
        fval = ctx.obj[field]
        for k,v in data[field].items():
            if k == fval or (k[0] == '~' and k[1:] in fval):
                notes += v
    if notes != '':
        attrs['notes'] = notes
    return attrs
