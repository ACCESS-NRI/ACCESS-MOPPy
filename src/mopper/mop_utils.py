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

import numpy as np
import re
import os
import stat
import yaml
import xarray as xr
import cmor
import click
import logging
import cftime
import copy
import json
from functools import partial
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

from mopper.calc_land import *
from mopper.calc_atmos import *
from mopper.calc_utils import *
from mopper.calc_seaice import *
from mopper.calc_ocean import *
from mopdb.utils import read_yaml, MopException
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
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
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
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
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

     1) Identifies all files with pattern/s defined for invars
     2) Retrieves time dimension, checks if file has multiple time axes
     3) Filters files in time range based on file timestamp (faster)
     4) If last step fails or multiple time axis are present reads first and
       last timestep from each file
 
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    Returns
    -------
    path_vars : dict(dict)
    """
    # Returns file list for each input var and list of vars for each file pattern
    var_log = logging.getLogger(ctx.obj['var_log'])
    path_vars = find_all_files()
    var_log.debug(f"get_files, path_vars size: {len(path_vars)}")
    # step 2 
    # step 3/4
    for pat,v in path_vars.items():
        paths = v['files']
        if v['duplicate'] != '':
            continue
        ds = xr.open_dataset(v['files'][0], decode_times=False)
        v['tdim'], multiple_times = get_time_dim(ds)
        del ds
        if multiple_times is True:
            v['files'] = check_timeaxis(paths, v['tdim'])
        else:
            try:
                v['files'] = check_timestamp(paths)
            except Exception as e:
                var_log.debug("get_files: using timestamp failed trying timeaxis")
                v['files'] = check_timeaxis(paths, v['tdim'])
        path_vars[pat] = v
        if path_vars[pat]['files'] == []:
            var_log.error(f"No data in requested time range for: {ctx.obj['filename']}")
    for pat,v in path_vars.items():
        if v['duplicate'] != '':
            path_vars[pat]['files'] = path_vars[v['duplicate']]['files']
    
    return path_vars


@click.pass_context
def find_all_files(ctx):
    """Find all the ACCESS file names which match the pattern/s associated with invars.
    Sort the filenames, assuming that the sorted filenames will
    be in chronological order because there is usually some sort of date
    and/or time information in the filename.
    Check that all variables needed are in file, otherwise add extra file pattern

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"Input file structure: {ctx.obj['infile']}")
    patterns = ctx.obj['infile'].split()
    var_log.debug(f"Input file patterns: {patterns}")
    #set normal set of files
    path_vars = {}
    for p in patterns:
        path_vars[p] = {}
        path_vars[p]['duplicate'] = ''
        path, match = p.split("**/")
        pattern_paths = [x for x in  Path(path).rglob(match)]
        if len(pattern_paths) == 0:
            var_log.warning(f"""Could not find files for pattern {p}.
                Make sure path correct and project storage flag included""")
        pattern_paths.sort( key=lambda x:x.name)
        path_vars[p]['files'] = pattern_paths
    # if there is more than one variable: make sure all vars are in
    # one of the file pattern and couple them
    missing = copy.deepcopy(ctx.obj['vin'])
    i = 0
    while len(missing) > 0 and i < len(patterns):
        pat = patterns[i]
        files = path_vars[pat]['files']
        missing, found, duplicate = check_vars_in_file(missing, files[0])
        var_log.debug(f"calling add_var_path: found {found}, duplicate {duplicate}")
        # if there are variables with different time axes duplicate paths 
        path_vars = add_var_path(path_vars, pat, files, found, duplicate)
        i+=1
    # if we couldn't find a variable check other files in same directory
    if len(missing) > 0:
        var_log.error(f"Input vars: {missing} not in files {ctx.obj['infile']}")
    return path_vars 


@click.pass_context
def add_var_path(ctx, path_vars, pat, files, found, duplicate):
    """
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    if len(found) > 0:
        if duplicate is False:
            path_vars[pat]['vars'] = found
        else:
            path_vars[pat]['vars'] = [found[0]]
            # duplicate paths for other variables
            for i,v in enumerate(found[1:]):
                path_vars[f"{pat}-{i}"] = {}
                path_vars[f"{pat}-{i}"]['vars'] = [v]
                path_vars[f"{pat}-{i}"]['files'] = files
                path_vars[f"{pat}-{i}"]['duplicate'] = pat
    return path_vars


@click.pass_context
def check_vars_in_file(ctx, invars, fname):
    """Check that all variables needed for calculation are in file
    else return extra filenames
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    ds = xr.open_dataset(fname, decode_times=False)
    tofind = [v for v in invars if v not in ds.variables]
    found = [v for v in invars if v not in tofind]
    tdims = []
    # Check if variables are using different time axes, if yes duplciate info
    duplicate = False
    for v in found:
        td = [d for d in ds[v].dims if 'time' in d] 
        var_log.debug(f"timedim for {v} is {td}")
        if td != []:
            tdims.append(td[0]) 
        var_log.debug(f"tdim list {tdims}")
    if len(tdims) > 1:
        duplicate = True
        var_log.debug("Found variables with different time axis in calculation")
    return tofind, found, duplicate


@click.pass_context
def get_time_dim(ctx, ds):
    """Find time info: time axis, reference time and set tstart and tend
       also return mutltiple_times True if more than one time axis

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    time_dim = None
    multiple_times = False
    # use first input variable found
    for varname in ctx.obj['vin']:
        if varname in ds.data_vars:
            break
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
    var_log.debug(f"Multiple time axis: {multiple_times}")
    del ds 
    return time_dim, multiple_times


@click.pass_context
def check_timestamp(ctx, all_files):
    """This function tries to guess the time coverage of a file based on its timestamp
    and return the files in range.

    Tries to detect timestamp in fileame by breaking it at [., _] and 
    matching possible date patterns.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    all_files : list(Path)
        List of Path obj for all files available

    Returns
    -------
    inrange : list(Path)
        List of Path obj for all files that have timstamp in
        [sel_start, sel_end]

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    inrange_files = []
    var_log.info("checking files timestamp ...")
    sel_start = ctx.obj['sel_start']
    sel_end = ctx.obj['sel_end']
    var_log.debug(f"sel_start, sel_end: {sel_start}, {sel_end}")
    #if we are using a time invariant parameter, just use a file with vin
    if 'fx' in ctx.obj['frequency']:
        inrange_files = [all_files[0]]
    else:
        # set potentially regex for dates in order of reliability
        # first group looks for at least a year starting with 0/1/2
        # second group is for year < 1000 where starting 0 is omitted
        rdates = [r"[0,1,2]\d{7}", r"[0,1,2]\d{5}",
            r"[0,1,2]\d{3}-d{2}-d{2}", r"[0,1,2]\d{3}-d{2}",
            r"[0,1,2]\d{3}", r"\d{7}", r"\d{5}", r"\d{3}-d{2}-d{2}",
            r"\d{3}-d{2}", r"\d{3}"]
        for infile in all_files:
            var_log.debug(f"infile: {infile}")
            inf = infile.name.replace('.','_')
            #inf = inf.replace('-','_')
            dummy = inf.split("_")
            var_log.debug(f"dummy: {dummy}")
            for d in reversed(dummy):
                pattern = [x for x in rdates if re.search(x, d)]
                if pattern != []:
                    break 
            if pattern == []:
                var_log.error(f"couldn't find timestamp for {infile}")
            tstamp = d.replace('-','')
            # to take into acocunt 3-5 hourly files from UM
            if tstamp[0:2] in ['p7', 'p8']:
                tstamp = tstamp[2:]
            #var_log.debug(f"first tstamp: {tstamp}")
            # check if timestamp as the date time separator T
            hhmm = ''
            if 'T' in tstamp:
                tstamp, hhmm = tstamp.split('T')
            # if tstamp start with number assume is date
            if not tstamp[0].isdigit():
                tstamp = re.sub("\\D", "", tstamp)
            tlen = len(tstamp)
            if tlen != 8:
                if tlen in [3, 5, 7] :
                    #assume year is yyy and 0
                    tstamp = '0' + tstamp
                if len(tstamp) == 4:
                    sel_start = sel_start[:4]
                    sel_end = sel_end[:4]
                elif len(tstamp) == 6:
                    sel_start = sel_start[:6]
                    sel_end = sel_end[:6]
            else:
            # if hhmm were present add them back to tstamp otherwise as 0000 
            #tstamp = tstamp + hhmm.ljust(4,'0')
                tstamp = tstamp + hhmm
                if len(tstamp) == 8:
                    sel_start = sel_start[:8]
                    sel_end = sel_end[:8]
            var_log.debug(f"tstamp for {inf}: {tstamp}")
            var_log.debug(f"sel start, end {sel_start}, {sel_end}")
            if sel_start <= tstamp <= sel_end:
                inrange_files.append(infile)
                var_log.debug("file selected")
    return inrange_files

 
@click.pass_context
def check_timeaxis(ctx, all_files, tdim):
    """Returns a list of files in time range.

    Opens each file and check based on time axis.
    This function is called only if check_timestamp fails or
    if multiple time axes are present.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    all_files : list()
        All files paths matching a specific pattern
    tdim : str
        Name of time dimension associated with datasets

    Returns
    -------
    inrange_files : list
        Files that contain data in desired time range

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    inrange_files = []
    var_log.info("loading files...")
    var_log.debug(f"time dimension: {tdim}")
    # switching to sel_start, sel_end because of unexpected values
    # for example UM aus2200 1 hourly saved at 01 min instead of 00
    tstart = ctx.obj['sel_start'].replace('T','')
    tend = ctx.obj['sel_end'].replace('T','')
    var_log.debug(f"tstart, tend from opts: {tstart}, {tend}")
    if 'fx' in ctx.obj['table']:
        inrange_files = [all_files[0]]
    else:
        for input_file in all_files:
            try:
                ds = xr.open_dataset(input_file, use_cftime=True)
            except Exception as e:
                var_log.error(f"Cannot open file: {input_file} - {e}")
                continue
            # If file has multiple time axes, it's possible they not all present 
            # in all the files
            if tdim not in ds.dims:
                continue
            # get first and last values as date string
            tmin = ds[tdim][0].dt.strftime('%4Y%m%d%H%M')
            tmax = ds[tdim][-1].dt.strftime('%4Y%m%d%H%M')
            var_log.debug(f"tmin, tmax from time dim: {str(tmin.values)}, {str(tmax.values)}")
            if not(tmin > tend or tmax < tstart):
                inrange_files.append(input_file)
            del ds
    var_log.debug(f"Number of files in time range: {len(inrange_files)}")
    var_log.info("Found all the files...")
    return inrange_files


@click.pass_context
def load_data(ctx, path_vars):
    """Returns a dictionary listing open ds obj for each input var

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    path_vars : dict

    Returns
    -------
    input_ds : dict
        Dictionary {input-var1: xarray dataset, ..}

    """
    # preprocessing to select only variables we need to avoid
    # concatenation issues with multiple coordinates
    # temporarily opening file without decoding times, fixing
    # faulty time bounds units and decoding times
    # this is to prevent issues with ocean files
    var_log = logging.getLogger(ctx.obj['var_log'])
    input_ds = {}
    first = ctx.obj['vin'][0]
    for k,v in path_vars.items():
        var_log.debug(f"load_data: pattern & paths: {k}, {v['files']}")
        var_log.debug(f"load_data: path_vars vars: {v['vars']}")
        preselect = partial(_preselect, varlist=v['vars'])
        dsin = xr.open_mfdataset(v['files'], preprocess=preselect,
            parallel=True, decode_times=False)
        if 'tdim' not in v.keys():
            tdim, multiple_times = get_time_dim(dsin) 
        else:
            tdim = v['tdim']
        # Get the units and other attrs of first variable
        if first in v['vars']:
            var_log.debug(f"load_data: getting attrs for {first}")
            in_units, in_missing, positive, coords = get_attrs(dsin,
                first)
        dsin = xr.decode_cf(dsin, use_cftime=True)
        if tdim is not None and 'fx' not in ctx.obj['frequency']:
            var_log.debug(f"load_data: slicing time {tdim}")
            dsin = dsin.sel({tdim: slice(ctx.obj['tstart'],
                ctx.obj['tend'])})
        for field in v['vars']:
            var_log.debug(f"load_data, var & path: {field}, {v['vars']}")
            input_ds[field] = dsin
    return input_ds, in_units, in_missing, positive, coords
 

@click.pass_context
def generic_name(ctx, aname, orig, cnames):
    """Get cmor name for z axes with generic name

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    aname : str
        Name of variable dimension 
    orig : str
        Cmor name for variable dimension to use to define cmor axis
    cnames : list
        List of possible cmor names for generic specified axis

    Returns
    -------
    cmor_name : str
        Cmor name for variable dimension to use to define cmor axis
      
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"generic_name axis name: {aname}")
    # get list of possible names for generic level
    cmor_name = orig
    if orig == "olevel":
        if aname in ["st_ocean", "sw_ocean"]:
            cmor_name = "depth_coord"
    elif orig == "alevel":
        if any(x in aname for x in ["theta_level_height", "rho_level_height"]):
            cmor_name = "hybrid_height2"
        elif "level_number" in aname:
            cmor_name = "hybrid_height"
    elif orig == "alevhalf":
        if any(x in aname for x in ["rho_level_number", "rho_level_height"]):
            cmor_name = "hybrid_height_half"
    if cmor_name == orig:
        var_log.error(f"""cmor name for axis {aname} and
            {cmor_name} not yet defined. Use correct cmor name
            in map file as temporary solution and open an issue""")
        raise MopException("cmor name not defined for generic axis")
    if cmor_name not in cnames:
        var_log.warning(f"{cmor_name} not in axes_names.yaml file")
    return cmor_name


@click.pass_context
def get_cmorname(ctx, axis_name):
    """Get cmor name for axes based on their name, cmor var definition
    and list of defined axes in cmor coordinate file.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    axis_name : str
        Name of variable dimension 

    Returns
    -------
    cmor_name : str
        Cmor name for variable dimension to use to define cmor axis

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"get_cmorname axis_name: {axis_name}")
    generic_axes = ["alevel", "alevhalf", "olevel", "olevhalf"]
    names = ctx.obj['axes'].split()
    cmor_name = []
    if axis_name in ['time', 'lat', 'lon', 'gridlat']:
        cmor_name = [x for x in names if axis_name in x]
    else:
        fname = import_files('mopdata').joinpath('axes_names.yaml')
        data = read_yaml(fname)
        if axis_name == 'p':
           cnames = data['pseudo_axes']
        else:
           cnames = data['Z_axes']
           # add specific names for generic axes
           cnames.extend([v for x in generic_axes for v in data[x]])
        var_log.debug(f"{cnames}")
        var_log.debug(f"{names}")
        cmor_name = [x for x in names if x in cnames]
    if cmor_name == []:
        cmor_name = None
        var_log.warning(f"Cannot detect cmor name for {axis_name}")
    else:
        cmor_name = cmor_name[0]
        if cmor_name in generic_axes:
            cmor_name = generic_name(axis_name, cmor_name, data[cmor_name])
    var_log.debug(f"Cmor name for axis {axis_name}: {cmor_name}")
    return cmor_name


#PP this should eventually just be generated directly by defining the dimension using the same terms 
# in calculation for meridional overturning
@click.pass_context
def create_axis(ctx, axis, table):
    """
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
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


@click.pass_context
def hybrid_axis(ctx, lev, z_ax_id, z_ids):
    """Setting up additional hybrid axis information
     PP this needs fixing can't possible work now without b_vals, b_bnds??
    lev is cmor_zName?

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    #var_log = logging.getLogger(ctx.obj['var_log'])
    hybrid_dict = {'hybrid_height': 'b',
                   'hybrid_height_half': 'b_half'}
    orog = get_ancil_var('orog','fld_s00i033')
    orog_vals = orog.isel(time=0).values
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
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"ij_axis: ax is {ax}")
    cmor.set_table(table)
    ax_id = cmor.axis(table_entry=ax_name,
        units='1',
        coord_vals=ax.values)
    return ax_id


@click.pass_context
def ll_axis(ctx, ax, ax_name, ds, table, bounds_list):
    """
    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug("in ll_axis")
    cmor.set_table(table)
    cmor_aName = get_cmorname(ax_name)
    ax_units = ax.attrs.get('units', 'degrees')
    a_bnds = None
    var_log.debug(f"found cmor name: {cmor_aName}")
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

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
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

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    # open ancil grid file to read vertices
    #PP be careful this is currently hardcoded which is not ok!
    ancil_dir = ctx.obj.get('ancils_path', '')
    ancil_file = ancil_dir + "/" + ctx.obj.get(f"grid_{ctx.obj['realm']}", '')
    if ancil_file == '' or not Path(ancil_file).exists():
        var_log.error(f"Ancil file {ancil_file} not set or inexistent")
        raise MopException(f"Ancil file {ancil_file} not set or inexistent")
    var_log.debug(f"getting lat/lon and bnds from ancil file: {ancil_file}")
    ds = xr.open_dataset(ancil_file)
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
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    axes = {'t_ax': None, 'z_ax': None, 'glat_ax': None,
            'lat_ax': None, 'lon_ax': None, 'j_ax': None,
            'i_ax': None, 'p_ax': [], 's_ax': []}
    axes_str = ""
    for dim in var.dims:
        if dim in var.coords:
            axis = var[dim]
            var_log.debug(f"axis found: {dim}")
        else:
            var_log.warning(f"No coordinate variable associated with the dimension {dim}")
            # have to add this because a simulation didn't have the dimension variables
            if any(x in dim.lower() for x in ['nj', 'yu_ocean', 'yt_ocean']):
                axes['j_ax'] = var[dim]
                axes_str += f"j_ax: {dim}; "
            elif any(x in dim.lower() for x in ['ni', 'xu_ocean', 'xt_ocean']):
                axes['i_ax'] = var[dim]
                axes_str += f"i_ax: {dim}; "
            axis = None
        if axis is not None:
            attrs = axis.attrs
            axis_attr = attrs.get('axis', None)
            var_log.debug(f"trying axis attrs: {axis_attr}")
            axis_attr = attrs.get('cartesian_axis', axis_attr)
            var_log.debug(f"trying cart axis attrs: {axis_attr}, {axis.name}")
            if axis_attr == 'T' or 'time' in dim.lower():
                axes['t_ax'] = axis
                axes_str += f"t_ax: {axis.name}; "
            elif axis_attr and 'Y' in axis_attr:
                if dim.lower() == 'gridlat':
                    axes['glat_ax'] = axis
                    axes_str += f"glat_ax: {axis.name}; "
                elif 'lat' in dim.lower():
                    axes['lat_ax'] = axis
                    axes_str += f"lat_ax: {axis.name}; "
                elif any(x in dim.lower() for x in ['nj', 'yu_ocean', 'yt_ocean']):
                    axes['j_ax'] = axis
                    axes_str += f"j_ax: {axis.name}; "
            elif axis_attr and 'X' in axis_attr:
                if 'glon' in dim.lower():
                    axes['glon_ax'] = axis
                    axes_str += f"glon_ax: {axis.name}; "
                elif 'lon' in dim.lower():
                    axes['lon_ax'] = axis
                    axes_str += f"lon_ax: {axis.name}; "
                elif any(x in dim.lower() for x in ['ni', 'xu_ocean', 'xt_ocean']):
                    axes['i_ax'] = axis
                    axes_str += f"i_ax: {axis.name}; "
            elif axis_attr == 'Z' or any(x in dim for x in
                    ['lev', 'heigth', 'depth']):
                axes['z_ax'] = axis
                axes_str += f"z_ax: {axis.name}; "
            else:
                fname = import_files('mopdata').joinpath('axes_names.yaml')
                data = read_yaml(fname)
                snames = data['singleton_axes']
                var_log.debug(f"{snames}")
                if axis.name in data['singleton_axes']:
                    axes['s_ax'].append(axis) 
                    axes_str += f"s_ax: {axis.name}; "
                else:
                    axes['p_ax'].append(axis)
                    axes_str += f"p_ax: {axis.name}; "
                    if len(axis) == 1:
                        var_log.warning(
                        f"Axis 1 value but not singleton: {axis.name}")
    var_log.debug(f"Detected axes: {axes_str}")
    return axes


@click.pass_context
def check_time_bnds(ctx, bnds, frequency):
    """Checks if dimension boundaries from file are wrong.

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"Time bnds 1,0: {bnds[0:1,1], bnds[0:1,0]}")
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

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
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
def bounds_change(ctx, axis):
    """Returns True if calculation/resample changes bnds of specified
       dimension.

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    #var_log = logging.getLogger(ctx.obj['var_log'])
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

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    ds : Xarray Dataset
       The input xarray dataset
    axis : Xarray DataArray
    cmor_name: 
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    dim = axis.name
    var_log.info(f"Getting bounds for axis: {dim}")
    changed_bnds = bounds_change(axis) 
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
            var_log.debug(f"New {axis.name} bnds: {dim_bnds_val}")
        except Exception as e:
            var_log.warning(f"dodgy bounds for dimension: {dim}")
            var_log.error(f"error: {e}")
        if 'time' in cmor_name:
            inrange = check_time_bnds(dim_bnds_val, frq)
            if inrange is False:
                var_log.error(f"Boundaries for {cmor_name} are "
                    + "wrong even after calculation")
                raise MopException(f"Boundaries for {cmor_name} wrong")
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

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    ds : Xarray Dataset
        The input xarray dataset
    bname : str
        Bounds variable name

    """
    calc = False
    var_log = logging.getLogger(ctx.obj['var_log'])
    var_log.debug(f"Getting bounds values for {bname}")
    ancil_file =  ctx.obj.get(f"grid_{ctx.obj['realm']}", '')
    if bname in ds.variables:
        var_log.debug(f"Bounds for {bname} in file")
        bnds_val = ds[bname].values
    elif ancil_file != "":     
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
def get_attrs(ctx, ds, var1):
    """
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
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
def extract_var(ctx, input_ds, in_missing):
    """
    This function extracts the required variables from the Xarray dataset.
    If no calculation then it just returns the variables to be saved.
    If calculation, it evaluates the calculation and returns the result.
    If resample, executes after calcualtion step.
    Re-selects time range in case resample or other operations have
    introduced extra timesteps

    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
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
        var_log.info(f"Adding variables {ctx.obj['vin']} to var list")
        for v in ctx.obj['vin']:
            try:
                var_log.debug(f"trying to append {v}")
                var.append(input_ds[v][v][:])
            except Exception as e:
                failed = True
                var_log.debug(f"{[x.name for x in input_ds[v].variables]}")
                var_log.error(f"Error appending variable, {v}: {e}")
                raise MopException(f"Error appending variable, {v}: {e}")
        var_log.info("Finished adding variables to var list")

        # Now try to perform the required calculation
        try:
            array = eval(ctx.obj['calculation'])
            var_log.debug(f"Variable after calculation: {array}")
        except Exception as e:
            failed = True
            mop_log.info(f"error evaluating calculation, {ctx.obj['filename']}")
            var_log.error(f"error evaluating calculation, {ctx.obj['calculation']}: {e}")
            raise MopException(f"Error evaluating calculation: {e}")
    #Call to resample operation is defined based on timeshot
    tdims = [d for d in array.dims if 'time' in d]
    if len(tdims) >= 1:
         tdim = tdims[0]
    else:
        tdim = None
    if ctx.obj['resample'] != '':
        array = time_resample(array, ctx.obj['resample'], tdim,
            stats=ctx.obj['timeshot'])
        var_log.debug(f"Variable after resample: {array}")

    # STill need to check if this is needed, it probably is need for integer values but the others?
    if array.dtype.kind == 'i':
        try:
            in_missing = int(in_missing)
        except Exception as e:
            in_missing = int(-999)
    else:
        array = array.fillna(in_missing)
        var_log.debug(f"Variable after fillna: {array}")
    # Some ops (e.g., resample) might introduce extra tstep: select time range 
    if tdim is not None and 'fx' not in ctx.obj['frequency']:
        var_log.debug(f"{ctx.obj['tstart']}, {ctx.obj['tend']}")
        # add some tolerance to slice to avoid missing steps at higher frequencies
        tol = relativedelta(minutes=2)
        ts = datetime.strptime(ctx.obj['tstart'], '%Y%m%dT%H%M') - tol
        te = datetime.strptime(ctx.obj['tend'], '%Y%m%dT%H%M') + tol
        tstart = ts.strftime('%4Y%m%dT%H%M')
        tend = te.strftime('%4Y%m%dT%H%M') 
        var_log.debug(f"Before slicing: {array[tdim][0].values}, {array[tdim][-1].values}")
        var_log.debug(f"tstart and tend: {tstart}, {tend}")
        array = array.sel({tdim: slice(tstart,tend)})
        var_log.debug(f"After: {array[tdim][0].values}, {array[tdim][-1].values}")
    return array, failed


@click.pass_context
def define_attrs(ctx):
    """Returns all global attributes to be set up by CMOR after
    checking if there are notes to be added for a specific field.

    Notes are read from src/data/notes.yaml
    NB for calculation is checking only if name of function used is
    listed in notes file, this is indicated by precending any function
    in file with a ~. For other fields it checks equality.
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    attrs = ctx.obj['attrs'].copy()
    notes = attrs.get('notes', '')
    var_log.debug(f"in define_attrs, notes: {notes}")
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
                notes += f" {v} "
    if notes != '':
        attrs['notes'] = notes.strip()
    var_log.debug(f"in define_attrs, attrs: {attrs}")
    return attrs
