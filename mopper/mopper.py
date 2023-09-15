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

18/04/23
PP - complete restructure: now cli.py with cli_functions.py include functionality of both app.py and app_wrapper.py
     to run 
     python cli.py wrapper
     I'm not yet sure if click is best used here, currently not using the args either (except for debug) but I'm leaving them in just in case
     Still using pool, mop_bulk() contains most of the all mop() function, however I generate many "subfunctions" mostly in cli_functions.py to avoid having a huge one. What stayed here is the cmor settings and writing
     using xarray to open all files, passing sometimes dataset sometime variables this is surely not consistent yet with app_functions

07/07/23 using logging for var_logs
To flush var_log explicitly:
    #var_log.handlers[0].flush()
     
'''


import os,sys
import warnings
import logging
import time as timetime
import traceback
#import multiprocessing as mp
import csv
import yaml
import ast
import calendar
import click
import sqlite3
import numpy as np
import xarray as xr
import cmor
from itertools import repeat
from mopper_utils import *
from mopper_utils import _preselect 
import concurrent.futures

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

#
#main function to post-process files
#
def mopper_catch():
    debug_logger = logging.getLogger('mop_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        mop()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--infile', '-i', type=str, required=True, 
                help='Input yaml file with experiment information')
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def mop(ctx, infile, debug):
    """Wrapper setup
    """
    with open(infile, 'r') as yfile:
        cfg = yaml.safe_load(yfile)

    ctx.obj = cfg['cmor']
    ctx.obj['attrs'] = cfg['attrs']
    # set up main mop log
    ctx.obj['log'] = config_log(debug, ctx.obj['mop_logs'])
    ctx.obj['debug'] = debug
    mop_log = ctx.obj['log']
    mop_log.info("\nstarting mop_wrapper...")

    mop_log.info(f"local experiment being processed: {ctx.obj['exp']}")
    mop_log.info(f"cmip6 table being processed: {ctx.obj['tables']}")
    mop_log.info(f"cmip6 variable being processed: {ctx.obj['variable_to_process']}")



@mop.command(name='wrapper')
@click.pass_context
def mop_wrapper(ctx):
    """Main method to select and process variables
    """
    mop_log = ctx.obj['log']
    #open database    
    conn=sqlite3.connect(ctx.obj['database'], timeout=200.0)
    conn.text_factory = str
    cursor = conn.cursor()

    #process only one file per mp process
    cursor.execute("select *,ROWID  from file_master where " +
        f"status=='unprocessed' and local_exp_id=='{ctx.obj['exp']}'")
    #fetch rows
    try:
       rows = cursor.fetchall()
    except:
       mop_log.info("no more rows to process")
    conn.commit()
    #process rows
    mop_log.info(f"number of rows: {len(rows)}")
    results = pool_handler(rows, ctx.obj['ncpus'])
    mop_log.info("mop_wrapper finished!\n")
    #summarise what was processed:
    mop_log.info("RESULTS:")
    for r in results:
        mop_log.info(r)


@click.pass_context
def mop_bulk(ctx, mop_log, var_log):
    start_time = timetime.time()
    var_log.info("starting main mop function...")
    default_cal = "gregorian"
    #
    cmor.setup(inpath=ctx.obj['tpath'],
        netcdf_file_action = cmor.CMOR_REPLACE_4,
        set_verbosity = cmor.CMOR_NORMAL,
        exit_control = cmor.CMOR_NORMAL,
        #exit_control=cmor.CMOR_EXIT_ON_MAJOR,
        logfile = f"{ctx.obj['cmor_logs']}/log", create_subdirectories=1)
    #
    #Define the dataset.
    #
    cmor.dataset_json(ctx.obj['json_file_path'])
    #
    for k,v in ctx.obj['attrs'].items():
        cmor.set_cur_dataset_attribute(k, v)
        
    #
    #Load the CMIP tables into memory.
    #
    tables = []
    tables.append(cmor.load_table(f"{ctx.obj['tpath']}/{ctx.obj['grids']}"))
    tables.append(cmor.load_table(f"{ctx.obj['tpath']}/{ctx.obj['table']}.json"))
    #
    #PP This now checks that input variables are available from listed paths if not stop execution
    # if they are all available pass list of vars in eachh pattern
    all_files, path_vars = find_files(var_log)

    # PP FUNCTION END return all_files, extra_files
    var_log.debug(f"access files from: {os.path.basename(all_files[0][0])}" +
                 f"to {os.path.basename(all_files[0][-1])}")
    ds = xr.open_dataset(all_files[0][0], decode_times=False)
    time_dim, inref_time, multiple_times = get_time_dim(ds, var_log)
    del ds
    #
    #Now find all the ACCESS files in the desired time range (and neglect files outside this range).
    # for each of the file patterns passed
    # First try to do so based on timestamp on file, if this fails
    # open files and read time axis
    # if file has more than 1 time axis it's safer to actually open the files
    # as timestamp might refer to only one file
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
            mop_log.warning(f"no data in requested time range for: {ctx.obj['filename']}")
            var_log.warning(f"no data in requested time range for: {ctx.obj['filename']}")
            return 0
    # open main input dataset and optional extra if one or more input variables in different files
    input_ds = load_data(inrange_files, path_vars, time_dim, var_log)
    #First try and get the units of first variable.
    #
    var1 = ctx.obj['vin'][0]
    dsin = input_ds[var1]
    in_units, in_missing, positive = get_attrs(dsin[var1], var_log) 

    var_log.info("writing data, and calculating if needed...")
    var_log.info(f"calculation: {ctx.obj['calculation']}")
    #
    #PP start from standard case and add modification when possible to cover other cases 
    #
    #if 'A10dayPt' in ctx.obj['table']:
    #    var_log.info("ONLY 1st, 11th, 21st days to be used")
    #    dsin = dsin.where(dsin[time_dim].dt.day.isin([1, 11, 21]),
    #                      drop=True)
    
    # Perform the calculation:
    try:
        out_var = extract_var(input_ds, time_dim, in_missing, mop_log, var_log)
        var_log.info("Calculation completed!")
    except Exception as e:
        mop_log.error(f"E: Unable to run calculation for {ctx.obj['filename']}")
        var_log.error(f"E: Unable to run calculation because: {e}")
    # Some operations like resample might introduce previous/after day data so trim before writing 
    var_log.info(f"{ctx.obj['tstart']}, {ctx.obj['tend']}")
    out_var = out_var.sel({time_dim: slice(ctx.obj['tstart'], ctx.obj['tend'])})
    var_log.info(f"{out_var[time_dim][0].values}, {out_var[time_dim][-1].values}")


    #calculate time integral of the first variable (possibly adding a second variable to each time)
    # PP I removed all the extra special calculations
    # adding axis etc after calculation will need to extract cmor bit from calc_... etc
    var_log.info("defining axes...")
    # get axis of each dimension
    var_log.debug(f"Var after calculation: {out_var}")
    # get list of coordinates thta require bounds
    bounds_list = require_bounds()
    var_log.debug(f"{bounds_list}")
    t_axis, z_axis, j_axis, i_axis, p_axis, e_axis = get_axis_dim(
        out_var, var_log)
    # should we just calculate at end??
    # PP not sure if we use this anymore
    n_grid_pnts = 1
    cmor.set_table(tables[1])
    axis_ids = []
    if t_axis is not None:
        cmor_tName = get_cmorname('t', t_axis, var_log)
        ctx.obj['reference_date'] = f"days since {ctx.obj['reference_date']}"
        t_axis_val = cftime.date2num(t_axis, units=ctx.obj['reference_date'],
            calendar=ctx.obj['attrs']['calendar'])
        t_bounds = None
        if cmor_tName in bounds_list:
            t_bounds = get_bounds(dsin, t_axis, cmor_tName,
                var_log, ax_val=t_axis_val)
        t_axis_id = cmor.axis(table_entry=cmor_tName,
            units=ctx.obj['reference_date'],
            length=len(t_axis),
            coord_vals=t_axis_val,
            cell_bounds=t_bounds,
            interval=None)
        axis_ids.append(t_axis_id)
    # possibly some if these don't need boundaries make sure that z_bounds None is returned
    if z_axis is not None:
        cmor_zName = get_cmorname('z', z_axis, var_log)
        var_log.debug(cmor_zName)
        z_bounds = None
        if cmor_zName in bounds_list:
            z_bounds = get_bounds(dsin, z_axis, cmor_zName, var_log)
        z_axis_id = cmor.axis(table_entry=cmor_zName,
            units=z_axis.units,
            length=len(z_axis),
            coord_vals=z_axis.values,
            cell_bounds=z_bounds,
            interval=None)
        axis_ids.append(z_axis_id)
        #set up additional hybrid coordinate information
        if cmor_zName in ['hybrid_height', 'hybrid_height_half']:
            zfactor_b_id, zfactor_orog_id = hybrid_axis(lev_name, var_log)
    if j_axis is None or i_axis.ndim == 2:
           j_axis_id = cmor.axis(table=tables[0],
               table_entry='j_index',
               units='1',
               coord_vals=np.arange(len(dim_values)))
           axis_ids.append(j_axis_id)
       #             n_grid_pts=len(dim_values)
    else:
        cmor_jName = get_cmorname('j', j_axis, var_log)
        var_log.debug(cmor_jName)
        j_bounds = None
        if cmor_jName in bounds_list:
            j_bounds = get_bounds(dsin, j_axis, cmor_jName, var_log)
        j_axis_id = cmor.axis(table_entry=cmor_jName,
            units=j_axis.units,
            length=len(j_axis),
            coord_vals=j_axis.values,
            cell_bounds=j_bounds,
            interval=None)
        axis_ids.append(j_axis_id)
    #    n_grid_pts = n_grid_pts * len(j_axis)
    if i_axis is None or i_axis.ndim == 2:
        setgrid = True
        i_axis_id = cmor.axis(table=tables[0],
             table_entry='i_index',
             units='1',
             coord_vals=np.arange(len(i_axis)))
        axis_ids.append(i_axis_id)
    else:
        setgrid = False
        cmor_iName = get_cmorname('i', i_axis, var_log)
        var_log.debug(cmor_iName)
        i_bounds = None
        if cmor_iName in bounds_list:
            i_bounds = get_bounds(dsin, i_axis, cmor_iName, var_log)
        i_axis_id = cmor.axis(table_entry=cmor_iName,
            units=i_axis.units,
            length=len(i_axis),
            coord_vals=np.mod(i_axis.values,360),
            cell_bounds=i_bounds,
            interval=None)
        axis_ids.append(i_axis_id)
        #n_grid_pts = n_grid_pts * len(j_axis)
    if p_axis is not None:
        cmor_pName, p_vals, p_len = pseudo_axis(p_axis) 
        p_axis_id = cmor.axis(table_entry=cmor_pName,
            units='',
            length=p_len,
            coord_vals=p_vals)
        axis_ids.append(p_axis_id)
    if e_axis is not None:
        e_axis_id = create_axis(axm, tables[1], var_log) 
        axis_ids.append(e_axis_id)
    var_log.debug(axis_ids)

    #If we are on a non-cartesian grid, Define the spatial grid
    if setgrid:
        grid_id = define_grid(i_axis_id, i_axis, j_axis_id, j_axis, tables[0], var_log)
    #PP need to find a different way to make this happens
    #create oline, siline, basin axis
    #for axm in ['oline', 'siline', 'basin']:
    #    if axm in ctx.obj['axes_modifier']:
    #        axis_id = create_axis(axm, tables[1], var_log)
    #        axis_ids.append(axis_id)

    # freeing up memory 
    del dsin
    #
    #Define the CMOR variable.
    #
    var_log.debug(f"cmor axis variables: {axis_ids}")
    #
    #Define the CMOR variable, taking account of possible direction information.
    #
    var_log.info("defining cmor variable...")
    try:    
        #set positive value from input variable attribute
        #PP potentially check somewhere that variable_id is in table
        cmor.set_table(tables[1])
        var_id = ctx.obj['variable_id'].replace('_','-')
        variable_id = cmor.variable(table_entry=var_id,
                    units=in_units,
                    axis_ids=axis_ids,
                    data_type='f',
                    missing_value=in_missing,
                    positive=positive)
    except Exception as e:
        mop_log.error(f"Unable to define the CMOR variable {ctx.obj['filename']}")
        var_log.error(f"Unable to define the CMOR variable {e}")
    var_log.info('writing...')
    #PP trying to remove ntimes_passed as it causes issues with plev variables
    # It is optional but haven't tested yet variable without time
    status = None
    #if time_dim != None:
    var_log.info(f"Variable shape is {out_var.shape}")
    status = cmor.write(variable_id, out_var.values)
    #else:
    #    status = cmor.write(variable_id, out_var.values, ntimes_passed=0)
    if status != 0:
        mop_log.error(f"Unable to write the CMOR variable: {ctx.obj['filename']}\n")
        var_log.error(f"Unable to write the CMOR variable to file\n"
                      + f"See cmor log, status: {status}")
    #
    #Close the CMOR file.
    #
    var_log.info(f"finished writing @ {timetime.time()-start_time}")
    path = cmor.close(variable_id, file_name=True)
    return path

#
#function to process set of rows in the database
#if override is true, write over files that already exist
#otherwise they will be skipped
#
#PP not sure if better passing dreq_years with context or as argument
@click.pass_context
def process_row(ctx, row, var_log):
    mop_log = ctx.obj['log']
    #set version number
    #set location of cmor tables
    cmip_table_path = ctx.obj['tpath']
    
    row['vin'] = row['vin'].split()
    # check that calculation is defined if more than one variable is passed as input
    if len(row['vin'])>1 and row['calculation'] == '':
        mop_log.error("Multiple input variables are given without a "
            + "description of the calculation: {ctx.obj['filename']}")
        var_log.error("Multiple input variables are given without a "
            + "description of the calculation")
        return -1
    row['notes'] = f"Local exp ID: {row['local_exp_id']}; Variable: {row['variable_id']} ({row['vin']})"
    row['exp_description'] = ctx.obj['attrs']['exp_description']
    #
    var_log.info("\n#---------------#---------------#---------------#---------------#\nprocessing row with details:\n")
    for k,v in row.items():
        ctx.obj[k] = v
        var_log.info(f"{k}= {v}")
    #
    try:
        #Do the processing:
        #
        expected_file = f"{row['filepath']}/{row['filename']}"
        successlists = ctx.obj['success_lists']
        var_msg = f"{row['table']},{row['variable_id']},{row['tstart']},{row['tend']}"
        #if file doesn't already exist (and we're not overriding), run the mop
        if ctx.obj['override'] or not os.path.exists(expected_file):
            #
            #version_number = f"v{version}"
            #process the file,
            ret = mop_bulk(mop_log, var_log)
            try:
                os.chmod(ret,0o644)
            except:
                pass
            var_log.info("\nreturning to mop_wrapper...")
            #
            #check different return codes from the APP. 
            #
            if ret == 0:
                msg = f"\ndata incomplete for variable: {row['variable_id']}\n"
                #PP temporarily commenting this
                #with open(ctx.obj['database_updater'],'a+') as dbu:
                #    dbu.write(f"setStatus('data_Unavailable',{rowid})\n")
                #dbu.close()
            elif ret == -1:
                msg = "\nreturn status from the APP shows an error\n"
                #with open(['database_updater'],'a+') as dbu:
                #    dbu.write(f"setStatus('unknown_return_code',{rowid})\n")
                #dbu.close()
            else:
                insuccesslist = 0
                with open(f"{successlists}/{ctx.obj['exp']}_success.csv",'a+') as c:
                    reader = csv.reader(c, delimiter=',')
                    for line in reader:
                        if (line[0] == row['table'] and line[1] == row['variable_id'] and
                            line[2] == row['tstart'] and line[3] == row['tend']):
                            insuccesslist = 1
                        else: 
                            pass
                    if insuccesslist == 0:
                        c.write(f"{var_msg},{ret}\n")
                        mop_log.info(f"added \'{var_msg},...\'" +
                              f"to {successlists}/{ctx.obj['exp']}_success.csv")
                    else:
                        pass
                c.close()
                #Assume processing has been successful
                #Check if output file matches what we expect
                #
                var_log.info(f"output file:   {ret}")
                if ret == expected_file:
                    var_log.info(f"expected and cmor file paths match")
                    msg = f"\nsuccessfully processed variable: {var_msg}\n"
                    #modify file permissions to globally readable
                    #oos.chmod(ret, 0o493)
                    #PP temporarily commenting this
                    #with open(ctx.obj['database_updater'],'a+') as dbu:
                    #    dbu.write(f"setStatus('processed',{rowid})\n")
                    #dbu.close()
                    #plot variable
                    #try:
                    #    if plot:
                    #        plotVar(outpath,ret,table,vcmip,source_id,experiment_id)
                    #except: 
                    #    msg = f"{msg},plot_fail: "
                    #    traceback.print_exc()
                else :
                    var_log.info(f"expected file: {expected_file}")
                    var_log.info("expected and cmor file paths do not match")
                    msg = f"\nproduced but file name does not match expected {var_msg}\n"
                    #PP temporarily commenting this
                    #with open(ctx.obj['database_updater'],'a+') as dbu:
                    #    dbu.write(f"setStatus('file_mismatch',{rowid})\n")
                    #dbu.close()
        else :
            #
            #we are not processing because the file already exists.     
            #
            msg = f"\nskipping because file already exists for variable: {var_msg}\n"
            var_log.info(f"file: {expected_file}")
            #PP temporarily commenting this
            #with open(ctx.obj['database_updater'],'a+') as dbu:
            #    dbu.write(f"setStatus('processed',{rowid})\n")
            #dbu.close()
    except Exception as e: #something has gone wrong in the processing
        mop_log.error(e)
        traceback.print_exc()
        infailedlist = 0
        with open(f"{successlists}/{ctx.obj['exp']}_failed.csv",'a+') as c:
            reader = csv.reader(c, delimiter=',')
            for line in reader:
                if (line[0] == row['variable_id'] and line[1] == row['table']
                    and line[2] == row['tstart'] and line[3] == row['tend']):
                    infailedlist = 1
                else:
                    pass
            if infailedlist == 0:
                c.write(f"{var_msg}\n")
                mop_log.info(f"added '{var_msg}' to {successlists}/{ctx.obj['exp']}_failed.csv")
            else:
                pass
        c.close()
        msg = f"\ncould not process file for variable: {var_msg}\n"
        #PP temporarily commenting this
        #with open(ctx.obj['database_updater'],'a+') as dbu:
        #    dbu.write(f"setStatus('processing_failed',{rowid})\n")
        #dbu.close()
    mop_log.info(msg)
    return msg


@click.pass_context
def process_experiment(ctx, row):
    record = {}
    header = ['infile', 'filepath', 'filename', 'vin', 'variable_id',
              'table', 'frequency', 'realm', 'timeshot', 'tstart',
              'tend', 'sel_start', 'sel_end', 'status', 'file_size',
              'local_exp_id', 'calculation', 'resample', 'in_units',
              'positive', 'cfname', 'source_id', 'access_version',
              'json_file_path', 'reference_date', 'version']  
    for i,val in enumerate(header):
        record[val] = row[i]
    table = record['table'].split('_')[1]
    # call logging 
    trange = record['filename'].replace('.nc.','').split("_")[-1]
    varlog_file = (f"{ctx.obj['var_logs']}/{record['variable_id']}"
                 + f"_{record['table']}_{record['tstart']}-"
                 + f"{record['tend']}.txt")
    var_log = config_varlog(ctx.obj['debug'], varlog_file) 
    ctx.obj['var_log'] = var_log 
    var_log.info(f"process: {os.getpid()}")
    t1=timetime.time()
    var_log.info(f"start time: {timetime.time()-t1}")
    var_log.info(f"processing row:")
    msg = process_row(record, var_log)
    var_log.info(f"end time: {timetime.time()-t1}")
    var_log.handlers[0].close()
    var_log.removeHandler(var_log.handlers[0])
    return msg


@click.pass_context
def pool_handler(ctx, rows, ncpus):
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=ncpus)
    result_futures = []
    for row in rows:
    # Using submit with a list instead of map lets you get past the first exception
    # Example: https://stackoverflow.com/a/53346191/7619676
        future = executor.submit(process_experiment, row)
        result_futures.append(future)

    # Wait for all results
    concurrent.futures.wait(result_futures)

# After a segfault is hit for any child process (i.e. is "terminated abruptly"), the process pool becomes unusable
# and all running/pending child processes' results are set to broken
    for future in result_futures:
        try:
            print(future.result())
        except concurrent.futures.process.BrokenProcessPool:
            print("broken")
    return result_futures


if __name__ == "__main__":
    mop()
