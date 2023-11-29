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
# Github: https://github.com/ACCESS-Hive/ACCESS-MOPPeR
#
# last updated 07/11/2023


import click
import logging
import sqlite3
import concurrent.futures
import os,sys
import warnings
import yaml
import cmor
import numpy as np
import xarray as xr

from mopper.mop_utils import *
from mopper.mop_setup import *
from mopdb.mopdb_utils import db_connect, create_table, query

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)


def mop_catch():
    debug_logger = logging.getLogger('mop_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        mop()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--cfile', '-c', type=str, required=True, 
                help='Experiment configuration as yaml file')
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def mop(ctx, cfile, debug):
    """Main command with 2 sub-commands:
    - setup to setup the job to run
    - run to execute the post-processing

    Parameters
    ----------
    ctx : obj
        Click context object
    cfile : str
        Name of yaml configuration file, run sub-command uses the 
        configuration created by setup
    debug : bool
        If true set logging level to debug
    """
    with open(cfile, 'r') as yfile:
        cfg = yaml.safe_load(yfile)
    ctx.obj = cfg['cmor']
    ctx.obj['attrs'] = cfg['attrs']
    # set up main mop log
    ctx.obj['log'] = config_log(debug, ctx.obj['appdir'])
    ctx.obj['debug'] = debug
    mop_log = ctx.obj['log']
    mop_log.info(f"Simulation to process: {ctx.obj['exp']}")


@mop.command(name='run')
@click.pass_context
def mop_run(ctx):
    """Subcommand that executes the processing.

    Use the configuration yaml file created in setup step as input.
    """
    mop_log = ctx.obj['log']
    # Open database and retrieve list of files to create
    conn = db_connect(ctx.obj['database'], mop_log)
    sql = f"""select *,ROWID  from filelist where
        status=='unprocessed' and exp_id=='{ctx.obj['exp']}'"""
    rows = query(conn, sql, first=False)
    if len(rows) == 0:
       mop_log.info("no more rows to process")
    # Set up pool handlers to create each file as a separate process
    mop_log.info(f"number of rows: {len(rows)}")
    results = pool_handler(rows, ctx.obj['ncpus'])
    mop_log.info("mop run finished!\n")
    # Summary or results and update status in db:
    mop_log.info("RESULTS:")
    for r in results:
        mop_log.info(r[0])
        cursor.execute("UPDATE filelist SET status=? WHERE rowid=?",[r[2],r[1]])
        conn.commit()
    return


@mop.command(name='setup')
@click.pass_context
def mop_setup(ctx):
    """Setup of mopper processing job and working environment.

    * Sets and creates paths
    * updates CV json file if necessary
    * selects variables and corresponding mappings based on table
      and constraints passed in config file
    * creates/updates database filelist table to list files to create
    * finalises configuration and save in new yaml file
    * writes job executable file and submits (optional) to queue
    """
    mop_log = ctx.obj['log']
    # then add setup_env to config
    ctx = setup_env()
    #cdict = config['cmor']
    manage_env()
    #json_cv = f"{cdict['outpath']}/{cdict['_control_vocabulary_file']}"
    json_cv = f"{ctx.obj['tpath']}/CMIP6_CV.json"
    fname = create_exp_json(json_cv)
    ctx.obj['json_file_path'] = fname
    if ctx.obj['mode'] == 'cmip6':
        edit_json_cv(json_cv, ctx.obj['attrs'])
        ctx = var_map(ctx.obj['attrs']['activity_id'])
    else:
        ctx = var_map()
    # setup database table
    database = ctx.obj['database']
    mop_log.info(f"creating & using database: {database}")
    conn = db_connect(database, mop_log)
    table_sql = filelist_sql()
    create_table(conn, table_sql, mop_log)
    populate_db(conn)
    nrows = count_rows(conn, ctx.obj['exp'], mop_log)
    tot_size = sum_file_sizes(conn)
    mop_log.info(f"Estimated total files size before compression is: {tot_size} GB")
    #write app_job.sh
    ctx = write_job(nrows)
    mop_log.info(f"app job script: {ctx.obj['app_job']}")
    # write setting to yaml file to pass to `mop run`
    fname = f"{ctx.obj['exp']}_config.yaml"
    mop_log.info("Exporting config data to yaml file")
    config = {}
    config['cmor'] = ctx.obj
    config['attrs'] = config['cmor'].pop('attrs')
    config['cmor'].pop('log')
    write_yaml(config, fname)
    #submit job
    if ctx.obj['test'] is False:
        os.chmod(ctx.obj['app_job'], 775)
        status = subprocess.run(f"qsub {ctx.obj['app_job']}", shell=True)
        if status.returncode != 0:
            mop_log.error(f"{ctx.obj['app_job']} submission failed, " +
                f"returned code is {status.returncode}.\n Try manually")
    return


@click.pass_context
def mop_process(ctx, mop_log, var_log):
    """Main processing workflow

    Sets up CMOR dataset, tables and axis. Extracts and/or calculates variable and 
    write to file using CMOR.
    Returns path of created file if successful or error code if not.
    """

    default_cal = "gregorian"
    logname = f"{ctx.obj['variable_id']}_{ctx.obj['table']}_{ctx.obj['tstart']}"
    
    # Setup CMOR
    cmor.setup(inpath=ctx.obj['tpath'],
        netcdf_file_action = cmor.CMOR_REPLACE_4,
        set_verbosity = cmor.CMOR_NORMAL,
        exit_control = cmor.CMOR_NORMAL,
        #exit_control=cmor.CMOR_EXIT_ON_MAJOR,
        logfile = f"{ctx.obj['cmor_logs']}/{logname}", create_subdirectories=1)
    
    # Define the CMOR dataset.
    cmor.dataset_json(ctx.obj['json_file_path'])
    # Pass all attributes from configuration to CMOR dataset
    for k,v in ctx.obj['attrs'].items():
        cmor.set_cur_dataset_attribute(k, v)
        
    #Load the CMIP/custom tables
    tables = []
    tables.append(cmor.load_table(f"{ctx.obj['tpath']}/{ctx.obj['grids']}"))
    tables.append(cmor.load_table(f"{ctx.obj['tpath']}/{ctx.obj['table']}.json"))

    # Select files to use and associate a path to each input variable
    inrange_files, path_vars, time_dim = get_files(var_log)

    # Open input datasets based on input files, return dict= {var: ds}
    dsin = load_data(inrange_files, path_vars, time_dim, var_log)

    #Get the units and other attrs of first variable.
    var1 = ctx.obj['vin'][0]
    in_units, in_missing, positive = get_attrs(dsin[var1], var_log) 

    # Extract variable and calculation:
    var_log.info("Loading variable and calculating if needed...")
    var_log.info(f"calculation: {ctx.obj['calculation']}")
    var_log.info(f"resample: {ctx.obj['resample']}")
    try:
        ovar, failed = extract_var(dsin, time_dim, in_missing, mop_log, var_log)
        var_log.info("Calculation completed.")
    except Exception as e:
        mop_log.error(f"E: Unable to extract var for {ctx.obj['filename']}")
        var_log.error(f"E: Unable to extract var because: {e}")
        return 1
    if failed is True:
        var_log.error("Calculation failed.")
        return 1

    # Define axis and variable for CMOR
    var_log.info("Defining axes...")
    # get list of coordinates that require bounds
    bounds_list = require_bounds(var_log)
    # get axis of each dimension
    t_axis, z_axis, j_axis, i_axis, p_axis, e_axis = get_axis_dim(
        ovar, var_log)
    cmor.set_table(tables[1])
    axis_ids = []
    if t_axis is not None:
        cmor_tName = get_cmorname('t', t_axis, var_log)
        ctx.obj['reference_date'] = f"days since {ctx.obj['reference_date']}"
        t_axis_val = cftime.date2num(t_axis, units=ctx.obj['reference_date'],
            calendar=ctx.obj['attrs']['calendar'])
        t_bounds = None
        if cmor_tName in bounds_list:
            t_bounds = get_bounds(dsin[var1], t_axis, cmor_tName,
                var_log, ax_val=t_axis_val)
        t_axis_id = cmor.axis(table_entry=cmor_tName,
            units=ctx.obj['reference_date'],
            length=len(t_axis),
            coord_vals=t_axis_val,
            cell_bounds=t_bounds,
            interval=None)
        axis_ids.append(t_axis_id)
    if z_axis is not None:
        zlen = len(z_axis)
        cmor_zName = get_cmorname('z', z_axis, var_log, z_len=zlen)
        z_bounds = None
        if cmor_zName in bounds_list:
            z_bounds = get_bounds(dsin[var1], z_axis, cmor_zName, var_log)
        z_axis_id = cmor.axis(table_entry=cmor_zName,
            units=z_axis.units,
            length=zlen,
            coord_vals=z_axis.values,
            cell_bounds=z_bounds,
            interval=None)
        axis_ids.append(z_axis_id)
        # Set up additional hybrid coordinate information
        if cmor_zName in ['hybrid_height', 'hybrid_height_half']:
            zfactor_b_id, zfactor_orog_id = hybrid_axis(lev_name, var_log)
    ax_dict = {"j": [j_axis, 'j_index'], "i": [i_axis, 'i_index']}
    for k in ["j", "i"]:
        ax = ax_dict[k][0]
        setgrid = True
        if ax is None or ax.ndim == 2:
           ax_id = cmor.axis(table=tables[0],
               table_entry=ax_dict[k][1],
               units='1',
               coord_vals=np.arange(len(dim_values)))
           axis_ids.append(ax_id)
        else:
            setgrid = False
            cmor_aName = get_cmorname(ax_dict[k][1], ax, var_log)
            a_bounds = None
            if cmor_aName in bounds_list:
                a_bounds = get_bounds(dsin[var1], ax, cmor_aName, var_log)
            ax_id = cmor.axis(table_entry=cmor_aName,
                units=ax.units,
                length=len(ax),
                coord_vals=ax.values,
                cell_bounds=a_bounds,
                interval=None)
            axis_ids.append(ax_id)
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

    # Define the spatial grid if non-cartesian grid
    if setgrid:
        grid_id = define_grid(i_axis_id, i_axis, j_axis_id, j_axis, tables[0], var_log)

    # Freeing up memory 
    del dsin
    
    #Define the CMOR variable
    var_log.info("Defining cmor variable...")
    try:    
        cmor.set_table(tables[1])
        var_id = ctx.obj['variable_id']
        dtype = 'f'
        if ovar.dtype.kind == 'i':
            dtype = 'l'
        variable_id = cmor.variable(table_entry=var_id,
                units=in_units,
                axis_ids=axis_ids,
                data_type=dtype,
                missing_value=in_missing,
                positive=positive)
    except Exception as e:
        mop_log.error(f"Unable to define the CMOR variable {ctx.obj['filename']}")
        var_log.error(f"Unable to define the CMOR variable {e}")
        return 2
    var_log.info('Writing...')
    var_log.info(f"Variable shape is {ovar.shape}")
    status = None
    # Write timesteps separately if variable potentially exceeding memory
    if float(ctx.obj['file_size']) > 4000.0 and time_dim != None:
        for i in range(ovar.shape[0]):
            data = ovar.isel({time_dim: i}).values
            status = cmor.write(variable_id, data, ntimes_passed=1)
            del data
    else:
        status = cmor.write(variable_id, ovar.values)
    if status != 0:
        mop_log.error(f"Unable to write the CMOR variable: {ctx.obj['filename']}\n")
        var_log.error(f"Unable to write the CMOR variable to file\n"
                      + f"See cmor log, status: {status}")
        return 2
    var_log.info(f"Finished writing")
    
    # Close the CMOR file.
    path = cmor.close(variable_id, file_name=True)
    return path


@click.pass_context
def process_file(ctx, row, var_log):
    """Processes file from database if status is unprocessed.
    If override is true, re-writes existing files. Called by process_row() and
    calls mop_process() to extract and write variable.

    Parameters
    ----------
    ctx : obj
        Click context object
    row : dict
        row from filelist db table describing one output file
    var_log : logging handler 
        Logging file handler specific to the file to process
    Returns
    -------
    out : tuple
        Output status message and code and db rowid for processed file
    """

    mop_log = ctx.obj['log']
    row['vin'] = row['vin'].split()
    # Check that calculation is defined if more than one variable is passed as input
    if len(row['vin']) > 1 and row['calculation'] == '':
        status = 'mapping_error' 
        msg = "Multiple input variables but no calculation"
        mop_log.error(f"{msg}: {ctx.obj['filename']}")
        var_log.error(f"{msg}")
        return (msg, status, row['rowid'])
    var_log.info(f"\n{'-'*50}\n Processing file with details:\n")
    for k,v in row.items():
        ctx.obj[k] = v
        var_log.info(f"{k}= {v}")
    
    # Processing:
    # run mop_process if file doesn't already exist and/or if overriding
    # return status based on return code 
    expected_file = f"{row['filepath']}/{row['filename']}"
    var_msg = f"{row['table']},{row['variable_id']},{row['tstart']},{row['tend']}"
    if ctx.obj['override'] or not os.path.exists(expected_file):
        try:
            ret = mop_process(mop_log, var_log)
        except Exception as e: #something has gone wrong in the processing
            ret = -1
            mop_log.error(e)
        if ret == 0:
            msg = f"Data incomplete for variable: {row['variable_id']}\n"
            status = "data_unavailable"
        elif ret == 1:
            msg = "Variable extraction/calculation failed\n"
            status = "calculation_failed"
        elif ret == 2:
            msg = "Cmor variable definition failed\n"
            status = "cmor_error"
        elif ret == 3:
            msg = "Cmor write failed\n"
            status = "cmor_error"
        elif ret == -1:
            msg = f"Could not process file for variable: {var_msg}\n"
            status = "processing_failed"
        else:
            #Assume processing has been successful
            with open(f"{ctx.obj['outpath']}/success.csv",'a+') as c:
                c.write(f"{var_msg}, {ret}\n")
            c.close()
            #Check if output file matches what we expect
            var_log.info(f"Output file:   {ret}")
            if ret == expected_file:
                var_log.info(f"Expected and cmor file paths match")
                msg = f"Successfully processed variable: {var_msg}\n"
                status = "processed"
            else :
                var_log.info(f"Expected file: {expected_file}")
                var_log.info("Expected and cmor file paths do not match")
                msg = f"Produced but file name does not match expected {var_msg}\n"
                status = "file_mismatch"
        if type(ret) is int:
            with open(f"{ctx.obj['outpath']}/failed.csv",'a+') as c:
                c.write(f"{var_msg}\n")
            c.close()
    else :
        msg = f"Skipping because file already exists for variable: {var_msg}\n"
        var_log.info(f"filename: {expected_file}")
        status = "processed"
    mop_log.info(msg)
    return (msg, status, row['rowid'])


@click.pass_context
def process_row(ctx, row):
    """Processes one db filelist row.
    Sets up variable log file, prepares dictionary with file details
    and calls process_file
    """
    record = {}
    header = ['infile', 'filepath', 'filename', 'vin', 'variable_id',
              'table', 'frequency', 'realm', 'timeshot', 'tstart',
              'tend', 'sel_start', 'sel_end', 'status', 'file_size',
              'exp_id', 'calculation', 'resample', 'in_units',
              'positive', 'cfname', 'source_id', 'access_version',
              'json_file_path', 'reference_date', 'version', 'rowid']  
    for i,val in enumerate(header):
        record[val] = row[i]
    table = record['table'].split('_')[1]
    # call logging 
    trange = record['filename'].replace('.nc.','').split("_")[-1]
    varlog_file = (f"{ctx.obj['var_logs']}/{record['variable_id']}"
                 + f"_{record['table']}_{record['tstart']}.txt")
    var_log = config_varlog(ctx.obj['debug'], varlog_file) 
    ctx.obj['var_log'] = var_log 
    var_log.info(f"Start processing")
    var_log.debug(f"Process id: {os.getpid()}")
    msg = process_file(record, var_log)
    var_log.handlers[0].close()
    var_log.removeHandler(var_log.handlers[0])
    return msg


@click.pass_context
def pool_handler(ctx, rows, ncpus):
    """Sets up the concurrent future pool executor and submits
    rows from filelist db table to process_row. Each row represents a file
    to process. 

    Returns
    -------
    result_futures : list
        list of process_row() outputs returned by futures, these are 
        tuples with status message and code, and rowid
    """
    mop_log = ctx.obj['log']
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=ncpus)
    futures = []
    for row in rows:
    # Using submit with a list instead of map lets you get past the first exception
    # Example: https://stackoverflow.com/a/53346191/7619676
        future = executor.submit(process_row, row)
        futures.append(future)
    # Wait for all results
    concurrent.futures.wait(futures)

# After a segfault is hit for any child process (i.e. is "terminated abruptly"), the process pool becomes unusable
# and all running/pending child processes' results are set to broken
    result_futures = []
    for future in futures:
        try:
            mop_log.info(f"{future.result()}")
            result_futures.append(future.result())
        except concurrent.futures.process.BrokenProcessPool:
            mop_log.info("process broken")
    return result_futures
