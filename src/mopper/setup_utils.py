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

import sys
import json
import sqlite3
import copy
import click
import pathlib
import logging

from datetime import datetime
from dateutil.relativedelta import relativedelta
from importlib.resources import files as import_files
from calendar import monthrange

from mopdb.utils import query, write_yaml, read_yaml, MopException
from mopper.cmip_utils import fix_years


def write_var_map(outpath, table, matches):
    """Write variables mapping to json file
    """
    with (outpath / f"{table}.json").open(mode='w') as fjson:
        json.dump(matches, fjson, indent=2)
    fjson.close()


def define_timeshot(frequency, resample, cell_methods):
    """Returns timeshot based on frequency, cell_methods and resample.

    It also fixes and returns frequency for instantaneous and
    climatology data.
    If data will be resampled timeshot is mean/max/min

    Parameters
    ----------
    v : dict
        Dictionary containing variable specifications
    frequency : str
        Current variable frequency

    Returns
    -------
    timeshot : str
    frequency : str
        Updated variable frequency

    """
    if 'time:' in cell_methods:
        bits = cell_methods.split()
        timeshot = bits[bits.index('time:') + 1]
    else:
        timeshot = ''
    if 'Pt' in frequency:
        timeshot = 'point'
        frequency = str(frequency)[:-2]
    elif frequency == 'monC':
        timeshot = 'clim'
        frequency = 'mon'
    # if timeshot is maximum/minimum/sum then leave it unalterated
    # otherwise resampled values is mean
    # for maximum, minimum pass timeshot as the resample method
    if resample != '':
        if timeshot in ['mean', 'point', '']:
            timeshot = 'mean'
        elif timeshot in ['maximum', 'minimum']:
            timeshot = timeshot[:3]
    return timeshot, frequency


def adjust_nsteps(v, frq):
    """Adjust variable grid size to new number of timesteps.

    Each variable master definition has size of one timestep and
    number of time steps. If frequency changes (for example by resample),
    then number of timesteps need to be adjusted.
    New number of time steps is:
      total_time(days) / nstep_day(new_frq)
      total_time (days) = nsteps * nstep_day(orig_frq) 

    Parameters
    ----------
    v : dict
        Dictionary containing variable specifications
    frq : str
        New frequency for variable

    Returns
    -------
    nsteps

    """
    # number of timesteps in a day for given frequency
    nstep_day = {'10min': 144, '30min': 48, '1hr': 24, '3hr': 8, 
                 '6hr': 4, 'day': 1, '10day': 0.1, 'mon': 1/30, 
                 'yr': 1/365, 'dec': 1/3652}
    nsteps = int(v['nsteps'])
    frequency = v['frequency'].replace('Pt', '')
    #  total time in days
    tot_days = nsteps / nstep_day[frequency]
    # new number of timesteps
    new_nsteps = tot_days * nstep_day[frq]
    return new_nsteps

@click.pass_context
def write_config(ctx, fname='exp_config.yaml'):
    """Write data to a yaml file

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    fname : str
        Yaml filename (default: exp_config.yaml)

    Returns
    -------
    """
    config = {'cmor': {}}
    for k,v in ctx.obj.items():
        if isinstance(v, pathlib.PurePath):
            config['cmor'][k] = str(v)
        else:
            config['cmor'][k] = v 
    config['attrs'] = config['cmor'].pop('attrs')
    write_yaml(config, fname, 'mop_log')
    return


@click.pass_context
def find_map_tables(ctx, mappings):
    """Returns list of tables files listed in mapping file
    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes

    Returns
    -------

    """
    mop_log = logging.getLogger('mop_log')
    tables = [x['cmor_table'] for x in mappings]
    tables = set(tables)
    mop_log.debug(f"Tables found: {tables}")
    return tables


@click.pass_context
def write_table(ctx, table, vardict, select):
    """Write CMOR table in working directory
       Includes only selected variables and adds deflate levels.

    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    """
    new = copy.deepcopy(vardict)
    for k in vardict['variable_entry'].keys():
        if k not in select:
            new['variable_entry'].pop(k)
        else:
            new['variable_entry'][k]['deflate'] = 1
            new['variable_entry'][k]['deflate_level'] = ctx.obj['deflate_level']
            new['variable_entry'][k]['shuffle'] = 1
    tjson = ctx.obj['tpath'] / f"{table}.json"
    with tjson.open(mode='w') as f:
        json.dump(new, f, indent=4, separators=(',', ': '))
    f.close
    return


#PP still creating a filelist table what to store in it might change!
def filelist_sql():
    """Returns sql to define filelist table

    Returns
    -------
    sql : str
        SQL style string defining mapping table
    """
    sql = '''create table if not exists filelist(
            infile text,
            filepath text,
            filename text,
            vin text,
            variable_id text,
            ctable text,
            frequency text,
            realm text,
            timeshot text,
            axes text,
            tstart text,
            tend text,
            sel_start text,
            sel_end text,
            status text,
            file_size real,
            exp_id text,
            calculation text,
            resample text,
            in_units text,
            positive text,
            cfname text,
            source_id text,
            access_version text,
            json_file_path text,
            reference_date text,
            version text,
            primary key(exp_id,variable_id,ctable,tstart,version))'''
    return sql


@click.pass_context
def write_job(ctx, nrows):
    """
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    """
    mop_log = logging.getLogger('mop_log')
    # define storage flag
    flag = "storage=gdata/hh5" 
    projects = ctx.obj['addprojs'] + [ctx.obj['project']]
    for proj in projects:
       flag += f"+scratch/{proj}+gdata/{proj}"
    # work out number of cpus based on number of files to process
    # hugemem requires minimum 6 cpus
    if ctx.obj['queue'] == 'hugemem' and nrows < 6:
        ctx.obj['ncpus'] = 6
    elif nrows <= ctx.obj['max_cpus']:
        ctx.obj['ncpus'] = nrows
    else:
        ctx.obj['ncpus'] =  ctx.obj['max_cpus']
    ctx.obj['nmem'] = ctx.obj['ncpus'] * ctx.obj['mem_per_cpu']
    if ctx.obj['nmem'] >= 1470: 
        ctx.obj['nmem'] = 1470
    mop_log.info(f"number of files to create: {nrows}")
    mop_log.info(f"number of cpus to be used: {ctx.obj['ncpus']}")
    mop_log.info(f"total amount of memory to be used: {ctx.obj['nmem']}GB")
    fpath = ctx.obj['app_job']
    template = define_template(flag, nrows)
    with fpath.open(mode='w') as f:
        f.write(template)
    return ctx


@click.pass_context
def create_exp_json(ctx, json_cv):
    """Create a json file as expected by CMOR to describe the dataset
    and passed the main global attributes. Add source and source_id to CV file
    if necessary.

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    json_cv : str
        Path of CV json file to edit

    Returns
    -------
    fname : str
        Name of created experiment json file

    """
    mop_log = logging.getLogger('mop_log')
    fname = ctx.obj['outpath'] / f"{ctx.obj['exp']}.json"
    attrs = ctx.obj['attrs']
    with json_cv.open(mode='r') as f:
        cv_dict = json.load(f)
    # check if source_id is present in CV as it is hardcoded
    # if present but source description is different overwrite file in custom mode
    if any(x not in attrs.keys() for x in ['source_id', 'source']):
        mop_log.error("Source and source_id need to be defined")
        raise MopException("Source and source_id need to be defined")
    at_sid, at_source = attrs['source_id'], attrs['source']  
    cv_sid = cv_dict['CV']['source_id'].get(at_sid,'')
    if cv_sid == '' or cv_sid['source'] != at_source:
       if cv_sid == '' and ctx.obj['mode'] == 'cmip6':
           mop_log.error(f"source_id {at_sid} not defined in CMIP6_CV.json file")
           raise MopException(f"source_id {at_sid} not defined in CMIP6_CV.json file")
       cv_dict['CV']['source_id'][at_sid] = {'source_id': at_sid,
           'source': at_source}
       with json_cv.open(mode='w') as f:
           json.dump(cv_dict, f, indent=4)
    # read required attributes from cv file
    # and add attributes for path and file template to required
    required = cv_dict['CV']['required_global_attributes']
    tmp_str = (ctx.obj['path_template'].replace('}/{','/') 
               + ctx.obj['file_template'].replace('}_{','/'))
    attrs_template = tmp_str.replace('}','').replace('{','').split('/') 
    required.extend( set(attrs_template))
    # plus any other attrs hardcoded in cmor
    required.extend(['_control_vocabulary_file',
        '_AXIS_ENTRY_FILE', '_FORMULA_VAR_FILE', 'outpath'] )
    # create global attributes dict to save
    glob_attrs = {}
    attrs_keys = [k for k in attrs.keys()]
    for k in required:
        if k in attrs_keys:
            glob_attrs[k] = attrs[k]
        else:
            glob_attrs[k] = ctx.obj.get(k, '')
    # temporary correction until CMIP6_CV file name is not anymore hardcoded in CMOR
    glob_attrs['_control_vocabulary_file'] = f"{ctx.obj['tpath']}/CMIP6_CV.json"
    # replace {} _ and / in output templates
    glob_attrs['output_path_template'] = ctx.obj['path_template'] \
        .replace('{','<').replace('}','>').replace('/','')
    glob_attrs['output_file_template'] = ctx.obj['file_template'] \
         .replace('}_{','><').replace('}','>').replace('{','<')
    if ctx.obj['mode'] == 'cmip6':
        glob_attrs['experiment'] = attrs['experiment_id']
    else:
        glob_attrs['experiment'] = ctx.obj.get('exp','')
    # write glob_attrs dict to json file
    # parent attrs don't seem to be included should I add them manually?
    # at least for mode = cmip6
    json_data = json.dumps(glob_attrs, indent = 4, sort_keys = True, default = str)
    with fname.open(mode='w') as f:
        f.write(json_data)
    f.close()
    return fname


@click.pass_context
def populate_db(ctx, conn):
    """Populate filelist db table, this will be used by app to
    process all files

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    conn : obj 
        DB connection object
    """
    mop_log = logging.getLogger('mop_log')
    cursor = conn.cursor()
    # process experiment information
    opts = {}
    opts['status'] = 'unprocessed'
    opts['outpath'] = str(ctx.obj['outpath'])
    version = ctx.obj['attrs'].get('version',
        datetime.today().strftime('%Y%m%d'))
    # ACDD uses product_version
    ctx.obj['attrs']['version'] = ctx.obj['attrs'].get(
        'product_version', version)
    #Experiment Details:
    for k,v in ctx.obj['attrs'].items():
        opts[k] = v
    opts['exp_id'] = ctx.obj['exp'] 
    opts['exp_dir'] = str(ctx.obj['datadir'])
    opts['reference_date'] = ctx.obj['reference_date']
    opts['exp_start'] = ctx.obj['start_date'] 
    opts['exp_end'] = ctx.obj['end_date']
    opts['access_version'] = ctx.obj['access_version']
    opts['json_file_path'] = str(ctx.obj['json_file_path']) 
    mop_log.info(f"Found experiment: {opts['exp_id']}")
    #monthly, daily unlimited except cable or moses specific diagnostics
    maps = []
    tables = ctx.obj['maps'].rglob("*.json")
    for table in tables:
        with table.open(mode='r') as fjson:
            mop_log.debug(f"Opening table: {table}")
            data = json.load(fjson)
        maps.extend(data)
    process_vars(maps, opts, cursor)
    conn.commit()
    return


def add_row(values, cursor, update):
    """Add a row to the filelist database table
       one row specifies the information to produce one output cmip5 file

    Parameters
    ----------
    values : list
        List of values of file attributes
    cursor : sqlite3.cursor obj 
        To execute sql statements on database
    update : bool
        If True update existing rows instead of adding them

    Returns
    -------
    """
    mop_log = logging.getLogger('mop_log')
    sql = '''insert into filelist
        (infile, filepath, filename, vin, variable_id, ctable,
        frequency, realm, timeshot, axes, tstart, tend, sel_start,
        sel_end, status, file_size, exp_id, calculation, resample,
        in_units, positive, cfname, source_id, access_version,
        json_file_path, reference_date, version)
        values
        (:infile, :filepath, :filename, :vin, :variable_id, :table,
        :frequency, :realm, :timeshot, :axes, :tstart, :tend,
        :sel_start, :sel_end, :status, :file_size, :exp_id,
        :calculation, :resample, :in_units, :positive, :cfname,
        :source_id, :access_version, :json_file_path, :reference_date,
        :version)'''
    if update:
         sql = sql.replace("insert", "replace")
    try:
        cursor.execute(sql, values)
    except sqlite3.IntegrityError as e:
        mop_log.warning(f"Row already exists:\n{e}")
    except Exception as e:
        mop_log.warning(f"Could not insert row for {values['filename']}:\n{e}")
    return cursor.lastrowid


def adjust_size(opts, insize):
    """

    Returns
    -------
    """
    # transport/transects/tiles should reduce size
    # volume,any vertical sum
    # resample will affect frequency but that should be already taken into account in mapping
    calc = opts['calculation']
    #resample = opts['resample']
    grid_size = insize
    if 'plevinterp' in calc:
        if "," in calc:
            plevnum = calc.split(',')[-1]
        else:
            raise('check plevinterp calculation def plev probably missing')
        f = filter(str.isdecimal,plevnum)
        plevnum = float("".join(f))
        grid_size = float(insize)/float(opts['levnum'])*plevnum
    return grid_size


#PP if this approach is ok I should move the interval definition out of here
@click.pass_context
# and as for everything else in yaml file
def compute_fsize(ctx, opts, grid_size, frequency):
    """Calculate an estimated output file size (in megabytes)
       and the interval to use to satisfy max_size decided by user

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    attrs: dict
        Dictionary with attributes defined for experiment

    Returns
    -------
    """
    mop_log = logging.getLogger('mop_log')
    # set small number for fx frequency so it always create only one file
    nstep_day = {'10min': 144, '30min': 48, '1hr': 24, '3hr': 8, 
                 '6hr': 4, 'day': 1, '10day': 0.1, 'mon': 1/30, 
                 'yr': 1/365,  '2yr': 1/730, '5yr': 1/1826, 
                 'dec': 1/3652, 'fx': 1/5000}
    max_size = ctx.obj['max_size']
    # work out if grid-size might change because of calculation
    if opts['calculation'] != '' or opts['resample'] != '':
        grid_size = adjust_size(opts, grid_size)
    size_tstep = int(grid_size)/(1024**2)

    # work out how long is the entire span in days
    start = datetime.strptime(str(ctx.obj['start_date']), '%Y%m%dT%H%M')
    finish = datetime.strptime(str(ctx.obj['end_date']), '%Y%m%dT%H%M')
    mop_log.debug(f"compute_fsize start, finish: {start}, {finish}")
    delta = (finish - start).days 
    # if overall interval less than a day use seconds as days will be 0
    if delta == 0:
        delta = (finish - start).seconds/(3600*24)
    mop_log.debug(f"compute_fsize full interval in days: {delta}")
    # calculate the size of potential file intervals depending on timestep frequency
    size = {}
    size['days=0.25'] = size_tstep * nstep_day[frequency] * 0.25
    size['days=0.5'] = size_tstep * nstep_day[frequency] * 0.5
    size['days=1'] = size_tstep * nstep_day[frequency]
    size[f'days={delta}'] = size['days=1'] * delta
    size['days=7'] = size['days=1'] * 7
    size['months=1'] = size['days=1'] * 30
    size['years=1'] = size['months=1'] * 12
    size['years=2'] = size['years=1'] * 2
    size['years=5'] = size['years=1'] * 5
    size['years=10'] = size['years=1'] * 10
    size['years=100'] = size['years=10'] * 10
    # Evaluate intervals in order starting from the maximum size (entire
    # timeseries) to the shorter option until size <= max_size*1.1
    if size[f'days={delta}'] <= max_size*1.1:
        interval = f'days={delta}' 
    else:
        for interval in ['years=100', 'years=10', 'years=2', 'years=5',
            'years=1', 'months=1', 'days=7', 'days=1', 'days=0.5',
            'days=0.25']:
            if size[interval] <= max_size*1.1:
                    break
    return interval, size[interval]


@click.pass_context
def build_filename(ctx, opts):
    """Builds name for file to be created based on template in config

    Trims tstart and tend from opts dictionary based on frequency.

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    opts : dict
        Dictionary with attributes for a specific variable

    Returns
    -------
    fpath : str
        Path for file to be created
    fname : str
        Name for file to be created

    """
    frequency = opts['frequency'].replace("Pt","").replace(
        "CM","").replace("C","")
    #stamp = '%4Y%m%d%H%M%S'
    idx = 15
    if frequency != 'fx':
        if frequency in ['yr', 'dec']:
            idx = 4
        elif frequency == 'mon':
            idx = 6
        elif frequency == 'day':
            idx = 8
        elif 'hr' in frequency:
            idx = 13
##PP restart from here how to apply format if already strin?
        tstart = (opts['tstart'] + '00')[:idx].replace('T', '')
        tend = (opts['tend'] + '00')[:idx].replace('T', '')
        opts['date_range'] = f"{tstart}-{tend}"
    else:
        opts['date_range'] = ""
    # PP we shouldn't need this as now we pas subhr and then the actual minutes separately
    if 'min' in frequency:
        opts['frequency'] = 'subhr'
    if opts['timeshot'] == 'point':
        opts['frequency'] += 'Pt'
    opts['version'] = opts['version'].replace('.', '-')
    path_template = f"{str(ctx.obj['outpath'])}/{ctx.obj['path_template']}"
    fpath = path_template.format(**opts)
    fname = ctx.obj['file_template'].format(**opts) + f"_{opts['date_range']}" 
    if opts['timeshot'] == "clim":
        fname = fname + "-clim"
    fname = fname + ".nc"
    if opts['timeshot'] == 'point' and opts['frequency'] != 'subhrPt':
        opts['frequency'] =  opts['frequency'].replace('Pt','')
    return fpath, fname


@click.pass_context
def process_vars(ctx, maps, opts, cursor):
    """Process the information needed to populate filelist table,
    at variable level, getting values from config and mapping.
    Works out how many files to generate based on grid size. 

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    maps : list(dict)
        List of dictionaries where each item represents one variable to process
    opts : dict
        Dictionary with attributes of specific variable to update
    cursor : sqlite3.cursor obj 
        To execute sql statements on database

    Returns
    -------
    """
    unchanged = ['frequency', 'realm', 'table', 'calculation',
                 'resample', 'positive', 'timeshot']  
    for mp in maps:
        for attr in unchanged:
            opts[attr] = mp[attr]
        table_id = mp['table'].split('_')[1]
        opts['table_id'] = table_id
        opts['variable_id'] = mp['cmor_var'] 
        opts['vin'] = mp['input_vars']
        paths = mp['file_structure'].split() 
        opts['infile'] = ''
        for x in paths:
            opts['infile'] += f"{opts['exp_dir']}/{x} "
        opts['in_units'] = mp['units']
        opts['levnum'] = ctx.obj['levnum']
        opts['cfname'] = mp['standard_name']
        opts['axes'] = mp['axes']
        add_files(cursor, opts, mp)
    return


@click.pass_context
def add_files(ctx, cursor, opts, mp):
    """Determines tstart and tend, filename and path and size for each file
    to produce for variable.
   
    Based on frequency, time range to cover and time interval for each file.
    This last is determined by maximum file size.
    These and other files details are saved in filelist db table.

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes
    cursor : sqlite3.cursor obj 
        To execute sql statements on database

    Returns
    -------


    """
    mop_log = logging.getLogger('mop_log')
    update = ctx.obj['update']
    exp_start = opts['exp_start']
    exp_end = opts['exp_end']
    # only used in cmip mode
    if mp['years'] != 'all' and ctx.obj['dreq_years']:
        exp_start, exp_end = fix_years(mp['years'], exp_start[:4], exp_end[:4]) 
        if exp_start is None:
            mop_log.info(f"""Years requested for variable are outside
                specified period: {mp['years']}""")
            return
    # set half and full time step for each frequency
    fname = import_files('mopdata').joinpath('tstep_delta.yaml')
    tstep_dict = read_yaml(fname)['tstep_dict']
    start = datetime.strptime(str(exp_start), '%Y%m%dT%H%M')
    finish = datetime.strptime(str(exp_end), '%Y%m%dT%H%M')
    frq = opts['frequency']
    if 'subhr' in frq:
        frq =  ctx.obj['subhr'] + frq.split('subhr')[1]
    tstep = eval(f"relativedelta({tstep_dict[frq][0]})")
    half_tstep = eval(f"relativedelta({tstep_dict[frq][1]})")
    mop_log.debug(f"add_files frq, half_tstep, tstep: {frq}, {half_tstep}, {tstep}")
    # interval is file temporal range as a string to evaluate timedelta
    interval, opts['file_size'] = compute_fsize(opts, mp['size'], frq)
    mop_log.debug(f"add_files time interval for 1 file: {interval}")
    delta = eval(f"relativedelta({interval})")
    #loop over times
    if frq == 'fx':
         finish = start + relativedelta(days=1)
         tstep_dict['fx'] = tstep_dict['day']
    while (start < finish):
        opts, newtime = define_file(opts, start, finish, delta,
            tstep, half_tstep)
        opts['filepath'], opts['filename'] = build_filename(opts)
        rowid = add_row(opts, cursor, update)
        mop_log.debug(f"Last added row id: {rowid}")
        start = newtime
    return

def define_file(opts, start, finish, delta, tstep, half_tstep):
    """
    """ 
    mop_log = logging.getLogger('mop_log')
    # correct half_step for start with monthly data
    if opts['frequency'] == 'mon':
        ndays = monthrange(start.year, start.month)[1] 
        half_tstep = relativedelta(days=ndays/2.0)
    newtime = min(start+delta, finish)
    if opts['timeshot'] == 'point':
        tstart = start + tstep
        tend = start + delta
    else:
        tstart = start + half_tstep
        tend = start + delta - half_tstep
        if opts['frequency'] == 'mon':
            mop_log.debug(f"define_file, tend before mon adjust: {tend}")
            ndays = monthrange(tend.year, tend.month)[1] 
            mop_log.debug(f"define_file, tend ndays: {ndays}")
            half_end = relativedelta(days=ndays/2.0)
            tend = start + delta - tstep + half_end
            mop_log.debug(f"define_file, tend after mon adjust: {tend}")
    opts['tstart'] = tstart.strftime('%4Y%m%dT%H%M')
    opts['tend'] = tend.strftime('%4Y%m%dT%H%M')
    # select files on 1 tstep wider interval to account for timestamp shifts 
    opts['sel_start'] = (tstart - tstep).strftime('%4Y%m%d%H%M')
    opts['sel_end'] = (tend + tstep).strftime('%4Y%m%d%H%M')
    return opts, newtime

def count_rows(conn, exp):
    """Returns number of files to process
    """
    mop_log = logging.getLogger('mop_log')
    sql = f"select * from filelist where status=='unprocessed' and exp_id=='{exp}'"
    rows = query(conn, sql, first=False)
    mop_log.info(f"Number of rows in filelist: {len(rows)}")
    return len(rows)


def sum_file_sizes(conn):
    """Returns estimate of total size of files to process
    """
    sql = 'select file_size from filelist'
    size_list = query(conn, sql, first=False)
    size=0.0
    for s in size_list:
        size += float(s[0])
    size = size/1024.
    return size


@click.pass_context
def define_template(ctx, flag, nrows):
    """Defines job file template

    Parameters
    ----------
    ctx : click context 
        Includes obj dict with 'cmor' settings, exp attributes

    Returns
    -------

    """
    template = f"""#!/bin/bash
#PBS -P {ctx.obj['project']}
#PBS -q {ctx.obj['queue']}
#PBS -l {flag}
#PBS -l ncpus={ctx.obj['ncpus']},walltime={ctx.obj['walltime']},mem={ctx.obj['nmem']}GB,wd
#PBS -j oe
#PBS -o {ctx.obj['job_output']}
#PBS -N mopper_{ctx.obj['exp']}

# the code assumes you are running this on gadi and have access to the hh5 project modules
# if this is not the case make sure you have loaded alternative python modules
# see https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR/blob/main/requirements.txt
# for a list of packages

module use /g/data/hh5/public/modules
module load conda/analysis3-unstable
{ctx.obj['conda_env']}

cd {ctx.obj['appdir']}
mop  run -c {ctx.obj['exp']}_config.yaml
echo 'APP completed for exp {ctx.obj['exp']}.'"""
    return template
