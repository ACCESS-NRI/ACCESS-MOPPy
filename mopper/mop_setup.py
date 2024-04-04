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
# last updated 27/11/2023

import os
import sys
import shutil
import yaml
import json
import csv
import click
from pathlib import Path
from json.decoder import JSONDecodeError

from mopper.setup_utils import *


def find_matches(table, var, realm, frequency, varlist, mop_log):
    """Finds variable matching constraints given by table and config
    settings and returns a dictionary with the variable specifications. 

    NB. if an exact match (cmor name, realm, frequency is not found) 
    will try to find same cmor name, ignoring if time is point or mean,
    and realm but different frequency. This can then potentially be 
    resampled to desired frequency.

    Parameters
    ----------
    table : str
        Variable table 
    var : str
        Variable cmor/cmip style name to match
    realm : str
        Variable realm to match
    frequency : str
        Variable frequency to match
    varlist : list
        List of variables, each represented by a dictionary with mappings
        used to find a match to "var" passed 
    mop_log : logging object 
        Log
    Returns
    -------
    match : dict
        Dictionary containing matched variable specifications
        or None if not matches
    """
    near_matches = []
    found = False
    match = None
    mop_log.debug(f"Looking for: {var}, {frequency}, {realm}")
    for v in varlist:
        mop_log.debug(f"{v['cmor_var']}, {v['frequency']}, {v['realm']}")
        if v['cmor_var'].startswith('#'):
            pass
        elif (v['cmor_var'] == var and v['realm'] == realm 
              and v['frequency'] == frequency):
            match = v
            found = True
        elif (v['cmor_var'].replace('_Pt','') == var
              and v['realm'] == realm):
            near_matches.append(v)
    if found is False and frequency != 'fx':
        v = find_nearest(near_matches, frequency, mop_log)
        if v is not None:
            match = v
            found = True
        else:
            mop_log.info(f"could not find match for {table}-{var}" +
                         f"-{frequency} from variables:")
            for v in varlist:
                mop_log.info(f"{v['cmor_var']}, {v['frequency']}, {v['realm']}")
    if found is True:
        resample = match.get('resample', '')
        timeshot, frequency = define_timeshot(frequency, resample,
            match['cell_methods'])
        match['resample'] = resample
        match['timeshot'] = timeshot
        match['table'] = table
        match['frequency'] = frequency
        if match['realm'] == 'land':
            realmdir = 'atmos'
        else:
            realmdir = match['realm']
        in_fname = match['filename'].split()
        match['file_structure'] = ''
        for f in in_fname:
            #match['file_structure'] += f"/{realmdir}/{f}*.nc "
            # problem with ocean files not having .nc at end of file, I think this needs fixing in the archiver!
            match['file_structure'] += f"/{realmdir}/{f}*"
            #match['file_structure'] = f"/atm/netCDF/{match['filename']}*.nc"
    return match


def find_nearest(varlist, frequency, mop_log):
    """If variable is present in file at different frequencies,
    finds the one with higher frequency nearest to desired frequency.
    Adds frequency to variable resample field.
    Checks if modifier is present for frequency, match freq+mod must equal 
    var frequency, however modifier is removed to find resample frequency

    Parameters
    ----------
    varlist : list
        Subset of variables with same realm and cmor name but different
        frequency
    frequency : str
        Variable frequency to match
    mop_log : logging object 
        Log

    Returns
    -------
    var : dict
        Dictionary containing matched variable specifications
        or None if not matches
    """
    var = None
    found = False
    freq = frequency
    if 'Pt' in frequency:
        freq = frequency.replace('Pt','')
    elif 'C' in frequency:
        freq = frequency.replace('C','')
    resample_order = ['10yr', 'yr', 'mon', '10day', '7day',
            'day', '12hr', '6hr', '3hr', '1hr', '30min', '10min']
    resample_frq = {'10yr': '10Y', 'yr': 'Y', 'mon': 'M', '10day': '10D',
                    '7day': '7D', 'day': 'D', '12hr': '12H', '6hr': '6H',
                    '3hr': '3H', '1hr': 'H', '30min': '30T'}
    freq_idx = resample_order.index(freq)
    mop_log.debug(f"In find_nearest, freq: {freq}, freq_idx: {freq_idx}")
    for frq in resample_order[freq_idx+1:]:
        for v in varlist:
            vfrq = v['frequency'].replace('Pt','').replace('C','')
            mop_log.debug(f"Var: {v}, var frq: {vfrq}")
            if vfrq == frq:
                v['resample'] = resample_frq[freq]
                v['nsteps'] = adjust_nsteps(v, freq)
                found = True
                var = v
                break
        if found:
            break
    return var


@click.pass_context
def setup_env(ctx):
    """Sets up the configuration dictionary based on config file input

    Parameters
    ----------
    ctx : click context obj
        Dictionary including 'cmor' settings and attributes for experiment

    Returns
    -------
    ctx : click context obj
        With updated dictionary including 'cmor' settings and
        attributes for experiment

    """
    mop_log = ctx.obj['log']
    cdict = ctx.obj
    cdict['appdir'] = Path(cdict['appdir'])
    appdir = cdict['appdir']
    mop_log.debug(f"appdir: {appdir}, {type(appdir)}")
    if cdict['outpath'] == 'default':
        cdict['outpath'] = (f"/scratch/{cdict['project']}/" + 
            f"{os.getenv('USER')}/MOPPER_output")
    if f"/{cdict['exp']}" not in cdict['outpath']:
        cdict['outpath'] = Path(cdict['outpath']) / cdict['exp']
    else:
        cdict['outpath'] = Path(cdict['outpath'])
    mop_log.debug(f"outpath: {cdict['outpath']}, {type(cdict['outpath'])}")
    cdict['master_map'] = appdir / cdict['master_map']
    cdict['tables_path'] = appdir / cdict['tables_path']
    cdict['ancils_path'] = appdir / cdict['ancils_path']
    # Output subdirectories
    outpath = cdict['outpath']
    cdict['maps'] = outpath / "maps"
    cdict['tpath'] = outpath / "tables"
    cdict['cmor_logs'] = outpath / "cmor_logs"
    cdict['var_logs'] = outpath / "variable_logs"
    # Output files
    cdict['app_job'] = outpath / "mopper_job.sh"
    cdict['job_output'] = outpath / "job_output.OU"
    cdict['database'] = outpath / "mopper.db"
    # reference_date
    if cdict['reference_date'] == 'default':
        cdict['reference_date'] = (f"{cdict['start_date'][:4]}-" + 
            f"{cdict['start_date'][4:6]}-{cdict['start_date'][6:8]}")
    # make sure tstart and tend include hh:mm
    if len(cdict['start_date']) < 13:
        cdict['start_date'] += 'T0000'
        cdict['end_date'] += 'T0000'#'T2359'
    # if parent False set parent attrs to 'no parent'
    if cdict['attrs']['parent'] is False and cdict['mode'] == 'cmip6':
        p_attrs = [k for k in cdict['attrs'].keys() if 'parent' in k]
        for k in p_attrs:
            cdict['attrs'][k] = 'no parent'
    ctx.obj = cdict
    return ctx


#PP this is where dreq process start it can probably be simplified
# if we can read dreq as any other variable list
# and change year start end according to experiment
@click.pass_context
def var_map(ctx, activity_id=None):
    """
    """
    mop_log = ctx.obj['log']
    tables = ctx.obj.get('tables', 'all')
    subset = ctx.obj.get('var_subset', False)
    sublist = ctx.obj.get('var_subset_list', None)
    if subset is True:
        if sublist is None:
            mop_log.error("var_subset is True but file with variable list not provided")
            sys.exit()
        elif Path(sublist).suffix not in ['.yaml', '.yml']:
            mop_log.error(f"{sublist} should be a yaml file")
            sys.exit()
        else:
            sublist = ctx.obj['appdir'] / sublist
# Custom mode vars
    if ctx.obj['mode'].lower() == 'custom':
        access_version = ctx.obj['access_version']
    if ctx.obj['force_dreq'] is True:
        if ctx.obj['dreq'] == 'default':
            ctx.obj['dreq'] = ( ctx.obj['appdir'] / 
                'data/dreq/cmvme_all_piControl_3_3.csv' )
    with ctx.obj['master_map'].open(mode='r') as f:
        reader = csv.DictReader(f, delimiter=';')
        masters = list(reader)
    f.close()
    mop_log.info(f"Creating variable maps in directory '{ctx.obj['maps']}'")
    if subset:
        selection = read_yaml(sublist)
        tables = [t for t in selection.keys()]
        for table in tables:
            mop_log.info(f"\n{table}:")
            create_var_map(table, masters, selection=selection[table])
    elif tables.lower() == 'all':
        mop_log.info(f"Experiment {ctx.obj['exp']}: processing all tables")
        if ctx.obj['force_dreq'] == True:
            tables = find_cmip_tables(ctx.obj['dreq'])
        else:
            tables = find_custom_tables()
        for table in tables:
            print(f"\n{table}:")
            create_var_map(table, masters, activity_id)
    else:
        create_var_map(tables, masters)
    return ctx


@click.pass_context
def create_var_map(ctx, table, mappings, activity_id=None, 
                   selection=None):
    """Create a mapping file for this specific experiment based on 
    model ouptut mappings, variables listed in table/s passed by config.
    Called by var_map

    Parameters
    ----------

    Returns
    -------
    """
    mop_log = ctx.obj['log']
    matches = []
    fpath = ctx.obj['tables_path'] / f"{table}.json"
    table_id = table.split('_')[1]
    mop_log.debug(f"Mappings: {mappings}")
    try:
        text = fpath.read_text()
        vardict = json.loads(text)
    except JSONDecodeError as e:
        mop_log.error(f"Invalid json {fpath}: {e}")
        raise 
    row_dict = vardict['variable_entry']
    all_vars = [v for v in row_dict.keys()]
    # work out which variables you want to process
    select = all_vars 
    if selection is not None:
        select = [v for v in all_vars if v in selection]
    elif ctx.obj['variable_to_process'] != 'all':
        select = [ctx.obj['variable_to_process']]
    elif ctx.obj['force_dreq'] is True:
        dreq_years = read_dreq_vars(table_id, activity_id)
        all_dreq = [v for v in dreq_years.keys()]
        select = set(select).intersection(all_dreq) 
    for var,row in row_dict.items():
        if var not in select:
            continue
        frq = row['frequency']
        realm = row['modeling_realm']
        years = 'all'
        if ctx.obj['force_dreq'] and var in all_dreq:
            years = dreq_years[var]
        if 'subhr' in frq:
            frq =  ctx.obj['subhr'] + frq.split('subhr')[1]
        match = find_matches(table, var, realm, frq, mappings, mop_log)
        if match is not None:
            match['years'] = years
            matches.append(match)
    if matches == []:
        mop_log.info(f"{table}: no matching variables found")
    else:
        mop_log.info(f"    Found {len(matches)} variables")
        write_var_map(ctx.obj['maps'], table, matches)
    write_table(table, vardict, select)
    return


@click.pass_context
def manage_env(ctx):
    """Prepare output directories and removes pre-existing ones
    """
    mop_log = ctx.obj['log']
    # check if output path already exists
    outpath = ctx.obj['outpath']
    if outpath.exists():
        answer = input(f"Output directory '{outpath}' exists.\n"+
                       "Delete and continue? [Y,n]\n")
        if answer == 'Y':
            try:
                shutil.rmtree(outpath)
            except OSError as e:
                raise(f"Error couldn't delete {outpath}: {e}")
        else:
            mop_log.info("Exiting")
            sys.exit()
    mop_log.info("Preparing job_files directory...")
    # Creating output directories
    ctx.obj['maps'].mkdir(parents=True)
    ctx.obj['tpath'].mkdir()
    ctx.obj['cmor_logs'].mkdir()
    ctx.obj['var_logs'].mkdir()
    # copy CV file to CMIP6_CV.json and formula and coordinate files
    cv_file = ctx.obj['tables_path'] / ctx.obj['_control_vocabulary_file']
    shutil.copyfile(cv_file, ctx.obj['tpath'] / "CMIP6_CV.json")
    for f in ['_AXIS_ENTRY_FILE', '_FORMULA_VAR_FILE', 'grids']:
        shutil.copyfile(ctx.obj['tables_path'] / ctx.obj[f],
                        ctx.obj['tpath'] / ctx.obj[f])
    shutil.copyfile(ctx.obj['appdir'] / "mopper/update_db.py",
                    ctx.obj['outpath'] / "update_db.py")
    return
