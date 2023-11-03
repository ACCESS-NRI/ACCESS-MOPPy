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
# last updated 28/07/2023

import os
import sys
import shutil
import calendar
import glob
import yaml
import json
import csv
import sqlite3
import subprocess
import ast
import copy
import re
from collections import OrderedDict
from datetime import datetime#, timedelta
from dateutil.relativedelta import relativedelta
from json.decoder import JSONDecodeError


def write_var_map(outpath, table, matches):
    """Write variables mapping to json file
    """
    with open(f"{outpath}/{table}.json", 'w') as fjson:
        json.dump(matches, fjson, indent=2)
    fjson.close()


def define_timeshot(frequency, resample, cell_methods):
    """Returns timeshot based on frequency, cell_methods and resample.
    It also fixes and returns frequency for pressure levels and
    climatology data.
    If data will be resample timeshot is mean/max/min
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


def find_matches(table, var, realm, frequency, varlist):
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

    Returns
    -------
    match : dict
        Dictionary containing matched variable specifications
        or None if not matches
    """
    near_matches = []
    found = False
    match = None
   # print(var, frequency, realm)
    for v in varlist:
        #print(v['cmor_var'], v['frequency'], v['realm'])
        if v['cmor_var'].startswith('#'):
            pass
        elif (v['cmor_var'] == var and v['realm'] == realm 
              and v['frequency'] == frequency):
            match = v
            found = True
        elif (v['cmor_var'].replace('_Pt','') == var
              and v['realm'] == realm):
            near_matches.append(v)
    if found is False:
        v = find_nearest(near_matches, frequency)
        if v is not None:
            match = v
            found = True
        else:
            print(f"could not find match for {table}-{var}-{frequency}")
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
            match['file_structure'] = f"/atm/netCDF/{match['filename']}*.nc"
    return match


def find_nearest(varlist, frequency):
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
    for frq in resample_order[freq_idx+1:]:
        for v in varlist:
            vfrq = v['frequency'].replace('Pt','').replace('C','')
            if vfrq == frq:
                v['resample'] = resample_frq[freq]
                v['nsteps'] = adjust_nsteps(v, freq)
                found = True
                var = v
                break
        if found:
            break
    return var


def adjust_nsteps(v, frq):
    """Adjust variable grid size to new number of timesteps,
    Each variable mapping definition has size of one timestep and
    number of time steps. If frequency changes as for resample
    then number of timesteps need to be adjusted.
    New number of time steps is:
      total_time(days) / nstep_day(new_frq)
    total_time (days) = nsteps*nstep_day(orig_frq) 
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


def read_yaml(fname):
    """Read yaml file
    """
    with open(fname, 'r') as yfile:
        data = yaml.safe_load(yfile)
    return data


def write_yaml(data, fname='exp_config.yaml'):
    """Write data to a yaml file

    Parameters
    ----------
    data : dict
        The file content as adictioanry 
    fname : str
        Yaml filename (default: exp_config.yaml)

    Returns
    -------
    """
    try:
        with open(fname, 'w') as f:
            yaml.dump(data, f)
    except:
        print(f"Check that {data} exists and it is an object compatible with json")
    return


def setup_env(config):
    """Sets up the configuration dictionary based on config file input

    Parameters
    ----------
    config : dict(dict)
        Dictionary including 'cmor' settings and attributes for experiment

    Returns
    -------
    config : dict(dict)
        Updated dictionary including 'cmor' settings and attributes for experiment
    """
    cdict = config['cmor']
    #output_loc and main are the same previously also outpath
    if cdict['outpath'] == 'default':
        cdict['outpath'] = f"/scratch/{cdict['project']}/{os.getenv('USER')}/MOPPER_output"
    #PP not sure it ever get used
    cdict['outpath'] = f"{cdict['outpath']}/{cdict['exp']}"
    # just making sure that custom_py is not in subroutines
    # cdict['appdir'] = cdict['appdir'].replace('/subroutines','')
    cdict['master_map'] = f"{cdict['appdir']}/{cdict['master_map']}"
    cdict['tables_path'] = f"{cdict['appdir']}/{cdict['tables_path']}"
    # Output subdirectories
    cdict['maps'] = f"{cdict['outpath']}/maps"
    cdict['tpath'] = f"{cdict['outpath']}/tables"
    cdict['cmor_logs'] = f"{cdict['outpath']}/cmor_logs"
    cdict['var_logs'] = f"{cdict['outpath']}/variable_logs"
    cdict['mop_logs'] = f"{cdict['outpath']}/mopper_logs"
    # Output files
    cdict['app_job'] = f"{cdict['outpath']}/mopper_job.sh"
    cdict['job_output'] =f"{cdict['outpath']}/job_output.OU"
    cdict['database'] = f"{cdict['outpath']}/mopper.db"
    # reference_date
    if cdict['reference_date'] == 'default':
        cdict['reference_date'] = f"{cdict['start_date'][:4]}-{cdict['start_date'][4:6]}-{cdict['start_date'][6:8]}"
    # make sure tstart and tend include hh:mm
    if len(cdict['start_date']) < 13:
        cdict['start_date'] += 'T0000'
        cdict['end_date'] += 'T0000'#'T2359'
    config['cmor'] = cdict
    # if parent False set parent attrs to 'no parent'
    print(config['attrs']['parent'])
    if config['attrs']['parent'] is False and cdict['mode'] == 'cmip6':
        p_attrs = [k for k in config['attrs'].keys() if 'parent' in k]
        for k in p_attrs:
            config['attrs'][k] = 'no parent'
    return config


def check_output_directory(path):
    """Check if mapping directory exists and remove pre-existing files 
    """
    if len(glob.glob(f"{path}/*.csv")) == 0:
        print(f"variable map directory: '{path}'")
    else:
        for fname in glob.glob(f"{path}/*.csv"):
            os.remove(fname)
        print(f"variable maps deleted from directory '{path}'")
    return


def check_path(path):
    """Check if path exists, if not creates it
    """
    if os.path.exists(path):
        print(f"found directory '{path}'")
    else:
        try:
            os.makedirs(path)
            print(f"created directory '{path}'")
        except OSError as e:
            sys.exit(f"failed to create directory '{path}';" +
                     "please create manually. \nexiting.")
    return


def find_custom_tables(cdict):
    """Returns list of tables files in custom table path
    """
    tables = []
    path = cdict['tables_path']
    tables = glob.glob(f"{path}/*_*.json")
    for f in table_files:
        f = f.replace(".json", "")
        #tables.append(f.split("_")[1])
        tables.append(f)
    print('should be here', tables)
    return tables


#PP part of using dreq need to double check e verything
def find_cmip_tables(dreq):
    """
    Returns
    -------
    """
    tables=[]
    with open(dreq, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if not row[0] in tables:
                if (row[0] != 'Notes') and (row[0] != 'MIP table') and (row[0] != '0'):
                    tables.append(f"CMIP6_{row[0]}")
    f.close()
    return tables


def check_file(fname):
    """Check if file exists, if not stop execution
    """
    if os.path.exists(fname):
        print(f"found file '{fname}'")
    else:
        sys.exit(f"file '{fname}' does not exist!")
    return


#PP part of dreq not needed otherwise
def reallocate_years(years, reference_date):
    """Reallocate years based on dreq years 
    Not sure what it does need to ask Chloe
    """
    reference_date = int(reference_date[:4])
    if reference_date < 1850:
        years = [year-1850+reference_date for year in years]
    else:
        pass
    return years


def fix_years(years, tstart, tend):
    """Update start and end date for experiment based on dreq
    constraints for years. It is called only if dreq and dreq_years are True

    Parameters
    ----------
    years : list
        List of years from dreq file
    tstart: str
        Date of experiment start as defined in config
    tend: str
        Date of experiment end as defined in config

    Returns
    -------
    tstart: str
        Updated date of experiment start
    tend: str
        Updated date of experiment end
    """
    if tstart >= years[0]:
        pass
    elif (tstart < years[0]) and (tend >= years[0]):
        tstart = years[0] + "0101T0000"
    else:
        tstart = None 
    if tend <= years[-1]:
        pass
    elif (tend > years[-1]) and (tstart <= years[-1]):
        tend = years[-1] + "1231T2359"
    else:
        tstart = None 
    return tstart, tend


def read_dreq_vars(cdict, table_id, activity_id):
    """Reads dreq variables file and returns a list of variables included in
    activity_id and experiment_id, also return dreq_years list

    Parameters
    ----------
    cdict : dict
        Dictionary with post-processing config 
    table_id : str
        CMIP table id
    activity_id: str
        CMIP activity_id

    Returns
    -------
    dreq_variables : dict
        Dictionary where keys are cmor name of selected variables and
        values are corresponding dreq years
    """
    with open(cdict['dreq'], 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        dreq_variables = {} 
        for row in reader:
            if (row[0] == table_id) and (row[12] not in ['', 'CMOR Name']):
                cmorname = row[12]
                mips = row[28].split(',')
                if activity_id not in mips:
                    continue
                try:
                    #PP if years==rangeplu surely calling this function will fail
                    # in any cas eis really unclear what reallocate years does and why, it returns different years
                    # if ref date before 1850???
                    if 'range' in row[31]:
                        years = reallocate_years(
                                ast.literal_eval(row[31]), cdict['reference_date'])
                        years = f'"{years}"'
                    elif 'All' in row[31]:
                        years = 'all'
                    else:
                        try:
                            years = ast.literal_eval(row[31])
                            years = reallocate_years(years, cdict['reference_date'])
                            years = f'"{years}"'
                        except:
                            years = 'all'
                except:
                    years = 'all'
                dreq_variables[cmorname] = years
    f.close()
    return dreq_variables


def create_var_map(cdict, table, mappings, activity_id=None, 
                        selection=None):
    """Create a mapping file for this specific experiment based on 
    model ouptut mappings, variables listed in table/s passed by config.
    Called by var_map

    Parameters
    ----------

    Returns
    -------
    """
    matches = []
    fpath = f"{cdict['tables_path']}/{table}.json"
    table_id = table.split('_')[1]
    try:
        with open(fpath, 'r') as fj:
             vardict = json.load(fj)
    except JSONDecodeError as e:
        print(f"Invalid json {fpath}: {e}")
        raise 
    row_dict = vardict['variable_entry']
    all_vars = [v for v in row_dict.keys()]
    # work out which variables you want to process
    select = all_vars 
    if selection is not None:
        select = [v for v in all_vars if v in selection]
    elif cdict['variable_to_process'] != 'all':
        select = [cdict['variable_to_process']]
    elif cdict['force_dreq'] is True:
        dreq_years = read_dreq_vars(cdict, table_id, activity_id)
        all_dreq = [v for v in dreq_years.keys()]
        select = set(select).intersection(all_dreq) 
    for var,row in row_dict.items():
        if var not in select:
            continue
        frq = row['frequency']
        realm = row['modeling_realm']
        years = 'all'
        if cdict['force_dreq'] and var in all_dreq:
            years = dreq_years[var]
        if 'subhr' in frq:
            frq =  cdict['subhr'] + frq.split('subhr')[1]
        match = find_matches(table, var, realm, frq, mappings)
        if match is not None:
            match['years'] = years
            matches.append(match)
    if matches == []:
        print(f"{table}:  no matching variables found")
    else:
        print(f"    Found {len(matches)} variables")
        write_var_map(cdict['maps'], table, matches)
    write_table(cdict, table, vardict, select)
    return


#PP this is where dreq process start it can probably be simplified
# if we can read dreq as any other variable list
# and change year start end according to experiment
def var_map(cdict, activity_id=None):
    """
    """
    tables = cdict.get('tables', 'all')
    subset = cdict.get('var_subset', False)
    sublist = cdict.get('var_subset_list', None)
    if subset is True:
        if sublist is None:
            print("var_subset is True but file with variable list not provided")
            sys.exit()
        elif sublist[-5:] != '.yaml':
            print(f"{sublist} should be a yaml file")
            sys.exit()
        else:
            sublist = f"{cdict['appdir']}/{sublist}"
            check_file(sublist)
# Custom mode vars
    if cdict['mode'].lower() == 'custom':
        access_version = cdict['access_version']
    # probably no need to check this!!
    check_path(cdict['maps'])
    if cdict['force_dreq'] is True:
        if cdict['dreq'] == 'default':
            cdict['dreq'] = 'data/dreq/cmvme_all_piControl_3_3.csv'
        check_file(cdict['dreq'])
    check_file(cdict['master_map'])
    with open(cdict['master_map'],'r') as f:
        reader = csv.DictReader(f, delimiter=';')
        mappings = list(reader)
    f.close()
    # this is removing .csv files from maps, is it necessary???
    check_output_directory(cdict['maps'])
    print(f"beginning creation of variable maps in directory '{cdict['maps']}'")
    if subset:
        selection = read_yaml(sublist)
        tables = [t for t in selection.keys()] 
        for table in tables:
            print(f"\n{table}:")
            create_var_map(cdict, table, mappings,
                selection=selection[table])
    elif tables.lower() == 'all':
        print(f"no priority list for local experiment '{cdict['exp']}', processing all variables")
        if cdict['force_dreq'] == True:
            tables = find_cmip_tables(cdict['dreq'])
        else:
            tables = find_custom_tables(cdict)
        for table in tables:
            print(f"\n{table}:")
            create_var_map(cdict, table, mappings, activity_id)
    else:
        create_var_map(cdict, tables, mappings)
    # make copy of tables with deflate_levels added
    return cdict


def write_table(cdict, table, vardict, select):
    """Write CMOR table in working directory
       Includes only selected variables and adds deflate levels.
    """
    new = copy.deepcopy(vardict)
    for k in vardict['variable_entry'].keys():
        if k not in select:
            new['variable_entry'].pop(k)
        else:
            new['variable_entry'][k]['deflate'] = 1
            new['variable_entry'][k]['deflate_level'] = cdict['deflate_level']
            new['variable_entry'][k]['shuffle'] = 1
    tjson = f"{cdict['tpath']}/{table}.json"
    with open(tjson,'w') as f:
        json.dump(new, f, indent=4, separators=(',', ': '))
    f.close
    return


#PP still creating a filelist table what to store in it might change!
def filelist_setup(conn):
    """Sets up filelist table in database
    """
    cursor = conn.cursor()
    #cursor.execute('drop table if exists filelist')
    #Create the filelist table
    try:
        cursor.execute('''create table if not exists filelist(
            infile text,
            filepath text,
            filename text,
            vin text,
            variable_id text,
            ctable text,
            frequency text,
            realm text,
            timeshot text,
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
            primary key(exp_id,variable_id,ctable,tstart,version))''')
    except Exception as e:
        print("Unable to create the APP filelist table.\n {e}")
        raise e
    conn.commit()
    return


def cleanup(config):
    """Prepare output directories and removes pre-existing ones
    """
    # check if output path already exists
    cdict = config['cmor']
    outpath = cdict['outpath']
    if os.path.exists(outpath):
        answer = input(f"Output directory '{outpath}' exists.\n"+
                       "Delete and continue? [Y,n]\n")
        if answer == 'Y':
            try:
                shutil.rmtree(outpath)
            except OSError as e:
                raise(f"Error couldn't delete {outpath}: {e}")
        else:
            print("Exiting")
            sys.exit()
    # If outpath doesn't exists or we just deleted
    # Get a list of all the file paths that ends with .txt from in specified directory
    toremove = glob.glob("./*.pyc'")
    for fpath in toremove:
        try:
            os.remove(filePath)
        except OSerror as e:
            print(f"Error while deleting {fpath}: {e}")
    print("Preparing job_files directory...")
    # Creating output directories
    os.makedirs(cdict['maps'], exist_ok=True)
    os.mkdir(cdict['tpath'])
    os.mkdir(cdict['cmor_logs'])
    os.mkdir(cdict['var_logs'])
    os.mkdir(cdict['mop_logs'])
    # copy CV file to CMIP6_CV.json and formula and coordinate files
    shutil.copyfile(f"{cdict['tables_path']}/{cdict['_control_vocabulary_file']}",
                    f"{cdict['tpath']}/CMIP6_CV.json")
    for f in ['_AXIS_ENTRY_FILE', '_FORMULA_VAR_FILE', 'grids']:
        shutil.copyfile(f"{cdict['tables_path']}/{cdict[f]}",
                        f"{cdict['tpath']}/{cdict[f]}")
    shutil.copyfile(f"{cdict['appdir']}/update_db.py",
                    f"{cdict['outpath']}/update_db.py")
    return


def define_template(cdict, flag, nrows):
    """Defines job file template
    not setting contact and I'm sure I don't need the other envs either!
    CONTACT={cdict['contact']}

    Parameters
    ----------
    cdict : dict
        Dictonary with cmor settings for experiment
    """
    template = f"""#!/bin/bash
#PBS -P {cdict['project']} 
#PBS -q {cdict['queue']}
#PBS -l {flag}
#PBS -l ncpus={cdict['ncpus']},walltime={cdict['walltime']},mem={cdict['nmem']}GB,wd
#PBS -j oe
#PBS -o {cdict['job_output']}
#PBS -e {cdict['job_output']}
#PBS -N mopper_{cdict['exp']}

module use /g/data/hh5/public/modules
module load conda/analysis3-23.04

# main
cd {cdict['appdir']}
python mopper.py  -i {cdict['exp']}_config.yaml wrapper 

# post
sort success.csv success_sorted.csv
mv success_sorted.csv success.csv
sort failed.csv failed_sorted.csv
mv failed_sorted.csv failed.csv
echo 'APP completed for exp {cdict['exp']}.'"""
    return template


def write_job(cdict, nrows):
    """
    """
    # define storage flag
    flag = "storage=gdata/hh5+gdata/access"
    projects = cdict['addprojs'] + [cdict['project']]
    for proj in projects:
       flag += f"+scratch/{proj}+gdata/{proj}"
    # work out number of cpus based on number of files to process
    if nrows <= 24:
        cdict['ncpus'] = nrows
    else:
        cdict['ncpus'] = 24
    cdict['nmem'] = cdict['ncpus'] * cdict['mem_per_cpu']
#NUM_MEM=$(echo "${NUM_CPUS} * ${MEM_PER_CPU}" | bc)
    if cdict['nmem'] >= 1470: 
        cdict['nmem'] = 1470
    print(f"number of files to create: {nrows}")
    print(f"number of cpus to be used: {cdict['ncpus']}")
    print(f"total amount of memory to be used: {cdict['nmem']}GB")
    fpath = cdict['app_job']
    template = define_template(cdict, flag, nrows)
    with open(fpath, 'w') as f:
        f.write(template)
    return cdict


def create_exp_json(config, json_cv):
    """Create a json file as expected by CMOR to describe the dataset
    and passed the main global attributes. Add source and source_id to CV file
    if necessary.

    Parameters
    ----------
    config : dict(dict)
        Dictionary with both cmor settings and attributes defined for experiment
    json_cv : str
        Path of CV json file to edit

    Returns
    -------
    fname : str
        Name of created experiment json file
    """
    # outpath empty, calendar not there
    # template outpath etc different use <> instead of {}
    cdict = config['cmor']
    attrs = config['attrs']
    with open(json_cv, 'r') as f:
        cv_dict = json.load(f)
    # check if source_id is present in CV as it is hardcoded
    # if present but source description is different overwrite file in custom mode
    if any(x not in attrs.keys() for x in ['source_id', 'source']):
        print('Source and source_id need to be defined')
        sys.exit()
    at_sid, at_source = attrs['source_id'], attrs['source']  
    cv_sid = cv_dict['CV']['source_id'].get(at_sid,'')
    if cv_sid == '' or cv_sid['source'] != at_source:
       if cv_sid == '' and cmor['mode'] == 'cmip6':
           print(f"source_id {at_sid} not defined in CMIP6_CV.json file")
           sys.exit()
       cv_dict['CV']['source_id'][at_sid] = {'source_id': at_sid,
           'source': at_source}
       #cv_data = json.dumps(cv_dict, indent=4, default = str)
       with open(json_cv, 'w') as f:
           json.dump(cv_dict, f, indent=4)
    # read required attributes from cv file
    # and add attributes for path and file template to required
    required = cv_dict['CV']['required_global_attributes']
    tmp_str = (cdict['path_template'].replace('}/{','/') 
               + cdict['file_template'].replace('}_{','/'))
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
            glob_attrs[k] = cdict.get(k, '')
    # temporary correction until CMIP6_CV file anme is not anymore hardcoded in CMOR
    glob_attrs['_control_vocabulary_file'] = f"{cdict['outpath']}/CMIP6_CV.json"
    # replace {} _ and / in output templates
    glob_attrs['output_path_template'] = cdict['path_template'].replace('{','<').replace('}','>').replace('/','')
    glob_attrs['output_file_template'] = cdict['file_template'].replace('}_{','><').replace('}','>').replace('{','<')
    #glob_attrs['table_id'] = cdict['table']
    if cdict['mode'] == 'cmip6':
        glob_attrs['experiment'] = attrs['experiment_id']
    else:
        glob_attrs['experiment'] = cdict.get('exp','')
    # write glob_attrs dict to json file
    fname = f"{cdict['outpath']}/{cdict['exp']}.json"
    # parent attrs don't seem to be included should I add them manually?
    # at least for mode = cmip6
    json_data = json.dumps(glob_attrs, indent = 4, sort_keys = True, default = str)
    with open(fname, 'w') as f:
        f.write(json_data)
    f.close()
    return fname


def edit_json_cv(json_cv, attrs):
    """Edit the CMIP6 CV json file to include extra activity_ids and
    experiment_ids, so they can be recognised by CMOR when following 
    CMIP6 standards.

    Parameters
    ----------
    json_cv : str
        Path of CV json file to edit
    attrs: dict
        Dictionary with attributes defined for experiment

    Returns
    -------
    """
    activity_id = attrs['activity_id']
    experiment_id = attrs['experiment_id']

    with open(json_cv, 'r') as f:
        json_cv_dict = json.load(f, object_pairs_hook=OrderedDict)
    f.close()

    if activity_id not in json_cv_dict['CV']['activity_id']:
        print(f"activity_id '{activity_id}' not in CV, adding")
        json_cv_dict['CV']['activity_id'][activity_id] = activity_id

    if experiment_id not in json_cv_dict['CV']['experiment_id']:
        print(f"experiment_id '{attrs['experiment_id']}' not in CV, adding")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [attrs['parent_activity_id']],
        'parent_experiment_id': [attrs['parent_experiment_id']],
        'required_model_components': [attrs['source_type']],
        'sub_experiment_id': ['none']
        })
    else:
        print(f"experiment_id '{experiment_id}' found, updating")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [attrs['parent_activity_id']],
        'parent_experiment_id': [attrs['parent_experiment_id']],
        'required_model_components': [attrs['source_type']],
        'sub_experiment_id': ['none']
        })
    with open(json_cv,'w') as f:
        json.dump(json_cv_dict, f, indent=4, separators=(',', ': '))
    f.close
    return


#PP I have the feeling that pupulate/ppulate_unlimtied etc might be joined into one?
def populate(conn, config):
    """Populate filelist db table, this will be used by app to
    process all files

    Parameters
    ----------
    conn : obj 
        DB connection object
    config : dict(dict) 
        Dictionary including 'cmor' settings and attributes for experiment

    Returns
    -------
    """
    #defaults
    #config['cmor']['status'] = 'unprocessed'
    #get experiment information
    opts = {}
    opts['status'] = 'unprocessed'
    opts['outpath'] = config['cmor']['outpath']
    version = config['attrs'].get('version', datetime.today().strftime('%Y%m%d'))
    # ACDD uses product_version
    config['attrs']['version'] = config['attrs'].get('product_version', version)
    #Experiment Details:
    for k,v in config['attrs'].items():
        opts[k] = v
    opts['exp_id'] = config['cmor']['exp'] 
    opts['exp_dir'] = config['cmor']['datadir']
    opts['reference_date'] = config['cmor']['reference_date']
    opts['exp_start'] = config['cmor']['start_date'] 
    opts['exp_end'] = config['cmor']['end_date']
    opts['access_version'] = config['cmor']['access_version']
    opts['json_file_path'] = config['cmor']['json_file_path'] 
    print(f"found local experiment: {opts['exp_id']}")
    cursor = conn.cursor()
    #monthly, daily unlimited except cable or moses specific diagnostics
    rows = []
    tables = glob.glob(f"{config['cmor']['maps']}/*.json")
    for table in tables:
        with open(table, 'r') as fjson:
            data = json.load(fjson)
        rows.extend(data)
    populate_rows(rows, config['cmor'], opts, cursor)
    conn.commit()
    return


def add_row(values, cursor):
    """Add a row to the filelist database table
       one row specifies the information to produce one output cmip5 file

    Parameters
    ----------
    values : list
        Path of CV json file to edit
    cursor : obj 
        Dictionary with attributes defined for experiment
    Returns
    -------
    """
    try:
        cursor.execute('''insert into filelist
            (infile, filepath, filename, vin, variable_id,
            ctable, frequency, realm, timeshot, tstart, tend,
            sel_start, sel_end, status, file_size, exp_id,
            calculation, resample, in_units, positive, cfname,
            source_id, access_version, json_file_path,
            reference_date, version)
            values
            (:infile, :filepath, :filename, :vin, :variable_id,
            :table, :frequency, :realm, :timeshot, :tstart, :tend,
            :sel_start, :sel_end, :status, :file_size, :exp_id,
            :calculation, :resample, :in_units, :positive, :cfname,
            :source_id, :access_version, :json_file_path,
            :reference_date, :version)''', values)
    except sqlite3.IntegrityError as e:
        print(f"Row already exists:\n{e}")
    except Exception as e:
        print(f"Could not insert row for {values['filename']}:\n{e}")
    return cursor.lastrowid


def check_calculation(opts, insize):
    """

    Returns
    -------
    """
    # transport/transects/tiles should reduce size
    # volume,any vertical sum
    # resample will affect frequency but that should be already taken into account in mapping
    calc = opts['calculation']
    resample = opts['resample']
    grid_size = insize
    if 'plevinterp' in calc:
        try:
            plevnum = calc.split(',')[-1]
        except:
            raise('check plevinterp calculation definition plev probably missing')
        plevnum = float(plevnum.replace(')',''))
        grid_size = float(insize)/float(opts['levnum'])*plevnum
    return grid_size


#PP if this approach is ok I should move the interval definition out of here
# and as for everything else in yaml file
def compute_fsize(cdict, opts, grid_size, frequency):
    """Calculate an estimated output file size (in megabytes)
       and the interval to use to satisfy max_size decided by user

    Parameters
    ----------
    json_cv : str
        Path of CV json file to edit
    attrs: dict
        Dictionary with attributes defined for experiment

    Returns
    -------
    """
    nstep_day = {'10min': 144, '30min': 48, '1hr': 24, '3hr': 8, 
                 '6hr': 4, 'day': 1, '10day': 0.1, 'mon': 1/30, 
                 'yr': 1/365, 'dec': 1/3652}
    max_size = cdict['max_size']
    # work out if grid-size might change because of calculation
    if opts['calculation'] != '' or opts['resample'] != '':
        grid_size = check_calculation(opts, grid_size)
    size_tstep = int(grid_size)/(1024**2)

    # work out how long is the entire span in days
    start = datetime.strptime(str(cdict['start_date']), '%Y%m%dT%H%M')
    finish = datetime.strptime(str(cdict['end_date']), '%Y%m%dT%H%M')
    delta = (finish - start).days 
    # if overall interval less than a day use seconds as days will be 0
    if delta == 0:
        delta = (finish - start).seconds/(3600*24)
    # calculate the size of potential file intervals depending on timestep frequency
    size = {}
    size['days=0.25'] = size_tstep * nstep_day[frequency] * 0.25
    size['days=0.5'] = size_tstep * nstep_day[frequency] * 0.5
    size['days=1'] = size_tstep * nstep_day[frequency]
    size[f'days={delta}'] = size['days=1'] * delta
    size['days=7'] = size['days=1'] * 7
    size['months=1'] = size['days=1'] * 30
    size['years=1'] = size['months=1'] * 12
    size['years=10'] = size['years=1'] * 10
    size['years=100'] = size['years=10'] * 10
    # Evaluate intervals in order starting from all timeseries 
    # and then from longer to shorter
    if size[f'days={delta}'] <= max_size*1.1:
        interval = f'days={delta}' 
    else:
        for interval in ['years=100', 'years=10', 'years=1', 'months=1',
                         'days=7', 'days=1', 'days=0.5', 'days=0.25']:
            if size[interval] <= max_size*1.1:
                    break
    return interval, size[interval]


#PP I super simplified this not sure there's much point in trying to double guess final name
# it might be enough to make sure dates are correct?
def build_filename(cdict, opts, tstart, tend, half_tstep):
    """Builds name for file to be created based on template in config
    NB we are using and approximations for dates
    not including here exact hour

    Parameters
    ----------
    cdict : dict
        Dictonary with cmor settings for experiment
    opts : dict
        Dictionary with attributes for a specific variable

    Returns
    -------
    fpath : str
        Path for file to be created
    fname : str
        Name for file to be created
    """
    frequency = opts['frequency'].replace("Pt","").replace("CM","").replace("C","")
    # add/subtract half timestep from start/end to mimic cmor
    if opts['timeshot'] == 'point':
        tstart = tstart + 2*half_tstep
    else:
        tstart = tstart + half_tstep
        tend = tend - half_tstep
    stamp = '%4Y%m%d%H%M%S'
    if frequency != 'fx':
        if frequency in ['yr', 'dec']:
            stamp = stamp[:3]
        elif frequency == 'mon':
            stamp = stamp[:5]
        elif frequency == 'day':
            stamp = stamp[:7]
        elif 'hr' in frequency:
            stamp = stamp[:11]
        tstart = tstart.strftime(stamp)
        tend = tend.strftime(stamp)
        opts['date_range'] = f"{tstart}-{tend}"
    else:
        opts['date_range'] = ""
    # PP we shouldn't need this as now we pas subhr and then the actual minutes spearately
    if 'min' in frequency:
        opts['frequency'] = 'subhr'
        if opts['timeshot'] == 'point':
            opts['frequency'] = 'subhrPt'
    opts['version'] = opts['version'].replace('.', '-')
    path_template = f"{cdict['outpath']}/{cdict['path_template']}"
    fpath = path_template.format(**opts)
    fname = cdict['file_template'].format(**opts) + f"_{opts['date_range']}" 
    if opts['timeshot'] == "clim":
        fname = fname + "-clim"
    fname = fname + ".nc"
    return fpath, fname


def populate_rows(rows, cdict, opts, cursor):
    """Populates filelist table, with values from config and mapping.
    Works out how many files to generate based on grid size. 

    Parameters
    ----------
    rows : list(dict)
        List of dictionaries where each item represents one file to create
    cdict : dict
        Dictonary with cmor settings for experiment
    opts : dict
        Dictionary with attributes of specific variable to update
    cursor : obj
        Cursor of db connection object

    Returns
    -------
    """
    tableToFreq = read_yaml(f"data/table2freq.yaml")
    tstep_dict = {'10min': 'minutes=10', '30min': 'minutes=30',
        '1hr': 'hours=1', '3hr': 'hours=3', '6hr': 'hours=6',
        'day': 'days=1', '10day': 'days=10', 'mon': 'months=1',
        'yr': 'years=1', 'dec': 'years=10'}
    for champ in rows:
        #from champions table:
        table_id = champ['table'].split('_')[1]
        opts['frequency'] = tableToFreq[table_id]
        opts['realm'] = champ['realm']
        opts['table'] = champ['table']
        opts['table_id'] = table_id
        opts['variable_id'] = champ['cmor_var'] # cmor_var
        opts['vin'] = champ['input_vars'] # access_vars
        paths = champ['file_structure'].split() 
        opts['infile'] = ''
        for x in paths:
            opts['infile'] += f"{opts['exp_dir']}/{x} "
        opts['calculation'] = champ['calculation']
        opts['resample'] = champ['resample']
        opts['in_units'] = champ['units']
        opts['positive'] = champ['positive']
        opts['timeshot'] = champ['timeshot']
        opts['levnum'] = cdict['levnum']
        opts['cfname'] = champ['standard_name']
        define_files(cursor, opts, champ, cdict)
    return


def define_files(cursor, opts, champ, cdict):
    """Determines tstart and tend, filename and path and size for each file
    to produce for variable. Based on frequency, time range to cover and 
    time interval for each file. This last is determined by maximum file size.
    These and other files details are saved in filelist db table.
    """
    exp_start = opts['exp_start']
    exp_end = opts['exp_end']
    if champ['years'] != 'all' and cdict['dreq_years']:
        exp_start, exp_end = fix_years(champ['years'], exp_start[:4], exp_end[:4]) 
        if exp_start is None:
            print("Years requested for variable are outside specified") 
            print((f"period: {table_id}, {var},",  
                   f"{match['tstart']}, {match['tend']}"))
            return
    tstep_dict = {'10min': ['minutes=10', 'minutes=5'],
              '30min': ['minutes=30', 'minutes=15'],
              '1hr': ['hours=1', 'minutes=30'],
              '3hr': ['hours=3', 'hours=1.5'],
              '6hr': ['hours=6', 'hours=3'],
              'day': ['days=1', 'hours=12'],
              '10day': ['days=10','days=5'],
              'mon': ['months=1', 'days=15'],
              'yr': ['years=1', 'months=6'],
              'dec': ['years=10', 'years=5']}
    start = datetime.strptime(str(exp_start), '%Y%m%dT%H%M')
    finish = datetime.strptime(str(exp_end), '%Y%m%dT%H%M')
    frq = opts['frequency']
    if 'subhr' in frq:
        frq =  cdict['subhr'] + frq.split('subhr')[1]
    # interval is file temporal range as a string to evaluate timedelta
    interval, opts['file_size'] = compute_fsize(cdict, opts,
        champ['size'], frq)
    #loop over times
    while (start < finish):
        tstep = eval(f"relativedelta({tstep_dict[frq][0]})")
        half_tstep = eval(f"relativedelta({tstep_dict[frq][1]})")
        delta = eval(f"relativedelta({interval})")
        newtime = min(start+delta, finish)
        tstart = start + half_tstep 
        opts['tstart'] = tstart.strftime('%4Y%m%dT%H%M')
        opts['tend'] = newtime.strftime('%4Y%m%dT%H%M')
        # select files on 1 tstep wider interval to account for timestamp shifts 
        opts['sel_start'] = start.strftime('%4Y%m%d%H%M')
        opts['sel_end'] = (newtime - half_tstep).strftime('%4Y%m%d%H%M')
        opts['filepath'], opts['filename'] = build_filename(cdict,
            opts, start, newtime, half_tstep)
        rowid = add_row(opts, cursor)
        start = newtime
    return


def count_rows(conn, exp):
    """Returns number of files to process
    """
    cursor=conn.cursor()
    cursor.execute(f"select * from filelist where status=='unprocessed' and exp_id=='{exp}'")
    #cursor.execute(f"select * from filelist")
    rows = cursor.fetchall()
    print(f"Number of rows in filelist: {len(rows)}")
    return len(rows)


def sum_file_sizes(conn):
    """Returns estimate of total size of files to process
    """
    cursor=conn.cursor()
    cursor.execute('select file_size from filelist')
    sizeList=cursor.fetchall()
    size=0.0
    for s in sizeList:
        size += float(s[0])
    size = size/1024.
    return size


def main():
    """Main section: 
    * takes one argument the config yaml file with list of settings
      and attributes to add to files
    * set up paths and config dictionaries
    * updates CV json file if necessary
    * select variables and corresponding mappings based on table
      and constraints passed in config file
    * create/update database filelist table to list files to create
    * write job executable file and submit to queue 
    """
    config_file = sys.argv[1]
    # first read config passed by user
    config = read_yaml(config_file)
    # then add setup_env to config
    config = setup_env(config)
    cdict = config['cmor']
    cleanup(config)
    #json_cv = f"{cdict['outpath']}/{cdict['_control_vocabulary_file']}"
    json_cv = f"{cdict['tpath']}/CMIP6_CV.json"
    fname = create_exp_json(config, json_cv)
    cdict['json_file_path'] = fname
    if cdict['mode'] == 'cmip6':
        edit_json_cv(json_cv, config['attrs'])
        cdict = var_map(cdict, config['attrs']['activity_id'])
    else:
        cdict = var_map(cdict)
    #database_manager
    database = cdict['database']
    print(f"creating & using database: {database}")
    conn = sqlite3.connect(database)
    conn.text_factory = str
    #setup database tables
    filelist_setup(conn)
    populate(conn, config)
    #PP this can be totally done directly in cli.py, if it needs doing at all!
    #create_database_updater()
    nrows = count_rows(conn, cdict['exp'])
    tot_size = sum_file_sizes(conn)
    print(f"Total file size before compression is: {tot_size} GB")
    #write app_job.sh
    config['cmor'] = write_job(cdict, nrows)
    print(f"app job script: {cdict['app_job']}")
    # write setting to yaml file to pass to wrapper
    fname = f"{cdict['exp']}_config.yaml"
    print("Exporting config data to yaml file")
    write_yaml(config, fname)
    #submint job
    if cdict['test'] is False:
        os.chmod(cdict['app_job'], 775)
        status = subprocess.run(f"qsub {cdict['app_job']}", shell=True)
        if status.returncode != 0:
            print(f"{cdict['app_job']} submission failed, " +
                  f"returned code is {status.returncode}.\n Try manually")
    

if __name__ == "__main__":
    main()
