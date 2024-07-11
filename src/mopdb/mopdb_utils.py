#!/usr/bin/env python
# Copyright 2023 ARC Centre of Excellence for Climate Extremes (CLEX)
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> for CLEX
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
# contact: paola.petrelli@utas.edu.au
#
# last updated 10/04/2024
#

import sqlite3
import logging
import sys
import os
import csv
import json
import stat
import xarray as xr
import numpy as np
import math
from datetime import datetime, date
from collections import Counter
from operator import itemgetter, attrgetter
from pathlib import Path

from mopdb.mopdb_class import FPattern, Variable

def config_log(debug):
    """Configures log file"""
    # start a logger
    logger = logging.getLogger('mopdb_log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    # set the level for the logger, has to be logging.LEVEL not a string
    level = logging.INFO
    flevel = logging.WARNING
    if debug:
        level = logging.DEBUG
        flevel = logging.DEBUG
    logger.setLevel(level)

    # add a handler to send WARNING level messages to console
    # or DEBUG level if debug is on
    clog = logging.StreamHandler()
    clog.setLevel(level)
    logger.addHandler(clog)

    # add a handler to send INFO level messages to file
    # the messagges will be appended to the same file
    # create a new log file every month
    day = date.today().strftime("%Y%m%d")
    logname = 'mopdb_log_' + day + '.txt'
    flog = logging.FileHandler(logname)
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except OSError:
        pass
    flog.setLevel(flevel)
    flog.setFormatter(formatter)
    logger.addHandler(flog)
    # return the logger object
    return logger


def db_connect(db):
    """Connects to ACCESS mapping sqlite database"""
    mopdb_log = logging.getLogger('mopdb_log')
    conn = sqlite3.connect(db, timeout=10, isolation_level=None)
    if conn.total_changes == 0:
        mopdb_log.info(f"Opened database {db} successfully")
    return conn 


def mapping_sql():
    """Returns sql to define mapping table

    Returns
    -------
    sql : str
        SQL style string defining mapping table
    """
    sql = ("""CREATE TABLE IF NOT EXISTS mapping (
                cmor_var TEXT,
                input_vars TEXT,
                calculation TEXT,
                units TEXT,
	        dimensions TEXT,
                frequency TEXT,
                realm TEXT,
                cell_methods TEXT,
                positive TEXT,
                cmor_table TEXT,
                model TEXT,
                notes TEXT,
                origin TEXT,
                PRIMARY KEY (cmor_var, input_vars, cmor_table, model)
                ) WITHOUT ROWID;""")
    return sql


def cmorvar_sql():
    """Returns sql definition of cmorvar table

    Returns
    -------  
    sql : str
        SQL style string defining cmorvar table
    """
    sql = ("""CREATE TABLE IF NOT EXISTS cmorvar (
                name TEXT PRIMARY KEY,
                frequency TEXT,
                modeling_realm TEXT,
                standard_name TEXT,
                units TEXT,
                cell_methods TEXT,
                cell_measures  TEXT,
                long_name TEXT,
                comment TEXT,
                dimensions TEXT,
                out_name TEXT,
                type TEXT,
                positive TEXT,
                valid_min TEXT,
                valid_max TEXT,
                flag_values TEXT,
                flag_meanings TEXT,
                ok_min_mean_abs TEXT,
                ok_max_mean_abs TEXT);""")
    return sql


def map_update_sql():
    """Returns sql needed to update mapping table

    Returns
    -------
    sql : str
        SQL style string updating mapping table
    should add RETURNING cmor_var at the end
    """
    cols = ['cmor_var', 'input_vars', 'calculation', 'units',
            'dimensions', 'frequency', 'realm', 'cell_methods',
            'positive', 'cmor_table', 'model', 'notes', 'origin']
    sql = f"""REPLACE INTO mapping ({', '.join(cols)}) VALUES
          ({','.join(['?']*len(cols))}) ON CONFLICT DO UPDATE SET
          {', '.join(x+' = excluded.'+x for x in cols)}"""
    return sql


def cmor_update_sql():
    """Returns sql needed to update cmorvar table

    Returns
    -------
    sql : str
        SQL style string updating cmorvar table
    """
    cols = ['name', 'frequency', 'modeling_realm', 'standard_name',
            'units', 'cell_methods', 'cell_measures', 'long_name',
            'comment', 'dimensions', 'out_name', 'type', 'positive',
            'valid_min', 'valid_max', 'flag_values', 'flag_meanings',
            'ok_min_mean_abs', 'ok_max_mean_abs']
    sql = f"""REPLACE INTO cmorvar ({', '.join(cols)}) VALUES
          ({','.join(['?']*len(cols))}) ON CONFLICT (name) DO UPDATE SET
          {', '.join(x+' = excluded.'+x for x in cols)}"""
    return sql


def create_table(conn, sql):
    """Creates table if database is empty

    Parameters
    ----------
    conn : connection object
    sql : str
        SQL style string defining table to create
    """
    mopdb_log = logging.getLogger('mopdb_log')
    try:
        c = conn.cursor()
        c.execute(sql)
    except Exception as e:
        mopdb_log.error(e)
    return


def update_db(conn, table, rows_list):
    """Adds to table new variables definitions

    Parameters
    ----------
    conn : connection object
    table : str
        Name of database table to use
    rows_list : list
        List of str represneting rows to add to table
    """
    mopdb_log = logging.getLogger('mopdb_log')
    # insert into db
    if table == 'cmorvar':
        sql = cmor_update_sql()
    elif table == 'mapping':
        sql = map_update_sql()
    else:
        mopdb_log.error("Provide an insert sql statement for table: {table}")
    if len(rows_list) > 0:
        mopdb_log.info('Updating db ...')
        with conn:
            c = conn.cursor()
            mopdb_log.debug(sql)
            c.executemany(sql, rows_list)
            nmodified = c.rowcount
            mopdb_log.info(f"Rows modified: {nmodified}")
    conn.close()
    mopdb_log.info('--- Done ---')
    return


def query(conn, sql, tup=(), first=True):
    """Executes generic sql query and returns row/s

    Parameters
    ----------
    conn : connection object
        Connection to sqlite database
    sql : str
        sql string representing query
    tup : tuple
        By default empty, used to pass values when placeholder ? is used
        in sql string
    first : boolean
        By default True will return only first record found, set to False
        to return all matching records

    Returns
    -------
    result : tuple/list(tuple)
        tuple or a list of, representing row/s returned by query 
    """
    mopdb_log = logging.getLogger('mopdb_log')
    with conn:
        c = conn.cursor()
        c.execute(sql, tup)
        if first:
            result = c.fetchone()
        else:
            result = [ x for x in c.fetchall() ]
        #columns = [description[0] for description in c.description]
        return result


def get_columns(conn, table):
    """Gets list of columns from db table
    """
    mopdb_log = logging.getLogger('mopdb_log')
    sql = f'PRAGMA table_info({table});'
    table_data = query(conn, sql, first=False)
    columns = [x[1] for x in table_data]
    return columns


def get_cmorname(conn, vobj, version):
    """Queries mapping table for cmip name given variable name as output
       by the model
    """
    mopdb_log = logging.getLogger('mopdb_log')
    sql = f"""SELECT cmor_var,model,cmor_table,frequency FROM mapping
        WHERE input_vars='{vobj.name}' and (calculation=''
        or calculation IS NULL)""" 
    results = query(conn, sql, first=False)
    names = list(x[0] for x in results) 
    tables = list(x[2] for x in results) 
    mopdb_log.debug(f"In get_cmorname query results: {results}")
    if len(names) == 0:
        vobj.cmor_var = ''
        vobj.cmor_table = ''
    elif len(names) == 1:
        vobj.cmor_var = names[0]
        vobj.cmor_table = tables[0]
    elif len(names) > 1:
        mopdb_log.debug(f"Found more than 1 definition for {vobj.name}:\n" +
                       f"{results}")
        match_found = False
        for r in results:
            if r[1] == version and r[3] == vobj.frequency:
                vobj.cmor_var, vobj.cmor_table = r[0], r[2]
                match_found = True
                break
        if not match_found:
            for r in results:
                if r[3] == vobj.frequency:
                    vobj.cmor_var, vobj.cmor_table = r[0], r[2]
                    match_found = True
                    break
        if not match_found:
            for r in results:
                if r[1] == version:
                    vobj.cmor_var, vobj.cmor_table = r[0], r[2]
                    match_found = True
                    break
        if not match_found:
            vobj.cmor_var = names[0]
            vobj.cmor_table = tables[0]
            mopdb_log.info(f"Found more than 1 definition for {vobj.name}:\n"+
                        f"{results}\n Using {vobj.cmor_var} from {vobj.cmor_table}")
    return vobj


def cmor_table_header(name, realm, frequency):
    """
    """
    today = date.today()
    interval = {'dec': "3650.0", 'yr': "365.0", 'mon': "30.0",
                'day': "1.0", '6hr': "0.25", '3hr': "0.125",
                '1hr': "0.041667", '10min': "0.006944", 'fx': "0.0"}
    header = {
        "data_specs_version": "01.00.33",
        "cmor_version": "3.5",
        "table_id": f"Table {name}",
        "realm": realm,
        "table_date": today.strftime("%d %B %Y"),
        "missing_value": "1e20",
        "int_missing_value": "-999",
        "product": "model-output",
        "approx_interval": interval[frequency],
        "generic_levels": "",
        "mip_era": "",
        "Conventions": "CF-1.7 ACDD1.3"
    }
    return header


def write_cmor_table(var_list, name):
    """
    """
    mopdb_log = logging.getLogger('mopdb_log')
    realms = [v[2] for v in var_list]
    setr = set(realms)
    if len(setr) > 1:
        realm = Counter(realms).most_common(1)[0][0]
        mopdb_log.info(f"More than one realms found for variables: {setr}")
        mopdb_log.info(f"Using: {realm}")
    else:
        realm = realms[0]
    freqs = [v[1] for v in var_list]
    setf = set(freqs)
    if len(setf) > 1:
        frequency = Counter(freqs).most_common(1)[0][0]
        mopdb_log.info(f"More than one freqs found for variables: {setf}")
        mopdb_log.info(f"Using: {frequency}")
    else:
        frequency = freqs[0]
    header = cmor_table_header(name, realm, frequency)
    out = {"Header": header, "variable_entry": []}
    keys = ["frequency", "modeling_realm",
            "standard_name", "units",
            "cell_methods", "cell_measures",
            "long_name", "comment", "dimensions",
            "out_name", "type", "positive",
            "valid_min", "valid_max",
            "ok_min_mean_abs", "ok_max_mean_abs"] 
    var_dict = {}
    for v in var_list:
        var_dict[v[0]] = dict(zip(keys, v[1:]))
    out["variable_entry"] = var_dict
    jfile = f"CMOR_{name}.json"
    with open(jfile, 'w') as f:
        json.dump(out, f, indent=4)
    return


def delete_record(conn, table, col, pairs):
    """Deletes record from table based on pairs of column and
    value passed for selection

    Parameters
    ----------
    conn : connection object
        connection to db
    table: str
        db table name
    col: str
        name of column to return with query
    pairs : list[tuple(str, str)]
        pairs of columns, values to select record/s
    """
    mopdb_log = logging.getLogger('mopdb_log')
    # Set up query
    sqlwhere = f"FROM {table} WHERE "
    for c,v in pairs:
        sqlwhere += f"{c}='{v}' AND "
    sql = f"SELECT {col} " + sqlwhere[:-4]
    mopdb_log.debug(f"Delete query: {sql}")
    xl = query(conn, sql, first=False)
    # Delete from db
    if xl is not None:
        mopdb_log.info(f"Found {len(xl)} records")
        for x in xl:
            mopdb_log.info(f"{x}")
        confirm = input('Confirm deletion from database: Y/N   ')
        if confirm == 'Y':
            mopdb_log.info('Updating db ...')
            with conn:
                c = conn.cursor()
                sql = "DELETE " + sqlwhere[:-4]
                mopdb_log.debug(f"Delete sql: {sql}")
                c.execute(sql)
                c.execute('select total_changes()')
                mopdb_log.info(f"Rows modified: {c.fetchall()[0][0]}")
    else:
        mopdb_log.info("The query did not return any records")
    conn.close()
    return


def get_file_frq(ds, fnext):
    """Return a dictionary with frequency for each time axis.

    Frequency is inferred by comparing interval between two consecutive
    timesteps with expected interval at a given frequency.
    Order time_axis so ones with only one step are last, so we can use 
    file frequency (interval_file) inferred from other time axes.
    This is called if there are more than one time axis in file 
    (usually only UM) or if frequency can be guessed from filename.
    """
    mopdb_log = logging.getLogger('mopdb_log')
    frq = {}
    int2frq = {'dec': 3652.0, 'yr': 365.0, 'mon': 30.0,
               'day': 1.0, '6hr': 0.25, '3hr': 0.125,
               '1hr': 0.041667, '30min': 0.020833, '10min': 0.006944}
    # retrieve all time axes
    time_axs = [d for d in ds.dims if 'time' in d]
    time_axs_len = set(len(ds[d]) for d in time_axs)
    time_axs.sort(key=lambda x: len(ds[x]), reverse=True)
    mopdb_log.debug(f"in get_file_frq, time_axs: {time_axs}")
    max_len = len(ds[time_axs[0]]) 
    # if all time axes have only 1 timestep we cannot infer frequency
    # so we open also next file but get only time axs
    if max_len == 1:
        dsnext = xr.open_dataset(fnext, decode_times = False)
        time_axs2 = [d for d in dsnext.dims if 'time' in d]
        ds = xr.concat([ds[time_axs], dsnext[time_axs2]], dim='time')
        time_axs = [d for d in ds.dims if 'time' in d]
        time_axs_len = set(len(ds[d]) for d in time_axs)
        time_axs.sort(key=lambda x: len(ds[x]), reverse=True)
    for t in time_axs: 
        mopdb_log.debug(f"len of time axis {t}: {len(ds[t])}")
        if len(ds[t]) > 1:
            interval = (ds[t][1]-ds[t][0]).values
            interval_file = (ds[t][-1] -ds[t][0]).values 
        else:
            interval = interval_file
        mopdb_log.debug(f"interval 2 timesteps for {t}: {interval}")
        for k,v in int2frq.items():
            if math.isclose(interval, v, rel_tol=0.05):
                frq[t] = k
                break
    return frq


def get_cell_methods(attrs, dims):
    """Get cell_methods from variable attributes.
       If cell_methods is not defined assumes values are instantaneous
       `time: point`
       If `area` not specified is added at start of string as `area: `
    """
    mopdb_log = logging.getLogger('mopdb_log')
    frqmod = ''
    val = attrs.get('cell_methods', "") 
    if 'area' not in val: 
        val = 'area: ' + val
    time_axs = [d for d in dims if 'time' in d]
    if len(time_axs) == 1:
        if 'time' not in val:
            val += "time: point"
            frqmod = 'Pt'
        else:
            val = val.replace(time_axs[0], 'time')
    return val, frqmod


def write_varlist(conn, indir, match, version, alias):
    """Based on model output files create a variable list and save it
       to a csv file. Main attributes needed to map output are provided
       for each variable
    """
    mopdb_log = logging.getLogger('mopdb_log')
    line_cols = ['name','cmor_var','units','dimensions','_frequency',
        '_realm','cell_methods','cmor_table','vtype','size',
        'nsteps','fobj.fpattern','long_name','standard_name']
    vobj_list = []
    files = FPattern.list_files(indir, match)
    mopdb_log.debug(f"Files after sorting: {files}")
    patterns = []
    if alias == '':
        alias = 'mopdb'
    fname = f"varlist_{alias}.csv"
    fcsv = open(fname, 'w')
    fwriter = csv.writer(fcsv, delimiter=';')
    fwriter.writerow(["name", "cmor_var", "units", "dimensions",
        "frequency", "realm", "cell_methods", "cmor_table", "vtype",
        "size", "nsteps", "fpattern", "long_name", "standard_name"])
    for fpath in files:
        # get filename pattern until date match
        mopdb_log.debug(f"Filename: {fpath.name}")
        fpattern = fpath.name.split(match)[0]
        if fpattern in patterns:
            continue
        patterns.append(fpattern)
        fobj = FPattern(fpattern, Path(indir))
        #pattern_list = list_files(indir, f"{fpattern}*")
        nfiles = len(fobj.files) 
        mopdb_log.debug(f"File pattern, number of files: {fpattern}, {nfiles}")
        #fwriter.writerow([f"#{fpattern}"])
        # get attributes for the file variables
        ds = xr.open_dataset(str(fobj.files[0]), decode_times=False)
        coords = [c for c in ds.coords] + ['latitude_longitude']
        #pass next file in case of 1 timestep per file and no frq in name
        fnext = str(fobj.files[1])
        if fobj.frequency == 'NAfrq' or fobj.realm == 'atmos':
            frq_dict = get_file_frq(ds, fnext)
            # if only one frequency detected empty dict
            if len(frq_dict) == 1:
                fobj.frequency = frq_dict.popitem()[1]
            else:
                fobj.multiple_frq = True
        mopdb_log.debug(f"Multiple frq: {fobj.multiple_frq}")
        if fobj.realm == "NArealm":
            fobj.realm = get_realm(version, ds)
        for vname in ds.variables:
            vobj = Variable(vname, fobj) 
            if vname not in coords and all(x not in vname for x in ['_bnds','_bounds']):
                v = ds[vname]
                mopdb_log.debug(f"Variable: {vobj.name}")
                # get size in bytes of grid for 1 timestep and number of timesteps
                vobj.size = v[0].nbytes
                vobj.nsteps = nfiles * v.shape[0]
                # assign time axis frequency if more than one is available
                if fobj.multiple_frq:
                    if 'time' in v.dims[0]:
                        vobj._frequency = frq_dict[v.dims[0]]
                    else:
                        mopdb_log.info(f"Could not detect frequency for variable: {v}")
                attrs = v.attrs
                vobj.cell_methods, frqmod = get_cell_methods(attrs, v.dims)
                vobj.frequency = vobj.frequency + frqmod
                mopdb_log.debug(f"Frequency var: {vobj.frequency}")
                # try to retrieve cmip name
                vobj = get_cmorname(conn, vobj, version)
                vobj.units = attrs.get('units', "")
                vobj.long_name = attrs.get('long_name', "")
                vobj.standard_name = attrs.get('standard_name', "")
                vobj.dimensions = " ".join(v.dims)
                vobj.vtype = v.dtype
                line = [attrgetter(k)(vobj) for k in line_cols]
                fwriter.writerow(line)
                vobj_list.append(vobj)
        mopdb_log.info(f"Variable list for {fpattern} successfully written")
    fcsv.close()
    return  fname, vobj_list


def read_map_app4(fname):
    """Reads APP4 style mapping """
    mopdb_log = logging.getLogger('mopdb_log')
    # old order
    #cmor_var,definable,input_vars,calculation,units,axes_mod,positive,ACCESS_ver[CM2/ESM/both],realm,notes
    var_list = []
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            # if row commented skip
            if row[0][0] == "#":
                continue
            else:
                version = row[7].replace('ESM', 'ESM1.5')
                newrow = [row[0], row[2], row[3], row[4], '', '',
                          row[8], '', row[6], version, row[9], 'app4']
                # if version both append two rows one for ESM1.5 one for CM2
                if version == 'both':
                    newrow[9] = 'CM2'
                    var_list.append(newrow)
                    newrow[9] = 'ESM1.5'
                var_list.append(newrow)
    return var_list


def read_map(fname, alias):
    """Reads complete mapping csv file and extract info necessary to create new records
       for the mapping table in access.db
    Fields from file:
    cmor_var, input_vars, calculation, units, dimensions, frequency,
    realm, cell_methods, positive, cmor_table, version, vtype, size, nsteps,
    filename, long_name, standard_name
    Fields in table:
    cmor_var, input_vars, calculation, units, dimensions, frequency,
    realm, cell_methods, positive, model, notes, origin 
    NB model and version are often the same but version should eventually be defined in a CV
    """
    mopdb_log = logging.getLogger('mopdb_log')
    var_list = []
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            # if row commented skip
            if row[0][0] == "#":
                continue
            else:
                mopdb_log.debug(f"In read_map: {row[0]}")
                mopdb_log.debug(f"In read_map row length: {len(row)}")
                if row[16] != '':
                    notes = row[16]
                else:
                    notes = row[15]
                if alias == '':
                    alias = fname.replace(".csv","")
                var_list.append(row[:11] + [notes, alias])
    return var_list


def match_stdname(conn, row, stdn):
    """Returns an updated stdn list if finds one or more variables
    in cmorvar table that match the standard name passed as input.
    It also return a False/True found_match boolean.
    """
    mopdb_log = logging.getLogger('mopdb_log')
    found_match = False
    sql = f"""SELECT name FROM cmorvar where 
        standard_name='{row['standard_name']}'"""
    results = query(conn, sql, first=False)
    matches = [x[0] for x in results]
    if len(matches) > 0:
        stdn = add_var(stdn, row, tuple([matches]+['']*7), stdnm=True)
        found_match = True

    return stdn, found_match


def match_var(row, version, mode, conn, records):
    """Returns match for variable if found after looping
       variables already mapped in database
    Parameters

    """
    mopdb_log = logging.getLogger('mopdb_log')
    found_match = False
    # build sql query based on mode
    sql_base = f"""SELECT cmor_var,input_vars,calculation,frequency,
        realm,model,cmor_table,positive,units FROM mapping where 
        input_vars='{row['name']}'"""
    sql_frq = f" and frequency='{row['frequency']}'"
    sql_ver = f" and model='{version}'"
    if mode == 'full':
        sql = sql_base + sql_frq + sql_ver
    elif mode == 'no_frq':
        sql = sql_base + sql_ver
    elif mode == 'no_ver':
        sql = sql_base + sql_frq
    # execute query and process results
    result = query(conn, sql, first=False)
    mopdb_log.debug(f"match_var: {result}, sql: {sql[110:]}") 
    if result is not None and result != []:
        for x in result:
            mopdb_log.debug(f"match: {x}")
            records = add_var(records, row, x)
        found_match = True

    return records, found_match


def parse_vars(conn, rows, version):
    """Returns records of variables to include in template mapping file,
    a list of all stash variables + frequency available in model output
    and a list of variables already defined in db
 
    Parameters
    ----------
    conn : connection object
    rows : list(dict)
         list of variables to match
    version : str
        model version to use to match variables

    Returns
    -------
    stash_vars : list
        varname-frequency for each listed variable, varname is from model output
    """
    mopdb_log = logging.getLogger('mopdb_log')
    full = []
    no_ver = []
    no_frq = []
    stdn = []
    no_match = []
    stash_vars = []

    # looping through variables from file and attempt matches to db 
    for row in rows:
        if row['name'][0] == "#" or row['name'] == 'name':
            continue
        else:
            full, found = match_var(row, version, 'full', conn, full)
        # if no match, ignore model version first and then frequency 
        mopdb_log.debug(f"found perfect match: {found}")
        if not found:
            no_ver, found = match_var(row, version, 'no_ver', conn, no_ver)
            mopdb_log.debug(f"found no ver match: {found}")
        if not found:
            no_frq, found = match_var(row, version, 'no_frq', conn, no_frq)
            mopdb_log.debug(f"found no frq match: {found}")
        # make a last attempt to match using standard_name
        if not found:
            if row['standard_name'] != '':
                stdn, found = match_stdname(conn, row, stdn)
            mopdb_log.debug(f"found stdnm match: {found}")
        if not found:
            no_match = add_var(no_match, row, tuple([row['name']]+['']*8)) 
        stash_vars.append(f"{row['name']}-{row['frequency']}")

    return full, no_ver, no_frq, stdn, no_match, stash_vars 


def add_var(vlist, row, match, stdnm=False):
    """Add information from match to variable list and re-order
    fields so they correspond to final mapping output.

    Parameters
    match : tuple
        match values (cmor_var,input_vars,calculation,frequency,
        realm,model(version),cmor_table,positive,units)
    """
    mopdb_log = logging.getLogger('mopdb_log')
    # assign cmor_var from match and swap place with input_vars
    mopdb_log.debug(f"Assign cmor_var: {match}")
    mopdb_log.debug(f"initial row: {row}")
    var = row.copy() 
    var['cmor_var'] = match[0]
    var['input_vars'] = match[1]
    orig_name = var.pop('name')
    # assign realm from match
    var['realm'] = match[4] 
    # with stdn assign cmorvar and table if only 1 match returned
    # otherwise assign table from match
    if stdnm: 
        var['input_vars'] = orig_name
        if len(var['cmor_var']) == 1:
            cmor_var, table = var['cmor_var'][0].split("-")
            var['cmor_var'] = cmor_var
            var['cmor_table'] = table 
    else:
        var['cmor_table'] = match[6] 
    # add calculation, positive and version 
    var['calculation'] = match[2]
    var['positive'] = match[7]
    var['version'] = match[5] 
    # maybe we should override units here rather than in check_realm_units
    # if units missing get them from match
    if var['units'] is None or var['units'] == '':
        var['units'] = match[8]
    vlist.append(var)
    return vlist


def remove_duplicate(vlist, extra=[], strict=True):
    """Returns list without duplicate variable definitions.

    Define unique definition for variable as tuple (cmor_var, input_vars,
    calculation, frequency, realm) in strict mode and (cmor_var, input_vars,
    calculation) only if strict is False
    If extra is defined if a variable exists in this additional set
    it is a duplicate
    """
    mopdb_log = logging.getLogger('mopdb_log')
    mopdb_log.debug(f'in duplicate, vlist {vlist}')
    vid_list = []
    keys = ['cmor_var', 'input_vars', 'calculation']
    if strict is True:
        keys += ['frequency', 'realm']
    if extra:
        vid_list = [tuple(x[k] for k in keys) for x in extra] 
    mopdb_log.debug(f"vid_list: {vid_list}")
    final = []
    for v in vlist:
        vid = tuple(v[k] for k in keys)
        mopdb_log.debug(f"var and vid: {v['cmor_var']}, {vid}")
        if vid not in vid_list:
            final.append(v)
        vid_list.append(vid)
    return final


def potential_vars(conn, rows, stash_vars, version):
    """Returns list of variables that can be potentially derived from
    model output.

    Loop across all model variables to match
    Select any mapping that contains the variable and if there's a calculation
    NB rows modified by add_row when assigning cmorname and positive values

    Parameters
    ----------
    conn : connection object
    rows : list(dict)
         list of variables to match
    stash_vars : list
        varname-frequency for each listed variable, varname is from model output
    version : str
        model version to use to match variables

    Returns
    -------
    """
    mopdb_log = logging.getLogger('mopdb_log')
    pot_full = [] 
    pot_part = []
    pot_varnames = set()
    for row in rows:
        sql = f"""SELECT cmor_var,input_vars,calculation,frequency,
            realm,model,cmor_table,positive,units FROM mapping 
            WHERE input_vars like '%{row['name']}%'"""
        results = query(conn, sql, first=False)
        mopdb_log.debug(f"In potential: var {row['name']}, db results {results}")
        for r in results:
            allinput = r[1].split(" ")
            mopdb_log.debug(f"{len(allinput)> 1}")
            mopdb_log.debug(all(f"{x}-{row['frequency']}" in stash_vars for x in allinput))
            if len(allinput) > 1 and all(f"{x}-{row['frequency']}" in stash_vars for x in allinput):
                # if both version and frequency of applied mapping match
                # consider this a full matching potential var 
                if r[5] == version and r[3] == row['frequency']:
                   pot_full = add_var(pot_full, row, r)
                else:
                    pot_part = add_var(pot_part, row, r)
                pot_varnames.add(r[0])
    return pot_full, pot_part, pot_varnames


def write_map_template(conn, full, no_ver, no_frq, stdn,
                       no_match, pot_full, pot_part, alias):
    """Write mapping csv file template based on list of variables to define 

    Input varlist file order:
    name, cmor_var, units, dimensions, frequency, realm, cell_methods,
    cmor_table, vtype, size, nsteps, filename, long_name, standard_name
    Mapping db order:
    cmor_var, input_vars, calculation, units, dimensions, frequency, realm,
    cell_methods, positive, cmor_table, model, notes, origin 
        for pot vars + vtype, size, nsteps, filename
    Final template order:
    cmor_var, input_vars, calculation, units, dimensions, frequency, realm,
    cell_methods, positive, cmor_table, version, vtype, size, nsteps, filename,
    long_name, standard_name
    """ 

    mopdb_log = logging.getLogger('mopdb_log')
    keys = ['cmor_var', 'input_vars', 'calculation', 'units',
            'dimensions', 'frequency', 'realm', 'cell_methods',
            'positive', 'cmor_table', 'version', 'vtype', 'size',
            'nsteps', 'filename', 'long_name', 'standard_name'] 

    with open(f"map_{alias}.csv", 'w') as fcsv:
        fwriter = csv.DictWriter(fcsv, keys, delimiter=';')
        write_vars(full, fwriter, keys, conn=conn)
        div = ("# Derived variables with matching version and " +
            "frequency: Use with caution!")
        write_vars(pot_full, fwriter, div, conn=conn)
            #pot=True, conn=conn, sortby=0)
        div = ("# Variables definitions coming from different " +
            "version")
        write_vars(no_ver, fwriter, div, conn=conn)
        div = ("# Variables with different frequency: Use with"
            + " caution!")
        write_vars(no_ver, fwriter, div, conn=conn)
        div = ("# Variables matched using standard_name: Use " +
            "with caution!")
        write_vars(stdn, fwriter, div, sortby='input_vars')
        div = "# Derived variables: Use with caution!"
        write_vars(pot_part, fwriter, div, conn=conn)
            #pot=True, conn=conn, sortby=0)
        div = "# Variables without mapping"
        write_vars(no_match, fwriter, div)
        mopdb_log.debug("Finished writing variables to mapping template")
        fcsv.close()

        return


def write_vars(vlist, fwriter, div, conn=None, sortby='cmor_var'):
    """
    """

    mopdb_log = logging.getLogger('mopdb_log')
    if len(vlist) > 0:
        if type(div) is str:
            divrow = {x:'' for x in vlist[0].keys()}
            divrow['cmor_var'] = div
        elif type(div) is list:
            divrow = {x:x for x in div}
        fwriter.writerow(divrow)
        for var in sorted(vlist, key=itemgetter(sortby)):
            if conn:
                var = check_realm_units(conn, var)
            fwriter.writerow(var)
    return


def check_realm_units(conn, var):
    """Checks that realm and units are consistent with values in 
    cmor table.
    """

    mopdb_log = logging.getLogger('mopdb_log')
    vname = f"{var['cmor_var']}-{var['cmor_table']}"
    if var['cmor_table'] is None or var['cmor_table'] == "":
        mopdb_log.warning(f"Variable: {vname} has no associated cmor_table")
    else:
    # retrieve modeling_realm, units from db cmor table
        sql = f"""SELECT modeling_realm, units FROM cmorvar
            WHERE name='{vname}' """ 
        result = query(conn, sql)
        mopdb_log.debug(f"In check_realm_units: {vname}, {result}")
        if result is not None:
            dbrealm = result[0] 
            dbunits = result[1] 
            # dbrealm could have two realms
            if var['realm'] not in [dbrealm] + dbrealm.split():
                mopdb_log.info(f"Changing {vname} realm from {var['realm']} to {dbrealm}")
                var['realm'] = dbrealm
            if var['units'] != dbunits :
                mopdb_log.info(f"Changing {vname} units from {var['units']} to {dbunits}")
                var['units'] = dbunits
        else:
            mopdb_log.warning(f"Variable {vname} not found in cmor table")
    return var 
       

def get_realm(version, ds):
    '''Try to retrieve realm if using path failed'''

    mopdb_log = logging.getLogger('mopdb_log')
    if version == 'AUS2200':
        realm = 'atmos'
    elif 'um_version' in ds.attrs.keys():
        realm = 'atmos'
    mopdb_log.debug(f"Realm is {realm}")
    return realm


def check_varlist(rows, fname):
    """Checks that varlist written to file has sensible information for frequency and realm
    to avoid incorrect mapping to be produced.

    At the moment we're checking only frequency and realm as they can be missed or wrong
    depending on the file structure.

    Parameters
    ----------
    rows : list(dict)
         list of variables to match
    """

    mopdb_log = logging.getLogger('mopdb_log')
    frq_list = ['min', 'hr', 'day', 'mon', 'yr'] 
    realm_list = ['ice', 'ocean', 'atmos', 'land']
    for row in rows:
        if row['name'][0] == "#" or row['name'] == 'name':
            continue
        elif (not any( x in row['frequency'] for x in frq_list) 
            or row['realm'] not in realm_list):
                mopdb_log.error(f"""  Check frequency and realm in {fname}.
  Some values might be invalid and need fixing""")
                sys.exit()
    return
