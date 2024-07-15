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
import csv
import json

from datetime import date
from collections import Counter

from mopdb.utils import *


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
    mopdb_log.info('--- Done ---')
    return

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
    fpattern, long_name, standard_name
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
        result = query(conn, sql, logname='mopdb_log')
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

def get_date_pattern(fname, fpattern):
    """Try to build a date range for each file pattern based
       on its filename
    """
    mopdb_log = logging.getLogger('mopdb_log')
    # assign False to any character which is not a digit
    date_pattern = [True if c.isdigit() else False for c in fname]
    # assign False to fpattern
    n = len(fpattern)
    date_pattern[:n] = [False] * n
    return date_pattern
