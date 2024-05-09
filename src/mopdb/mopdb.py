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
# last updated 08/04/2024

import click
import sqlite3
import logging
import sys
import csv
import json

from importlib.resources import files as import_files

from mopdb.mopdb_utils import *


def mopdb_catch():
    """
    """
    debug_logger = logging.getLogger('app_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        mopdb()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


def db_args(f):
    """Define database click arguments
    """
    constraints = [
        click.option('--fname', '-f', type=str, required=True,
            help='Input file: used to update db table (mapping/cmor),' +
                 'or to pass output model variables (list)'),
        click.option('--dbname', type=str, required=False, default='default',
            help='Database relative path by default is package access.db'),
        click.option('--alias', '-a', type=str, required=False, default=None,
            help='Table alias to use when updating cmor var table or creating map template with list' +
                 ' to keep track of variable definition origin. If none passed uses input filename')]
    for c in reversed(constraints):
        f = c(f)
    return f


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def mopdb(ctx, debug):
    """Main group command, initialises log and context object
    """
    ctx.obj={}
    # set up a default value for flow if none selected for logging
    ctx.obj['debug'] = debug
    ctx.obj['log'] = config_log(debug)


@mopdb.command(name='check')
@click.option('--dbname', type=str, required=False, default='default',
            help='Database relative path by default is package access.db')
@click.pass_context
def check_cmor(ctx, dbname):
    """Prints list of cmor_var defined in mapping table but not in
    cmorvar table.


    Parameters
    ----------
    ctx : obj
        Click context object
    dbname : str
        Database relative path (default is data/access.db)
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    if dbname == 'default':
        dbname = import_files('data').joinpath('access.db')
    conn = db_connect(dbname, db_log)
    # get list of variables already in db
    sql = 'SELECT name, out_name FROM cmorvar'
    results = query(conn, sql, first=False)
    # first set is the actual cmip variable name 
    # second set is the name used in tables to distinguish different dims/freq
    # original maps files use the second style
    cmor_vars = set(x[1] for x in results)
    cmor_vars2 = set(x[0].split('-')[0] for x in results)
    cmor_vars.update(cmor_vars2)

    sql = 'SELECT cmor_var FROM mapping'
    results = query(conn, sql, first=False)
    map_vars = [x[0] for x in results]
    missing = set(map_vars) - set(cmor_vars)
    db_log.info("Variables not yet defined in cmorvar table:")
    for v in missing:
        db_log.info(f"{v}")
    conn.close()
    return


@mopdb.command(name='table')
@db_args
@click.option('--label', '-l', required=False, default='CMIP6',
    type=click.Choice(['CMIP6', 'AUS2200', 'CM2']), show_default=True,
    help='Label indicating origin of CMOR variable definitions. Currently only CMIP6, AUS2200 and CM2')
@click.pass_context
def cmor_table(ctx, dbname, fname, alias, label):
    """Create CMIP style table containing new variable definitions
    fname  from output to extract cmor_var, frequency, realm 
    If these var/freq/realm/dims combs don't exist in cmorvar add var to table.
    `alias` here act as the new table name.

    Parameters
    ----------
    ctx : obj
        Click context object
    dbname : str
        Database relative path (default is data/access.db)
    fname : str
        Mapping file??? 
    alias : str
           not used here
    label : str
        Label indicating preferred cmor variable definitions 
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    if dbname == 'default':
        dbname = import_files('data').joinpath('access.db')
    conn = db_connect(dbname, db_log)
    # get list of variables already in db
    sql = "SELECT out_name, frequency, modeling_realm FROM cmorvar"
    results = query(conn, sql, first=False)
    # cmor_vars is the actual cmip variable name 
    # this sometime differs from name used in tables tohat can distinguish different dims/freq
    cmor_vars = set(x[0] for x in results)
    # read variable list from map_ file
    vlist = read_map(fname, alias, db_log)
    # extract cmor_var,units,dimensions,frequency,realm,cell_methods
    var_list = []
    for v in vlist[1:]:
        vid = (v[0], v[5], v[6])
        # This was adding variables to the table just if they didn't exists in other tables
        if v[0][:4] != 'fld_':
            if v[0] not in cmor_vars:
                db_log.warning(f"Variable {v[0]} not defined in cmorvar table")
            else:
                
                sql = f"SELECT * FROM cmorvar WHERE out_name='{v[0]}'"
                records = query(conn, sql, first=False)
                record = records[0]
                if len(records) > 1:
                    for r in records:
                        if f"-{label}_" in r[0]:
                            record = r
                            break
                definition = list(record)
                #definition[0] = f"{v[0]}-{alias}"
                definition[0] = v[0].replace('_', '-')
                definition[1] = v[5]
                definition[2] = v[6]
                # if units are different print warning!
                if v[3] != record[4]:
                    db_log.warning(f"Variable {v[0]} units orig/table are different: {v[3]}/{record[4]}")
                if v[7] != '' and v[7] != record[5]:
                    db_log.warning(f"Variable {v[0]} cell_methods orig/table are different: {v[7]}/{record[5]}")
                if len(v[4].split()) != len(record[9].split()):
                    db_log.warning(f"Variable {v[0]} number of dims orig/table are different: {v[4]}/{record[9]}")
                var_list.append(definition)
    write_cmor_table(var_list, alias, db_log)
    conn.close()
    return


@mopdb.command(name='cmor')
@db_args
@click.pass_context
def update_cmor(ctx, dbname, fname, alias):
    """Open/create database and create/update cmorvar table. Table is 
    populated with data passed via input json file.
    Input json file is written in same style as CMOR CMIP6 tables.

    Parameters
    ----------
    ctx : obj
        Click context object
    dbname : str
        Database relative path (default is data/access.db)
    fname : str
        Name of json input file with records to add
    alias : str
        Indicates origin of records to add, if None json filename
        base is used instead

    Returns
    -------
    """

    db_log = ctx.obj['log']
    if alias is None:
        alias = fname.split("/")[-1]
        alias = alias.replace('.json', '')
    db_log.info(f"Adding {alias} to variable name to track origin")
    # connect to db, this will create one if not existing
    dbcentral = import_files('data').joinpath('access.db')
    if dbname in [dbcentral, 'default']:
        db_log.error("The package database cannot be updated")
        sys.exit()
    conn = db_connect(dbname, db_log)
    # create table if not existing
    table_sql = cmorvar_sql()
    create_table(conn, table_sql, db_log)
    # get list of variables already in db in debug mode
    if ctx.obj['debug']:
        sql = 'SELECT name FROM cmorvar'
        results = query(conn, sql, first=False)
        existing_vars = [x[0] for x in results]
        db_log.debug(f"Variables already in db: {existing_vars}")

    # read list of vars from file
    with open(fname, 'r') as fj:
         vardict = json.load(fj)
    row_dict = vardict['variable_entry']
    vars_list = []
    for name,row in row_dict.items():
    # alter the name so it reflects also its origin
        name = f"{name}-{alias}" 
        values = [x for x in row.values()]
        # check if flag attrs present if not add them
        if 'flag_values' not in row.keys():
            values = values[:-2] + ['',''] + values[-2:]
        vars_list.append(tuple([name] + values))
    db_log.debug(f"Variables list: {vars_list}")
    # check that all tuples have len == 19
    for r in vars_list:
        if len(r) != 19:
            db_log.error(r)
            sys.exit()
    # insert new vars and update existing ones
    update_db(conn, 'cmorvar', vars_list, db_log)

    return


@mopdb.command(name='template')
@db_args
@click.option('--version', '-v', required=True,
    type=click.Choice(['ESM1.5', 'CM2', 'AUS2200', 'OM2']), show_default=True,
    help='ACCESS version currently only CM2, ESM1.5, AUS2200, OM2')
@click.pass_context
def map_template(ctx, dbname, fname, alias, version):
    """Writes a template of mapping file needed to run setup.
       First opens database and check if variables match any in
       mapping table. If not tries to partially match them.

    Parameters
    ----------
    ctx : obj
        Click context object
    dbname : str
        Database relative path (default is data/access.db)
    fname : str
        Name of csv input file with output variables to map
    alias : str
        Indicates origin of records to add, if None csv filename
        base is used instead
    version : str
        Version of ACCESS model used to generate variables

    Returns
    -------
    """
    db_log = ctx.obj['log']
    if alias is None:
        alias = fname.split(".")[0]
    # connect to db, check first if db exists or exit 
    if dbname == 'default':
        dbname = import_files('data').joinpath('access.db')
    conn = db_connect(dbname, db_log)
    # read list of vars from file
    with open(fname, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        rows = list(reader)
    # return lists of fully/partially matching variables and stash_vars 
    # these are input_vars for calculation defined in already in mapping db
    full, no_ver, no_frq, stdn, no_match, stash_vars = parse_vars(conn, 
        rows, version, db_log)

    # remove duplicates from partially matched variables 
    no_ver = remove_duplicate(no_ver, db_log)
    no_frq = remove_duplicate(no_frq, db_log, strict=False)
    no_match = remove_duplicate(no_match, db_log, strict=False)

    # check if more derived variables can be added based on all
    # input_vars being available
    pot_full, pot_part, pot_varnames = potential_vars(conn, rows,
        stash_vars, version, db_log)
    # potential vars have always duplicates: 1 for each input_var
    pot_full = remove_duplicate(pot_full, db_log, strict=False)
    pot_part = remove_duplicate(pot_part, db_log, extra=pot_full,
        strict=False)
    db_log.info(f"Definable cmip var: {pot_varnames}")
    write_map_template(conn, full, no_ver, no_frq, stdn, 
        no_match, pot_full, pot_part, alias, db_log)
    conn.close()

    return


@mopdb.command(name='map')
@db_args
@click.pass_context
def update_map(ctx, dbname, fname, alias):
    """Open database and create/update populating with rows
       mapping file passed as input
       alias indicates origin: if old style use 'app4'

    Parameters
    ----------
    ctx : obj
        Click context object
    dbname : str
        Database relative path (default is data/access.db)
    fname : str
        Name of csv input file with mapping records
    alias : str
        Indicates origin of records to add, if None csv filename
        base is used instead

    Returns
    -------
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    dbcentral = import_files('data').joinpath('access.db')
    if dbname in [dbcentral, 'default']:
        db_log.error("The package database cannot be updated")
        sys.exit()
    conn = db_connect(dbname, db_log)
    # create table if not existing
    table_sql = mapping_sql()
    create_table(conn, table_sql, db_log)
    # get list of variables already in db in debug mode
    if ctx.obj['debug']:
        sql = 'SELECT cmor_var FROM mapping'
        results = query(conn, sql, first=False)
        existing_vars = [x[0] for x in results]
        db_log.debug(f"Variables already in db: {existing_vars}")
    # read list of vars from file
    if alias == 'app4':
        var_list = read_map_app4(fname)
    else:
        var_list = read_map(fname, alias, db_log)
    # update mapping table
    update_db(conn, 'mapping', var_list, db_log)
    return


@mopdb.command(name='varlist')
@click.option('--indir', '-i', type=str, required=True,
    help='Converted model output directory')
@click.option('--startdate', '-d', type=str, required=True,
    help='Start date of model run as YYYYMMDD')
@click.option('--dbname', type=str, required=False, default='default',
    help='Database relative path by default is package access.db')
@click.option('--version', '-v', required=False, default='CM2',
    type=click.Choice(['ESM1.5', 'CM2', 'AUS2200', 'OM2']), show_default=True,
    help='ACCESS version currently only CM2, ESM1.5, AUS2200, OM2')
@click.pass_context
def model_vars(ctx, indir, startdate, dbname, version):
    """Read variables from model output
       opens one file for each kind, save variable list as csv file
       alias is not used so far

    Parameters
    ----------
    ctx : obj
        Click context object
    indir : str
        Path for model output files
    startdate : str
        Date or other string to match to individuate one file per type
    dbname : str
        Database relative path (default is data/access.db)
    version : str
        Version of ACCESS model to use as preferred mapping

    Returns
    -------
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    if dbname == 'default':
        dbname = import_files('data').joinpath('access.db')
    conn = db_connect(dbname, db_log)
    write_varlist(conn, indir, startdate, version, db_log)
    conn.close()
    return


@mopdb.command(name='del')
@click.option('--dbname', type=str, required=True,
    help='Database relative path')
@click.option('--table', '-t', type=str, required=True,
    help='DB table to remove records from')
@click.option('--pair', '-p', type=(str, str), required=True,
    multiple=True,
    help='''Pair of "column value" to select record/s to delete.
        At least one is required, can be passed multiple times''')
@click.pass_context
def remove_record(ctx, dbname, table, pair):
    """Selects and removes records based on constraints
    passed as input

    Parameters
    ----------
    ctx : obj
        Click context object
    dbname : str
        Database relative path
    pair : list[tuple(str, str)]
        list of all the column:value tuples to be used to select
        record/s to delete

    Returns
    -------
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    dbcentral = import_files('data').joinpath('access.db')
    if dbname == dbcentral:
        db_log.error("The package database cannot be updated")
        sys.exit()
    conn = db_connect(dbname, db_log)
    # set which columns to show based on table
    if table == 'cmorvar':
        col = "name"
    elif table == 'mapping':
        col = "cmor_var,frequency,realm,cmor_table" 
    # select, confirm, delete record/s 
    delete_record(conn, table, col, pair, db_log)
    return
    
