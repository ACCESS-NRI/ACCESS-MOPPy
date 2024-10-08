#!/usr/bin/env python
# Copyright 2024 ARC Centre of Excellence for Climate Extremes (CLEX)
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
# last updated 08/10/2024
#

import sqlite3
import logging
import os
import stat
import yaml

from datetime import date


class MopException(Exception):
    pass

def config_log(debug, logname):
    """Configures log file"""
    # start a logger
    logger = logging.getLogger(logname)
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
    logname = f"{logname}_{day}.txt"
    flog = logging.FileHandler(logname)
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    except OSError:
        pass
    flog.setLevel(flevel)
    flog.setFormatter(formatter)
    logger.addHandler(flog)
    # return the logger object
    return logger

def db_connect(db, logname='__name__'):
    """Connects to ACCESS mapping sqlite database"""
    log = logging.getLogger(logname)
    conn = sqlite3.connect(db, timeout=10, isolation_level=None)
    if conn.total_changes == 0:
        log.info(f"Opened database {db} successfully")
    return conn 

def create_table(conn, sql, logname='__name__'):
    """Creates table if database is empty

    Parameters
    ----------
    conn : connection object
    sql : str
        SQL style string defining table to create
    """
    log = logging.getLogger(logname)
    try:
        c = conn.cursor()
        c.execute(sql)
    except Exception as e:
        log.error(e)
        raise MopException(e)
    return

def query(conn, sql, tup=(), first=True, logname='__name__'):
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
    #log = logging.getLogger(logname)
    with conn:
        c = conn.cursor()
        c.execute(sql, tup)
        if first:
            result = c.fetchone()
        else:
            result = [ x for x in c.fetchall() ]
        #columns = [description[0] for description in c.description]
        return result


def get_columns(conn, table, logname='__name__'):
    """Gets list of columns from db table
    """
    #log = logging.getLogger(logname)
    sql = f'PRAGMA table_info({table});'
    table_data = query(conn, sql, first=False, logname=logname)
    columns = [x[1] for x in table_data]
    return columns


def delete_record(conn, table, col, pairs, logname='__name__'):
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
    log = logging.getLogger(logname)
    # Set up query
    sqlwhere = f"FROM {table} WHERE "
    for c,v in pairs:
        sqlwhere += f"{c}='{v}' AND "
    sql = f"SELECT {col} " + sqlwhere[:-4]
    log.debug(f"Delete query: {sql}")
    xl = query(conn, sql, first=False, logname=logname)
    # Delete from db
    if xl is not None:
        log.info(f"Found {len(xl)} records")
        for x in xl:
            log.info(f"{x}")
        confirm = input('Confirm deletion from database: Y/N   ')
        if confirm == 'Y':
            log.info('Updating db ...')
            with conn:
                c = conn.cursor()
                sql = "DELETE " + sqlwhere[:-4]
                log.debug(f"Delete sql: {sql}")
                c.execute(sql)
                c.execute('select total_changes()')
                log.info(f"Rows modified: {c.fetchall()[0][0]}")
    else:
        log.info("The query did not return any records")
    return

def read_yaml(fname, logname='__name__'):
    """Read yaml file
    """
    log = logging.getLogger(logname)
    try:
        with fname.open(mode='r') as yfile:
            data = yaml.safe_load(yfile)
    except Exception as e:
        log.error(f"Check that {fname} exists and it is a valid yaml file")
        raise MopException(e)
    return data

def write_yaml(data, fname, logname='__name__'):
    """Write data to a yaml file

    Parameters
    ----------
    data : dict
        The file content as a dictionary
    fname : str
        Yaml filename

    Returns
    -------
    """
    log = logging.getLogger(logname)
    try:
        with open(fname, 'w') as f:
            yaml.dump(data, f)
    except Exception as e:
        log.error(f"Check {data} exists and is yaml object")
        raise MopException(e)
    return
