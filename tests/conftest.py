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

import pytest
import os
import sqlite3
import click
import logging
import csv
import pyfakefs
from pathlib import Path

from mopdb.mopdb_utils import mapping_sql, cmorvar_sql
from mopdb.mopdb_class import MapVariable, Variable, FPattern
from mopper.setup_utils import filelist_sql


TESTS_HOME = os.path.abspath(os.path.dirname(__file__))
TESTS_DATA = os.path.join(TESTS_HOME, "testdata")
# consecutive files with multiple time axes
dsmulti = os.path.join(TESTS_DATA, "multitime.nc")
dsmulti2 = os.path.join(TESTS_DATA, "multitime_next.nc")
# consecutive files with a 1-time step time axis
dsonestep = os.path.join(TESTS_DATA, "onetstep.nc")
dsonestep2 = os.path.join(TESTS_DATA, "onetstep_next.nc")
# varlist, map file examples

@pytest.fixture
def fake_fs(fs):  # pylint:disable=invalid-name
    """Variable name 'fs' causes a pylint warning. Provide a longer name
    acceptable to pylint for use in tests.
    """
    yield fs

@pytest.fixture
def ctx():
    ctx = click.Context(click.Command('cmd'),
        obj={'sel_start': '198302170600', 'sel_end': '198302181300',
        'realm': 'atmos', 'frequency': '1hr', 'var_log': 'varlog_1'})
    return ctx

@pytest.fixture
def vlistcsv():
    vlistcsv = os.path.join(TESTS_DATA, "varlist.csv")
    return vlistcsv

# setting up fixtures for databases:a ccess.db and mopper.db
@pytest.fixture
def session(): 
    connection = sqlite3.connect(':memory:')
    db_session = connection.cursor()
    yield db_session
    connection.close()

@pytest.fixture
def input_dir(fake_fs):
    dfrq = {'d': 'dai', '8': '3h', '7': '6h', 'm': 'mon'}  
    for date in ['201312', '201401', '201402']:
        for k,v in dfrq.items():
            filebase = f"cm000a.p{k}{date}_{v}.nc"
            fake_fs.create_file("/raw/atmos/"+ filebase)
    assert os.path.exists("/raw/atmos/cm000a.p8201402_3h.nc")
       

@pytest.fixture
def setup_access_db(session):
    map_sql = mapping_sql()
    cmor_sql = cmorvar_sql()
    session.execute(cmor_sql)
    session.execute(map_sql)
    session.execute('''INSERT INTO cmorvar VALUES ("tas-CMIP6_Amon",
        "mon", "atmos", "air_temperature", "K", "area: time: mean", 
        "area: areacella", "Near-Surface Air Temperature",
        "near-surface (usually, 2 meter) air temperature",
        "longitude latitude time height2m", "tas", "real", "", "", "",
        "", "", "", "")''')
    session.execute('''INSERT INTO mapping VALUES ("tas",
        "fld_s03i236", "", "K", "time lat lon", 
        "longitude latitude time", "mon", "atmos", "area: time: mean",
        "", "CMIP6_Amon", "CM2", "air_temperature", "cm000")''')
    session.connection.commit()


@pytest.fixture
def setup_mopper_db(session):
    flist_sql = filelist_sql()
    session.execute(flist_sql)
    session.execute('''INSERT INTO filelist VALUES (
        "/testdata/atmos/umnsa_spec_*.nc", "/testdata/mjo-elnino/v1-0/A10min/",
        "tas_AUS2200_mjo-elnino_subhrPt_20160101001000-20160102000000.nc",
        "fld_s03i236", "tas", "AUS2200_A10min", "subhrPt", "atmos", "point",
        "longitude latitude time", "20160101T0005", "20160102T0000", 
        "201601010000", "201601012355", "unprocessed", "3027.83203125",
        "mjo-elnino", "K", "AUS2200", "AUS2200",
        "/testdata/mjo-elnino/mjo-elnino.json",	"1970-01-01", "v1-0")''')
    session.connection.commit()


def test_check_timestamp(caplog):
    global ctx, logger
    caplog.set_level(logging.DEBUG, logger='mop_log')

@pytest.fixture
def varlist_rows():
    # read list of vars from example file
    with open('testdata/varlist_ex.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        rows = list(reader)
    return rows

@pytest.fixture
def matches():
    matches = [("tas", "fld_s03i236", "", "1hr", "atmos", "AUS2200", "AUS2200_A1hr", "", "K", "lo la t"),
        ("siconca", "fld_s00i031", "", "mon", "ocean", "CM2", "CMIP6_OImon", "", "1", "lo la t"), 
        ("hfls", "fld_s03i234", "", "mon", "atmos", "CM2", "CMIP6_Amon", "up", "W/m2", "lo la t")]
    return matches

@pytest.fixture
def add_var_out():
    vlist = [{'cmor_var': '', 'input_vars': '', 'calculation': '', 'units': ''
              ,'realm': '', 'positive': '', 'version': '', 'cmor_table': ''}
            ]
    return vlist

@pytest.fixture
def map_rows():
    maps = [["fld_s03i236","tas","K","time_0 lat lon","1hr","atmos",
        "area: time: mean","","AUS2200_A1hr","float32","22048000","96",
        "umnsa_slv_","TEMPERATURE AT 1.5M","air_temperature"]]
    return maps

@pytest.fixture
def fobj(input_dir):
    fobj = FPattern("cm000a.", Path("/raw/atmos/"))
    return fobj

@pytest.fixture
def var_obj(fobj):
    vobj = Variable('tas', fobj)
    return vobj

@pytest.fixture
def mapvar_obj(var_obj):
    match = ('','','','','','','','','','')
    mvobj = MapVariable(match, var_obj)
    return mvobj

@pytest.fixture
def varobjs(mapvar_obj):
    mvobj = mapvar_obj
    vobjs = []
    vobjs.append(mvobj)
    mvobj.name = 'siconca' 
    vobjs.append(mvobj)
    mvobj.name = 'hfls' 
    vobjs.append(mvobj)
    return vobjs


@pytest.fixture
def output_file(tmp_path):
    # create your file manually here using the tmp_path fixture
    # or just import a static pre-built mock file
    # something like : 
    target_output = os.path.join(tmp_path,'mydoc.csv')
    with open(target_output, 'w+'):
        pass
        # write stuff here
    return target_output
