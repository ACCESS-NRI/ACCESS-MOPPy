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
import xarray as xr
import numpy as np
import pandas as pd
import datetime
from mopdb.mopdb_utils import mapping_sql, cmorvar_sql
from mopper.setup_utils import filelist_sql


TESTS_HOME = os.path.abspath(os.path.dirname(__file__))
TESTS_DATA = os.path.join(TESTS_HOME, "testdata")


@pytest.fixture
def session(): 
    connection = sqlite3.connect(':memory:')
    db_session = connection.cursor()
    yield db_session
    connection.close()


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
    session.execute('''INSERT INTO mapping VALUES ("tas", "fld_s03i236",
        "", "K", "time lat lon", "mon", "atmos", "area: time: mean",
        "", "CMIP6_Amon", "CM2", "air_temperature", "cm000")''')
    session.connection.commit()


@pytest.fixture
def setup_mopper_db(session):
    filelist_sql = mapping_sql()
    session.execute(filelist_sql)
    session.execute('''INSERT INTO filelist VALUES ("/testdata/atmos/umnsa_spec_*.nc", 	"/testdata/mjo-elnino/v1-0/A10min/", "tas_AUS2200_mjo-elnino_subhrPt_20160101001000-20160102000000.nc", "fld_s03i236", "tas", "AUS2200_A10min", "subhrPt", "atmos", "point", "20160101T0005", "20160102T0000", "201601010000", "201601012355", "unprocessed", "3027.83203125", "mjo-elnino", "K", "AUS2200", "AUS2200", "/testdata/mjo-elnino/mjo-elnino.json",	"1970-01-01", "v1-0")''')
    session.connection.commit()


@pytest.fixture
def varlist_rows():
    lines = ["fld_s03i236;tas;K;time_0 lat lon;1hr;atmos;area: time: mean;AUS2200_A1hr;float32;22048000;96;umnsa_slv_;TEMPERATURE AT 1.5M;air_temperature",
    "fld_s03i236;;K;time_0 lat lon;1hr;atmos;area: time: mean;AUS2200_A1hr;float32;22048000;96;umnsa_slv_;TEMPERATURE AT 1.5M;air_temperature",
    "fld_s03i236;tas;;time_0 lat lon;1hr;atmos;area: time: mean;AUS2200_A1hr;float32;22048000;96;umnsa_slv_;TEMPERATURE AT 1.5M;air_temperature"]
    rows = [l.split(";") for l in lines]
    return rows
