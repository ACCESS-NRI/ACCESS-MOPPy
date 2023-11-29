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
from mopdb.mopdb_utils import *

#from click.testing import CliRunner

@pytest.fixture
def db_log():
    return config_log(False)


@pytest.fixture
def db_log_debug():
    return config_log(True)


@pytest.mark.parametrize('idx', [0,1,2])
def test_add_var(varlist_rows, idx, db_log):
    vlist = []
    vlistout = [["fld_s03i236","tas","K","time_0 lat lon","1hr","atmos",
        "area: time: mean","","AUS2200_A1hr","float32","22048000","96",
        "umnsa_slv_","TEMPERATURE AT 1.5M","air_temperature"]]
    match = ("tas", "", "K")
    vlist = add_var(vlist, varlist_rows[idx], match, db_log)
    assert vlist == vlistout
