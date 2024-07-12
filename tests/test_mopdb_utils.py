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
import itertools
from mopdb.mopdb_utils import *
from conftest import um_multi_time

#from click.testing import CliRunner


@pytest.mark.parametrize('idx', [0,1,2])
def test_add_var(varlist_rows, matches, idx, caplog):
    caplog.set_level(logging.DEBUG, logger='mopdb_log')
    vlist = []
    vlist = add_var(vlist, varlist_rows[idx], matches[idx])
    assert vlist[0]['cmor_var'] == matches[idx][0] 


def test_build_umfrq(um_multi_time, caplog):
    caplog.set_level(logging.DEBUG, logger='mopdb_log')
    time_axs = [d for d in um_multi_time.dims if 'time' in d]
    umfrq = {'time': 'day', 'time_0': '1hr', 'time_1': '30min'}
    out = build_umfrq(time_axs, um_multi_time)
    assert umfrq == out
    
#@pytest.mark.parametrize('fname', [0,1,2])
def test_get_date_pattern(caplog):
    caplog.set_level(logging.DEBUG, logger='mopdb_log')
    fname = 'ocean_month.nc-09961231'
    fpattern = 'ocean_month.nc-'
    dp = get_date_pattern(fname, fpattern)
    date = ''.join(x for x in itertools.compress(fname,dp))
    assert date == '09961231'
    fname = 'umnsa_cldrad_20160603T0000.nc'
    fpattern = 'umnsa_cldrad_'
    dp = get_date_pattern(fname, fpattern)
    date = ''.join(x for x in itertools.compress(fname,dp))
    assert date == '201606030000'
    fname = 'cw323a.pm095101_mon.nc'
    fpattern = 'cw323a.pm'
    dp = get_date_pattern(fname, fpattern)
    date = ''.join(x for x in itertools.compress(fname,dp))
    assert date == '095101'
