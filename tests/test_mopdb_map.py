#!/usr/bin/env python
# Copyright 2023 ARC Centre of Excellence for Climate Extremes
# author: Paola Petrelli <paola.petrelli@utas.edu.au>
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
import logging
import xarray as xr
from mopdb.mopdb_map import (add_var, get_file_frq)
#from mopdb.mopdb_class import MapVariable, Variable, FPattern
#from conftest import *


TESTS_HOME = os.path.abspath(os.path.dirname(__file__))
TESTS_DATA = os.path.join(TESTS_HOME, "testdata")
# consecutive files with multiple time axes
dsmulti = os.path.join(TESTS_DATA, "multitime.nc")
dsmulti2 = os.path.join(TESTS_DATA, "multitime_next.nc")
# consecutive files with a 1-time step time axis
dsonestep = os.path.join(TESTS_DATA, "onetstep.nc")
dsonestep2 = os.path.join(TESTS_DATA, "onetstep_next.nc")

@pytest.mark.parametrize('idx', [0,1,2])
def test_add_var(varobjs, matches, idx, caplog):
    caplog.set_level(logging.DEBUG, logger='mopdb_log')
    vlist = []
    vlist = add_var(vlist, varobjs[idx], matches[idx])
    assert vlist[0].cmor_var == matches[idx][0] 


def test_get_file_frq(caplog):
    global dsmulti, dsmulti2, dsonestep, dsonestep2
    caplog.set_level(logging.DEBUG, logger='mopdb_log')
    umfrq = {'time': 'day', 'time_0': '1hr', 'time_1': '30min'}
    int2frq = {'day': 1.0, '1hr':  0.041667, '30min': 0.020833}
    # multi time axes in file
    ds =  xr.open_dataset(dsmulti, decode_times=False)
    out = get_file_frq(ds, dsmulti2, int2frq)
    assert umfrq == out
    # only one time axis in file with 1 value
    ds =  xr.open_dataset(dsonestep, decode_times=False)
    out = get_file_frq(ds, dsonestep2, int2frq)
    umfrq = {'time': 'day'}
    assert umfrq == out
    
