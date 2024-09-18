#!/usr/bin/env python
# Copyright 2024 ARC Centre of Excellence for Climate Extremes
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

#import pytest
import click
import logging
import pytest
from mopper.setup_utils import (compute_fsize, adjust_size, adjust_nsteps,
    build_filename,  define_timeshot, define_files)

ctx = click.Context(click.Command('cmd'),
    obj={'start_date': '19830101T0000', 'end_date': '19830201T0000',
         'realm': 'atmos', 'frequency': '1hr', 'max_size': 2048.0})
# to test fx frequency
ctx2 = click.Context(click.Command('cmd'),
    obj={'start_date': '19830101T0000', 'end_date': '19830201T0000',
         'realm': 'atmos', 'frequency': 'fx', 'max_size': 2048.0})

def test_compute_fsize(caplog):
    caplog.set_level(logging.DEBUG, logger='mop_log')
    grid_size = 22048000.0 
    opts = {'calculation': '', 'resample': ''}
    # 31 days, 1hr frq and max size ~2 GB, expected 1 file * day
    with ctx:
        interval, fsize = compute_fsize(opts, grid_size, ctx.obj['frequency'])
    assert int(fsize) == 504
    assert interval == 'days=1'
    # 31 days, fx frq and max size ~2 GB, expected 1 file (entire interval)
    with ctx2:
        interval, fsize = compute_fsize(opts, grid_size, ctx2.obj['frequency'])
    assert pytest.approx(fsize, 0.01) == 0.13
    assert interval == 'days=31'

def test_adjust_size(caplog):
    caplog.set_level(logging.DEBUG, logger='mop_log')
    insize = 22048000.0 
    opts = {'calculation': 'plevinterp(var[0], var[1], 3)',
            'resample': '', 'levnum': 30}
    with ctx:
         assert insize/10.0 == adjust_size(opts, insize)

def test_adjust_nsteps(caplog):
    frq = 'day'
    vdict = {'nsteps': 240, 'frequency': '1hr'}
    assert  10 == adjust_nsteps(vdict, frq)

#@pytest.mark.parametrize('tshot', ['mean','max','min'])
def test_define_timeshot():
    pass
    frq = "day"
    resample = ""
    cell_methods = 'time: mean'
    #cell_methods = f"area: time: {tshot}"
    timeshot, frequency = define_timeshot(frq, resample, cell_methods)
    assert frequency == frq
    assert timeshot == "mean"
    # test that timeshot is updated from point to mean with resample
    cell_methods = "area: mean time: point"
    resample = "D"
    timeshot, frequency = define_timeshot(frq, resample, cell_methods)
    assert frequency == "day"
    assert timeshot == "mean"
    # test that timeshot is updated from maximum to max with resample
    cell_methods = "area: mean time: maximum"
    resample = "D"
    timeshot, frequency = define_timeshot(frq, resample, cell_methods)
    assert frequency == "day"
    assert timeshot == "max"
    # test timeshot point if Pt in frequency
    #cell_methods = "area: mean time: maximum"
    resample = ""
    frq = "1hrPt"
    timeshot, frequency = define_timeshot(frq, resample, cell_methods)
    assert frequency == "1hr"
    assert timeshot == "point"
