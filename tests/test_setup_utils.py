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

from datetime import datetime
from dateutil.relativedelta import relativedelta

from mopper.setup_utils import (compute_fsize, adjust_size, adjust_nsteps,
    build_filename,  define_timeshot, define_file)

ctx = click.Context(click.Command('cmd'),
    obj={'start_date': '19830101T0000', 'end_date': '19830201T0000',
         'realm': 'atmos', 'frequency': '1hr', 'max_size': 2048.0})
# to test fx frequency4yy
ctx2 = click.Context(click.Command('cmd'),
    obj={'start_date': '19830101T0000', 'end_date': '19830201T0000',
         'realm': 'atmos', 'frequency': 'fx', 'max_size': 2048.0})

ctx3 = click.Context(click.Command('cmd'),
    obj={'start_date': '20230101T0000', 'end_date': '20250101T0000',
         'frequency': 'mon', 'outpath': '/g/da/exp',
         'path_template': '{version}/{frequency}', 'file_template': 
         '{variable_id}_{frequency}'})

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
    # test that timeshot stays sum with resample
    cell_methods = "area: mean time: sum"
    resample = "D"
    timeshot, frequency = define_timeshot(frq, resample, cell_methods)
    assert frequency == "day"
    assert timeshot == "sum"
    # test timeshot point if Pt in frequency
    resample = ""
    frq = "1hrPt"
    timeshot, frequency = define_timeshot(frq, resample, cell_methods)
    assert frequency == "1hr"
    assert timeshot == "point"


def test_build_filename():
    # test monthly frequency 
    half_tstep = relativedelta(days=15)
    opts = { 'frequency': 'mon', 'timeshot': 'mean',
         'version': 'v1.0', 'variable_id': 'tas',
         'tstart': '20230116T1200', 'tend': '20231216T1200'}
    with ctx3:
        fpath, fname = build_filename(opts)
    assert fpath == "/g/da/exp/v1-0/mon" 
    assert fname == "tas_mon_202301-202312.nc"
    # test daily frequency 
    opts['tstart'] = '20230101T1200'
    opts['tend'] = '20231231T1200'
    opts['frequency'] = 'day'
    ctx3.obj['frequency'] = 'day'
    half_tstep = relativedelta(hours=12)
    with ctx3:
        fpath, fname = build_filename(opts)
    assert fpath == "/g/da/exp/v1-0/day" 
    assert fname == "tas_day_20230101-20231231.nc"
     # test 10min frequency and timeshot point
    opts['tstart'] = '20230101T0010'
    opts['tend'] = '20240101T0000'
    opts['frequency'] = '10min'
    opts['timeshot'] = 'point'
    half_tstep = relativedelta(minutes=5)
    ctx3.obj['frequency'] = '10minPt'
    #ctx3.obj['end_date'] = '20250101T0001'
    with ctx3:
        fpath, fname = build_filename(opts)
    assert fpath == "/g/da/exp/v1-0/subhrPt" 
    assert fname == "tas_subhrPt_20230101001000-20240101000000.nc"

def test_define_file():
    frm = '%Y%m%dT%H%M'
    st = datetime.strptime('20230101T0000', frm)
    fin = datetime.strptime('20240101T0000', frm)
    delta = relativedelta(months=1)
    half_tstep = relativedelta(hours=12)
    tstep = relativedelta(days=1)
    opts, newtime = define_file({'timeshot':'mean', 'frequency':'day'},
        st, fin, delta, tstep, half_tstep)
    assert opts['tstart'] == '20230101T1200'
    assert opts['sel_start'] == '202212311200'
    assert opts['tend'] == '20230131T1200'
    assert opts['sel_end'] == '202302011200'
    assert newtime == datetime.strptime('20230201T0000', frm)
    # test 10min frequency
    delta = relativedelta(days=1)
    half_tstep = relativedelta(minutes=5)
    tstep = relativedelta(minutes=10)
    opts, newtime = define_file({'timeshot':'point', 'frequency': '10min'},
        st, fin, delta, tstep, half_tstep)
    assert opts['tstart'] == '20230101T0010'
    assert opts['sel_start'] == '202301010000'
    assert opts['tend'] == '20230102T0000'
    assert opts['sel_end'] == '202301020010'
    assert newtime == datetime.strptime('20230102T0000', frm)
    # test monthly frequency
    delta = relativedelta(years=1)
    half_tstep = relativedelta(days=15)
    tstep = relativedelta(months=1)
    opts, newtime = define_file({'timeshot':'mean', 'frequency': 'mon'},
        st, fin, delta, tstep, half_tstep)
    assert opts['tstart'] == '20230116T1200'
    assert opts['sel_start'] == '202212161200'
    assert opts['tend'] == '20231216T1200'
    assert opts['sel_end'] == '202401161200'
    assert newtime == datetime.strptime('20240101T0000', frm)
    # test monthly frequency for February and 1 timestep file
    delta = relativedelta(months=1)
    st = datetime.strptime('20230201T0000', frm)
    opts, newtime = define_file({'timeshot':'mean', 'frequency': 'mon'},
        st, fin, delta, tstep, half_tstep)
    assert opts['tstart'] == '20230215T0000'
    assert opts['sel_start'] == '202301150000'
    assert opts['tend'] == '20230215T0000'
    assert opts['sel_end'] == '202303150000'
    assert newtime == datetime.strptime('20230301T0000', frm)
    # assert that last files end at finish
    delta = relativedelta(years=1)
    fin = datetime.strptime('20230701T0000', frm)
    opts, newtime = define_file({'timeshot':'mean', 'frequency': 'mon'},
        st, fin, delta, tstep, half_tstep)
    assert newtime == datetime.strptime('20230701T0000', frm)
