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

#import pytest
import click
import xarray as xr
import numpy as np
import pandas as pd
import logging
from pathlib import Path

from mopper.mop_utils import (check_timestamp, get_cmorname,
    define_attrs)


ctx = click.Context(click.Command('cmd'),
    obj={'sel_start': '198302170600', 'sel_end': '198302181300',
         'realm': 'atmos', 'frequency': '1hr', 'var_log': 'varlog_1'})
# to test 6 hourly files
ctx2 = click.Context(click.Command('cmd'),
    obj={'sel_start': '198302170000', 'sel_end': '198302182100',
         'realm': 'atmos', 'frequency': '6hr', 'var_log': 'varlog_1'})

def test_check_timestamp(caplog):
    global ctx
    caplog.set_level(logging.DEBUG, logger='mop_log')
    caplog.set_level(logging.DEBUG, logger='varlog_1')
    # test atmos 1hr files
    files = [Path(f'obj_198302{d}T{str(h).zfill(2)}01_1hr.nc') 
             for d in ['17','18','19'] for h in range(24)] 
    inrange = files[6:37]
    with ctx:
        out1 = check_timestamp(files)
    assert out1 == inrange
    # get only first file is frequency is fx
    ctx.obj['frequency'] = 'fx'
    inrange = [files[0]]
    with ctx:
        out2 = check_timestamp(files)
    assert out2 == inrange
    # test atmos 6hr files
    files = [Path(f'obj_198302{d}T{str(h).zfill(2)}01_6hr.nc')
             for d in ['17','18','19'] for h in range(0,24,6)] 
    inrange = files[:8]
    with ctx2:
        out3 = check_timestamp(files)
    assert out3 == inrange
    # test atmos 1hr AUS2200 style files
    ctx2.obj['frequency'] = '1hr'
    ctx2.obj['sel_start'] =  '198302150530'
    ctx2.obj['sel_end'] =  '198302151130'
    files = [Path(f'/g/d/h/A/f-e/19830215T0000/a/um_cl_19830215T{str(h).zfill(2)}00_1hr.nc')
             for h in range(0,24)]
    inrange = files[6:12]
    with ctx2:
        out4 = check_timestamp(files)
    assert out4 == inrange
    # function is now independent from realm no need to fix it in ctx
    # test ocean files
    ctx.obj['frequency'] = 'day'
    files = [Path(f'ocn_daily.nc-198302{str(d).zfill(2)}') for d in range(1,29)] 
    inrange = files[16:18]
    with ctx:
        out5 = check_timestamp(files)
    assert out5 == inrange
    # test ice files
    # this pass but because month and year are separated by "-" 
    # it selects more than we would expect as tstamp is only 1983
    ctx2.obj['sel_start'] =  '198301010000'
    ctx2.obj['sel_end'] =  '198312311200'
    files = [Path(f'iceh_d.1983-{str(m).zfill(2)}.nc') for m in range(1,12)] 
    inrange = files
    with ctx2:
        out6 = check_timestamp(files)
    assert out6 == inrange
    # test with 3 digit number in filename which is not a date
    files = [Path(f'/sc/AM3/di787/di787a.pd198303.nc')] 
    with ctx2:
        out7 = check_timestamp(files)
    assert out7 == files
    # test with 3 digit number in filename which is not a date
    # and missing 0 at start of year
    ctx2.obj['sel_start'] =  '078301010000'
    ctx2.obj['sel_end'] =  '078312311200'
    files = [Path(f'/sc/AM3/di787/di787a.pd78303.nc')] 
    with ctx2:
        out8 = check_timestamp(files)
    assert out8 == files


def test_get_cmorname(caplog):
    global ctx
    caplog.set_level(logging.DEBUG, logger='mop_log')
    # axis_name t
    ctx.obj['axes'] = 'longitude latitude plev3 time'
    #data = np.random.rand(3, 5, 3, 6)
    #tdata = pd.date_range("2000-01-01", periods=5)
    #lats = np.linspace(-20.0, 10.0, num=3)
    #lons = np.linspace(120.5, 150.0, num=6)
    #levs = np.arange(1, 4)
    #foo = xr.DataArray(data, coords=[levs, tdata, lats, lons],
    #      dims=["lev", "t", "lat", "lon"])
    with ctx:
        tname = get_cmorname('time')
        iname = get_cmorname('lon')
        jname = get_cmorname('lat')
        zname = get_cmorname('lev')
    assert tname == 'time'
    assert iname == 'longitude'
    assert jname == 'latitude'
    assert zname == 'plev3'
    ctx.obj['axes'] = 'longitude latitude alevel time'
    with ctx:
        zname = get_cmorname('theta_model_level_number')
    assert zname == 'hybrid_height'

def test_define_attrs(caplog):
    global ctx
    caplog.set_level(logging.DEBUG, logger='varlog_1')
    ctx.obj['attrs'] = {'notes': "some existing note"}
    ctx.obj['variable_id'] = "ta"
    ctx.obj['calculation'] = "... plevinterp(var[0]) "
    with ctx:
        out = define_attrs()
    assert out['notes'] == "some existing note Linearly interpolated from model levels using numpy.interp() function. NaNs are assigned to pressure levels falling out of the height range covered by the model"
    # repeating to make sure we are not using reference to ctx see issue #190
    with ctx:
        out = define_attrs()
    assert out['notes'] == "some existing note Linearly interpolated from model levels using numpy.interp() function. NaNs are assigned to pressure levels falling out of the height range covered by the model"
    ctx.obj['attrs'] = {}
    with ctx:
        out = define_attrs()
    assert out['notes'] == "Linearly interpolated from model levels using numpy.interp() function. NaNs are assigned to pressure levels falling out of the height range covered by the model"
