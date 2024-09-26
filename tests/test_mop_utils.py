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

from mopper.mop_utils import (check_timestamp, get_cmorname,)


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
    # test ocn files
    ctx.obj['frequency'] = 'day'
    ctx.obj['realm'] = 'ocean'
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
    ctx.obj['realm'] = 'seaice'
    files = [Path(f'iceh_d.1983-{str(m).zfill(2)}.nc') for m in range(1,12)] 
    inrange = files
    with ctx:
            out5 = check_timestamp(files)
    assert out5 == inrange


def test_get_cmorname(caplog):
    global ctx
    caplog.set_level(logging.DEBUG, logger='mop_log')
    # axis_name t
    ctx.obj['calculation'] = "plevinterp(var[0], var[1], 3)"
    ctx.obj['variable_id'] = "ta3"
    ctx.obj['timeshot'] = 'mean'
    data = np.random.rand(3, 5, 3, 6)
    tdata = pd.date_range("2000-01-01", periods=5)
    lats = np.linspace(-20.0, 10.0, num=3)
    lons = np.linspace(120.5, 150.0, num=6)
    levs = np.arange(1, 4)
    foo = xr.DataArray(data, coords=[levs, tdata, lats, lons],
          dims=["lev", "t", "lat", "lon"])
    with ctx:
        tname = get_cmorname('t', foo.t, z_len=None)
        iname = get_cmorname('lon', foo.lon, z_len=None)
        jname = get_cmorname('lat', foo.lat, z_len=None)
        zname = get_cmorname('z', foo.lev, z_len=3)
    assert tname == 'time'
    assert iname == 'longitude'
    assert jname == 'latitude'
    assert zname == 'plev3'
