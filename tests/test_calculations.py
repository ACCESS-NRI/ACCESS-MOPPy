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

import xarray.testing as xrtest
import numpy.testing as nptest
import xarray as xr
import numpy as np
import pandas as pd
import logging
from mopper.calculations import *

logger = logging.getLogger('var_log')
ctx = click.Context(click.Command('cmd'),
    obj={'sel_start': '198302170600', 'sel_end': '198302181300',
         'realm': 'atmos', 'frequency': '1hr', 'var_log': logger})

def create_var(nlat, nlon, ntime=None, nlev=None, sdepth=False, seed=100):

    np.random.seed(seed)
    lat = np.linspace(-90, 90, nlat, endpoint=True)
    lon = np.linspace(-180, 180, nlon+1, endpoint=True)[1:]
    coords = {'lat': lat, 'lon': lon}
    dims = ['lat', 'lon']
    shape = [nlat, nlon]

    if nlev is not None:
        lev = np.arange(1, nlev)
        dims.insert(0, 'lev')
        coords['lev'] = lev
        shape.insert(0, nlev)
    elif sdepth is True:
        depth = np.array([0.05, 0.2, 0.5, 1])
        dims.insert(0, 'depth')
        coords['depth'] = depth
        shape.insert(0, 4)
    if ntime is not None:
        time = pd.date_range(start='2000-01-01', freq='D', periods=ntime)
        dims.insert(0, 'time')
        coords['time'] = time
        shape.insert(0, ntime)

    da = xr.DataArray( np.random.random(shape), 
            dims=tuple(dims),
            coords=coords,
            attrs={'name': 'random'})
    return da

mrsol = create_var(2, 3, ntime=4, sdepth=True)

def test_calc_topsoil():
    global mrsol
    expected = mrsol.isel(depth=0) + mrsol.isel(depth=1)/3.0
    out = calc_topsoil(mrsol)
    xrtest.assert_allclose(out, expected, rtol=1e-05) 


def test_overturn_stream():
    global ctx, logger
    # set up input
    dims = ['time', 'depth', 'lat', 'lon']
    time = pd.date_range("2014-09-06", periods=1)
    depth = [ 5., 10.]
    lat = [10., -10.]
    lon = [50., 60.]
    ty_val = np.array([[[[1., 2. ],[2., 0.5]],
        [[ 3., 1.], [1.5, 4.]]]])
    ty = xr.DataArray(data=ty_val, dims=dims,
        coords=dict( lon=(["lon"], lon), lat=(["lat"], lat),
            time=time, depth=(["depth"], depth)), name='ty') 
    ty_gm = xr.DataArray(data=ty_val*2, dims=dims,
        coords=dict( lon=(["lon"], lon), lat=(["lat"], lat),
            time=time, depth=(["depth"], depth)), name='ty_gm') 
    ty_submeso = xr.DataArray(data=ty_val*3, dims=dims,
        coords=dict( lon=(["lon"], lon), lat=(["lat"], lat),
            time=time, depth=(["depth"], depth)), name='ty_submeso')
    # set up expected output
    # test with only ty_trans variable
    varlist = [ty] 
    res1 = np.array([[[-4. , -5.5], [ 0. ,  0. ]]])
    with ctx:
        out1 = overturn_stream(varlist)
    nptest.assert_array_equal(res1, out1)
    # test units are sverdrup
    with ctx:
        outsv = overturn_stream(varlist, sv=True)
    res1 = res1 * 10**9
    nptest.assert_array_equal(res1, outsv)
    # test with ty_trans and gm variable
    varlist = [ty, ty_gm] 
    res2 = np.array([[[2. , -0.5], [ 8. ,  11. ]]])
    with ctx:
        out2 = overturn_stream(varlist)
    nptest.assert_array_equal(res2, out2)
    # test with ty_trans and submeso variables
    varlist = [ty, ty_submeso] 
    res3 = np.array([[[5. , 2], [ 12. ,  16.5 ]]])
    with ctx:
        out3 = overturn_stream(varlist)
    nptest.assert_array_equal(res3, out3)
    # test with ty_trans, gm and submeso variables
    # and that input order shouldn't matter
    varlist = [ty_gm, ty_submeso, ty] 
    res4 = np.array([[[11. , 7], [ 20. ,  27.5 ]]])
    with ctx:
        out4 = overturn_stream(varlist)
    nptest.assert_array_equal(res4, out4)
