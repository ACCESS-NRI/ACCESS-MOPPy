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
import xarray as xr
import numpy as np
import pandas as pd
from mopper.calculations import *

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

def test_calc_mrsos():
    global mrsol
    expected = mrsol.isel(depth=0) + mrsol.isel(depth=1)/3.0
    out = calc_mrsos(mrsol)
    xrtest.assert_allclose(out, expected, rtol=1e-05) 
