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
import logging
import itertools
from pathlib import Path

from mopdb.mopdb_utils import (get_date_pattern, identify_patterns)
#from mopdb.mopdb_class import MapVariable, Variable, FPattern



    
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


def test_identify_patterns(caplog):
    # test CM2 style run with p7/p8 files
    files = ['cw323a.pm095103_mon.nc',  'cw323a.pm095106_mon.nc',  'cw323a.pm095109_mon.nc',
         'cw323a.pm095112_mon.nc', 'ocean_month.nc-09961231', 'cw323a.p7095103_mon.nc',
         'cw323a.p8095103_mon.nc', 'cw323a.pd095106_dai.nc',
         'ocean_daily.nc-09981231',  'ocean_daily.nc-10001231', 'ocean_month.nc-09971231',
         'ocean_scalar.nc-09991231', 'ocean_scalar.nc-09991231',
         'iceh_d.1000-01.nc', 'iceh_m.1000-01.nc', 'iceh_d.0999-12.nc', 'iceh_m.0999-12.nc'
         'iceh_d.1000-02.nc', 'iceh_m.1000-02.nc', 'iceh_d.0999-11.nc', 'iceh_m.0999-11.nc']
    paths = [Path(x) for x in sorted(files)]
    patterns = identify_patterns(paths)
    assert patterns == ['cw323a.p7', 'cw323a.p8', 'cw323a.pd', 'cw323a.pm',
                        'iceh_d.', 'iceh_m.', 'ocean_daily', 'ocean_month',
                        'ocean_scalar']
    # test CM2 style ocean & ice run with only 1 file
    files = ['ocean_daily.nc-09981231', 'ocean_month.nc-09971231',
         'ocean_scalar.nc-09991231', 'iceh_d.1000-01.nc', 'iceh_m.1000-01.nc']
    paths = [Path(x) for x in sorted(files)]
    patterns = identify_patterns(paths)
    assert patterns == ['iceh_d', 'iceh_m.1000-01.nc', 'ocean_d',
                        'ocean_m', 'ocean_scalar.nc-09991231']
    # test AUS2200 style files
    files = ['umnsa_cldrad_20220222T0000.nc', 'umnsa_mdl_20220222T0200.nc',
        'umnsa_slv_20220222T0400.nc', 'umnsaa_pa000.nc', 'umnsa_cldrad_20220222T0100.nc',
        'umnsa_mdl_20220222T0300.nc', 'umnsa_slv_20220222T0500.nc', 'umnsaa_pvera000.nc',
        'umnsa_cldrad_20220222T0200.nc', 'umnsa_mdl_20220222T0400.nc',
        'umnsa_spec_20220222T0000.nc', 'umnsaa_pverb000.nc', 'umnsa_spec_20220222T0100.nc',
        'umnsaa_pverc000.nc', 'umnsaa_pverd000.nc', 'umnsa_cldrad_20220222T0500.nc',
        'umnsa_mdl_20220222T0100.nc', 'umnsa_slv_20220222T0300.nc', 'umnsa_spec_20220222T0500.nc']
    paths = [Path(x) for x in sorted(files)]
    patterns = identify_patterns(paths)
    assert patterns == ['umnsa_cldrad_', 'umnsa_mdl_', 'umnsa_slv_',
       'umnsa_spec_', 'umnsaa_pa', 'umnsaa_pvera', 'umnsaa_pverb',
       'umnsaa_pverc', 'umnsaa_pverd000.nc']
    # test patterns with jan, feb labels
    files = ['br565Wa.pd0989apr.nc', 'br565Wa.pd0989aug.nc', 'br565Wa.pd0988apr.nc', 'br565Wa.pd0988aug.nc']
    paths = [Path(x) for x in sorted(files)]
    patterns = identify_patterns(paths)
    assert patterns == ['br565Wa.pd']
    # test patterns with T and/or "-' in stem works
    files = ['b56Ta-so.pd0989apr.nc', 'b56Ta-so.pd0989aug.nc', 'b56Ta-so.pd0988apr.nc', 'b56Ta-so.pd0988aug.nc']
    paths = [Path(x) for x in sorted(files)]
    patterns = identify_patterns(paths)
    assert patterns == ['b56Ta-so.pd']
