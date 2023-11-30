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
from mopdb.mopdb import *
from click.testing import CliRunner

@pytest.mark.parametrize('subcommand', ['varlist', 'template', 'check', 'cmor', 'table', 'map'])
def test_cmip(command, runner):
    result = runner.invoke(mopdb, ['--help'])
    assert result.exit_code == 0
    result = runner.invoke(mopdb, [subcommand, '--help'])
    assert result.exit_code == 0

@pytest.mark.usefixtures("setup_db") # 1
def test_template(session):

    runner = CliRunner()

    with runner.isolated_filesystem():
        with open('varlist.txt', 'w') as f:
            f.write('name;cmor_var;units;dimensions;frequency;realm;cell_methods;cmor_table;dtype;size;nsteps;file_name;long_name;standard_name')
            f.write('fld_s03i236;tas;K;time lat lon,mon;atmos;area: time: mean;CMIP6_Amon;float32;110592;2081;cm000a.pm;TEMPERATURE AT 1.5M;air_temperature')
            f.write('fld_s03i237;huss;1;time lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;110592;2081;cm000a.pm;SPECIFIC HUMIDITY  AT 1.5M;specific_humidity')
            f.write('fld_s05i205;prrc;kg m-2 s-1;time_0 lat lon;3hr;atmos;area: time: mean;CMIP6_E3hr;float32;110592;578880;cm000a.p8;CONVECTIVE RAINFALL RATE     KG/M2/S;convective_rainfall_flux')
            f.write('fld_s03i236;tas;K;time lat lon;day;atmos;area: time: mean;CMIP6_day;float32;110592;74772;cm000a.pd;TEMPERATURE AT 1.5M;air_temperature')

        result = runner.invoke(mopdb, ['template', '-f varlist.txt', '-vCM2'])
        #assert result.exit_code == 0
        assert 'Opened database successfully' in result.output
        assert 'Definable cmip var' in result.output 
#Pass temp_dir to control where the temporary directory is created. The directory will not be removed by Click in this case. This is useful to integrate with a framework like Pytest that manages temporary files.

#def test_keep_dir(tmp_path):
#    runner = CliRunner()

#    with runner.isolated_filesystem(temp_dir=tmp_path) as td:
#        ...

def test_with_context():
    ctx = click.Context(click.Command('cmd'), obj={'prop': 'A Context'})
    with ctx:
        process_cmd()
