#!/usr/bin/env python
# Copyright 2024 ARC Centre of Excellence for Climate Extremes (CLEX)
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> for CLEX
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
#
# contact: paola.petrelli@utas.edu.au
#
# last updated 06/07/2024

class Variable():
  
  #  __slots__ = ('name', 'pattern', 'files', 'frequency', 'realm',
  #      'cmor_var', 'cmor_table', 'version', 'units', 'dimensions',
  #      'cell_methods', 'positive', 'long_name', 'standard_name',
  #      'vtype', 'size', 'nsteps')

    def __init__(self, varname, fpattern):
        self.name = varname
        # path attributes
        self.pattern = fpattern
        self.files = [] 
        # mapping attributes
        self.frequency = 'NAfrq' 
        self.realm = 'NArealm' 
        self.cmor_var = '' 
        self.cmor_table = '' 
        self.version = ''
        # descriptive attributes
        self.units = '' 
        self.dimensions = '' 
        self.cell_methods = ''
        self.positive = ''
        self.long_name = '' 
        self.standard_name = '' 
        # type and size attributes
        self.vtype = ''
        self.size = 0
        self.nsteps = 0


    @property
    def frequency(self):
        return self._frequency

    @frequency.setter
    def frequency(self, value):
        fix_frq = {'dCai': 'day', '3h': '3hr', '6h': '6hr'}
        if value in fix_frq.keys():
            self._frequency = fix_frq[value]
        value = value.replace('hPt', 'hrPt')
        if not any(x in value for x in 
            ['min', 'hr', 'day', 'mon', 'yr']):
            self._frequency = 'NAfrq' 
        self._frequency = value


    @property
    def realm(self):
        return self._realm

    @realm.setter
    def realm(self, value):
        fix_realm = {'atm': 'atmos', 'ice': 'seaIce', 'ocn': 'ocean'}
        if value in fix_realm.keys():
            self._realm = fix_realm[value]
        if not any(x in value for x in 
            ['atmos', 'seaIce', 'ocean', 'land']):
            self._realm = 'NArealm' 

    def list_files(self):
        """Returns list of files matching input directory and match"""
        self.files = [x for x in Path(self.indir).rglob(f"{self.match}") if x.is_file()]
        return files.sort(key=lambda x:x.name)
