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

from pathlib import Path

class FPattern():
    """This class represent a file pattern with a set list of variables
       its attributes represents features of the variables which are shared.
    """ 

    def __init__(self, fpattern: str, fpath: Path):
        self.fpattern = fpattern
        self.fpath = fpath
        self.files = self.get_files() 
        self.realm =  self.get_realm()
        self.frequency = self.get_frequency() 
        self.version = ''
        self.multiple_frq = False
        self.varlist = []

    def get_frequency(self):
        frequency = 'NAfrq'
        fname = str(self.files[0])
        if self.realm == 'atmos':
            fbits = fname.split("_")
            frequency = fbits[-1].replace(".nc", "")
        elif self.realm == 'ocean':
            if any(x in fname for x in ['scalar', 'month']):
                frequency = 'mon'
            elif 'daily' in fname:
                frequency = 'day'
        elif self.realm == 'seaIce':
            if '_m.' in fname:
                frequency = 'mon'
            elif '_d.' in fname:
                frequency = 'day'
        return frequency


    def get_realm(self):
        realm = 'NArealm'
        realm = next((x for x in ['atmos', 'ocean', 'ice', 'ocn','atm']
            if x in self.fpath.parts), 'NArealm')
        fix_realm = {'atm': 'atmos', 'ice': 'seaIce', 'ocn': 'ocean'}
        if realm in fix_realm.keys():
            realm = fix_realm[realm]
        return realm

    def get_files(self):
        return self.list_files(self.fpath, self.fpattern)

    @staticmethod
    def list_files(indir, match):
        """Returns list of files matching input directory and match"""
        files = [x for x in Path(indir).rglob(f"*{match}*")
            if x.is_file() and  '.nc' in str(x)]
        files.sort(key=lambda x:x.name)
        return files


class Variable():
    """This class represent a single variable with attributes derived from file
       and the one added by mapping.
    """ 

  #  __slots__ = ('name', 'pattern', 'files', 'frequency', 'realm',
  #      'cmor_var', 'cmor_table', 'version', 'units', 'dimensions',
  #      'cell_methods', 'positive', 'long_name', 'standard_name',
  #      'vtype', 'size', 'nsteps')

    def __init__(self, varname: str, fobj: FPattern):
        self.name = varname
        # path object
        self.fpattern = fobj.fpattern
        #self.fpath = fobj.fpath
        #self.files = fobj.files
        # mapping attributes
        self._frequency = fobj.frequency 
        self._realm = fobj.realm
        self.cmor_var = '' 
        self.cmor_table = '' 
        #self.version = self.fpattern.version
        self.match = False
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
        value = value.replace('hPt', 'hrPt')
        if not any(x in value for x in 
            ['min', 'hr', 'day', 'mon', 'yr']):
            value = 'NAfrq' 
        self._frequency = value


    @property
    def realm(self):
        return self._realm

    @realm.setter
    def realm(self, value):
        if not any(x in value for x in 
            ['atmos', 'seaIce', 'ocean', 'land', 'landIce']):
            value = 'NArealm' 
        self.realm = value
