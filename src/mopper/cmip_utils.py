#!/usr/bin/env python
# Copyright 2023 ARC Centre of Excellence for Climate Extremes
# author: Paola Petrelli <paola.petrelli@utas.edu.au>
# author: Sam Green <sam.green@unsw.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is the ACCESS Model Output Post Processor, derived from the APP4
# originally written for CMIP5 by Peter Uhe and dapted for CMIP6 by Chloe Mackallah
# ( https://doi.org/10.5281/zenodo.7703469 )
#
# last updated 06/11/2023
#
# This file contains functions needed when processing CMIP files via dreq

import json
import csv
import ast
import copy
import click
from collections import OrderedDict


def find_cmip_tables(dreq):
    """
    Returns
    -------
    """
    tables=[]
    with dreq.open(mode='r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if not row[0] in tables:
                if (row[0] != 'Notes') and (row[0] != 'MIP table') and (row[0] != '0'):
                    tables.append(f"CMIP6_{row[0]}")
    f.close()
    return tables


def reallocate_years(years, reference_date):
    """Reallocate years based on dreq years 
    Not sure what it does need to ask Chloe
    """
    reference_date = int(reference_date[:4])
    if reference_date < 1850:
        years = [year-1850+reference_date for year in years]
    else:
        pass
    return years


def fix_years(years, tstart, tend):
    """Update start and end date for experiment based on dreq
    constraints for years. It is called only if dreq and dreq_years are True

    Parameters
    ----------
    years : list
        List of years from dreq file
    tstart: str
        Date of experiment start as defined in config
    tend: str
        Date of experiment end as defined in config

    Returns
    -------
    tstart: str
        Updated date of experiment start
    tend: str
        Updated date of experiment end
    """
    if tstart >= years[0]:
        pass
    elif (tstart < years[0]) and (tend >= years[0]):
        tstart = years[0] + "0101T0000"
    else:
        tstart = None 
    if tend <= years[-1]:
        pass
    elif (tend > years[-1]) and (tstart <= years[-1]):
        tend = years[-1] + "1231T2359"
    else:
        tstart = None 
    return tstart, tend


@click.pass_context
def read_dreq_vars(ctx, table_id, activity_id):
    """Reads dreq variables file and returns a list of variables included in
    activity_id and experiment_id, also return dreq_years list

    Parameters
    ----------
    cdict : dict
        Dictionary with post-processing config 
    table_id : str
        CMIP table id
    activity_id: str
        CMIP activity_id

    Returns
    -------
    dreq_variables : dict
        Dictionary where keys are cmor name of selected variables and
        values are corresponding dreq years
    """
    with open(ctx.obj['dreq'], 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        dreq_variables = {} 
        for row in reader:
            if (row[0] == table_id) and (row[12] not in ['', 'CMOR Name']):
                cmorname = row[12]
                mips = row[28].split(',')
                if activity_id not in mips:
                    continue
                try:
                    #PP if years==rangeplu surely calling this function will fail
                    # in any cas eis really unclear what reallocate years does and why, it returns different years
                    # if ref date before 1850???
                    if 'range' in row[31]:
                        years = reallocate_years(
                                ast.literal_eval(row[31]), ctx.obj['reference_date'])
                        years = f'"{years}"'
                    elif 'All' in row[31]:
                        years = 'all'
                    else:
                        try:
                            years = ast.literal_eval(row[31])
                            years = reallocate_years(years, ctx.obj['reference_date'])
                            years = f'"{years}"'
                        except:
                            years = 'all'
                except:
                    years = 'all'
                dreq_variables[cmorname] = years
    f.close()
    return dreq_variables


def edit_json_cv(json_cv, attrs):
    """Edit the CMIP6 CV json file to include extra activity_ids and
    experiment_ids, so they can be recognised by CMOR when following 
    CMIP6 standards.

    Parameters
    ----------
    json_cv : str
        Path of CV json file to edit
    attrs: dict
        Dictionary with attributes defined for experiment

    Returns
    -------
    """
    activity_id = attrs['activity_id']
    experiment_id = attrs['experiment_id']

    with open(json_cv, 'r') as f:
        json_cv_dict = json.load(f, object_pairs_hook=OrderedDict)
    f.close()

    if activity_id not in json_cv_dict['CV']['activity_id']:
        print(f"activity_id '{activity_id}' not in CV, adding")
        json_cv_dict['CV']['activity_id'][activity_id] = activity_id

    if experiment_id not in json_cv_dict['CV']['experiment_id']:
        print(f"experiment_id '{attrs['experiment_id']}' not in CV, adding")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [attrs['parent_activity_id']],
        'parent_experiment_id': [attrs['parent_experiment_id']],
        'required_model_components': [attrs['source_type']],
        'sub_experiment_id': ['none']
        })
    else:
        print(f"experiment_id '{experiment_id}' found, updating")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [attrs['parent_activity_id']],
        'parent_experiment_id': [attrs['parent_experiment_id']],
        'required_model_components': [attrs['source_type']],
        'sub_experiment_id': ['none']
        })
    with open(json_cv,'w') as f:
        json.dump(json_cv_dict, f, indent=4, separators=(',', ': '))
    f.close
    return
