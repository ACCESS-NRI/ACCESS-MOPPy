Understanding the CMOR3 structure 
=================================

CMOR writes netcdf files to disk following a strict set of rules.
Some of these are hardcoded in the code itself but the majority of requirements are defined by a Controlled Vocabulary (CV) file. As CMOR was developed for CMIP6 the first available CV file was the CMIP6_CV.json, other CV files are now afvailable for other projects.
A CV file is composed of two parts:
* a list of required attributes
* controlled vocabularies to define valid values for the attributes (optional)

Not all the attributes needs to have pre-defined values, this depends on the conventions to apply.

.. note::
    A generic CV file `ACDD_CV.json` with a minimum number of required attributes based on the `ACDD conventions <https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-3>`_. The ACDD is used with CF conventions by NCI when publishing data.

The CV file defines the conventions to use, the `CMOR tables` (also json files) are used to define the variables to produce.
Finally, the `experiment` configuration file lists the required and extra attributes defined for a simulation, these will be used to create the netcdf global attributes. 

CMOR tables
+++++++++++
The CMOR tables are lists of variables definitions including their names and attributes. For CMIP6 each table is a combination of realm, frequency and vertical levels. i.e. Omon is ocean monthly data, 6hrPtlev are 6 hourly data on pressure levels etc.
Each variable as a specific cmor-name which is the key to the definition in the json file, this can be different from the actual variable name used in the file. In this way it is possible to define, for example, two tas variable with different frequency in the same table.
This cmor-name is what is used in the mappings between model output and cmor variables.

A CMOR table is a json file with two main keys: 
 * header
 * variable_entry

The last has sub-dictionaries each representing a variable with cmor-name as key and a dictionary of values representing the variable attributes.

.. literalinclude:: table_example.json
  :language: JSON

Definitions of coordinates, grids and formula terms are stored in separate tables. See:
 * `CMIP6_grids.json <https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_grids.json>`_
 * `CMIP6_coordinate.json <https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_coordinate.json>`_
 * `CMIP6_formula_terms.json <https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_formula_terms.json>`_


The original CMIP6 tables are included in this repository in 
 `/data/cmip6-cmor-tables/Tables/`.
A separate folder 
 `/data/custom-cmor-table/` 
includes all the CMOR tables plus other custom defined tables. So far we added custom tables for the AUS2200 amip runs configuration. This was necessary as the AUS2200 has a lot of output at higher frequencies and variables which aren't covered by the original tables. Similarly a user can define new tables if they want to post-process variables not yet incuded or if they want to adapt some of the available variable definitions.

Experiment configuration file
+++++++++++++++++++++++++++++
It also includes few special attributes:
 * coordinate
 * CV
 * ..

The `mop setup` command will create a configuration file as expected by CMOR based on the main configuration file passed by the user. This is described in the relevant section.

Troubleshooting
+++++++++++++++

While we took as much care as possible to get our tool to create CMOR compliant tables and configuration files, if required attributes are removed this can create segmentation faults in the cmor code which go undetected. If this happens your code might hang or crush without an error message. This is as because there's no way to propogate the error from the CMOR C program to the python interface. 

.. warning::
  If you get the following warning in your cmor_log:
  Warning: while closing variable 0 (htovgyre, table Omon)
  ! we noticed you wrote 0 time steps for the variable,
  ! but its time axis 0 (time) has 2 time steps
  It can usually be safely ignored, see the `relevant github issue <https://github.com/PCMDI/cmor/issues/697>`_

