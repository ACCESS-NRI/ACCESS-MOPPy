Post-processing ACCESS model output with MOPPeR
===============================================


Subtitle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CMOR write netcdf files to disk following a strict set of rules.
Some of these are hardcoded in the code itself but the majority of requirements are defined by a Controlled Vocabulary file. As CMOR was developed for CMIP6 the first CV file is the CMIP6_CV.json file available from ...
A CV file is composed of two parts:
* a list of required attributes
* controlled vocabularies to define valid values for the attributes (optional)

Not all the attributes needs to have pre-defined values, this depends on the conventions to apply.

The CV file defines the conventions to use, the `CMOR tables` (also json files) are used to define the variables to produce.
Finally, the `experiment` configuration file lists the required and extra attributes defined for a simulation, these will be used to create the netcdf global attributes. 

CMOR tables
+++++++++++
The CMOR tables are lists of variables definitions including their names and attributes. For CMIP6 each table is a combination of realm, frequency and vertical levels. i.e. Omon is ocean monthly data, 6hrPtlev are 6 hourly data on pressure levels etc.
Each variable as a specific cmor-name which is the ey to the definition in the json file, this can be different form the actual output name to use in the file. In this way it is possible to define, for example, two tas variable with different frequency in the same table.
This cmor-name is what is used in the mappings between model output and cmor variables.

A CMOR table is a json file with two main keys: 
* header
* variable.. this as sub-dictionaries each representing a variable with cmor-name as key and a dictionary of values representing the variable attributes.

Special tables are the ones for coordinates and grids


The original CMIP6 tables are included in this tool in data/cmip6-cmor-tables/Tables.
A separate folder data/custom-cmor-table/.. includes all the CMOR tables plus other custom defined tables. So far we added custom tables for the AUS2200 amip runs configuration. This was necessary as the AUS2200 has a lot of output at higher frequencies and variables which aren't covered by the original tables. Similarly a user can defined new tables if they want to post-process variables not yet incuded or if they want to adapt some of the available variable defintions.

Experiment configuration file
+++++++++++++++++++++++++++++
It also includes few special attributes:
* coordinate
* CV
* ..

The ACCESS-MOPPER wrapper will create a configuration file as expected by CMOR based on the main configuration file passed by the user. This is described in the getting started section.

Important
+++++++++
While to took as much care as possible to get our tool to create CMOR compliant tables and configuration files. If required attributes are removed this can create segmentation faults in the cmor code which go undetected....
