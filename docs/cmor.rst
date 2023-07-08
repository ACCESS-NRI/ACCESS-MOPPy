Post-processing ACCESS model output with MOPPeR
===============================================


Subtitle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CMOR write netcdf files to disk following a strict set of rules.
Some of these are hardcoded in the code itself but mostly the requireemtns are defined by a Controlled Vocabulary file. As CMOR was developed for CMIP6 the first CV file is the CMIP6_CV.json file available from ...
A CV file is basically composed of two parts:
* a list of required attributes
* controlled vocabularies to define valid values for the attributes (optional)

Not all the attributes needs to have pre-defined values, this depends on the conventions to apply.

If the CV file defined the conventions to use other json files are used to define the specific simulation (usually refer to as `experiment`) and the variables to produced (CMOR tables).
The `experiment` file lists the required and extra attributes defined for a simulation and that will be used to create the netcdf global attributes. It also includes few special attributes:
* coordinate
* CV
* ..
The CMOR tables are lists of variables definitions including their names and attributes. For CMIP6 each table is a combination of realm, frequency and vertical levels. i.e. Omon is ocean monthly data, 6hrPtlev are 6 hourly data on pressure levels etc.
Each variable as a specific cmor-name which is the ey to the definition in the json file, this can be different form the actual output name to use in the file. In this way it is possible to define, for example, two tas variable with different frequency in the same table.
This cmor-name is what is used in the mappings between model output and cmor variables.

A CMOR table is a json file with tow main keys: 
* header
* variable.. this as sub-dictionaries each representing a variable with cmor-name as key and a dictionary of values representing the variable attributes.

Special tables are the ones for coordinates and grids


The original CMIP6 tables are included in this tool in data/cmip6-cmor-tables/Tables.
A separate folder data/custom-cmor-table/Tables/.. includes all the CMOR tables plus other custom defined tables. So far we added custom tables for the AUS2200 amip runs configuration. This was necessary as the AUS2200 has a lot of output at higher frequencies and variables which aren't covered by the original tables. Similarly a user can defined new tables if they want to post-process variables not yet incuded or if they want to adapt some of the available variable defintions.

