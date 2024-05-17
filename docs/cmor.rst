Understanding how CMOR3 works 
=============================

`CMOR3 <https://cmor.llnl.gov>`_ writes netcdf files to disk following a strict set of rules.
Some of these are hardcoded in the code itself but most requirements are defined by a Controlled Vocabulary (CV) file. As CMOR was developed for CMIP6 the first available CV file was the CMIP6_CV.json, other CV files are now available based on other projects.

A CV file is composed of two parts:
* a list of required attributes
* controlled vocabularies to define valid values for the attributes (optional)

Not all the attributes need to have pre-defined values, this depends on the conventions to apply.

.. note::
    A generic CV file `ACDD_CV.json` with a minimum number of required attributes based on the `ACDD conventions <https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-3>`_. The ACDD is used with CF conventions by NCI when publishing data.

The CV file defines the conventions to use, the `CMOR tables` (also json files) are used to define the variables to produce.
Finally, the `experiment` configuration file lists the required and extra attributes defined for a simulation; these will be used to create the netcdf global attributes. 

CMOR tables
+++++++++++
The CMOR tables are lists of variables definitions including their names and attributes. For CMIP6 each table is a combination of realm, frequency and vertical levels. For example, Omon is ocean monthly data, 6hrPtlev are 6 hourly data on pressure levels etc.
Each variable as a specific cmor-name which is the key to the definition in the json file, this can be different from the actual variable name used in the file. In this way it is possible to define, for example, two `tas` variables with different frequency in the same table.
The cmor-name and cmor-table are the fields used in the mappings table to identify which variable definition to apply.

A CMOR table is provided as a json file with two main keys: 
 * header
 * variable_entry

The `variable_entry` is a list of dictionaries each representing a variable with cmor-name as key and a dictionary of values to represent the variable attributes.

.. literalinclude:: table_example.json
  :language: json

Definitions of coordinates, grids and formula terms are stored in separate tables. See:
 * `CMIP6_grids.json <https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_grids.json>`_
 * `CMIP6_coordinate.json <https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_coordinate.json>`_
 * `CMIP6_formula_terms.json <https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_formula_terms.json>`_


We included all the original CMIP6 tables and a few custom ones in the repository data in `src/data/cmor_tables/`.
There are custom tables for CM2 variables not yet included in the CMIP6 tables and tables for the AUS2200 AMIP runs configurations. The AUS2200 has a lot of output at higher frequencies and variables which aren't covered by the original tables. Similarly, a user can define new tables if they want to post-process variables not yet included or if they want to adapt some of the available variable definitions. See :ref:`custom-variables` for more information.


Experiment input file
+++++++++++++++++++++++++++++
This provides user-supplied metadata and configuration directives used by CMOR, in cluding which controlled vocabulary (CV), grids and coordinate definitions to use and values for the attributes describing the model and simulation.

We simplified this process so the user only has to pass one configuration file to control all the necessary inputs.
The `mop setup` command will then create an experiment file as expected by CMOR based on this and the selected CV file. This is described in the relevant section.


.. literalinclude:: experiment_input.json
  :language: JSON

Troubleshooting
+++++++++++++++

CMOR can fail with a segmentation fault error because a required attribute is missing. This can be hard to diagnose as your job might hang or crush without an error message.
We took as much care as possible so that `mopper` would create CMOR compliant tables and configuration files, however we cannot fix this issue currently as there's no way to propagate the error from the CMOR C program to the python interface. 

.. warning::
  If you get the following warning in your cmor_log:
  Warning: while closing variable 0 (htovgyre, table Omon)
  ! we noticed you wrote 0 time steps for the variable,
  ! but its time axis 0 (time) has 2 time steps
  It can usually be safely ignored, see the `relevant github issue <https://github.com/PCMDI/cmor/issues/697>`_

**AUS2200 version**

The AUS2200 configuration outputs some variables at 10 minutes frequency. To limit the amount of storage needed for these, the 4D variables were saved on only one model level (or as a reduction over all levels). Consequently, most of the 10 minutes variables are using the original 4D variable UM codes but are representing a different physical quantity. 

While we have created correct mappings for these variables at all different frequencies available, `mopdb template` output will match some of them to both the correct and an incorrect mapping, as the tool can't distinguish between different uses of a UM code in the same version.
It's up to the user to check for duplicates and select the relevant one.
