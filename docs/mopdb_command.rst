Variable mappings: mopdb
========================

The `mopdb` command allows to create all the configuration files necessary to customise MOPPeR starting from the actual model output and the mapping information already available in the access.db database.
As `mopdb` can only match predefined variables and the named variables in the model output can be defined differently for different model configuration, it is ultimately the user responsibility to make sure that the proposed mappings are correct.

Overview
--------

This module is used to manage the mapping of raw output to CMIP style variables.

- **varlist**  creates an initial list of variables and attributes based on actual files
- **template** uses the above list to generate a template of mappings to use in the processing
- **intake**   uses the mappings to create an intake catalogue of the raw model output
- **cmor**     populates the database cmor variables table
- **map**      populates the database mappings table
- **check**    checks a variable list against the cmor database table to individuate variables without a definition
- **table**    creates a CMOR style table based on a variable list
- **del**      selects and removes records from database tables based on constraints passed as input

Populate database cmorvar table
-------------------------------

.. code-block::

   mopdb cmor -f

Run recursively over all available CMOR tables if initialising database for first time.

NB This should be done before populating mapping!


Populate/update database mapping table
--------------------------------------

.. code-block:: console

   $ mopdb map --dbname access.db -f map_ocean_OM2.csv
   Opened database access.db successfully
   Updating db ...
   Rows modified: 81
   --- Done ---

If initialising the database for the first time, start by adding existing mappings files as shown above. The mappings files we used for our database are available in the repository `mappings` folder.

The `-a/--alias` argument in the second example "app4" indicates that these tables were originated for the APP4 tool, and they use a different style of mapping file.
To add the current style of mapping files you can omit the `alias`, as in the first example, or pass a different `alias`.
If omitted the tool will use the file name as alias.
The `alias` value is saved in the table and can then later be used to identify the preferred mappings to use.
e.g. use aus2200 for mappings related to the AUS2200 configuration:

.. code-block::

    mopdb map -f master_aus2200.csv -a aus2200

A user that wants to create a mapping table for another AUS2200 simulation can use this value to select appropriate mappings (see how to do that below).

Create a mapping file
---------------------

This can be done by providing the model output path or directly a varlist file

From output path:
  
.. code-block::

    mopdb template  -f <output-path> -v <access-version>

From varlist file:

.. code-block::

    mopdb template  -f <varlist-out> -v <access-version>

This will create a map_<exp>.csv file using, if available, information from the mapping table.
As the command name highlights the resulting file is just a template of a working mapping file. The results are divided in sections depending on how reliable the mappings are considered. 

The first group of mappings are usually ready to use as they are perfect matches of `version`, `frequency` and `input-variables`. These records are ready for the post-processing. The second group also matches the three fields above, but they are all derived variables. For these `mopdb` will check that all the necessary input-variables are present. These records should be also ready to be used but be mindful of potential changes to calculation functions.

The other groups of records require checking, as either the version or the frequency do not match those of the model output, or more than one possible match is listed if records are matched using their standard_name. Finally, the last group is records for which wasn't possible to find a mapping.

.. _template example:
.. dropdown:: Example output of template

   .. literalinclude:: map_ex1.csv
      :language: csv



Create an intake catalogue
--------------------------

This represents an extra step on top of the mapping, so it can be start directly from an existing mapping or from scratch by providing the model output path.

From output path:
  
.. code-block::

    mopdb intake  -f <output-path> -v <access-version> { -a <alias> }

From varlist file:

.. code-block::

    mopdb intake  -f <output-path> -fl <varlist-out> -v <access-version> { -a <alias> }

From mapping file:

.. code-block::

    mopdb intake  -f <output-path> -fl <mapping-out> -v <access-version> { -a <alias> }

NB the model output path is still needed even when passing an existing mapping or variable list.
 
`intake` will generate:
* intake_<alias>.yaml - the main intake catalogue;
* intake_<alias>.json - the intake-esm catalogue;
* catalogue.csv.xz - a csv file containing a list of the assets.

The esm-catalogue is a multi-variable catalogue, which means that each file can have more than one variable as it is usual for raw model output. While each file contains a lot of variables, a user can select just one or few and only these will be loaded as an xarray dataset. This is helpful with the UM output where variables with different dimensions can co-exist in a file. In such case, it's necessary to use preprocess to select variables with consistent dimensions to avoid concatenation issues. As this is the standard behaviour for multi-variable intake-esm catalogues, the user doesn't need to worry about it.

The esm-intake catalogue also lists separately each variable that can be mapped to a cmor name and/or standard_name. This allows to use the cmor names and/or the standard_names more effectively to query the data.  

Get a list of variables from the model output
---------------------------------------------
.. code-block::

    mopdb varlist -f <output-path> 

this will create a list of variables with useful attributes

.. _varlist example:
.. dropdown:: Example output of varlist

   name;cmor_var;units;dimensions;frequency;realm;cell_methods;cmor_table;vtype;size;nsteps;filename;long_name;standard_name
   #cw323a.pm
   fld_s00i004;theta;K;time model_theta_level_number lat lon;mon;atmos;area: time: mean;CM2_mon;float32;9400320;12;cw323a.pm;THETA AFTER TIMESTEP;air_potential_temperature
   fld_s00i010;hus;1;time model_theta_level_number lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;9400320;12;cw323a.pm;SPECIFIC HUMIDITY AFTER TIMESTEP;specific_humidity
   fld_s00i024;ts;K;time lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;110592;12;cw323a.pm;SURFACE TEMPERATURE AFTER TIMESTEP;surface_temperature
   fld_s00i030;;1;time lat lon;mon;atmos;area: time: mean;;float32;110592;12;cw323a.pm;LAND MASK (No halo) (LAND=TRUE);land_binary_mask
   fld_s00i031;siconca;1;time lat lon;mon;atmos;area: time: mean;CMIP6_SImon;float32;110592;12;cw323a.pm;FRAC OF SEA ICE IN SEA AFTER TSTEP;sea_ice_area_fraction
   ...

Doing this step separately can be useful if the model output is using a random directory structure, as it's more likely in such a case that important attributes like frequency and realm which are used for the mapping might be incorrect or missing. In such a case it might be more efficient processing different kind of files separately first, making sure frequency and realm are correct and then combining them into one file to pass to template.
The template command will stop execution if detects potentially wrong values for these fields and save 

Check which variables aren't yet defined
----------------------------------------
.. code-block:: console

   $ mopdb check
   Opened database ~/.local/lib/python3.10/site-packages/data/access.db successfully
   Variables not yet defined in cmorvar table:
   husuvgrid
   rho
   rinum
   hfsifrazil3d

This compares mapping and cmorvar tables from the database to see if all variables in the mapping table are defined in the cmorvar table. 

If a variable is not defined in a cmor table, CMOR writing will fail!


Adding new variable definitions to cmor table
---------------------------------------------

If the cmor variable table doesn't include a field you want to post-process, you can add a new definition to an existing custom table or build a new CMIP style table from scratch.

Then you can load the new table as shown below. If you have modified an existing table new records will be added, and existing ones will be updated. This helps keeping the content of cmovar database table consistent with the cmor tables.

.. code-block:: console

    mopdb cmor -f <modified-cmor-table> 


Create a CMOR variable table
----------------------------
Anyone can create new CMOR tables to include all the variable definitions not yet present in other CMOR tables. As a variable definition includes all the variable attributes, if any of them is different (i.e. dimensions, frequency cell_methods) etc., a new variable definition is needed.

A new table can be built manually:

.. code-block::

   { "Header": {},
     "variable_entry": {
      <var1>: {...},
      <var2>: {...},
    }}

If there is an existing CMOR table that be adapted quickly to your model output then copying it and editing it is relatively easy. 

Or using `mopdb table` subcommand:
.. code-block:: 

    mopdb table -f <map_file> -a <newtable name>

The new table should then be loaded as shown above to the database.

Delete records from the database
--------------------------------

.. code-block:: 

    mopdb del --dbname test.db -t cmorvar -p out_name amwet -p frequency mon

The `del` sub-command allows to delete one or more records from the selected table. First, the records matching the constraints pairs passed as input are selected and the result printed to screen. The user will then be prompted to confirm the delete operation.


Selecting a database
--------------------

By default mopdb will use the `access.db` database which comes with the installed package.
If a user wants to modify the database, they will need to get a copy of the official database or define a new one from scratch as shown above.
Then the `--dbname <database-name>` option can be used to select the custom database.
 
.. warning::
   Any command that writes or updates the database will fail with the default database. This is true regardless of the user having writing access to the file. The tool will abort the sub-commands `del`, `cmor` and `map` if the default option or the actual path to the default database is passed.
   This is by design so any change to the official database happens under version control.
