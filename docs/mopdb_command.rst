Variable mappings: mopdb
========================

The `mopdb` command allows to create all the configuration files necessary to customise MOPPeR starting from the actual model output and the mapping information already available in the access.db database.
As mopdb can only match predefined variables and the named variables in the model output can be defined differently for different model configuration, it is ultimately the user responsibility to make sure that the proposed mappings are correct.


Populate database cmorvar table
-------------------------------

.. code::

   mopdb cmor -f

Run recursively over all available CMOR tables if initialising database for first time.

NB This should be done before populating mapping!


Populate/update database mapping table
--------------------------------------

.. code::

   mopdb map -f map_ocean.csv
   mopdb map -f master_map_om2.csv -a app4

If initialising the database for the first time, start by adding existing mappings files as shown above. The mappings files we used for our database are available in the repository `mappings` folder.

The `-a/--alias` argument in the second example "app4" indicates that these tables were originated for the APP4 tool and they use a different style of mapping file.
To add the current style of mapping files you can omit the `alias`, as in the first example, or pass a different `alias`.
If omitted the tool will use the file name as alias.
The `alias` value is saved in the table and can then later be used to identify the preferred mappings to use.
e.g. use aus2200 for mappings related to the AUS2200 configuration:

.. code::

    mopdb map -f master_aus2200.csv -a aus2200

A user that wants to create a mapping table for another AUS2200 simulation can use this value to select appropriate mappings (see how to do that below).

Get a list of variables from the model output
---------------------------------------------
.. code::

    mopdb varlist -i <output-path> -d <start-date>

this will create for each output file a list of variables with useful attributes
These can be concatenated into one or used to create separate mappings.

.. _varlist example:
.. dropdown:: Example output of varlist
   name;cmor_var;units;dimensions;frequency;realm;cell_methods;cmor_table;vtype;size;nsteps;filename;long_name;standard_name
   fld_s00i004;theta;K;time model_theta_level_number lat lon;mon;atmos;area: time: mean;CM2_mon;float32;9400320;12;cw323a.pm;THETA AFTER TIMESTEP;air_potential_temperature
   fld_s00i010;hus;1;time model_theta_level_number lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;9400320;12;cw323a.pm;SPECIFIC HUMIDITY AFTER TIMESTEP;specific_humidity
   fld_s00i024;ts;K;time lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;110592;12;cw323a.pm;SURFACE TEMPERATURE AFTER TIMESTEP;surface_temperature
   fld_s00i030;;1;time lat lon;mon;atmos;area: time: mean;;float32;110592;12;cw323a.pm;LAND MASK (No halo) (LAND=TRUE);land_binary_mask
   fld_s00i031;siconca;1;time lat lon;mon;atmos;area: time: mean;CMIP6_SImon;float32;110592;12;cw323a.pm;FRAC OF SEA ICE IN SEA AFTER TSTEP;sea_ice_area_fraction
   ...

Create a mapping file starting from variable list
-------------------------------------------------
.. code::

    mopdb template  -f <varlist-out> -v <access-version>

This will create a map_<exp>.csv file partly using, if available, information from the mapping table.
As the command name highlights the resulting file is just a template of a working mapping file. The results are divided in sections depending on how reliable the mappings are considered. 
The first group of mappings are usually ready to use as they are perfect matches of `version`, `frequency` and `input-variables`. These records are ready for the post-processing. The second group also matches the three fields above, but they are all derived variables. For these mopdb will check that all the necessary input-variables are present. These records should be also ready to be used but be mindful of potential changes to calculation functions.
The other groups of records require checking, as either the version or the frequency do not match those of the model output, or more than one possible match is listed if records are matched using their standard_name. Finally, the last group is records for which wasn't possible to find a mapping.

.. _template example:
.. dropdown:: Example output of template
   cmor_var;input_vars;calculation;units;dimensions;frequency;realm;cell_methods;positive;cmor_table;version;vtype;size;nsteps;filename;long_name;standard_name
   agesno;fld_s03i832;;day;time pseudo_level_1 lat lon;mon;landIce land;area: time: mean;;CMIP6_LImon;CM2;float32;1880064;12;cw323a.pm;CABLE SNOW AGE ON TILES;age_of_surface_snow
   amdry;fld_s30i403;;kg m-2;time lat lon;mon;atmos;area: time: mean;;CM2_mon;CM2;float32;110592;12;cw323a.pm;TOTAL COLUMN DRY MASS  RHO GRID;
   amwet;fld_s30i404;;kg m-2;time lat lon;mon;atmos;area: time: mean;;CM2_mon;CM2;float32;110592;12;cw323a.pm;TOTAL COLUMN WET MASS  RHO GRID;atmosphere_mass_per_unit_area
   ci;fld_s05i269;;1;time lat lon;mon;atmos;area: time: mean;;CMIP6_Amon;CM2;float32;110592;12;cw323a.pm;deep convection indicator;
   ...
   # Derived variables with matching version and frequency: Use with caution!;;;;;;;;;;;;;;;;
   baresoilFrac;fld_s03i317 fld_s03i395;extract_tilefrac(var[0],14,landfrac=var[1]);1;time pseudo_level_1 lat lon;mon;land;area: time: mean;;CMIP6_Lmon;CM2;float32;1880064;12;cw323a.pm;SURFACE TILE FRACTIONS;
   c3PftFrac;fld_s03i317 fld_s03i395;extract_tilefrac(var[0],[1,2,3,4,5,6,8,9,11],landfrac=var[1]);1;time pseudo_level_1 lat lon;mon;land;area: time: mean;;CMIP6_Lmon;CM2;float32;1880064;12;cw323a.pm;SURFACE TILE FRACTIONS; 
   # Variables definitions coming from different version;;;;;;;;;;;;;;;;
   rlntds;fld_s02i203;;W m-2;time lat lon;mon;ocean;area: time: mean; time: mean;;CMIP6_Omon;float32;110592;12;cw323a.pm;NET DN LW RAD FLUX:OPEN SEA:SEA MEAN;surface_net_downward_longwave_flux
   rssntds;fld_s01i203;;W m-2;time lat lon;mon;ocean;area: time: mean; time: mean;;CM2_mon;float32;110592;12;cw323a.pm;NET DN SW RAD FLUX:OPEN SEA:SEA MEAN;surface_net_downward_shortwave_flux
   # Variables with different frequency: Use with caution!;;;;;;;;;;;;;;;;
   rlntds;fld_s02i203;;W m-2;time lat lon;mon;ocean;area: time: mean; time: mean;;CMIP6_Omon;float32;110592;12;cw323a.pm;NET DN LW RAD FLUX:OPEN SEA:SEA MEAN;surface_net_downward_longwave_flux
   rssntds;fld_s01i203;;W m-2;time lat lon;mon;ocean;area: time: mean; time: mean;;CM2_mon;float32;110592;12;cw323a.pm;NET DN SW RAD FLUX:OPEN SEA:SEA MEAN;surface_net_downward_shortwave_flux
   # Variables matched using standard_name: Use with caution!;;;;;;;;;;;;;;;;
   ['huss-CMIP6_3hr', 'hus-CMIP6_6hrLev', 'hus4-CMIP6_6hrPlev', 'hus27-CMIP6_6hrPlevPt', 'hus7h-CMIP6_6hrPlevPt', 'huss-CMIP6_6hrPlevPt', 'hus-CMIP6_Amon', 'huss-CMIP6_Amon', 'hus-CMIP6_CFday', 'hus-CMIP6_CFmon', 'hus-CMIP6_CFsubhr', 'huss-CMIP6_CFsubhr', 'hus-CMIP6_day', 'huss-CMIP6_day', 'hus-CMIP6_E3hrPt', 'hus7h-CMIP6_E3hrPt', 'hus-CMIP6_Eday', 'hus850-CMIP6_Eday', 'hus-CMIP6_EdayZ', 'hus-CMIP6_Emon', 'hus27-CMIP6_Emon', 'hussLut-CMIP6_Emon', 'hus-CMIP6_Esubhr', 'huss-CMIP6_Esubhr', 'huss-AUS2200_A10min', 'hus-AUS2200_A1hr', 'huss-AUS2200_A1hr', 'hus24-AUS2200_A1hrPlev', 'hus3-AUS2200_A1hrPlev'];;;1;time model_theta_level_number lat lon;mon;;area: time: mean;;CMIP6_Amon;;float32;9400320;12;cw323a.pm;SPECIFIC HUMIDITY AFTER TIMESTEP;specific_humidity 
   ...
   # Derived variables: Use with caution!;;;;;;;;;;;;;;;;
   hus24;fld_s00i010 fld_s00i408;plevinterp(var[0], var[1], 24);1;time model_theta_level_number lat lon;mon;atmos;area: time: mean;;AUS2200_A1hrPlev;AUS2200;float32;9400320;12;cw323a.pm;SPECIFIC HUMIDITY AFTER TIMESTEP;specific_humidity
   sifllatstop;fld_s03i234 fld_s00i031;maskSeaIce(var[0],var[1]);1;time lat lon;mon;seaIce;area: time: mean;up;AUS2200_A1hr;AUS2200;float32;110592;12;cw323a.pm;FRAC OF SEA ICE IN SEA AFTER TSTEP;sea_ice_area_fraction
   theta24;fld_s00i004 fld_s00i408;plevinterp(var[0], var[1], 24);K;time model_theta_level_number lat lon;mon;atmos;area: time: mean;;AUS2200_A1hrPlev;AUS2200;float32;9400320;12;cw323a.pm;THETA AFTER TIMESTEP;air_potential_temperature
   # Variables without mapping;;;;;;;;;;;;;;;;
   fld_s00i211;;;1;time model_theta_level_number lat lon;mon;;area: time: mean;;;;float32;9400320;12;cw323a.pm;Convective cloud amount with anvil;
   fld_s00i253;;;;time model_rho_level_number lat lon;mon;;area: time: mean;;;;float32;9400320;12;cw323a.pm;DENSITY*R*R AFTER TIMESTEP;
   fld_s00i413;;;1;time pseudo_level lat lon;mon;;area: time: mean;;;;float32;552960;12;cw323a.pm;Sea ice concentration by categories;
   ...


Check which variables aren't yet defined
----------------------------------------
.. code::

   mopdb check 

This compares mapping and cmorvar tables from the database to see if all variables in the mapping table are defined in the cmorvar table. 

If a variable is not defined in a cmor table, CMOR writing will fail!


Adding new variable definitions to cmor table
---------------------------------------------

If the cmor variable table doesn't include a field you want to post-process, you can add a new definition to an existing custom table or build a new CMIP style table from scratch.

Then you can load the new table as shown below. If you have modified an existing table new records will be added and existing ones will be updated. This helps keeping the content of cmovar database table consistent with the cmor tables.

.. code::

    mopdb cmor -f <modified-cmor-table> 


Create a CMOR variable table
----------------------------
Anyone can create new CMOR tables to include all the variable definitions not yet present in other CMOR tables. As a variable definition includes all the variable attributes, if any of them is different (i.e. dimensions, frequency cell_methods) etc., a new variable definition is needed.

A new table can be built manually:

.. code::

   { "Header": {},
     "variable_entry": {
      <var1>: {...},
      <var2>: {...},
    }}

If there is an existing CMOR table that be adapted quickly to your model output then copying it and editing it is relatively easy. 

Or using `mopdb table` subcommand:
.. code:: 

    mopdb table -f <map_file> -a <newtable name>

The new table should then be loaded as shown above to the database.

Delete records from the database
--------------------------------

.. code:: 

    mopdb del --dbname test.db -t cmorvar -p out_name amwet -p frequency mon

The `del` sub-command allows to delete one or more records from the selected table. First, the records matching the constraints pairs passed as input are selected and the result printed to screen. The user will then be prompted to confirm the delete operation.


Selecting a database
--------------------

By default, if using the package installed in the hh5 conda environment, mopdb will use the `access.db` database which comes with the package.
If a user wants to modify the database, they will need to get a copy of the official database or define a new one from scratch as shown above.
Then the `--dbname <database-name>` option ican be used to select the custom database.
 
.. warning::
   Any command that writes or updates the database will fail with the default database. This is true regardless of the user having writing access to the file. The tool will abort the sub-commands `del`, `cmor` and `map` if the default option or the actual path to the default database is passed.
   This is by design so any change to the official database happens under version control.
