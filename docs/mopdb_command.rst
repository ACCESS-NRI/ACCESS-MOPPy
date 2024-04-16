Using mopdb to create mappings
------------------------------

This utility allows you to create all the configuration files necessary to customise MOPPeR starting from your own model output and mapping information already available in the access.db database.
As the tool can only match pre-defined variables and the named variables in the model output can be defined differently for different model configuration, it is ultimately the user responsability to make sure that the proposed mappings are correct.


Populate database cmorvar table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code::

   mopdb cmor -f

Run recursively over all available CMOR tables if initialising database for first time
NB This should be done before populating mapping!


Populate/update database mapping table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code::

   mopdb map -f master_map.csv -a app4
   mopdb map -f master_map_om2.csv -a app4

If initialising the database for the first start by adding existing old style master files, as shown above.
The `-a/--alias` argument here indicates that these tables were originated for the APP4 tool and they use a different style of mapping file.
The MOPPeR master files have additional fields and some of the old fields were removed.
To add new style master files pass a different `alias`, the value should be related to the configuration used.
The `alias` value is saved in the table and can then later be used to identify the preferred mappings to use.
i.e. we used  aus2200 for mappings related to the AUS2200 configuration:

.. code::

    mopdb map -f master_aus2200.csv -a aus2200

A user that wants to create a mapping table for another AUS2200 simulation can use this value to select appropriate mappings (see how to that below).

Get a list of variables from the model output
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code::

    mopdb varlist -i <output-path> -d <start-date>

this will create for each file output a list of variables with useful attributes
These can be concatenated into one or used to create separate mappings.

.. _varlist example:
.. note:: Example output of varlist
    :class: toggle
   name;cmor_var;units;dimensions;frequency;realm;cell_methods;cmor_table;vtype;size;nsteps;filename;long_name;standard_name
   fld_s00i004;theta;K;time model_theta_level_number lat lon;mon;atmos;area: time: mean;CM2_mon;float32;9400320;12;cw323a.pm;THETA AFTER TIMESTEP;air_potential_temperature
   fld_s00i010;hus;1;time model_theta_level_number lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;9400320;12;cw323a.pm;SPECIFIC HUMIDITY AFTER TIMESTEP;specific_humidity
   fld_s00i024;ts;K;time lat lon;mon;atmos;area: time: mean;CMIP6_Amon;float32;110592;12;cw323a.pm;SURFACE TEMPERATURE AFTER TIMESTEP;surface_temperature
   fld_s00i030;;1;time lat lon;mon;atmos;area: time: mean;;float32;110592;12;cw323a.pm;LAND MASK (No halo) (LAND=TRUE);land_binary_mask
   fld_s00i031;siconca;1;time lat lon;mon;atmos;area: time: mean;CMIP6_SImon;float32;110592;12;cw323a.pm;FRAC OF SEA ICE IN SEA AFTER TSTEP;sea_ice_area_fraction
   ...

Create a mapping file starting from variable list
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code::

    mopdb template  -f <varlist-out> -v <access-version>

This will create a map_<exp>.csv file partly using, if available, information from the mapping table.
As the command name highlights the resulting file is just a template of a working mapping file. The results are divided in sections depending on how reliable the mappings are considered. 
The first group of mappings are usually ready to use as they are perfect matches of `version`, `frequency` and `input-variables`. These records are ready for the post-processing. The second group also matches the three fields above but they are all derive variables. The tool has checked that all inout-variables needed are present. These records should be also ready to be used, but be mindful of potential changes to calculation functions.
The other groups of records require checking, as either the version or the frequency do not match those of the model output, or more than one possible match is listed if record are matched using standard_name. Finally the last group is records for which is wasn't possible to find a mapping.

.. _template example:
.. note:: Example output of template
    :class: toggle
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code::

   mopdb check 

This compares mapping and cmorvar tables from the database to see if all variables in the mapping table are defined in cmor table. 
If a variable is not defined in a cmor table CMOR writing will fail.


Adding new variable definitions to cmor table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the cmor variable table doesn't include a field you want to post-process, you can add a new definition
to an existing table or build a new CMIP style table from scratch.


Then you can load the new table as shown below. If you have modified an existing table only the new records will be added.
w
If a record already exists on the database but has been modified in the file, it will be updated. 

.. code::

    mopdb cmor -f <modified-cmor-table> 


Create a CMOR variable table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can create new CMOR tables to include all the variable definitions not yet present in other CMOR tables. As a variable definition includes all the variable attributes, if any of this is different (i.e. dimensions, frequency cell_methods) etc., you will need a new variable definition.

You can build a new table manually:

.. code::

   { "Header": {},
     "variable_entry": {
      <var1>: {...},
      <var2>: {...},
    }}

If there is an existing CMOR table that be adapted quickly to your model output then copying it and editing it is relatively easy. You should then load, as shown in ... above, the table so new variable definitions are added to the `cmorvar` table.

Or using `mopdb table` subcommand:
.. code:: 

    mopdb table -f <map_file> -a <newtable name>


(TO BE COMPLETED)

Delete records from the database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: 

    mopdb del --dbname test.db -t cmorvar -p out_name amwet -p frequency mon

The `del` sub-command allows to delete one or more records from the selected table. The tool will first select the records matching the constraints pairs passed as input, it will print any matching records and ask the user to confirm if they want to delete them.


Selecting a database
~~~~~~~~~~~~~~~~~~~~

By default if you're using the package installed in the hh5 conda environment, all these commands will use the `access.db` database which comes with the package.
If you want to modify the database you need to get a copy of the official database or defined a new ones as shown above.
Then you use the `--dbname <database-name>` option to select this database.
 
.. warning::
   Any command that writes or updates the database will fail with the default database. This is true regardless if you are a manager that has writing access to the file. The tool will abort the sub-commands `del`, `cmor` and `map` if the default option or the actual path to the default db is passed.
   This is by design so any change to the official database happens under version control.
