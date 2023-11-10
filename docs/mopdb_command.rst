Using mopdb to create mappings
------------------------------

This utility allows you to create all the configuration files necessary to customise MOPPeR starting from your own model output and mapping information already available in the access.db database.
As the tool can only match pre-defined variables and the named variables in the model output can be defined differently for different model configuration, it is ultimately the user responsability to make sure that the proposed mappings are correct.


Populate database cmorvar table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

   python mopdb.py cmor -f

Run recursively over all available CMOR tables if initialising database for first time
NB This should be done before populating mapping!


Populate/update database mapping table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

   python mopdb.py map -f master_map.csv -a app4
   python mopdb.py map -f master_map_om2.csv -a app4

If initialising the database for the first start by adding existing old style master files, as shown above.
The `-a/--alias` argument here indicates that these tables were originated for the APP4 tool and they use a different style of mapping file.
The MOPPeR master files have additional fields and some of the old fields were removed.
To add new style master files pass a different `alias`, the value should be related to the configuration used.
The `alias` value is saved in the table and can then later be used to identify the preferred mappings to use.
i.e. we used  aus2200 for mappings related to the AUS2200 configuration:

.. code:: ipython3
    python mopdb.py map -f master_aus2200.csv -a aus2200

A user that wants to create a mapping table for another AUS2200 simulation can use this value to select appropriate mappings (see how to that below).

Get a list of variables from the model output
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: ipython3

    python mopdb.py varlist -i <output-path> -d <start-date>

this will create for each file output a list of variables with useful attributes
These can be concatenated into one, plus filled in empty fields (as cmip_var names if not yet in database mapping)


Create a mapping file starting from variable list
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: ipython3

    python mopdb.py template  -f <varlist-out> -v <access-version>

This will create a master_<exp>.csv file partly using, if available, info in mapping table.
Again fill in missing information, check calculations are correct
then this is ready to run the post-processing.
(We should add to this a match of the cmip6 table)


Adding new variable definitions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: ipython3

    python mopdb.py cmor -f <fake-cmor-table> 

Similarly if cmor variable doesn't include something you want to define
You can fake a CMIP table to provide completely new variables definitions
{ "Header": {},
  "variable_entry": {
   <var1>: {...},
   <var2>: {...},
}}
and repeat step1)
we've done:
python mopdb.py cmor -f aus2200_variables.json

6) To check which variables aren't yet defined
python mopdb.py check #  currently is comparing mapping and cmorvar not an inpout file
This should be necessary only if variable aren't defined for any frequency realm, if you just want to add exisitng variables but with new frequency/realm combination you can simply create a bespoke table using the "table" command and adding variable from cmorvar table but modifying the frequency/realm fields.
You can then load this table as usual


Create a CMOR variable table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can create new CMOR tables to include all the variable definitions not yet present in other CMOR tables. As a variable definition includes all the variable attributes, if any of this is different (i.e. dimensions, frequency cell_methods) etc., you will need a new variable definition.

There are two ways to approach this: manually and using mopdb with the `table` subcommand.
If there is an existing CMOR table that be adapted quickly to your model output then copying it and editing it is relatively easy. You should then load, as shown in ... above, the table so new variable definitions are added to the `cmorvar` table.

If you 
.. code:: ipython3

    python mopdb.py table -f <master_map> -a <newtable name>



This should always be final steps once you know if you need to add a completely new definition, have updated mapping database and create a master_map for your own simulation.
This will also create the final master_map which includes the name of CMIP table to use for a specific variablesas we cannot imply that from realm and frequency alone

All of these commands will get --dbname or -d <database-name> if you don't want to use default access.db
