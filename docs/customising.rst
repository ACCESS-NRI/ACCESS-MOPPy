Customising 
===========

If using a different model configuration, or if the output contains variables which are not included in the mappings, then a user will have to customise the mappings.
This is because it is not possible to include by default in the database all the possible mappings. For example, the UM model can use the same code for different physical quantities in different model configurations, or some calculations need inputs that might be dependent on the model configuration.

CMOR needs exact definitions for all the variables and coordinates to be written to file. Even the same variable at different frequencies or defined as instantaneous value rather than a mean over the timestep will need a separate definition. 

In this section, we will cover the following use cases:

* New model configuration
* New variable
* New calculation

New model configuration
-----------------------
Unless a user is post-processing output from a model version and configuration which is already mapped in the database, they will have to make some adjustments.
However, the amount of customisation needed to prepare the post-processor mappings will vary depending on how different the configuration is from existing ones.

A user should still select the nearest to their case of the available versions, otherwise by default mopper will use the mappings linked to the ACCESS-CM2 version.

Different grid
--------------
Where possible any hardcoded reference to grid size was removed, grid-dependent information is usually retrieved from the ancillary files. The path to these files can be defined in the main configuration yaml file. Potentially there might be calculations or mappings with grid-dependent input arguments, we will add these requirements in the relevant documentation section.

Different stash version
-----------------------
If the UM stash version used is different from the stash versions provided, some of the mappings listed in the template could be wrong. We added "standard-name" and/or "long-name" from the original files to the mappings to make spotting these mismatches easier. 

Potentially, this could also happen with different land or ocean model versions, but it's less likely as they use variable names rather than codes.
In both cases the template should be fixed manually.

Different output selection  
--------------------------
Even if the UM codes represent the same variables there could be a mismatch in the original frequency and the frequency of the mappings available in the database mappings. 
These variables will be listed in a section labelled:

.. code-block:: bash

   # Variables with different frequency: Use with caution!

The frequency listed in the mapping template is taken directly from the file, make sure it matches the one of the listed cmor table. If they differ the `cmor table` should be updated with one that has a definition with the correct frequency.

.. warning:: 
   Pay attention if the variable is an instantaneous value or not (i.e. time: point vs mean, sum, max etc in the cell_methods).    This should match the frequency in the cmor table definition.
   If it doesn't a new variable with the correct frequency, cell_methods and time _axis should be defined, see below.

A similar message precedes all the variables mapped from a different version from the selected (or default) one: 

.. code-block::

   # Variables definitions coming from different model version: Use with caution!

.. _custom-variables:

New variable
------------
If a desired variable is not yet defined in a CMOR table:

1) Add it to a new CMOR table.
2) Create a mapping for the variable that you can add directly to your mapping template and/or later load it to the database `mapping` table.
3) Load (or reload in case of an existing table) the table to the `cmor` database table.
4) Load your edited template to the database `mapping` table, so that it can be used.

For both steps existing identical records will be overwritten. Once new records are added, they can be used by the `varlist` and `template` tasks. 

While it's possible to modify an existing CMOR table, it's probably better to do so only for a custom table as CMIP6 has strict standards and it's important to keep these tables the same as their official version. Other things to pay attention to are:

 * Check that the table to modify doesn't contain a conflicting variable, for example a variable that uses the same output name. In most cases the output name and the variable name used as key for the record are the same. However, the key name is what is used to point to the correct variable definition in the mapping table and can be different from the output name. This allows two variables with the same output name to be part of the same cmor table.
 * When adding a variable, a sub-hourly frequency the frequency in the table should be `subhr` or most often `subhrPt`. This is because of the way CMOR3 is structured it only accept a defined set of frequencies. As mopper uses the frequencies to estimate the file sizes, if working with sub-hourly data you need to then specify what the exact interval is in the configuration file using the `subhr` field and `min` as units. 

.. warning:: 
   Make sure to use wherever possible existing coordinates (see ACDD_coordinate.json) as dimensions for the variable, if this is not possible then define a new coordinate in ACDD_coordinate.json. NB you can only use one coordinate file, so all the coordinates' definitions need to be in the same file! The same applies to grids (see ACDD_grids.json) and formula terms (ACDD_formula_terms.json).
 
New calculation 
---------------
There are two ways to define a new calculation for a derived variable. Which one to use depends on how complex the calculation is.

As an example, let's look at surface soil moisture for AUS2200:

.. code-block:: bash

   mrsos;fld_s08i223;var[0].isel(depth=0)

With this configuration the topsoil level fits exactly the definition of surface soil moisture, so all is needed is selecting the top level from the input variable using `xarray isel`. This expression will be evaluated when the post-processing is run and it's simple enough to be added directly in the calculation field.

If the calculation to be executed is more complex, then a new function should be added to the *src/mopper/calculation.py* file, and then the calculation field in the mapping should be updated to call the function with the right inputs.

Here we're showing how the pressure level calculation is defined for air temperature:

.. code-block:: bash

   ua24;fld_s00i002 fld_s00i407;plevinterp(var[0], var[1], 24)

For context this is the function definition:

.. code-block:: python

   def plevinterp(ctx, var, pmod, levnum):

where `ctx` is the `context` of the specific file including information on the original variable. This is automatically passed to the function and should not be included in the function call.

.. note::

   We are planning to provide a simplified way to introduce new calculations and to update the central database with user provided mappings and variable definitions. For the moment open a `new issue on github <https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR/issues/new>`_ so we can review the updates and add them to the official version.
 
