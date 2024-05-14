Customising 
===========

Depending on the model configuration you used, the variables you output and the derived variables you want to calculate, you might have to customise the tool to produce the desired output.
This is because it is not possible to include by default in the tool database all the possible mappings. For example, the UM model can use the same code for different quantities in different model configurations, or some calculations might need to have inputs values that need to be customised. Also CMOR needs all the variables and coordinates to be written to be defined in json tables. So any new variable needs to be defined to an exsisting or new table.
In this section, we will cover the following use cases:

* New model configuration
* New variable
* New calculation

New model configuration
-----------------------
Strictly speaking, unless you are using exactly the same version and output of a configuration already mapped in the database this case will apply to you.
However, the amount of customisation needed to prepare the post-processor mappings will vary depending on how different your configuration is.
You should still select the nearest to your case of the available versions, otherwise by default the tool will use the mappings linked to the ACCESS-CM2 version.

Different grid
~~~~~~~~~~~~~~
We removed any hardcoded reference to grid size, in some functions where this was more difficult we move the grid-dependent information to input arguments. So pontetially these are the only changes you might need to do in the mapping template. Refer to the calculation section to see a list of functions which have grid-dependent input arguments.

Different stash version
~~~~~~~~~~~~~~~~~~~~~~~

If the UM stash version used for your configuration is different from the stash versions provided, potentially some of the mappings listed in the template will be wrong. We added "standard-name" and/or "long-name" from the original files to the mappings to make spotting these mismatches easier. 
Potentially, this could also happen with different land or ocean model versions, but it's less likely as they use variable names rather than codes.
In both cases the template should be fixed manually.

Different output selection  
~~~~~~~~~~~~~~~~~~~~~~~~~~

Even if the UM codes represent the same variables there could be mismatch in the original frequency and the frequency of the mappings available in the db. 
Usually these variables will be listed in a section labelled:
`` "# Variables with different frequency: Use with caution!" `` 

The frequency listed in the mapping template is taken directly from the file, make sure it matches the one of the listed cmor table. If this differes you might have to update the cmor table with one that has a definition with the correct frequency.

.. warning:: 
   You should also pay attention if the variable is an instantenous value or not (i.e. time: point vs mean, sum, max etc in the cell_methods).    This should match the frequency in the cmor table definition.
   If it doesn't you might have to define a new variable with the correct frequency, cell_methods and time _axis, see below.

A similar message preceeds all the variables mapped from a different version from the selected (or default) one: 
 `` "# Variables definitions coming from different model version: Use with caution!" ``

.. _Custom_variables:

New variable
------------
If the variable you want to extract/calculate is not yet defined in a CMOR table you will need to:

1) Add it to an existing/new CMOR table.
2) Create a mapping for the variable that you can add directly to your mapping template and/or later load it to the database `mapping` table.
 You can then optionally add this information to the central access database.

3) Load (or reload in case of an existing table) the table to the `cmor` database table.
4) Load your edited template to the database `mapping` table, so that it can be used.

For both steps existing identical records will be ignored, once you added new records they can be used by the `varlist` and `template` tasks. 

New calculation 
---------------
There are two ways to define a new calculation for a derived variable. Which one you will use depends on how complex your calculation is.

As an example let's look at surface soil moisture for AUS2200:
.. code-block:: bash

   mrsos;fld_s08i223;var[0].isel(depth=0)

With this configuration the top soil level fits exactly the definition of surface soil moisture so all we needed to do was to select the top level from the input variable using `xarray isel`. This expression will be evaluated when the post-processing is run and it's simple enough to be added directly in the calculation field.

If the calcuation to be executed is more complex, then you need first to add a new function to the calculation.py code and then use the calculation filed in the mapping to call the fucntion with the right inputs.

Here we're showing how the pressure level calculation is defined for air temperature:
.. code-block:: bash

   ua24;fld_s00i002 fld_s00i407;plevinterp(var[0], var[1], 24)

For context this is the function definition:
.. code-block:: ipython3

   def plevinterp(ctx, var, pmod, levnum):

where `ctx` is the `context` of the specific file including information on the original variable.
