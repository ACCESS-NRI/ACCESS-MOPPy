Customising 
===========

Depending on the model configuration you used, the variable you output and the derived variables you wan tto calculate you might have to customise the tool to produced all the desired output.
This is because it is not possible to include by default in the tool database all the possible mappings. For example, the UM model can use the same code for different quantities in different model configurations, or some calculations might need to have inputs values that need to be customised. Also CMOR needs all the variables and coordinates to be written to be defined in json tables. So any new variable needs to be defined to an exsiting or new table.
In this section, we will cover the following use cases:

* New model configuration
* New variable
* New calculation

New model configuration
-----------------------
Strictly speaking, unless you are using exactly the same version and output of a configuration already mapped in the database this case will apply to you.
However, the amount of customisation needed to prepare the post-processor mappings will vary depending on how different your configuration is.
You should still select the nearest to your case of the available versions, otherwise by default the tool will use the mappings linked to the ACCESS-CM2 version.

* different grid
We removed any hardcoded reference to grid size, in some functions where this was more difficult we move the grid-dependent information to input arguments. So pontetially these are the only changes you might need to do in the mapping template. Refer to the calculation section to see a list of functions which have grid-dependent input arguments.

* different stash version
If the UM stash version used for your configuration is different from the stash versions provided, potentially some of the mappings listed in the template will be wrong. We added "standard-name" and/or "long-name" from the original files to the mappings to make spotting these mismatches easier. 
Potentially, this could also happen with different land or ocean model versions, but it's less likely as they use variable names rather than codes.
In both cases the template should be fixed manually.

* different output selection  
Even if the UM codes represent the same variables there could be mismatch in the original frequency and the frequency of the mappings available in the db. 
Usually these variables will be listed in a section labelled:
> "# Variables with different frequency: Use with caution!" 

The frequency listed in the mapping template is taken directly from the file, make sure it matches the one of the listed cmor table. If this differes you might have to update the cmor table with one that has a definition with the correct frequency.

.. warning:: 
   You should also pay attention if the variable is an instantenous value or not (i.e. time: point vs mean, sum, max etc in the cell_methods). This should match the frequency in the cmor table definition.
 If it doesn't you might have to define a new variable with the correct frequency, cell_methods and time _axis, see below.

A similar message preceeds all the variables mapped from a different version from the selected (or default) one: 
 > "# Variables definitions coming from different model version: Use with caution!"
