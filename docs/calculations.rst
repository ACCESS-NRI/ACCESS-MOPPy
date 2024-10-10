Derived variables calculations
==============================

Calculations are used to derive a variable from one or multiple inputs, to resample a variable to a new frequency or generally to modify a variable so it will match fully the corresponding definition in a CMOR table.

How calculations work
---------------------
Calculations are defined in the mapping file under the filed by the same name. The `calculation` string gets literally evaluated by the tool using python eval() function.
As an example 
 simple calculation could be summing  avariable across all its vertical levels:

.. code-block:: bash

    mrso;fld_s08i223;var[0].sum(dim='depth')

`var` represents the list of input variables, in this case there's only one which var[0] in the calculation string. In this case the calculation is very simple and can be fully defined in the mapping itself. If the calculation is more complex it's easier to use a pre-defined function, for example:

.. code-block:: bash

    hus24;fld_s00i010 fld_s00i408;plevinterp(var[0], var[1], 24)

Here plevinterp is called to interpolate specific humidity from model levels to pressure levels, this function takes three input arguments, the variable to interpolate, pressure at model levels and finally the number of pressure levels, which corresponds to a specific definition of the pressure levels coordinate.
Already available functions are listed below.

.. note::

    When more than one variable is used as input, if the variables are not all in the same file, more than one file pattern can be specified in the mapping row.   

Resample
^^^^^^^^
If a variable is available in the raw model output but not at the desired frequency, the tool will try to see if a higher frequency is available to be resampled. For example, if a user is interested in daily surface temperature but this is available only as hourly data, during the `mop setup` phase the tool will add a `resample` attribute with value 'D' to the variable and this will used as argument for the resample function. Which kind of statistics to use for the function is defined based on the `timeshot` attribute, so if a variable is defined as a maximum, minimum or sum these are used in the resample instead of the mean.

Contributing
------------
TBA

Available functions
-------------------

Atmosphere and aerosol
^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: mopper.calc_atmos
    :members:
    :undoc-members:
    :show-inheritance:

Ocean
^^^^^
.. automodule:: mopper.calc_ocean
    :members:
    :undoc-members:
    :show-inheritance:

SeaIce
^^^^^^
.. automodule:: mopper.calc_seaice
    :members:
    :undoc-members:
    :show-inheritance:

Land
^^^^
.. automodule:: mopper.calc_land
    :members:
    :undoc-members:
    :show-inheritance:

Other
^^^^^
.. automodule:: mopper.calc_utils
    :members:
    :undoc-members:
    :show-inheritance:
