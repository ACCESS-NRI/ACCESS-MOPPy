Variable Mapping Reference
==========================

.. contents:: Table of Contents
   :local:
   :depth: 3

This page is intended for **developers** who need to understand how ACCESS-MOPPy maps
raw ACCESS model output variables to CMIP-compliant output, or who want to add support
for new variables or models.

Overview
--------

ACCESS-MOPPy uses **JSON mapping files** to describe how raw model variables (e.g.
UM STASH codes such as ``fld_s02i208``, or MOM5/MOM6 diagnostics such as ``temp``)
correspond to CMIP output variables (e.g. ``Amon.rsds``).

At runtime, :func:`~access_moppy.utilities.load_model_mappings` reads the appropriate
JSON file, finds the requested CMIP variable, and returns the mapping dictionary.
The relevant :class:`~access_moppy.base.CMORiser` subclass then uses the mapping to
load, transform, and write the data.

The mapping system also handles CMIP7 compound names transparently: a CMIP7 name is
first resolved to its CMIP6 equivalent via a separate translation table, and the
CMIP6 mapping is then applied as normal.

Mapping Files
-------------

Location
^^^^^^^^

All mapping files live inside the installed package under::

    src/access_moppy/mappings/

The files shipped with ACCESS-MOPPy are:

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - File
     - Description
   * - ``ACCESS-ESM1.6_mappings.json``
     - Primary mapping file for ACCESS-ESM1.6 (atmosphere, ocean, land, sea ice, aerosol)
   * - ``ACCESS-CM3_mappings.json``
     - Mapping file for ACCESS-CM3 (atmosphere, ocean)
   * - ``ACCESS-OM3_mappings.json``
     - Mapping file for ACCESS-OM3 (ocean, time-invariant)
   * - ``cmip7_to_cmip6_compound_name_mapping.json``
     - Translation table: CMIP7 branded name → CMIP6 ``table.variable``
   * - ``cmip6_to_cmip7_compound_name_mapping.json``
     - Translation table: CMIP6 ``table.variable`` → CMIP7 branded name

Selecting a mapping file
^^^^^^^^^^^^^^^^^^^^^^^^

The mapping file to use is determined by the ``model_id`` argument of
``ACCESS_ESM_CMORiser`` (default: ``"ACCESS-ESM1.6"``).
:func:`~access_moppy.utilities.load_model_mappings` constructs the filename as
``{model_id}_mappings.json`` and looks for it inside the ``access_moppy.mappings``
package resource directory.

Top-level Structure of a Mapping File
--------------------------------------

Each mapping file is a JSON object with the following top-level keys:

.. code-block:: json

   {
     "model_info": { ... },
     "aerosol":    { "var1": { ... }, "var2": { ... } },
     "atmosphere": { "var1": { ... }, "var2": { ... } },
     "land":       { "var1": { ... }, "var2": { ... } },
     "landIce":    { "var1": { ... }, "var2": { ... } },
     "ocean":      { "var1": { ... }, "var2": { ... } },
     "sea_ice":    { "var1": { ... }, "var2": { ... } }
   }

``model_info``
   A metadata block describing the model and which components have mappings.

   .. code-block:: json

      {
        "model_id": "ACCESS-ESM1.6",
        "components": ["aerosol", "atmosphere", "land", "landIce", "ocean", "sea_ice"],
        "description": "Variable mappings for ACCESS-ESM1.6 Earth System Model"
      }

Each **component** key (``aerosol``, ``atmosphere``, etc.) maps CMIP variable names to
their entry dictionaries.  When :func:`~access_moppy.utilities.load_model_mappings` is
called with, say, ``compound_name="Amon.rsds"``, it extracts the CMIP name ``rsds``
and searches each component in turn until it finds the entry.

Variable Entry Fields
---------------------

Each variable entry inside a component block shares the same set of optional and
required fields:

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Field
     - Required
     - Description
   * - ``CF standard Name``
     - Yes
     - The `CF conventions <https://cfconventions.org/>`_ standard name for the output variable.
       May be an empty string ``""`` when no standard name has been assigned.
   * - ``dimensions``
     - Yes
     - Ordered dictionary that maps **model dimension names** (keys) to **CMIP dimension names** (values).
       This tells the CMORiser how to rename coordinates.  Example::

           "dimensions": {"time": "time", "lat": "lat", "lon": "lon"}
   * - ``units``
     - Yes
     - Expected physical units of the CMIP output variable (e.g. ``"W m-2"``, ``"kg m-2 s-1"``).
   * - ``positive``
     - Yes
     - Sign convention: ``"up"``, ``"down"``, or ``null`` if not applicable.
   * - ``model_variables``
     - Yes
     - List of raw model variable names that must be loaded from the input files.
       These are passed by name into the calculation context.
       ``null`` is allowed for ``internal`` calculations that produce data without any input file.
   * - ``calculation``
     - Yes
     - Dictionary that specifies *how* to derive the output variable.
       See :ref:`calculation-types` below.
   * - ``zaxis``
     - No
     - Present for variables on vertical levels.  Describes the vertical coordinate type
       and the variables needed to reconstruct it.
       See :ref:`zaxis-field` below.
   * - ``ressource_file``
     - No
     - Name of a bundled NetCDF resource file (stored under ``src/access_moppy/resources/``)
       that should be used instead of (or in addition to) user-provided input data.
       When this field is set and no ``input_data`` is passed to ``ACCESS_ESM_CMORiser``,
       the bundled file is used automatically.

Example — simple direct variable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

   "rldscs": {
     "CF standard Name": "surface_downwelling_longwave_flux_in_air_assuming_clear_sky",
     "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
     "units": "W m-2",
     "positive": "down",
     "model_variables": ["fld_s02i208"],
     "calculation": {
       "type": "direct",
       "formula": "fld_s02i208"
     }
   }

.. _calculation-types:

Calculation Types
-----------------

The ``calculation`` dictionary always contains a ``"type"`` key.  The five supported
types are described below.

``direct``
^^^^^^^^^^

The output variable is taken straight from one model variable with no transformation.

.. code-block:: json

   "calculation": {
     "type": "direct",
     "formula": "<model_variable_name>"
   }

``formula``
^^^^^^^^^^^

The output is derived by calling a **registered function** from the
:data:`~access_moppy.derivations.custom_functions` registry (see
:ref:`custom-functions`).

.. code-block:: json

   "calculation": {
     "type": "formula",
     "operation": "<function_name>",
     "args":    ["<var1>", "<var2>", ...],
     "kwargs":  {"<key>": "<var_or_literal>"}
   }

- ``args`` is a list of positional arguments.  Each item is either a string
  (variable name looked up in the input context), a number (used as-is), or a
  nested expression dictionary (see :ref:`expression-language` below).
- ``kwargs`` is a dictionary of keyword arguments.  Values follow the same rules as
  ``args`` items.
- Alternatively, ``operands`` may be used instead of ``args`` for legacy entries —
  both are treated identically by the expression evaluator.

Optional operands example (ocean ``hfds``):

.. code-block:: json

   "calculation": {
     "type": "formula",
     "operation": "calc_hfds",
     "args": ["sfc_hflux_from_runoff", "sfc_hflux_coupler", "sfc_hflux_pme"],
     "kwargs": {
       "frazil_3d_int_z": {"optional": "frazil_3d_int_z"},
       "frazil_2d":       {"optional": "frazil_2d"}
     }
   }

Wrapping a value in ``{"optional": "<var>"}`` means the variable is passed as
``None`` if it is absent from the input dataset, instead of raising a
``KeyError``.

``operation``
^^^^^^^^^^^^^

A shorthand for common two-argument arithmetic operations.  Functionally equivalent
to ``formula`` but expressed more compactly:

.. code-block:: json

   "calculation": {
     "type": "operation",
     "operation": "<op_name>",
     "args": ["<var1>", "<var2>"]
   }

Supported ``operation`` values: ``"add"``, ``"subtract"``, ``"multiply"``,
``"divide"``, ``"power"``.

Example (land ``npp`` — net primary productivity divided by tile fraction):

.. code-block:: json

   "calculation": {
     "type": "operation",
     "operation": "divide",
     "args": ["fld_s03i262", "fld_s03i395"]
   }

``dataset_function``
^^^^^^^^^^^^^^^^^^^^

Calls a more complex **dataset-level function** that receives the entire
xarray Dataset and may modify dimensions or coordinates (e.g. interpolating
from hybrid-height levels to physical height levels).

.. code-block:: json

   "calculation": {
     "type": "dataset_function",
     "function": "<function_name>",
     "kwargs": {}
   }

Available ``dataset_function`` values: ``"cl_level_to_height"``,
``"cli_level_to_height"``, ``"clw_level_to_height"``, ``"level_to_height"``.

These functions are defined in
:mod:`access_moppy.derivations.calc_atmos` and registered in
:data:`~access_moppy.derivations.custom_functions`.

``internal``
^^^^^^^^^^^^

The output variable is computed entirely internally from ancillary information
(grid geometry, etc.) without reading any user-provided input file.

.. code-block:: json

   "calculation": {
     "type": "internal",
     "function": "<function_name>",
     "args": []
   }

Currently the only available function is ``"calculate_areacella"`` (atmospheric
grid-cell area, computed from latitude/longitude coordinate arrays).

Variables that use this type do **not** require ``input_data`` to be passed to
``ACCESS_ESM_CMORiser``.

.. _expression-language:

Expression Language
-------------------

The ``formula`` calculation type uses a small recursive expression language that is
evaluated by :func:`~access_moppy.derivations.evaluate_expression`.
An expression can be one of:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Expression form
     - Meaning
   * - ``"<variable_name>"``
     - Look up the named variable in the input context (an xarray DataArray).
   * - ``<number>``
     - A literal numeric value (integer or float).
   * - ``{"literal": <value>}``
     - Explicit literal — useful when the value might be a string or ambiguous.
   * - ``{"optional": "<variable_name>"}``
     - Look up the variable; return ``None`` if absent instead of raising an error.
   * - ``{"operation": "<op>", "args": [...], "kwargs": {...}}``
     - Nested function call: recursively evaluate ``args``/``kwargs``, then call the
       registered function ``<op>``.

Expressions can be arbitrarily nested, allowing compound derivations to be expressed
in a single JSON structure.

.. _custom-functions:

Custom Functions Registry
--------------------------

All functions available to the ``formula``, ``operation``, and ``dataset_function``
calculation types are registered in the dictionary
:data:`access_moppy.derivations.custom_functions`.

Built-in operations
^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Name
     - Description
   * - ``add``
     - Sum of any number of arguments: ``a + b + c + ...``
   * - ``subtract``
     - Difference: ``a - b``
   * - ``multiply``
     - Product: ``a * b``
   * - ``divide``
     - Ratio: ``a / b``
   * - ``power``
     - Exponentiation: ``a ** b``
   * - ``sum``
     - ``xarray.DataArray.sum(**kwargs)``
   * - ``mean``
     - Arithmetic mean of multiple arguments
   * - ``kelvin_to_celsius``
     - ``x - 273.15``
   * - ``celsius_to_kelvin``
     - ``x + 273.15``
   * - ``isel``
     - Select a single index slice: ``x.isel(**kwargs)``
   * - ``calculate_monthly_minimum``
     - Resample to monthly minimum
   * - ``calculate_monthly_maximum``
     - Resample to monthly maximum
   * - ``drop_axis``
     - Drop a named dimension/axis
   * - ``drop_time_axis``
     - Drop the time dimension (for time-invariant fields stored in time-varying files)
   * - ``squeeze_axis``
     - Squeeze (remove) size-1 dimensions

Atmosphere functions
^^^^^^^^^^^^^^^^^^^^

Defined in :mod:`access_moppy.derivations.calc_atmos`.

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Name
     - Description
   * - ``cl_level_to_height``
     - Convert cloud fraction from hybrid-height levels to physical height levels
   * - ``cli_level_to_height``
     - Convert cloud ice content from hybrid-height levels to physical height levels
   * - ``clw_level_to_height``
     - Convert cloud liquid water from hybrid-height levels to physical height levels
   * - ``level_to_height``
     - Generic hybrid-height level → physical height conversion
   * - ``calculate_areacella``
     - Compute atmospheric grid-cell area from lat/lon coordinates

Aerosol functions
^^^^^^^^^^^^^^^^^

Defined in :mod:`access_moppy.derivations.calc_aerosol`.

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Name
     - Description
   * - ``optical_depth``
     - Sum spectral band optical depths to produce a broadband aerosol optical depth

Land functions
^^^^^^^^^^^^^^

Defined in :mod:`access_moppy.derivations.calc_land`.

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Name
     - Description
   * - ``calc_topsoil``
     - Extract top-soil layer diagnostic
   * - ``calc_landcover``
     - Derive land cover fractions from tile data
   * - ``extract_tilefrac``
     - Extract a specific tile fraction
   * - ``weighted_tile_sum``
     - Weighted sum over surface tiles
   * - ``calc_carbon_pool_kg_m2``
     - Convert carbon pool units to kg m⁻²
   * - ``calc_cland_with_wood_products``
     - Total land carbon including wood products
   * - ``calc_mass_pool_kg_m2``
     - Convert mass pool to kg m⁻²
   * - ``calc_nitrogen_pool_kg_m2``
     - Convert nitrogen pool units to kg m⁻²
   * - ``calc_mrsfl``
     - Compute frozen soil moisture
   * - ``calc_mrsll``
     - Compute liquid soil moisture
   * - ``calc_mrsol``
     - Compute total soil moisture
   * - ``calc_tsl``
     - Compute soil temperature profile

Ocean functions
^^^^^^^^^^^^^^^

Defined in :mod:`access_moppy.derivations.calc_ocean`.

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Name
     - Description
   * - ``calc_areacello``
     - Compute ocean grid-cell area
   * - ``calc_hfds``
     - Downward ocean heat flux (composite of runoff, coupler, P-E terms, plus optional frazil)
   * - ``calc_hfgeou``
     - Upward geothermal heat flux
   * - ``calc_msftbarot``
     - Barotropic mass streamfunction
   * - ``calc_overturning_streamfunction``
     - Meridional overturning circulation streamfunction
   * - ``calc_rsdoabsorb``
     - Shortwave radiation absorbed in ocean
   * - ``calc_global_ave_ocean``
     - Volume-weighted global ocean average
   * - ``calc_total_mass_transport``
     - Total mass transport across an ocean section
   * - ``calc_umo_corrected``
     - Zonal mass transport corrected for barotropic flow
   * - ``calc_vmo_corrected``
     - Meridional mass transport corrected for barotropic flow
   * - ``calc_zostoga``
     - Global mean thermosteric sea level change
   * - ``ocean_floor``
     - Extract ocean floor (bottom-cell) values

Sea ice functions
^^^^^^^^^^^^^^^^^

Defined in :mod:`access_moppy.derivations.calc_seaice`.

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Name
     - Description
   * - ``calc_seaice_extent``
     - Sea ice extent (area where concentration > 15 %)
   * - ``calc_hemi_seaice``
     - Hemisphere-specific sea ice aggregate
   * - ``calc_siarean`` / ``calc_siareas``
     - Northern/southern hemisphere sea ice area
   * - ``calc_sivoln`` / ``calc_sivols``
     - Northern/southern hemisphere sea ice volume
   * - ``calc_sisnmassn`` / ``calc_sisnmasss``
     - Northern/southern hemisphere sea ice snow mass
   * - ``calc_siextentn`` / ``calc_siextents``
     - Northern/southern hemisphere sea ice extent

.. _zaxis-field:

Vertical Axis (``zaxis``) Field
---------------------------------

For variables defined on vertical levels the mapping entry may include a ``zaxis``
block that describes the vertical coordinate:

.. code-block:: json

   "zaxis": {
     "type": "hybrid_height",
     "coordinate_variables": {
       "sigma_theta":       "b",
       "surface_altitude":  "orog",
       "theta_level_height": "lev"
     },
     "formula": "z = a + b*orog"
   }

- ``type``: currently always ``"hybrid_height"`` (UM eta-based hybrid height coordinate).
- ``coordinate_variables``: mapping from the UM variable name (key) to the CMIP output
  coordinate name (value).
- ``formula``: human-readable label for the vertical coordinate reconstruction formula.

The actual vertical interpolation is carried out by the ``dataset_function``
registered functions (e.g. ``level_to_height``) using the auxiliary variables
identified in ``coordinate_variables``.

Resource Files
--------------

Some variables (e.g. ``areacello``, ``zfull``) are derived from static ancillary
data that is bundled with ACCESS-MOPPy rather than read from user-supplied files.
These are listed in the ``ressource_file`` field (note the non-standard spelling,
kept for historical compatibility).

Bundled resource files live under::

    src/access_moppy/resources/

When ``ressource_file`` is set and no ``input_data`` is provided to
``ACCESS_ESM_CMORiser``, the bundled file is resolved via
:func:`importlib.resources.files` and used automatically.

CMIP7 Compound Name Translation
--------------------------------

CMIP7 uses a longer "branded" compound name format:
``realm.variable.operation.frequency.domain``
(e.g. ``atmos.tas.tavg-h2m-hxy-u.mon.glb``).

The files ``cmip7_to_cmip6_compound_name_mapping.json`` and
``cmip6_to_cmip7_compound_name_mapping.json`` provide a bidirectional look-up
table between these names and the familiar CMIP6 ``table.variable`` form.

These mappings are generated from the official CMIP7 Data Request API and contain
~1 974 entries.  The function
:func:`~access_moppy.utilities._get_cmip7_to_cmip6_mapping` resolves a CMIP7 name
to its CMIP6 equivalent (with support for regex patterns when a single exact match
is not available).

The resolved CMIP6 name is then passed to :func:`~access_moppy.utilities.load_model_mappings`
as usual, so the variable-level mapping files only need to be maintained in CMIP6
terms.

Adding New Mappings
-------------------

To add support for a new variable, open the relevant model mapping JSON file and add
an entry under the appropriate component key.

Checklist
^^^^^^^^^

1. Identify the correct **component** (``atmosphere``, ``ocean``, etc.) based on the
   model realm.
2. Use the **CMIP6 variable short name** as the JSON key.
3. Fill in all required fields: ``CF standard Name``, ``dimensions``, ``units``,
   ``positive``, ``model_variables``, ``calculation``.
4. Choose the simplest applicable ``calculation.type``:

   - Single variable, no transform → ``direct``
   - Arithmetic on two variables → ``operation``
   - Custom function with ≥ 1 argument → ``formula``
   - Dataset-level level interpolation → ``dataset_function``
   - No input data needed → ``internal``

5. If the function you need does not yet exist in
   :data:`~access_moppy.derivations.custom_functions`, implement it in the
   appropriate ``calc_*.py`` module under :mod:`access_moppy.derivations`, import
   it in :mod:`access_moppy.derivations.__init__`, and register it in the
   ``custom_functions`` dictionary.
6. Run the test suite to ensure no regressions.

Example — adding a new atmosphere variable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose you want to add ``huss`` (near-surface specific humidity, ``fld_s03i237``):

.. code-block:: json

   "huss": {
     "CF standard Name": "specific_humidity",
     "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
     "units": "1",
     "positive": null,
     "model_variables": ["fld_s03i237"],
     "calculation": {
       "type": "direct",
       "formula": "fld_s03i237"
     }
   }

Adding a new model
^^^^^^^^^^^^^^^^^^

1. Create ``src/access_moppy/mappings/<MODEL_ID>_mappings.json`` following the
   same top-level structure (``model_info`` + component keys).
2. Pass ``model_id="<MODEL_ID>"`` to ``ACCESS_ESM_CMORiser`` to activate the new
   mapping file.
3. If the model uses a different CMORiser class (e.g. a new ocean component), implement
   a :class:`~access_moppy.base.CMORiser` subclass and wire it up in
   :mod:`access_moppy.driver`.
