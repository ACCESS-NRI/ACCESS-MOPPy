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

.. code-block:: text

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
   A metadata block describing the model, its components, and — most
   importantly — how to discover raw output files automatically.

   .. code-block:: text

      {
        "model_id": "ACCESS-ESM1.6",
        "components": ["aerosol", "atmosphere", "land", "landIce", "ocean", "sea_ice"],
        "description": "Variable mappings for ACCESS-ESM1.6 Earth System Model",
        "file_discovery": { ... }
      }

   The ``file_discovery`` sub-block drives automatic file discovery (see
   :ref:`file-discovery` below).  Two further optional sub-blocks describe the
   grid a component writes on, both keyed by component and then by the stagger
   point a field sits on (``"T"``/``"U"``/``"V"``/``"other"``, with
   ``"default"`` as the fallback):

   .. code-block:: json

      {
        "cmip7_grid_labels": {
          "atmosphere": {"default": "g115", "U": "g109", "V": "g108", "other": "g110"},
          "sea_ice":    {"default": "g118", "U": "g116", "V": "g116"}
        },
        "cell_measures": {
          "ocean": {"T": "area: areacello"}
        }
      }

   ``cmip7_grid_labels``
      The CMIP7 grid label registered for each point, used when the caller does
      not pin ``grid_label`` explicitly.  The point is taken from the mapping
      entry's dimension names for the atmosphere, and from the inferred grid
      type at runtime for ocean and sea ice.  Ocean output by basin and
      latitude (the overturning streamfunctions) uses the ``"basin"`` key;
      ACCESS-ESM1.6 sets it to its velocity grid ``g116``, whose rows these
      streamfunctions are summed along, until a basin grid of its own is
      registered.

   ``cell_measures``
      The measure this model publishes for each point, e.g.
      ``"area: areacello"``.  It is used **only** where the CMOR table declines
      to name one and leaves it to the modelling centre — the ``"--MODEL"``,
      ``"--OPT"`` and ``"--UGRID"`` placeholders, which affect the sea-ice
      velocities and drift stresses (``siu``, ``siv``, ``sidmasstranx``, ...),
      the ocean grid spacings and the sub-daily aerosol mixing ratios.  Where
      the table names a real measure that value is authoritative and this block
      is ignored.

      Leave it out unless the model actually publishes an area measure for that
      point.  With no entry, the placeholder is dropped and no
      ``cell_measures`` attribute is written, which is what CMOR itself does.
      ACCESS-ESM1.6 ships no ``cell_measures`` block: CMIP7 registers an area
      measure for the atmosphere (``areacella``), ocean (``areacello``), river
      (``areacellr``) and ice-sheet (``areacellg``) grids, but none for the
      sea-ice B-grid corner that ``siu``/``siv`` sit on, so there is nothing
      valid to name there.

Each **component** key (``aerosol``, ``atmosphere``, etc.) maps variable names or
exact ``table.variable`` names to complete recipes. Processing, file discovery
and the ESMValTool variable index use the same selection rules:

1. Look for an exact key, such as ``Emon.rsds``, across all components.
2. If no exact key exists, look for the short name, such as ``rsds``. When its
   entry has ``supported_compound_names``, the requested product must be listed.
3. Reject ambiguous matches. An unsupported request returns ``{}`` from
   ``load_model_mappings``; the driver raises ``ValueError`` before discovering
   or opening inputs, including explicitly supplied files and xarray datasets.

CMIP7 branded names are translated to their CMIP6 equivalents before selection.
Legacy short-name entries without an allowlist retain their previous behavior.
For new or revised recipes, list the products that may share the calculation.
The allowlist scopes the recipe; it does not guarantee that an archive contains
all inputs at the required cadence. Compatible MIP table names such as
``APmon.rsds`` and ``LPmon.tran`` remain listed alongside the legacy names.

Table-specific recipes
^^^^^^^^^^^^^^^^^^^^^^

Add an exact key when two products need different calculations or inputs.
For example, the ``Emon.rsds`` entry takes precedence over the shared
``rsds`` entry, even though they sit under different components. It must contain
the full recipe: ``model_variables``, ``calculation``, units, dimensions and any
other required fields. Fields are **not merged** with the short-name entry.
An exact entry that excludes the request with its own allowlist does not fall
back to the short-name recipe.

A shared entry can be scoped like this (other recipe fields omitted)::

    "rsds": {
      "supported_compound_names": ["Amon.rsds", "day.rsds", "3hr.rsds"],
      ...
    }

Put the recipe under the component whose file-discovery configuration locates
its native inputs, or supply a ``file_pattern``. The output realm comes from
the requested vocabulary; it does not select or mask the native data.
``load_variable_entry`` and ``load_variable_component`` accept a
``table.variable`` name. Bare-name inspection raises ``ValueError`` when more
than one recipe exists for that name.

Annual wood-product carbon fluxes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ACCESS-ESM1-5 and ACCESS-ESM1-6 ``fDeforestToProduct`` and
``fProductDecomp`` recipes use annual native diagnostics in g m-2 yr-1:
``fld_s03i895`` for transfers into products, and the sum of
``fld_s03i907``, ``fld_s03i908`` and ``fld_s03i909`` for decomposition.
These contributions already include tile-area weighting and must be summed
over tiles without multiplying by the current tile fractions.

Both mappings use ``calc_product_pool_kg_m2`` to sum the tile contributions
and convert grams to kilograms, followed by multiplication by
``1 / (365.2422 * 86400)`` to convert annual values to per-second fluxes.
The fixed year length follows the convention documented in issue #802;
it is not the duration of the output month. ``fProductDecomp`` no longer
requires ``fld_s03i317`` as an input.

The #808 follow-up passes ``landfrac=1.0`` and masks pure ocean with
``mask_land(result, fld_s03i395)``. This preserves the annual conversion
from #802 while retaining the required land-area denominator. The shared
helper's default whole-cell behavior is unchanged.

Baseline land means and coastal normalization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ACCESS-ESM1-5 and ACCESS-ESM1-6 ``cSoil``, ``cVeg``, ``lai``, ``fBNF``
and ``snc`` recipes return means over land, consistent with
``area: mean where land``. Native surface tile fractions (``fld_s03i317``)
sum to one over the land portion of a cell. Retain this tile weighting;
do not multiply the result by native land fraction (``fld_s03i395``).
A cell with 10% land must retain the same land-area value as a fully
land-covered cell with the same tile values and fractions.

The five recipes pass ``landfrac=1.0`` to their existing calculation and
then apply ``mask_land(result, fld_s03i395)``. Pure-ocean cells are missing,
including when the inner calculation returns zero. Snow-free land remains
zero for ``snc``. The calculations are:

.. list-table:: Preserved tile calculations and unit conversions
   :header-rows: 1
   :widths: 15 85

   * - Variable
     - Land-area calculation
   * - ``cSoil``
     - Sum ``fld_s03i858 + fld_s03i859 + fld_s03i860`` per tile,
       weight by tile fraction, sum tiles, divide by 1000 (g to kg).
   * - ``cVeg``
     - Sum ``fld_s03i851 + fld_s03i852 + fld_s03i853 + fld_s03i854`` per
       tile, weight by tile fraction, sum tiles, divide by 1000.
   * - ``lai``
     - Sum ``fld_s03i893 * tilefrac`` over tiles (dimensionless).
   * - ``fBNF``
     - Sum ``fld_s03i885 * tilefrac`` over tiles and retain the existing
       factor ``1 / (1000 * 86400)`` to kg m-2 s-1. The annual product-flux
       conversion from #802 does not apply here.
   * - ``snc``
     - ``100 * sum(tilefrac where fld_s08i236 > 0)``. This retains the
       existing snow-presence approximation on monthly tile snow amounts.

Caller audit and scope
~~~~~~~~~~~~~~~~~~~~~~

Both model mappings use the same native diagnostics. The four carbon, LAI
and nitrogen-fixation products are land means in CMIP6 and CMIP7. Snow cover
needs an era-specific calculation: CMIP6 ``LImon.snc`` explicitly describes
the percentage of the whole grid cell occupied by snow resting on land;
CMIP7 ``landIce.snc.tavg-u-hxy-lnd.mon.glb`` instead declares a land mean.
The exact ``LImon.snc`` recipe therefore keeps the legacy whole-cell
calculation by default and supplies the corrected land mean under
``calculation_overrides.CMIP7``. This replaces only the calculation; input
fields and discovery are identical. ``load_model_mappings`` infers CMIP7
from a branded name, or accepts ``cmip_version="CMIP7"`` with a legacy
request. The driver passes the output vocabulary version when resolving
CMIP7 requests in either syntax. Other snow-cover products are unchanged.

Shared helper implementations and defaults are unchanged.
``calc_carbon_pool_kg_m2`` delegates to
``calc_mass_pool_kg_m2`` and is also used by ``cLeaf``, ``cLitter`` and
``cRoot``. The mass helper also has the nitrogen-pool alias.
``weighted_tile_sum`` additionally serves ``cSoilFast``, ``cSoilMedium``,
``cSoilSlow``, ``fNdep``, ``fNgas``, ``fNleach``, ``fNloss``, ``fNnetmin``,
``fNup``, ``tran`` and (ESM1-5 only) ``nbp``. Before this correction,
``calc_snc`` had only the two model ``snc`` mapping callers. These other
recipes were outside the five-variable correction; the additional carbon and
nitrogen recipes are addressed below. Valid whole-cell recipes,
including ``3hr.tran``, retain their required land-fraction scaling.
The annual conversion and product tile-summing changes in PR #809 are retained.

Validation and published-data follow-up
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Numerical mapping regressions cover both models, CMIP6/7 request names,
``pseudo_level_0`` and ``pseudo_level`` axes, eager and Dask inputs,
unequal tiles, full land, fractional coasts, ocean masking and snow-free
land. The 72 land-mean cases fail on the original mappings and pass after
correction; eight additional cases preserve CMIP6 whole-cell snow cover.
Global integrals must use land area, ``areacella * sftlf / 100``, where
``sftlf`` is in percent. Check coastal values and ocean masking separately;
a global total alone can conceal the wrong averaging domain.

The optional native integration tests exercise CMIP7 writing for all five
variables and compare every cell and monthly land-area integral with an
explicit calculation from native ACCESS-ESM1-6 historical inputs::

    ACCESS_MOPPY_DATA_ROOT=/path/to/esm-historical PYTHONPATH=src \
      python -m pytest tests/integration/test_baseline_land_means.py -q

Local validation on 2 November 2026 passed for all twelve months of 1951,
including CMOR writing and CMIP7 repacking. The snow tests additionally
verify CMIP7 selection using the legacy request name and preserve CMIP6
whole-cell output. Direct mapping checks also passed for June 1951, 1952
and 1953, with 2,434 fractional coastal cells per grid (relative tolerance
1e-12; written float32 output uses 1e-6).

For June 1952 (``output102/atmosphere/netCDF/aiihca.pa-195206_mon.nc``),
the following global integrals use land area from native bounds and land
fraction, with a spherical radius of 6,371,000 m. ``snc`` is divided by
100 before integrating. These are historical validation results, not
estimates of the published piControl release's global error.

.. list-table:: June 1952 land-area integrals
   :header-rows: 1
   :widths: 15 30 25 30

   * - Variable
     - Units
     - Corrected integral
     - Original recipe with land area
   * - ``cSoil``
     - Pg C
     - 819.0460
     - 786.0390
   * - ``cVeg``
     - Pg C
     - 630.4198
     - 595.9141
   * - ``lai``
     - Million km2 leaf area
     - 170.3869
     - 161.3088
   * - ``fBNF``
     - kg N s-1
     - 6645.1850
     - 6383.2033
   * - ``snc``
     - Million km2 snow-covered land
     - 29.4636
     - 28.0644

At the 10%-land cell (31.25 S, 153.75 E), corrected ``cSoil``, ``cVeg``,
``lai`` and ``fBNF`` are 9.675680, 10.066345, 3.052340 and 4.286376e-12
in their mapped units. At (58.75 N, 281.25 E), ``snc`` is 100% instead of
10%. Native carbon-pool and nitrogen-fixation unit attributes are absent
from these files; the existing conversions were retained, not independently
re-established from model source in this validation.

`Issue #808 <https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/808>`_
tracks the published-data erratum. The inspected published samples are
ACCESS-ESM1-6 piControl/r1i1p1f1, g115/v20261002, model years 0101-0109,
recording MOPPy 1.8.1. The published/fraction map values illustrate an
inferred correction; they are not independently validated replacements.
Historical native samples are from a different run and period. No native
ACCESS-ESM1-5 run has been validated for this correction.

The following remain outstanding after this code change:

* Compare the sampled published piControl files with matching native output.
* Inventory exact affected experiment/member/dataset/version identifiers
  and producing MOPPy releases across the published archive.
* Prepare and formally register the erratum with the publication team.
* Reprocess production data, validate replacements and record new versions
  and the handling of affected versions in #808.

Merging this code fix does not resolve the published-data erratum.

Additional carbon and nitrogen land means
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The follow-up to #808 applies the same land-area normalization to these
19 recipes in both ACCESS-ESM1-5 and ACCESS-ESM1-6. Their monthly CMIP6 and
CMIP7 definitions declare ``area: mean where land``:

.. list-table:: Additional corrected land means
   :header-rows: 1
   :widths: 25 75

   * - Group
     - Variables
   * - Carbon pools
     - ``cLand``, ``cLeaf``, ``cLitter``, ``cProduct``, ``cRoot``
   * - Nitrogen pools
     - ``nLand``, ``nLitter``, ``nMineral``, ``nProduct``, ``nSoil``, ``nVeg``
   * - Nitrogen fluxes
     - ``fNdep``, ``fNgas``, ``fNleach``, ``fNloss``, ``fNnetmin``, ``fNup``
   * - Product fluxes
     - ``fDeforestToProduct``, ``fProductDecomp``

Each recipe passes unity for the final land fraction and applies the native
land mask. Tile-density diagnostics retain their tile-fraction weighting.
Product diagnostics already weighted over land are summed without applying
tile fractions again; ``cLand`` and ``nLand`` retain both kinds of terms.
The g-to-kg conversions, daily nitrogen-flux constants and annual product-flux
conversion remain unchanged.

``cRoot`` also corrects a separate input error: it uses native root carbon
``fld_s03i854``, rather than leaf carbon ``fld_s03i852``. Unlike the area-only
correction, this input change can affect fully land-covered cells as well.
The native integration test compares the written field with weighted root
diagnostic 854; the synthetic test supplies distinct root and leaf values.

The shared-helper caller audit leaves ``cSoilFast``, ``cSoilMedium`` and
``cSoilSlow`` outside this change, along with daily snow. They require separate
follow-up. Whole-cell transpiration and the ESM1-5 ``nbp`` expression retain
their existing internal land-fraction factors. Native grid-cell productivity
and respiration fluxes still require division by land fraction, as described
in `#115 <https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/115>`_; this is
different from removing a mapping-applied factor from a land-area quantity.

The 304 numerical regression cases cover all 19 variables, both model mappings,
CMIP6/7 requests, both tile-axis names, eager/Dask inputs, unequal tile values,
pure land, 50% and 10% coasts, and zero/missing/finite ocean inputs. They fail
on the pre-correction mappings. Product-flux tests also preserve the annual
unit conversion from #802.

Native integration tests exercise the actual mappings through CMOR writing
and CMIP7 repacking. Select a complete native year with::

    ACCESS_MOPPY_DATA_ROOT=/path/to/esm-historical \
      ACCESS_MOPPY_TEST_YEAR=1951 PYTHONPATH=src \
      python -m pytest tests/integration/test_additional_land_means.py -q

Local validation on 2 November 2026 passed all 19 cases for each of 1951,
1952 and 1953: 57 CMOR/repack tests covering 684 variable-months and 2,434
fractional coastal cells per month. These runs use the corrected production
recipes, including root diagnostic 854 for ``cRoot``.

The tests compare every monthly cell, output coordinates, ocean masks and
global integrals using ``areacella * sftlf / 100``. Written values use
``rtol=1e-6`` and zero absolute tolerance. For signed ``fNnetmin`` contributions
that nearly cancel, a per-cell float32 rounding bound is additionally allowed:
``gamma_(n+4) * sum(abs(tile_value * tile_fraction)) * conversion``, where
``gamma_k = k*eps / (1-k*eps)``. The output must also exactly match an
independent calculation using the native float32 precision. This does not
relax global-integral tolerances.

These historical inputs are from a different run and period than the published
piControl sample. Existing conversions are retained, since native pool/flux
unit attributes are absent. ESM1-5 native validation, matching published-run
validation, a complete affected-release inventory, formal erratum registration
and production reprocessing remain outstanding.

Land-only incoming shortwave radiation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``Emon.rsds`` (CMIP7 ``land.rsds.tavg-u-hxy-lnd.mon.glb``) is supported for
ACCESS-ESM1-5 and ACCESS-ESM1-6 by its own recipe. It uses native incoming
shortwave radiation ``fld_s01i235`` and land fraction ``fld_s03i395``::

    rsds = fld_s01i235.where(fld_s03i395 > 0)

Pure-ocean cells are missing. Cells containing any land, including fractional
coastal cells, retain their incoming flux without multiplication or division
by land fraction. UM supplies the same incoming radiation across the grid cell;
the `upstream mapping clarification
<https://github.com/UKNCSP/CDDS-CMIP7-mappings/issues/1004#issuecomment-3606641544>`_
identifies this product as the global ``rsds`` field masked over land.
This rule is specific to incoming radiation; it does not establish the same
rule for upward radiation or other surface fluxes.

Use ``cmip_version="CMIP7"`` with either name. Discovery includes both native
fields, and an explicit input must also contain both. The recipe works lazily
with Dask and can be partitioned in time. Regenerate earlier ``Emon.rsds``
output to remove the previously unmasked ocean values.

Other averaging-domain variants
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The other products in `issue #803
<https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/803>`_ use these rules for
ACCESS-ESM1-5 and ACCESS-ESM1-6. ``land_tile_mean`` weights by ``fld_s03i317``
and divides by the total tile fraction; it returns missing over ocean and
propagates missing values on occupied tiles.

.. list-table::
   :widths: 20 45 35
   :header-rows: 1

   * - Product
     - Calculation
     - Required inputs and availability
   * - ``Eday.clt``
     - ``100 * fld_s02i204``, masked where the sum of tile fractions is zero.
       Coastal cloud cover is unchanged.
     - Daily ``fld_s02i204`` and ``fld_s03i317``. Both are present in the
       sample ACCESS-ESM1-6 archive.
   * - ``LImon.sbl``
     - Land tile mean of ``fld_s03i331``. Snow-free land is missing, using
       ``fld_s08i236`` (snow on tiles); ocean is missing.
     - Monthly ``fld_s03i331``, ``fld_s03i317`` and ``fld_s08i236``.
       Present in the sample archive; enabled in the additional templates.
   * - ``Eday.evspsbl``
     - Land tile mean of ``fld_s03i287 + fld_s03i288 + fld_s03i331``:
       canopy evaporation + transpiration/soil evaporation + sublimation.
     - All three fluxes and ``fld_s03i317`` at daily frequency. The sample
       archive saves these fluxes monthly only, so its daily files are insufficient.
   * - ``3hr.tran``
     - Land tile mean of ``fld_s03i174``, multiplied by ``fld_s03i395``
       to express the flux per whole-cell area. Pure ocean is zero.
     - Three-hourly ``fld_s03i174``, ``fld_s03i317`` and ``fld_s03i395``.
       The sample archive lacks these three-hourly inputs.
   * - ``Emon.rsus``
     - Still blocked: whole-cell upward radiation cannot supply a coastal
       land-only mean.
     - Needs a verified land-only radiation diagnostic. The sample archive
       lacks ``fld_s01i257`` (net shortwave over land).

``Eday.clt`` follows the `upstream land-mask clarification
<https://github.com/UKNCSP/CDDS-CMIP7-mappings/issues/711#issuecomment-5423827523>`_.
The sublimation tile diagnostic follows the `upstream mapping
<https://github.com/UKNCSP/CDDS-CMIP7-mappings/issues/1250>`_.
The evaporation sum was checked against the native total moisture flux
``fld_s03i223`` on pure-land cells in monthly ACCESS-ESM1-6 output, agreeing
to numerical precision; this validates the budget, not the availability of
daily inputs. Monthly data must not be substituted for missing daily or
three-hourly diagnostics. Without the required native output at the requested
frequency, these products cannot be CMORised, even when a mapping exists.
``Eday.evspsbl`` and ``3hr.tran`` are excluded from the shipped batch configs.

Discovery recognises ``Eday`` and land three-hourly streams. These recipes
also accept their CMIP7 aliases and explicit inputs containing the required
diagnostics. Numerical tests cover ocean, fractional coast and pure land;
integration tests compare written cloud cover and sublimation to native data.

``LImon.sbl`` declares ``native_realm: "land"`` in its recipe, so native grid
resolution comes from CABLE. Its output realm remains ``landIce``. This does
not enable unsupported ice-sheet products. ``native_realm`` is an optional
mapping field for this distinction; the named source component must have a
registered native resolution. The whole-cell ``Amon.sbl`` recipe retains
``fld_s03i298`` and identifies its native component as ``atmos``.
The ``LImon.sbl`` recipe supports CMIP6 and CMIP7 only: the newer MIP table
selected for CMIP6Plus uses that name for a whole-cell mean, so it is rejected
rather than applying the legacy land-only calculation.

The `upstream rsus discussion
<https://github.com/UKNCSP/CDDS-CMIP7-mappings/issues/1010#issuecomment-3612151513>`_
suggests ``fld_s01i235 - fld_s01i257``, but explicitly calls for checking what
those diagnostics contain. ``Emon.rsus`` is removed from the batch configs
until its native output is available and its coastal values can be verified. ``Eday.sbl``
is also excluded: the bundled CMIP6 definition is a land mean, while its CMIP7
alias describes a whole-cell mean.

For surface fluxes that differ between land and ocean, masking ocean cells
alone cannot recover a land-only mean in a mixed coastal cell.
Dividing a whole-cell flux by land fraction is valid only when its ocean
contribution is known to be zero. Conversely, a per-land-area transpiration
flux needs land-fraction weighting to become a whole-cell flux. Do not infer
these operations from the output realm or ``area_label`` alone.

To enable a held product, verify the native diagnostic's definition and area
normalization, add its complete recipe, and test pure-ocean, mixed-coast and
pure-land cells numerically. Check that discovery selects all required inputs
and that the CMIP6 and CMIP7 requests select the intended recipe. Metadata
compliance alone cannot validate this calculation. Previously generated
``Emon.rsus`` files should be withheld from publication and
regenerated after their recipes are validated.

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
     - Sign convention metadata: ``"up"``, ``"down"``, or ``null`` if not applicable.
   * - ``model_variables``
     - Yes
     - List of raw model variable names that must be loaded from the input files.
       These are passed by name into the calculation context.
       ``null`` is allowed for ``internal`` calculations that produce data without any input file.
   * - ``supported_compound_names``
     - No
     - Exact CMIP6 ``table.variable`` names allowed to use this entry. Omit for
       legacy entries that apply wherever the variable name is requested.
   * - ``supported_cmip_versions``
     - No
     - CMIP versions allowed by the driver. Use when the same table name has
       changed meaning across vocabularies, e.g. ``["CMIP6", "CMIP7"]``.
   * - ``native_realm``
     - No
     - Source component used for native nominal resolution, without changing
       the output realm. For example, ``"land"`` for CABLE's ``LImon.sbl``.
   * - ``calculation``
     - Yes
     - Dictionary that specifies *how* to derive the output variable.
       See :ref:`calculation-types` below.
   * - ``calculation_overrides``
     - No
     - Complete replacement calculations keyed by output vocabulary version,
       for example ``CMIP7``. Native input fields and all other recipe fields
       remain shared. Used by ``LImon.snc`` because its averaging domain
       changes between CMIP6 and CMIP7.
   * - ``zaxis``
     - No
     - Present for variables on vertical levels.  Describes the vertical coordinate type
       and the variables needed to reconstruct it.
       See :ref:`zaxis-field` below.
   * - ``file_pattern``
     - No
     - A glob pattern (or list of patterns), **relative to** ``input_folder``, that
       overrides the component-level auto-discovery for this specific variable.
       Use this for edge-cases: unusual filenames, legacy layouts, or variables
       spread across multiple file types.  Example::

           "file_pattern": "output[0-9][0-9][0-9]/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc"

       When absent, discovery falls back to the ``file_discovery`` block in ``model_info``.
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

.. code-block:: text

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
   * - ``calc_nland_with_wood_products``
     - Total land nitrogen including wood products
   * - ``calc_product_pool_kg_m2``
     - Wood-harvest product pool in kg m⁻² (already per land area: no tile weighting)
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
(e.g. ``atmos.rsds.tavg-u-hxy-u.mon.GLB``).

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
2. Add a ``file_discovery`` block to ``model_info`` (see :ref:`file-discovery`)
   so that MOPPy can auto-discover raw files for the new model.
3. Pass ``model_id="<MODEL_ID>"`` to ``ACCESS_ESM_CMORiser`` to activate the new
   mapping file.
4. If the model uses a different CMORiser class (e.g. a new ocean component), implement
   a :class:`~access_moppy.base.CMORiser` subclass and wire it up in
   :mod:`access_moppy.driver`.

.. _file-discovery:

File Discovery
--------------

Overview
^^^^^^^^

:func:`~access_moppy.file_discovery.discover_files` automatically locates raw
model output files for a CMIP variable given only an archive root directory.
It is used as a fallback by the batch system when no explicit ``file_patterns``
entry is present in the batch config.

Resolution order
^^^^^^^^^^^^^^^^

1. **Per-variable** ``file_pattern`` in the mapping entry — explicit override
   for edge-cases (unusual filenames, legacy layouts, or derived variables
   drawing from multiple file types).
2. **Component-level** ``frequency_patterns`` from ``model_info.file_discovery``
   — the normal path, resolved by substituting ``{model_var}`` from
   ``model_variables`` and globbing under ``input_folder``.
3. :class:`~access_moppy.file_discovery.FileDiscoveryError` — raised when
   neither source provides a pattern; the batch job fails with an actionable
   message.

``file_discovery`` block in ``model_info``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

   "file_discovery": {
     "output_dir_pattern": "output[0-9][0-9][0-9]",
     "components": {
       "atmosphere": {
         "subdir": "atmosphere/netCDF",
         "frequency_patterns": {
           "mon":   "*.pa-*_mon.nc",
           "day":   "*.pe-*_dai.nc",
           "3hr":   "*.pi-*_3hr.nc",
           "6hr":   "*.pj-*_6hr.nc",
           "subhr": "*.pc-*.nc"
         }
       },
       "sea_ice": {
         "subdir": "ice",
         "frequency_patterns": {
           "mon": "iceh-1monthly-mean_*.nc",
           "day": "iceh-1daily-mean_*.nc"
         }
       },
       "ocean": {
         "subdir": "ocean",
         "frequency_patterns": {
           "mon": "ocean-*-{model_var}-1mon-mean-y_*.nc",
           "day": "ocean-*-{model_var}-1day-mean-y_*.nc",
           "yr":  "ocean-*-{model_var}-1yr-mean-y_*.nc",
           "fx":  "ocean-*-{model_var}-fx.nc"
         }
       }
     }
   }

``output_dir_pattern``
   Glob fragment matched against the top-level output sub-directories
   (e.g. ``output000``, ``output001``, …).

``subdir``
   Path relative to the output directory where the component's files live.

``frequency_patterns``
   Dictionary keyed by **frequency token** (``"mon"``, ``"day"``, ``"3hr"``,
   ``"6hr"``, ``"yr"``, ``"fx"``) mapping to a filename glob.

   When the pattern contains ``{model_var}``, it is substituted with each
   entry in the variable's ``model_variables`` list and all results are
   merged — this is how multi-variable derivations (e.g. ocean overturning
   computed from two transport fields) collect all required files.

   When the pattern has **no** ``{model_var}`` placeholder (atmosphere, sea
   ice), a single glob is issued that returns all files for that frequency,
   regardless of which variable is requested.  All variables at that
   frequency are packed into the same file in those components.

``frequency_patterns`` keys
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The key is derived from the CMIP table name of the requested variable:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - CMIP tables that map to it
   * - ``mon``
     - ``Amon``, ``Lmon``, ``Omon``, ``SImon``, ``AERmon``, ``CFmon``, ``Emon``, …
   * - ``day``
     - ``day``, ``Oday``, ``SIday``
   * - ``3hr``
     - ``3hr``, ``E3hr``, ``CF3hr``
   * - ``6hr``
     - ``6hrLev``, ``6hrPlev``, ``6hrPlevPt``
   * - ``subhr``
     - ``1hr``, ``Esubhr``
   * - ``yr``
     - ``Oyr``, ``yr``, ``Eyr``
   * - ``fx``
     - ``fx``, ``Ofx``

Year-based filtering
^^^^^^^^^^^^^^^^^^^^

:func:`~access_moppy.file_discovery.discover_files` accepts optional
``start_year`` and ``end_year`` arguments.  After globbing, files outside the
requested range are removed by parsing the year directly from the filename
— no files are opened, so filtering adds negligible overhead even for large
archives.

The following filename conventions are recognised:

* ``ocean-2d-tos-1mon-mean-y_1850.nc`` → year ``1850``
* ``iceh-1monthly-mean_1850-01.nc`` → year ``1850``
* ``aiihca.pa-185001_mon.nc`` → year ``1850``
* ``tos_mean_ocean_1mon_185001-185012.nc`` → start year ``1850``
  (proposed unified naming scheme)

Adapting to a different file layout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each model version can have its own mapping file with a different
``file_discovery`` block.  To add support for a new file organisation:

1. Create (or copy) a mapping JSON — e.g.
   ``ACCESS-ESM1.6-unified_mappings.json``.
2. Update ``model_info.file_discovery`` with the new ``subdir`` values and
   ``frequency_patterns`` globs.
3. For per-variable overrides (renamed files, unusual paths), add a
   ``file_pattern`` field directly to the variable entry.
4. Point the batch config at the new mapping: ``model_id: ACCESS-ESM1.6-unified``.

Old and new runs can be processed side-by-side; the ``model_id`` in each
batch config selects the correct layout independently.
