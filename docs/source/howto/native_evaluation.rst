Native-grid evaluation of ACCESS-OM3 output
===========================================

MOPPy can prepare selected ACCESS-OM3 fields for evaluation directly from
native MOM6 and CICE output. This is a prototype workflow for monthly means;
it does not write files or perform scientific regridding or vector rotation.
Existing CMOR cache and ESMValTool commands remain available for the CMORized
workflow.

Install the optional dependencies with::

   pip install 'access_moppy[esmval-native]'

The extra provides ESMValCore (>=2.15), scitools-iris, and ncdata. Native
inputs are discovered through the manifest-backed
``access_moppy.esmval.data_source.MoppyDataSource``. The example manifest is
at ``access_moppy/examples/native_evaluation_manifest.json``.

Configure the source in an ESMValTool user configuration (replace the example
manifest location with the path to your own manifest)::

   projects:
     native_om3:
       cmor_table:
         type: esmvalcore.cmor.table.CMIP6Info
         paths: [cmip6/Tables, cmip6-custom]
       preprocessor_filename_template: "{project}_{dataset}_{mip}_{exp}_{ensemble}_{short_name}_{grid}_{version}"
       data:
         moppy:
           type: access_moppy.esmval.data_source.MoppyDataSource
           manifest: /path/to/native_evaluation_manifest.json
           priority: 1

Each manifest record describes one variable and its source files, grid file,
component, run, and native grid. The included two-month CICE record is
illustrative only. Its facet timerange and file names must match the actual
data; do not use it to describe a longer evaluation period.
Use ``YYYY/YYYY`` or ``YYYYMM/YYYYMM`` for its complete monthly coverage.
Combine files for the same variable identity into one manifest record;
separate simulations or grids need distinct facets. Missing or duplicate
months fail before Core preprocessing. Core applies the recipe's requested
time selection after loading.

For direct use in Python, ``access_moppy.evaluation.normalize_native`` takes
an xarray Dataset, the variable's short name, component (``mom6`` or ``cice``),
grid Dataset, and run ID. MOM6 vector fields can also require a supergrid
Dataset. For example::

   import xarray as xr
   from access_moppy.evaluation import normalize_native

   with xr.open_dataset("iceh.2000-01.nc", chunks={}) as native, \
        xr.open_dataset("cice_grid.nc", chunks={}) as grid:
       normalized = normalize_native(
           native,
           short_name="siconc",
           component="cice",
           grid=grid,
           run_id="my-development-run",
           model_id="ACCESS-OM3",
           options={"native_grid": "T"},
       )
       # Consume lazy results while the input datasets remain open.
       monthly_area = (normalized.siconc / 100 * normalized.areacello).sum(
           ("nj", "ni"), min_count=1
       ).compute()

``native_grid`` is explicit: MOM6 supports ``T``, ``Cu``, and ``Cv``; CICE
supports ``T``. Normalization checks finite and missing values and validates
coordinate bounds. It does not rotate vector components or interpolate fields
between grids. Supply monthly-mean input and truthful time bounds. The
native and static CICE coordinates must agree. If native files omit latitude
or longitude, explicitly verify their grid association from the run's
configuration/catalogue, then supply ``grid_identity_verified=True`` and a
``grid_reference`` recording that evidence. Equal array sizes alone are not
accepted as evidence. Missing coordinates on masked land cells are permitted;
wet-cell coordinates and supplied corners must be finite.

The optional ``required_period`` setting accepts a start and end year, such as
``[1986, 2005]``; the default example files are much shorter and do not meet
that period.

The supported MOM6 fields are ``thetao``, ``so``, ``uo``, ``vo``, ``tauuo``,
``hfds``, ``areacello``, and ``volcello``. ``fgco2`` is supported only when
its WOMBATlite source components have been explicitly audited. pH estimation
is opt-in with ``short_name='phos'``, ``allow_pht_estimate=True`` and
``co2_calc='mocsy'``. It computes ``-log10(monthly mean Htotal)``; this is an
estimate and must not be presented as exact monthly mean pHT.

Both BGC paths require ``run_config_reference`` identifying the inspected run
configuration. Carbon additionally requires
``dic_surface_components_verified=True`` and explicit ``parameters`` entries
``dic_global`` (mol kg-1), ``sal_global`` (g kg-1), and
``runoff_dic_mol_kg`` (mol kg-1). The caller must verify that subtracting the
virtual salinity and river DIC contributions leaves air-sea gas exchange for
that run. The API records this provenance; it does not infer a universal
WOMBATlite configuration.

MOM6 raw variable names default to the requested short name, except
``volcello``, which uses ``thkcello`` times the static T-cell area. Override a
raw name with ``native: {variable: <name>}`` in a manifest entry. CICE defaults
to ``aice_m``, ``tarea``, ``TLAT``, ``TLON``, and ``tmask``; its ``native``
mapping accepts the keys ``concentration``, ``area``, ``latitude``,
``longitude``, and ``mask``.

MOM6 ``depth_selection`` can select one scalar depth by ``index``, retain all
centers down to ``maximum`` metres, or retain one level ``nearest_shallower``
than a depth in metres. ``index_selection`` accepts explicit horizontal
dimension slices, for example ``{"yh": [0, 20]}``. These selections preserve
the selected native positions and bounds; they do not interpolate.

MOPPy supplies the normalized fields and their provenance. Scientific
validation of a front or transport diagnostic belongs to the evaluation
recipe using those fields.
