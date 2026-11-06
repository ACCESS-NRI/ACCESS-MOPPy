Native-grid evaluation of ACCESS-OM3 output
===========================================

The manifest-backed ``access_moppy.esmval.data_source.MoppyDataSource`` loads
selected native ACCESS-OM3 files for ESMValCore. For every request it runs the
standard ``ACCESS_ESM_CMORiser`` pipeline with MOPPy's official CMIP variable
and axis tables and the relevant MOM6 or CICE component CMORiser, then
converts the result to Iris cubes in memory. It does not run a separate
normalization path or create an intermediate CMOR file. The existing CMOR
cache workflow remains available.

Install the optional dependencies with::

   pip install 'access_moppy[esmval-cmor]'

This extra includes ESMValCore (>=2.15), scitools-iris, and ncdata. Configure
the project in ESMValTool, using the official CMIP6 tables and your custom
table directory where needed::

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

The manifest example is at ``access_moppy/examples/native_evaluation_manifest.json``.
It shows a two-month CICE ``siconc`` record and a local CMIP6 project identity.
Its data paths are illustrative: replace them, the run identity, and the grid
reference with values for the actual experiment. The example data files are
not included. ``cmoriser`` must provide ``experiment_id``, ``source_id``, and
``variant_label``. It may also set ``model_id``, ``cmip_version`` (``CMIP6``
by default), and ``project_cv``. The CV addition supplies local experiment and
source identities while leaving official CMIP variable and axis definitions
unchanged. See the example manifest for the project CV shape and required identity
metadata.

Use actual monthly mean files with explicit time bounds. A monthly manifest
timerange must declare complete coverage (``YYYY/YYYY`` or ``YYYYMM/YYYYMM``),
and the file intervals must match it exactly. Combine files for one variable
identity in one entry; give distinct simulations or grids distinct facets.
Missing or duplicate months fail before Core preprocessing. Core applies the
recipe's requested time selection after loading. MOM6 supports native ``T``,
``Cu``, and ``Cv`` grids; CICE uses ``T``. The adapter passes the manifest's
``grid_file`` and optional ``supergrid_file`` as geometry to the component
CMORiser. If CICE files omit latitude and longitude, first verify their grid
association from the run configuration or catalogue, then record
``grid_identity_verified: true`` and a descriptive ``grid_reference`` in the
entry. Matching array sizes alone is insufficient evidence.

Grid aliases are interpreted within the declared ``component``: ``T`` means
MOM6 ``yh``/``xh`` for ``mom6`` and CICE ``nj``/``ni`` for ``cice``. The explicit
aliases ``mom6-t``, ``mom6-u``, ``mom6-v`` and ``cice-t`` are also accepted.
Optional ``index_selection`` entries use native dimension names and Python
slice lists, for example ``{"nj": [0, 10], "ni": [20, 30]}`` for CICE. These
subsets are applied after CMORisation. Unknown dimensions, incompatible grid
aliases, and malformed slices fail during manifest discovery.

The table-driven field set includes CICE ``siconc`` and MOM6 ``thetao``,
``so``, ``uo``, ``vo``, ``tauuo``, ``hfds``, ``areacello``, and ``volcello``.
MOM6 ``fgco2`` is available through its mapped calculation only after the
run's WOMBATlite source components and assumptions have been audited; pass
any run-specific calculation parameters using ``calculation_kwargs`` in the
manifest. There is no pH mapping: monthly mean total hydrogen cannot produce
an exact monthly mean pH.

MOM6 corner bounds come from the explicitly supplied model supergrid, whose
cell centres are checked against the native static grid. CICE uses its native
four-corner bounds or a verified supergrid. Scientific regridding, vector
rotation, and validation of front or transport diagnostics remain the
responsibility of the evaluation recipe.

Validate a new archive using its actual grids, calendar and run parameters.
The automated acceptance tests compare in-memory results with written
CMORiser output, including masks, units, coordinate bounds and calendars.
Recipe execution and scientific agreement with independent raw-data
calculations are separate checks; neither follows from valid metadata alone.
