Changelog
=========

This CHANGELOG documents only key changes between versions. For a full description
of all changes see https://github.com/ACCESS-NRI/ACCESS-MOPPy/releases

moppy-v1.7.15b (2026-08-13)
----------------------------

**Batch Compliance Gate**

* **New features**:

  * Add the ``compliance_check`` batch option, which runs the ``cf:1.11`` and matching WCRP compliance-checker suites on the first file a variable writes and stops that variable when it fails, so a broken CMORisation is caught after the first ``source_partition_years`` partition instead of after the full time series. The JSON report is kept in the per-variable script directory, and a failing file is renamed with a ``.compliance_failed`` suffix. Failure severity is controlled by ``compliance_check_min_weight`` (default ``3``, mandatory checks only) and the suites by ``compliance_check_suite`` (default derived from ``cmip_version``, so a CMIP6 batch is never validated against the CMIP7 vocabulary). The report records which suites, enforced weight and esgvoc vocabulary version produced it, and WCRP checks that fail only because the esgvoc vocabulary database is unavailable are reported but not enforced

moppy-v1.7.14b (2026-08-13)
----------------------------

**CMIP7 Parent Metadata & Compound Metadata Fixes**

* **Bug fixes**:

  * Correct CMIP7 `piControl`/`esm-piControl` parent metadata handling so parent requirements follow the CV's `parent_experiment_id` instead of being hardcoded as root experiments, and default parent metadata now points to the correct spin-up experiments (#609)
  * Include the optional CMIP7 `experiment` vocabulary value in global attributes, omitting it when the selected experiment vocabulary does not provide it (#611)
  * Apply CMIP7 compound-name `cell_measures` and `long_name` overrides from the bundled compound-name mappings (#612)

moppy-v1.7.13b (2026-08-11)
----------------------------

**Batch Report Identifiers & CMIP7 Experiment ID Fix**

* **Features**:

  * Include `source_id` and `variant_label` in batch reports alongside `experiment_id` for downstream tooling (#605)

* **Bug fixes**:

  * Remove the case-fuzzy CMIP7 `experiment_id` alias table so lowercase spellings like `esm-picontrol` no longer silently resolve to the distinct `piControl` experiment's metadata (#608)

* **Documentation**:

  * Clarify Gadi/submodule install docs and raise an actionable error when vocabulary submodules are missing (#607)

* **Infrastructure**:

  * Bump `prefix-dev/setup-pixi` from 0.10.0 to 0.10.1 (#606)

moppy-v1.7.12b (2026-08-10)
----------------------------

**Chunking Fix, Recursion Guard & Source Partitioning Scope**

* **Bug fixes**:

  * Guard `CMORiser.__getattr__` against dunder probes and mid-unpickling attribute access that could previously trigger infinite recursion (#601)
  * Batch multiple time steps in `DatasetChunker` under the max chunk-size bound instead of always falling back to one step per task when a single step already meets the minimum target (#603)
  * Skip `source_partition_years` instead of failing for self-contained mappings and for frequencies other than monthly/daily (#604)

* **Performance**:

  * Enable `parallel=True` for `open_mfdataset` reads when a safe single-threaded, multi-process Dask client is active (#601)

moppy-v1.7.11b (2026-08-07)
----------------------------

**Atmosphere/Land Batch Performance**

* **Performance**:

  * Compute `time_bnds` directly from the sliced time coordinate instead of carrying it through from the raw multi-file merge, fixing atmos/land monthly variables timing out on multi-century batch runs (#599)

moppy-v1.7.10b (2026-08-07)
----------------------------

**Source Partitioning & Ocean Scalar Coordinates**

* **Bug fixes**:

  * Skip source file partitioning instead of failing when a variable's calculation type does not support it (#598)
  * Synthesize the `deltasigt` scalar coordinate for `mlotst` so it is no longer missing from ocean output (#597)

moppy-v1.7.9b (2026-08-06)
----------------------------

**Source File Partitioning**

* **Performance**:

  * Add opt-in year-based source file partitioning to build independent Dask graphs for long direct-mapping runs (#596)

moppy-v1.7.8b (2026-08-06)
----------------------------

**Resumable Batches & Active Monitor Updates**

* **Features**:

  * Resume partially completed CMORisation batches from the first unfinished time period (#593)
  * Append variables to an active batch monitor without submitting a second monitor (#594)

* **Bug fixes**:

  * Clear stale timing, error, memory, and PBS metadata when rerunning failed tasks (#595)

* **Infrastructure**:

  * Update the CMIP7 CMOR table submodule pointer (#592)

moppy-v1.7.7b (2026-08-05)
----------------------------

**Large-Batch Scheduling & Publication Throttling**

* **Features**:

  * Add a rolling limit for in-flight variable jobs in large batches (#591)

* **Performance**:

  * Run generated experiments sequentially and throttle publication to shared storage (#590)
  * Batch PBS worker-state queries to reduce scheduler load (#590)

moppy-v1.7.6b (2026-08-05)
----------------------------

**JobFS Finishing, Sea-Ice Grid Labels & Documentation**

* **Bug fixes**:

  * Apply the correct final CMIP7 grid labels to sea-ice output (#589)

* **Performance**:

  * Finish CMIP7 split files on JobFS before publishing them to shared storage (#588)
  * Generate one initial map and one full-period timeseries instead of per-split QC plots (#588)

* **Documentation**:

  * Update the project status and document current ocean and sea-ice CMORisation support (#585)

moppy-v1.7.5b (2026-08-04)
----------------------------

**Bug Fixes**

* **Bug fixes**:

  * Fix CMIP7 sea-ice grid labels (#580)
  * Allow loading files with duplicate non-time coordinate indexes (#581)
  * Fix parent metadata for root experiments (#582)

moppy-v1.7.4b (2026-08-03)
----------------------------

**Adaptive Dask Sizing, QC Staging & Dashboard Fixes**

* **Features**:

  * Add cross-experiment Dask worker-memory calibration and a worker-scaling recommendation tool (#576)
  * Add a reusable batch configuration generator for ACCESS-ESM1-6 ensembles (#578)

* **Bug fixes**:

  * Sort file-pattern glob results to ensure deterministic Dask worker sizing (#576)
  * Show dashboard timestamps in the user's timezone (#579)

* **Performance**:

  * Generate per-file QC plots from jobfs staging to reduce shared filesystem load (#578)

* **Infrastructure**:

  * Update the CMIP7 CMOR tables submodule pointer (#577)

moppy-v1.7.3b (2026-07-29)
----------------------------

**Bug Fixes**

* **Bug fixes**:

  * Fix time_bnds dtype drifting from time coordinate's declared type (#575)

moppy-v1.7.2b (2026-07-29)
----------------------------

**Dashboard Fix, QC Plot Fix & Documentation**

* **Bug fixes**:

  * Fix moppy-tui failures panel scroll keys appearing dead (#574)
  * Fix missing attributes for qc_plots (#573)

* **Documentation**:

  * Major documentation overhaul (#572)

moppy-v1.7.1b (2026-07-29)
----------------------------

**Dask Memory Reporting & Bug Fixes**

* **Features**:

  * Add reporting of Dask worker peak memory usage (#571)

* **Bug fixes**:

  * Fix CMIP7 variables written as double instead of float, causing fill value precision drift (#570)

* **Infrastructure**:

  * Update submodule pointers to latest branch HEADs (#568)

moppy-v1.7.0b (2026-07-28)
----------------------------

**Jobfs Staging, Dashboard Improvements & Reliability Fixes**

* **Features**:

  * Add opt-in jobfs write-staging for batch CMORisation (#567)
  * Add ACCESS experiment metadata to NetCDF outputs (#564)
  * Add fail panel navigation to the CLI dashboard (#566)
  * Add support for hourly frequency ``E1hr`` in CMIP6 table parsing

* **Bug fixes**:

  * Fix driver memory growth during chunked Dask writes (#565)
  * Update monitor loop to handle PBS ``E`` state and improve output logging
  * Disable graph optimization in the CMORiser ``compute`` method
  * Fix timezone handling in the CLI dashboard

moppy-v1.6.12b (2026-07-27)
----------------------------

**Bug Fixes & Testing**

* **Bug fixes**:

  * Treat CMIP7 range QC violations as warnings instead of failures (#561)
  * Preserve daily time resolution for daily ``tasmin`` and ``tasmax`` outputs (#562, #563)

* **Testing**:

  * Make the CMOR integration-test output directory configurable and clean up generated output

moppy-v1.6.11b (2026-07-27)
----------------------------

**Features, Bug Fixes & Performance**

* **Features**:

  * Add input-file completeness checks before CMORisation (#554)
  * Add QC plot support for split output files (#553)

* **Bug fixes**:

  * Fix CMIP7 filename time ranges to use frequency-appropriate precision (#557)
  * Propagate chunk settings to atmosphere, ocean, and sea-ice CMORisers (#559)
  * Fix the ``moppy-tui`` dashboard footer
  * Remove unused ocean variables and update resource settings in the ACCESS-ESM1-6 baseline configuration

* **Performance**:

  * Make CMIP7 QC reductions lazy (#560)
  * Bound Dask spatial chunks and defer CMIP7 compression to ``cmip7repack`` (#555)
  * Prefetch Dask slices during NetCDF writes (#556)
  * Align Dask worker sizing with chunked-write memory requirements (#558)

* **Infrastructure**:

  * Update pixi dependency requirements and PyPI exclusion settings

moppy-v1.6.10b (2026-07-24)
----------------------------

**Features, Bug Fixes & Performance**

* **Features**:

  * CLI: only select specific variables when ``--variable`` is passed (#552)

* **Bug fixes**:

  * Fix handling of missing ``grid_label`` in job scripts and Python driver (#548 follow-up)
  * Change QC range violations to emit warnings instead of raising errors (#550)
  * Improve time-gap error messages with diagnostic detail (#551)

* **Performance**:

  * Parallelise ``cmip7repack`` calls across split output files (#549)

* **Infrastructure**:

  * Update batch configuration: extend walltime to 6 hours and add 1 hr file mapping
  * Update batch configuration: adjust scheduler options and walltime for variable resources

moppy-v1.6.9b (2026-07-22)
---------------------------

**Features**

* **Features**:

  * Resolve CMIP7 grid labels on a per-variable basis and remove ``grid_label`` from CMIP7 metadata configuration (#548)

moppy-v1.6.8b (2026-07-22)
---------------------------

**Features, Bug Fixes & Infrastructure**

* **Features**:

  * Add ``moppy-qc-plots`` CLI for regenerating QC diagnostic plots (#537)
  * Add ``--rerun-variable`` and ``--force`` flags to ``moppy-cmorise`` (#538)
  * Add batch rerun CLI support (#539)
  * Add carbon and soil variables to ACCESS-ESM1-6 baseline batch config example

* **Bug fixes**:

  * Fix QC plots to mask fill values and render them as gray (#543)
  * Fix ``_iter_time_chunks`` to decode numeric CF time and enable ``split_years`` (#544)
  * Fix PBS job configuration and optimise resource allocation for ocean and atmosphere variables

* **Infrastructure**:

  * Remove CMIP7 shims for ACCESS consortium (#547)
  * Update submodule pointers to latest branch HEADs (#536, #540, #546)
  * Bump ``actions/checkout`` from 7.0.0 to 7.0.1 (#541)
  * Bump ``actions/setup-python`` from 6 to 7 (#542)

moppy-v1.6.7b (2026-07-17)
---------------------------

**Bug Fixes & Infrastructure**

* **Bug fixes**:

  * Fix QC plots to process only files written by the current variable (#533)

* **Documentation**:

  * Update README (#532)

* **Infrastructure**:

  * Update submodule pointers to latest branch HEADs (#534)
  * Update GitHub CD workflow

moppy-v1.6.6b (2026-07-16)
---------------------------

**Bug Fixes**

* **Bug fixes**:

  * Fix bug in Jinja CMORisation template

moppy-v1.6.5b (2026-07-16)
---------------------------

**QC Plots & DOI Provenance**

* **Features**:

  * Implement QC plots for per-variable diagnostics (#530)
  * Implement ACCESS-ESM1.5 overlay on QC plots for CMIP6 comparison (#531)
  * Add DOI attribute for ACCESS-MOPPy provenance in CMIP6 and CMIP7 vocabularies

* **Documentation**:

  * Add DOI badge to README

moppy-v1.6.4b (2026-07-16)
---------------------------

**Features & Bug Fixes**

* **Features**:

  * Add functionality to split output files (#529)

* **Bug fixes**:

  * Fix circular import and restore version retrieval
  * Replace deprecated ``input_paths`` with ``input_data`` in template and README (#527)

* **Infrastructure**:

  * Add ``cmip7-repack`` dependency to ``access-moppy-esmval`` package

moppy-v1.6.3b (2026-07-15)
---------------------------

**Bug Fixes & Features**

* **Features**:

  * Add ``--help``/``-h`` flag to ``moppy-cmorise`` CLI (#525)

* **Bug fixes**:

  * Use ``Path(__file__)`` for ``cmor-cvs.json`` to avoid hyphen in package name (#526)

moppy-v1.6.2b (2026-07-15)
---------------------------

**Bug Fixes**

* **Bug fixes**:

  * Add templates ``__init__.py`` and ``*.j2`` glob to package-data (#524)

moppy-v1.6.1b (2026-07-15)
---------------------------

**Bug Fixes, Performance & Batch Workflow Improvements**

* **Features**:

  * Group per-variable log dirs under ``logs/`` and enable DRS by default (#519)
  * Make fixed (fx) fields discoverable: skip discovery for self-contained variables, add fx patterns for atmosphere/land (#515)
  * Speed up batch CMORisation: multi-process Dask, per-variable worker sizing, and skip redundant daily-file validation (#499)
  * Optimise memory allocation in batch processing (#501)

* **Bug fixes**:

  * Fix integer-overflow in ``sftof`` standardisation and log full traceback (#523)
  * Fix missing entries in ``TABLE_TO_FREQ``
  * Clip ``huss`` to zero to remove numerical noise (#521)
  * Fix unit issues for ``thetao`` and other variables
  * Fix monitor task killed due to OOM (#518)
  * Fix three defects in the batch PBS worker template (#509)
  * Fix ``nbp`` mapping formula for ACCESS-ESM1-6 (#502)
  * Add ``cmip7-repack`` to conda package run dependencies (#514)

* **Infrastructure**:

  * Update conda module version to latest in batch configuration files
  * Update submodule pointers to latest branch HEADs (#516, #500)

moppy-v1.6.0b (2026-07-10)
---------------------------

**CMIP7 QC Hardening, Ocean/Time-Axis Reliability & Batch Workflow Enhancements**

* **Features**:

  * Add CMIP7 QC per-variable rules for all 293 ACCESS-ESM1-6 variables with batch report integration (#456)
  * Add ``ilamb_input_format`` option to batch processing (#484)
  * Add static target handling in resampling validation (#494)

* **Bug fixes**:

  * Enforce strict CMOR time-axis integrity checks for sorting, duplicates, and interval continuity (#470)
  * Fix Oyr ocean resampling pipeline (resampling path, yearly time/bounds/filename, calendar) (#463)
  * Align ocean CMOR output with published reference (vertices, bounds, level axis) (#465)
  * Fix chunked indexer failure in ``ocean_floor`` for CMIP7 ``tob`` processing (#486)
  * Restore ``tob`` Kelvin conversion and load bundled ``areacello`` for ACCESS-ESM1-6 ocean/sea-ice mappings (#488, #489)
  * Fix CMIP7 QC output variable selection for vertices auxiliaries and closed-range boundary checks (#485, #495)
  * Fix duplicate timestamps for multi-variable ocean inputs and normalize hybrid ``b_bnds`` ordering (#482, #480)
  * Fix 6hr/3hr frequency handling and missing-value sentinel behavior (#497, #498, #496)

* **Testing / Infrastructure**:

  * Add CMIP7 baseline integration pytest suite and dedicated full-CMORisation test entry points by CMIP generation (#474, #472)
  * Optimize batch report generation and include timestamps in ``batch_report.json`` filenames (#475, #483)
  * Bump ``prefix-dev/setup-pixi`` from 0.9.6 to 0.10.0 (#467)
  * Update submodule pointers to latest branch HEADs (#493, #487, #476, #471, #466, #460, #458)

moppy-v1.5.1b (2026-06-23)
---------------------------

**CMIP7 Data Output & Infrastructure Updates**

* **Features**:

  * Enable DRS output for WCRP full CMOR integration tests (#450)
  * Repack CMIP7 NetCDF outputs (#449)

* **Bug fixes**:

  * Fix casing for ACCESS institution ID in CMIP7 shims

* **Infrastructure**:

  * Remove CMIP7_CVs submodule
  * Bump ``peter-evans/create-pull-request`` from 7 to 8 (#448)
  * Bump ``actions/checkout`` from 6.0.3 to 7.0.0 (#447)

moppy-v1.5.0b (2026-06-23)
---------------------------

**CMIP7 Compliance Updates, Test Data Improvements & Mapping Fixes**

* **Features**:

  * Add variable-level test granularity and CMIP7 support (#443)
  * Update CMIP7 compliance checker fixes (#446)

* **Bug fixes**:

  * Fix ``calculate_differ`` in ACCESS-ESM1-5 mapping
  * Fix ``Oyr.osalttend`` to use yearly ``salt_tendency_expl`` (#440)
  * Fix time units handling for ``tasmax`` and ``tasmin`` when source units contain ``?`` (#439)

* **Testing / Infrastructure**:

  * Use external test data root for integration and end-to-end tests (#442)
  * Update submodule pointers to latest branch HEADs (#445)

moppy-v1.4.1b (2026-06-17)
---------------------------

**Bug Fixes, CI Improvements & Documentation**

* **Features**:

  * Support ``ACCESS-ESM1-6`` as ``source_id`` in CMIP6/CMIP6Plus vocabularies (#436)

* **Bug fixes**:

  * Fix strip leading slash from pattern before ``os.path.join`` in CMOR template (#435)
  * Improve ``_diagnose_no_files`` message when ``start_year``/``end_year`` are unset (#434)

* **Documentation**:

  * Document CMIP7 FastTrack baseline workflow (#431)
  * Derive docs version from package metadata (#430)
  * Merge duplicate batch processing docs (#429)

* **CI / Infrastructure**:

  * Add daily submodule auto-update workflow (#437)
  * Fix submodule auto-update workflow POSIX compatibility and ref issues

moppy-v1.4.0b (2026-06-16)
---------------------------

**CMIP7 Support, Automatic File Discovery & Batch Report Enhancements**

* **CMIP7 / CMIP6Plus support**:

  * Add ``mip-cmor-tables`` backend for CMIP6Plus (CMIP7-ready) (#424)
  * Add temporary CMIP7 ACCESS source shim (#426)
  * Add ACCESS-ESM1-6 CMIP7 Baseline batch config example (#427)
  * Fix MIP table name routing to correct CMORiser (follow-up to #424) (#425)

* **Automatic file discovery**:

  * Add automatic file discovery for CMORisation (#423)

* **Batch reports**:

  * Add durable batch JSON report (#419)
  * Capture structured PBS metadata in batch reports (#421)

* **Bug fixes**:

  * Fix atmosphere and ocean variables workflow geophysical variable fails (#402)
  * Fix ``cl`` and related CMORisation issue (#403)
  * Fix WCRP Geophysical Variable Detection + time bounds for sea-ice (SImon/SIday) (#404)
  * Fix: normalize CF time units to canonical HH:MM:SS for WCRP ATTR004 (#394)
  * Delete redundant ``height_0`` in variable coordinate attributes (#410)
  * Add missing time coordinate attributes (#396)
  * Refactor modeling realm handling to support list and string formats

* **Code quality**:

  * Improve core batch tracking docstrings and types (#417)
  * Improve public docstrings and type annotations (#416)

* **CI / Infrastructure**:

  * Add environment configuration to prevent system Python module conflicts
  * Update pixi version to 0.49.0 in CI workflows (#398)
  * Bump ``codecov/codecov-action`` from 6 to 7 (#422)
  * Bump ``actions/checkout`` from 6.0.2 to 6.0.3 (#415)

moppy-v1.3.0b (2026-05-20)
---------------------------

**Batch Processing Dashboard, ESMValTool Maturation & Mapping Stabilization**

* **Batch processing and CLI dashboard**:

  * Add CLI dashboard for batch processing workflows (#379)
  * Add batch-processing monitor to avoid database status mistracking (#384)
  * Improve SQLite handling and reliability, including Lustre journal mode fix (#367)
  * Improve batch-processing documentation and related module documentation

* **ESMValTool integration**:

  * Add and refine ESMValTool integration support with dedicated tests (#382, #386)
  * Add ``access-moppy-esmval`` package support (#354)

* **Variables, derivations and mappings**:

  * Add ``slthick`` mapping support
  * Fix mapping issues across realms and coordinate attribute handling (#389, #385)
  * Fix redundant ocean coordinates and tile fraction extraction (#358, #366)
  * Fix land derivation logic and improve ``snc`` / nominal resolution calculations (#368, #359)
  * Add helper function to address ``zostoga`` CMORisation issue (#357)

* **Documentation and compliance**:

  * Add compliance testing documentation section (#362)

moppy-v1.2.0b (2026-04-30)
---------------------------

**ESMValTool Integration, New Variables & Bug Fixes**

* **ESMValTool integration**: First prototype of ESMValTool integration via
  ``access_moppy.esmval`` module (#345)
* **New variables**: ``snc`` (LImon, via tile-based snow fraction derivation),
  ``sitimefrac``, ``sisnconc``, ``sisnthick``, ``CFday``, ``SIday`` table support
* **Bug fixes**:

  * Fix nominal resolution calculation logic (#342, #344)
  * Fix ``calc_zostoga``: reference thickness, temperature-dependent alpha,
    optional ``temp_ref`` (#288)
  * Fix ``nep`` and ``npp`` land fraction scaling (#333)
  * Fix ``sftlf`` mapping issue (#336)
  * Fix ``mrsos`` mapping issue (#302)
  * Fix ``mrfso`` mapping issue (#301)
  * Fix units, CF standard names, and calculations in ACCESS-ESM1.6 mappings (#330)
  * Fix inconsistencies in ACCESS-ESM1-6 mappings (#319)
  * Fix time-probe detection issue (#337)
  * Solve data loading issue (#329)

* **Improvements**:

  * Divide ``ra``, ``rh``, ``nbp`` by land fraction for CMIP land-mean compliance (#333)
  * Update ACCESS-ESM1.6 mappings to use degC and improve ``ocean_floor`` calculation (#300)
  * Enhanced logging throughout the codebase for better traceability (#318)
  * Multiple performance and correctness fixes (#317)
  * Warn for non-existent CMIP variable or missing model mapping (#316)
  * Check for newer version on PyPI at import time (#315)

* **Testing & Documentation**:

  * Add integration tests for Ofx variables (#339)
  * Add developer documentation for variable mapping system (#313)
  * Bump ``conda-incubator/setup-miniconda`` from 3 to 4 (#327)

moppy-v1.1.0b (2026-04-24)
---------------------------

**Bug Fixes & Extended Variable Support**

* Numerous bug fixes across atmosphere, ocean, sea-ice, and land components
* Extended variable support: ``tran``, ``hfgeou``, ``msftbarot``, ``sftof``, ``zfull``,
  ``landCoverFrac``, ``tsl``, ``gpp``, ``cl``, ``siconc``, ``hfds``, ``zg``, ``so``,
  ``sos``, ``tasmax``, ``tasmin``, and more
* Add support for CMIP6, CMIP6Plus, and CMIP7 controlled vocabularies simultaneously
* ILAMB workflow: batch processing and softlink generator for evaluation of historical
  runs (see documentation and ``Tutorial_CMORise_ILAMB_Variables.ipynb``)
* Re-enable Python 3.13 support
* Improved unit test coverage for derivation modules
* Documentation improvements

moppy-v1.0.0 (2025-10-27)
--------------------------

**Major Rebranding Release**

* **BREAKING CHANGE**: Rebranded from ACCESS-MOPPeR to access_moppy
* **Package name**: Changed from ``access_mopper`` to ``access_moppy``
* **New versioning**: Reset to v1.0.0 with new tag prefix ``moppy-v``
* **Import changes**: All imports now use ``from access_moppy import ...``
* **Installation**: Now install via ``pip install access_moppy``

This release marks the official rebranding of the package while maintaining
all existing functionality. Please update your imports and installation
commands accordingly.
