Changelog
=========

This CHANGELOG documents only key changes between versions. For a full description
of all changes see https://github.com/ACCESS-NRI/ACCESS-MOPPy/releases

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
