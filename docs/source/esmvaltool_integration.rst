ESMValTool Integration
======================

This guide explains how to use ACCESS-MOPPy as a transparent CMORisation
pre-processor for `ESMValTool <https://www.esmvaltool.org/>`_ and
`ESMValCore <https://github.com/ESMValGroup/ESMValCore>`_.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
--------

ESMValTool assumes that the data it reads is already in CMIP-compliant
format.  Raw ACCESS-ESM1.6 output uses UM STASH codes, MOM5 variable
names, and a non-standard directory structure, so ESMValTool cannot use it
directly.

The ``access_moppy.esmval`` subpackage bridges this gap **without
modifying ESMValCore or ESMValTool**.  It:

1. Parses an ESMValTool recipe YAML to find which variables and time ranges
   are needed.
2. Locates the corresponding raw ACCESS-ESM1.6 files on disk.
3. Runs ACCESS-MOPPy's CMORisation pipeline and writes CMIP DRS-structured
   NetCDF output to a local cache directory.
4. Generates an ESMValCore 2.14+ ``LocalDataSource`` config file and places
   it in the ESMValCore user config directory
   (``~/.config/esmvaltool/``) so ESMValTool finds the data automatically
   — no ``--config`` flag required.
it is simply reading well-formed CMIP6 data.

Architecture
------------

.. code-block:: text

   ┌──────────────────────────────────────────────────────────────┐
   │  User runs:  moppy-esmval-run my_recipe.yml                  │
   └──────────────────────────┬───────────────────────────────────┘
                              │
             ┌────────────────▼──────────────────┐
             │  CMORiseOrchestrator               │
             │                                   │
             │  1. Parse recipe YAML             │
             │  2. Extract ACCESS-ESM1-6 datasets│
             │  3. Map (mip, short_name) →       │
             │     compound_name                 │
             │  4. Locate raw ACCESS files       │
             │  5. Run ACCESS_ESM_CMORiser       │
             │     (skip if cached & current)    │
             │  6. Write CMIP DRS output tree    │
             └────────────────┬──────────────────┘
                              │
             ┌────────────────▼──────────────────┐
             │  ~/.config/esmvaltool/            │
             │  moppy-esmval-data.yml             │
             │                                   │
             │  projects:                        │
             │    CMIP6:                         │
             │      data:                        │
             │        moppy-cache:               │
             │          type: LocalDataSource    │
             │          rootpath: ~/.cache/…     │
             └────────────────┬──────────────────┘
                              │ (auto-loaded by ESMValCore)
             ┌────────────────▼──────────────────┐
             │  esmvaltool run my_recipe.yml      │
             │  (no --config flag needed)         │
             └───────────────────────────────────┘

Installation
------------

Install ACCESS-MOPPy with the ESMValTool integration extras::

    pip install "access_moppy[esmval]"

This pulls in ``esmvalcore>=2.14`` as an optional dependency.  ESMValTool
itself is not strictly required (only ESMValCore is needed to run recipes
using the generated config overlay); install it separately if needed::

    pip install ESMValTool

Quick Start
-----------

**Step 1 — Prepare a recipe**

Write or obtain a normal ESMValTool recipe that references
``dataset: ACCESS-ESM1-6`` and ``project: CMIP6``:

.. code-block:: yaml

   # my_recipe.yml
   documentation:
     title: ACCESS-ESM1-6 surface temperature example
     description: Minimal recipe demonstrating ACCESS-MOPPy ESMValTool integration.
     authors:
       - anonymous

   datasets:
     - {dataset: ACCESS-ESM1-6, project: CMIP6, exp: historical,
        ensemble: r1i1p1f1, grid: gn, timerange: '2000/2005'}

   diagnostics:
     temperature_bias:
       variables:
         tas:
           mip: Amon
       scripts:
         plot:
           script: examples/plot_map.py

**Step 2 — CMORise and run (two-step)**

.. code-block:: bash

   # CMORise required data and write ESMValCore config
   # (written to ~/.config/esmvaltool/moppy-esmval-data.yml automatically):
   moppy-esmval-prepare my_recipe.yml \
       --input-root /g/data/p73/archive/.../MyRun \
       --cache-dir ~/.cache/moppy-esmval

   # Run ESMValTool — config is picked up automatically, no --config flag:
   esmvaltool run my_recipe.yml

**Step 3 — Or use the one-step wrapper**

.. code-block:: bash

   moppy-esmval-run my_recipe.yml \
       --input-root /g/data/p73/archive/.../MyRun \
       --cache-dir ~/.cache/moppy-esmval

This is equivalent to running both commands above sequentially.

**Step 4 — Or via the esmvaltool CLI**

Once ACCESS-MOPPy is installed alongside ESMValCore, it registers a new
sub-command on the ``esmvaltool`` executable::

   esmvaltool cmorise my_recipe.yml \
       --input-root /g/data/p73/archive/.../MyRun \
       --cache-dir ~/.cache/moppy-esmval

   esmvaltool run my_recipe.yml

Command Reference
-----------------

``moppy-esmval-prepare``
~~~~~~~~~~~~~~~~~~~~~~~~

CMORise the data required by a recipe without invoking ESMValTool.

.. code-block:: text

   usage: moppy-esmval-prepare [-h]
       RECIPE
       --input-root PATH
       --cache-dir PATH
       [--model-id ID]
       [--config FILE]
       [--output-config FILE]
       [--workers N]
       [--dry-run]
       [--pattern COMPOUND_NAME:GLOB]
       [-v]

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Argument
     - Description
   * - ``RECIPE``
     - Path to the ESMValTool YAML recipe (required).
   * - ``--input-root``
     - Root directory of the raw ACCESS-ESM1.6 archive (required).
   * - ``--cache-dir``
     - Directory where CMORised files will be written in CMIP DRS structure (required).
   * - ``--model-id``
     - ACCESS-MOPPy model identifier. Default: ``ACCESS-ESM1.6``.
   * - ``--config``
     - Path to any existing file in the user's ESMValCore config directory.
       The MOPPy data-source file is written into the same directory.
   * - ``--output-config``
     - Where to write the generated ESMValCore data-source config file
       (default: ``~/.config/esmvaltool/moppy-esmval-data.yml``).
   * - ``--workers``
     - Number of parallel CMORisation workers. Default: ``1``.
   * - ``--dry-run``
     - Log what would be done without running CMORisation.
   * - ``--pattern``
     - Raw-file glob pattern override for a specific variable, e.g.
       ``Amon.tas:/output*/atmosphere/netCDF/*mon.nc``. Can be repeated.
   * - ``-v / --verbose``
     - Enable DEBUG-level logging.

``moppy-esmval-run``
~~~~~~~~~~~~~~~~~~~~

CMORise and immediately invoke ``esmvaltool run``.  Accepts all the same
arguments as ``moppy-esmval-prepare`` plus:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Argument
     - Description
   * - ``--esmvaltool-args``
     - Extra arguments forwarded verbatim to ``esmvaltool run`` (quoted string).

``esmvaltool cmorise``
~~~~~~~~~~~~~~~~~~~~~~

Registered via the ``esmvaltool_commands`` entry-point group.  Performs
the same preparation step as ``moppy-esmval-prepare``.  All parameters
are the same, passed as keyword arguments because the ``esmvaltool`` CLI
uses Google `fire <https://github.com/google/python-fire>`_ for dispatch.

Python API
----------

All components are importable directly for use in scripts and notebooks:

.. code-block:: python

   from access_moppy.esmval import CMORiseOrchestrator, RecipeReader, VariableIndex

   # Parse recipe
   reader = RecipeReader("my_recipe.yml")
   print(reader.tasks)   # list of CMORTask objects

   # Check which variables are supported
   idx = VariableIndex()
   print(idx.is_supported("Amon", "tas"))  # True

   # Run orchestrator
   orch = CMORiseOrchestrator(
       input_root="/g/data/p73/archive/.../MyRun",
       cache_dir="~/.cache/moppy-esmval",
   )
   results = orch.prepare_recipe("my_recipe.yml")
   CMORiseOrchestrator.summarise(results)

   # Write ESMValCore 2.14+ config (placed in ~/.config/esmvaltool/ by default)
   from access_moppy.esmval.config_gen import write_esmval_config
   cfg = write_esmval_config("~/.cache/moppy-esmval")
   print(f"Config written to: {cfg}")
   # esmvaltool run my_recipe.yml   # no --config flag needed

File Pattern Overrides
----------------------

By default the file finder uses broad component-level glob patterns
relative to ``--input-root``.  When the default patterns do not match
your archive layout you can override them per variable:

.. code-block:: bash

   moppy-esmval-prepare my_recipe.yml \
       --input-root /data/archive/MyRun \
       --cache-dir ~/.cache/moppy-esmval \
       --pattern "Amon.tas:/output[0-4]*/atmosphere/netCDF/*mon.nc" \
       --pattern "Omon.tos:/output[0-4]*/ocean/ocean-2d-surface_temp*.nc"

Or in Python:

.. code-block:: python

   orch = CMORiseOrchestrator(
       input_root="/data/archive/MyRun",
       cache_dir="~/.cache/moppy-esmval",
       pattern_overrides={
           "Amon.tas": "output[0-4]*/atmosphere/netCDF/*mon.nc",
           "Omon.tos": "output[0-4]*/ocean/ocean-2d-surface_temp*.nc",
       },
   )

The default patterns for each component are:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Component
     - Default glob patterns (relative to ``--input-root``)
   * - ``atmosphere``, ``land``, ``aerosol``
     - ``output[0-9]*/atmosphere/netCDF/*mon.nc`` (plus ``*dai.nc``, ``*3hr.nc``, ``*6hr.nc``)
   * - ``ocean``
     - ``output[0-9]*/ocean/ocean-{1,2,3}d-*.nc``
   * - ``sea_ice``
     - ``output[0-9]*/ice/iceh-1monthly-mean*.nc``

Caching
-------

The orchestrator caches results: if a CMORised file already exists under
``--cache-dir`` *and* is newer than all raw input files, the variable is
skipped and reported as ``"cached"``.  To force re-CMORisation, either
delete the relevant files from the cache or use ``--dry-run`` first to
inspect what is cached.

HPC Usage (NCI Gadi)
---------------------

On NCI Gadi the raw ACCESS-ESM1.6 output typically lives under
``/g/data/p73/archive/`` or ``/g/data/access/ACCESS-ESM1-6/``.  A typical
workflow uses the ``--workers`` flag together with a pre-existing PBS
interactive session:

.. code-block:: bash

   # Inside a PBS interactive session or a Gadi login node
   moppy-esmval-run my_recipe.yml \
       --input-root /g/data/p73/archive/CMIP7/ACCESS-ESM1-6/.../MyRun \
       --cache-dir /scratch/tm70/$USER/moppy-esmval-cache \
       --workers 8 \
       --esmvaltool-args "--max-parallel-tasks 4"

For very large variable sets, combine with the existing ``moppy-cmorise``
PBS batch system to CMORise first, then run ESMValTool against the output:

.. code-block:: bash

   # 1. CMORise with PBS (schedules a job per variable)
   moppy-cmorise my_batch_config.yml

   # 2. Once jobs finish, point ESMValTool at the output
   moppy-esmval-prepare my_recipe.yml \
       --input-root /g/data/.../MyRun \
       --cache-dir /scratch/tm70/$USER/moppy-output \
       --dry-run   # nothing to CMORise — will just write config file

   esmvaltool run my_recipe.yml

Troubleshooting
---------------

**"No supported ACCESS-ESM datasets found in recipe"**

  Check that your recipe includes ``project: CMIP6`` and
  ``dataset: ACCESS-ESM1-5`` or ``dataset: ACCESS-ESM1-6`` in the datasets block.

**"No raw files found for 'Amon.xxx'"**

  The default glob patterns may not match your archive layout.  Use
  ``--pattern`` to override, and add ``-v`` to see the exact paths being
  searched.

**"No mapping found for 'Amon.xxx'"**

  The variable is not yet supported in the ACCESS-ESM1.6 mapping file.
  Check ``access_moppy.esmval.VariableIndex().all_compound_names()`` for
  the full list of supported variables.

**ESMValTool cannot find the CMORised data**

  Verify that ``--cache-dir`` in the prepare step matches the ``rootpath``
  in ``~/.config/esmvaltool/moppy-esmval-data.yml``.  If you wrote the
  config file to a non-default location, set the ``ESMVALTOOL_CONFIG_DIR``
  environment variable to that directory before calling
  ``esmvaltool run``.

API Reference
-------------

.. autoclass:: access_moppy.esmval.recipe_reader.RecipeReader
   :members:
   :undoc-members:

.. autoclass:: access_moppy.esmval.recipe_reader.CMORTask
   :members:

.. autoclass:: access_moppy.esmval.variable_mapper.VariableIndex
   :members:
   :undoc-members:

.. autoclass:: access_moppy.esmval.file_finder.RawFileFinder
   :members:
   :undoc-members:

.. autoclass:: access_moppy.esmval.orchestrator.CMORiseOrchestrator
   :members:
   :undoc-members:

.. autofunction:: access_moppy.esmval.config_gen.write_esmval_config

.. autofunction:: access_moppy.esmval.config_gen.merge_into_existing_config
