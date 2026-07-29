CMIP7 FastTrack Baseline Guide
==============================

This page is a practical starting point for people contributing to the
CMORisation of ACCESS-ESM1-6 runs for the CMIP7 FastTrack baseline variables.
It is intentionally focused on the common NCI Gadi workflow rather than every
possible deployment pattern.

ACCESS-MOPPy is still an alpha tool and the ACCESS to CMIP7 mappings are still
under review. Treat this workflow as contribution and verification work, not as
the final publication path.

.. contents:: Table of Contents
   :local:
   :depth: 2

Who This Is For
---------------

Use this guide if you want to:

- start checking or extending CMORisation coverage for ACCESS-ESM1-6
- run a subset of FastTrack baseline variables on NCI Gadi
- capture failures clearly so they can be fixed in mappings, derivations, or
  metadata handling

If you need the full batch configuration reference, use :doc:`/howto/batch_processing`.
If you want a broader introduction to the Python API, start with
:doc:`/tutorials/getting_started`.

What To Prepare On Gadi
-----------------------

Before you start, make sure you have:

- an NCI account with access to the relevant ACCESS archive and output projects
- write access to a scratch area for generated NetCDF files, logs, and tracker
  databases
- access to the ``xp65`` module path used to load the packaged
  ``conda/analysis3`` environment
- the location of the ACCESS-ESM1-6 run you want to work on, usually under a
  Payu archive tree on ``/g/data``

A typical module setup on Gadi is:

.. code-block:: bash

   module use /g/data/xp65/public/modules
   module load conda/analysis3-26.04

This gives you access to ``moppy-cmorise`` and ``moppy-tui`` in the managed
analysis environment used by the project examples.

CMIP7-Focused Working Conventions
---------------------------------

For FastTrack baseline work in ACCESS-MOPPy:

- use ``cmip_version: CMIP7``
- use CMIP7 compound names such as
  ``atmos.tas.tavg-h2m-hxy-u.mon.glb``
- keep ``source_id: ACCESS-ESM1-6`` unless you are explicitly working on a
  different source entry
- start from the maintained example config at
  ``src/access_moppy/examples/batch_config_esm1-6_cmip7_baseline.yml``

That example file already contains:

- the current baseline variable list
- comments marking variables that are known to be unmapped
- Gadi-oriented ``worker_init`` settings

For most ACCESS-ESM1-6 baseline work, you should use the built-in file finder,
not manual ``file_patterns`` entries. The preferred workflow is:

- set ``input_folder`` to the run archive root
- let ACCESS-MOPPy use the mapping file's ``file_discovery`` configuration
- add ``file_patterns`` only if the run layout is unusual or you are debugging a
  discovery problem

Recommended First Contribution Loop
-----------------------------------

When you are new to a run or a variable family, avoid launching the full
baseline list first. A small, well-documented test run is much easier to debug.

1. Copy the example configuration to your working area.
2. Reduce ``variables:`` to a small subset, ideally 1 to 5 variables from one
   realm.
3. Remove or comment out ``file_patterns`` so the file finder is the active
   path.
4. Update ``input_folder`` and ``output_folder``.
5. Confirm ``experiment_id``, ``variant_label``, and any project-specific PBS
   settings.
6. Submit and monitor the batch.
7. Record what passed, what failed, and what needs follow-up.

Example:

.. code-block:: bash

   cp src/access_moppy/examples/batch_config_esm1-6_cmip7_baseline.yml \
      baseline_test.yml

Then edit ``baseline_test.yml`` so it contains only a small starter set, for
example:

.. code-block:: yaml

   variables:
     - atmos.tas.tavg-h2m-hxy-u.mon.glb
     - atmos.pr.tavg-u-hxy-u.mon.glb
     - atmos.rsds.tavg-u-hxy-u.mon.glb

   cmip_version: CMIP7
   experiment_id: historical
   source_id: ACCESS-ESM1-6
   variant_label: r1i1p1f1
   grid_label: gn
   activity_id: CMIP

   input_folder: "/g/data/<project>/archive/CMIP7/ACCESS-ESM1-6/historical/<run>"
   output_folder: "/scratch/<project>/<user>/cmor/historical_test"

   worker_init: |
     module use /g/data/xp65/public/modules
     module load conda/analysis3-26.04

In that starter config, leave ``file_patterns`` out entirely unless you already
know the file finder does not match your archive layout. The file finder uses
the ACCESS-ESM1-6 mapping metadata to discover the expected files from
``input_folder`` automatically.

Run It On NCI Gadi
------------------

From a Gadi login node:

.. code-block:: bash

   module use /g/data/xp65/public/modules
   module load conda/analysis3-26.04
   moppy-cmorise baseline_test.yml

This will:

- create a tracker database in ``output_folder/cmor_tasks.db``
- generate PBS job scripts
- submit one worker job per variable
- start the monitor workflow

Monitor Progress
----------------

For quick checks over SSH, the terminal dashboard is usually the easiest:

.. code-block:: bash

   moppy-tui --db /scratch/<project>/<user>/cmor/historical_test/cmor_tasks.db

Useful follow-up commands:

.. code-block:: bash

   moppy-tui --status failed,running --db /scratch/<project>/<user>/cmor/historical_test/cmor_tasks.db
   moppy-batch-report --db /scratch/<project>/<user>/cmor/historical_test/cmor_tasks.db

Use :doc:`/howto/batch_processing` for the Streamlit dashboard, JSON reporting, and
more detailed monitoring options.

What To Look For
----------------

Common outcomes in early FastTrack baseline work include:

- missing mapping entries for a CMIP7 compound name
- file finder mismatches because the run layout differs from the expected
  ACCESS-ESM1-6 archive structure
- metadata mismatches such as invalid vocabulary values
- derivation problems for variables computed from multiple native fields
- memory or walltime limits that are too small for a specific variable

Useful checks after a run:

- confirm whether output NetCDF files were written
- inspect the relevant ``.out`` and ``.err`` files in the generated job-script
  directory
- inspect ``cmor_tasks.db`` with ``moppy-tui`` or ``moppy-batch-report``
- if a variable completed, spot-check the output metadata and dimensions

Reporting Issues Clearly
------------------------

Please report problems as GitHub issues so they can be tracked centrally:

`ACCESS-MOPPy issue tracker <https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues>`_

Include the following in each report:

- the CMIP7 compound name, for example
  ``atmos.tas.tavg-h2m-hxy-u.mon.glb``
- the ACCESS run context:
  ``source_id``, ``experiment_id``, ``variant_label``, and a short run label
- whether the issue is on Gadi and which module environment you used
- whether you were using the default file finder or a manual ``file_patterns``
  override
- whether the failure is a missing mapping, a runtime failure, a metadata
  problem, or an unexpected scientific result
- the smallest config snippet needed to reproduce the problem
- the relevant traceback or error message from the worker ``.err`` file
- the path to the generated batch report or the key fields copied from it
- whether the same issue appears for one variable only or for a whole group

Good issue titles are specific, for example:

- ``CMIP7 FastTrack: atmos.rsds.tavg-u-hxy-u.mon.glb fails on ACCESS-ESM1-6 historical r1i1p1f1``
- ``CMIP7 FastTrack: ocean.zos.tavg-u-hxy-sea.day.glb missing mapping in ACCESS-ESM1-6 baseline example``

If you are unsure whether something is a code bug or a mapping gap, still
report it. The important thing is to preserve enough context for someone else
to reproduce it.

Suggested Workflow For Teams
----------------------------

When several people are helping with baseline coverage, it usually works well
to split work by realm or frequency, for example:

- atmosphere monthly
- atmosphere daily and sub-daily
- land and land-ice
- ocean
- sea ice

Keep one issue per distinct problem rather than one large running list. That
makes it much easier to link fixes, tests, and validation checks back to the
original failure.

Where To Go Next
----------------

- Use :doc:`/howto/batch_processing` for the full PBS and monitoring reference.
- Use :doc:`/tutorials/getting_started` if you want to inspect a single variable through
  the Python API before moving back to batch mode.
- Use :doc:`/reference/mapping_reference` when you need to understand how file discovery
  and model-variable mappings are defined.
