CMIP7 FastTrack quick start
===========================

Minimal batch config for CMORising ACCESS-ESM1-6 output to CMIP7 FastTrack.

This page assumes you have read :doc:`index` — it only shows what is specific
to CMIP7. For the full contribution workflow (what to test, how to report
failures) see :doc:`/howto/cmip7_fasttrack_baseline`.

What makes a run "CMIP7"
------------------------

Three things:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Setting
     - Value
   * - ``cmip_version``
     - ``CMIP7``
   * - ``source_id``
     - ``ACCESS-ESM1-6``
   * - variable names
     - CMIP7 **branded names**, e.g. ``atmos.tas.tavg-h2m-hxy-u.mon.glb``

CMIP7 replaces the CMIP6 ``table.variable`` form (``Amon.tas``) with a branded
name that spells out realm, variable, cell methods, frequency, and domain::

    atmos . tas . tavg-h2m-hxy-u . mon . glb
    │       │     │                │     └─ domain (global)
    │       │     │                └─ frequency
    │       │     └─ time mean, 2 m height, horizontal x/y, unmasked
    │       └─ variable
    └─ realm

You do not need to memorise these. The maintained baseline config lists them
all — copy the lines you want out of it (see below).

Note that ``grid_label`` is left out: for CMIP7 MOPPy resolves it per variable
from the model mapping.

Minimal config
--------------

.. code-block:: yaml

   # cmip7_test.yml

   variables:
     - atmos.tas.tavg-h2m-hxy-u.mon.glb
     - atmos.pr.tavg-u-hxy-u.mon.glb
     - ocean.tos.tavg-u-hxy-sea.mon.glb

   cmip_version: CMIP7
   experiment_id: historical
   source_id: ACCESS-ESM1-6
   variant_label: r1i1p1f1
   activity_id: CMIP

   # Describes what this run branched from. Required for publication;
   # set branch_time_in_parent to the real branch day for your run.
   parent_info:
     parent_experiment_id: piControl
     parent_activity_id: CMIP
     parent_source_id: ACCESS-ESM1-6
     parent_variant_label: r1i1p1f1
     parent_time_units: "days since 0001-01-01 00:00:00"
     parent_mip_era: CMIP7
     branch_time_in_child: 0.0
     branch_time_in_parent: 0.0
     branch_method: standard

   input_folder: "/g/data/<project>/archive/CMIP7/ACCESS-ESM1-6/historical/<run>"
   output_folder: "/scratch/<project>/<user>/cmor/cmip7_test"

   # Keep the first run short while you check the output.
   start_year: 1850
   end_year: 1850

   queue: "normalbw"
   cpus_per_node: 12
   mem: "64GB"
   jobfs: 100GB
   walltime: "06:00:00"
   scheduler_options: "#PBS -P <project>"
   storage: "gdata/p73+gdata/xp65+scratch/<project>"

   worker_init: |
     module use /g/data/xp65/public/modules
     module load conda/analysis3

Run and monitor it exactly as in :doc:`index`:

.. code-block:: bash

   module use /g/data/xp65/public/modules
   module load conda/analysis3
   moppy-cmorise cmip7_test.yml
   moppy-tui --db /scratch/<project>/<user>/cmor/cmip7_test/cmor_tasks.db

Scaling up to the baseline
--------------------------

Once a small run works, start from the maintained baseline configuration
rather than writing the variable list by hand:

.. code-block:: bash

   cp src/access_moppy/examples/batch_config_esm1-6_cmip7_baseline.yml baseline.yml

It contains the full FastTrack baseline variable list (with unmapped variables
commented out and marked), plus per-variable PBS resource overrides — 3-D
pressure-level and sub-daily fields need considerably more memory and walltime
than 2-D monthly ones:

.. code-block:: yaml

   variable_resources:
     atmos.ta.tavg-p19-hxy-air.mon.glb:
       cpus_per_node: 7
       mem: "128GB"
       walltime: "12:00:00"

Other useful starting points bundled with the package:

- ``batch_config_esm1-6_cmip7_daily_one_year.yml`` — daily, one-year resource test
- ``batch_config_esm1-6_cmip7_3hourly_one_year.yml`` — 3-hourly
- ``batch_config_esm1-6_cmip7_6hourly_one_year.yml`` — 6-hourly
- ``batch_config_esm1-6_cmip7_hourly_pr_one_year.yml`` — hourly precipitation

Next steps
----------

- :doc:`/howto/cmip7_fasttrack_baseline` — the full contribution guide, and how
  to report mapping gaps and failures
- :doc:`/howto/batch_processing` — resource tuning and error recovery
- :doc:`/reference/configuration` — every configuration key
