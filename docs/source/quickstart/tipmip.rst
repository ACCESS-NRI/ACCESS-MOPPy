TIPMIP quick start
==================

Minimal batch config for CMORising ACCESS output for TIPMIP (the Tipping Point
Model Intercomparison Project).

This page assumes you have read :doc:`index` — it only shows what is specific
to TIPMIP.

TIPMIP is CMIP6Plus
-------------------

.. important::

   TIPMIP lives in the **CMIP6Plus** controlled vocabularies, not CMIP6. The
   TIPMIP activity and its ``esm-up2p0*`` experiments simply do not exist in
   the CMIP6 CVs, so a config left on the default ``cmip_version: CMIP6``
   fails immediately with "Experiment ... not found in controlled
   vocabularies".

So a TIPMIP config is a CMIP6Plus config (see :doc:`cmip6plus`) with two
changes:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Setting
     - Value
   * - ``cmip_version``
     - ``CMIP6Plus``
   * - ``activity_id``
     - ``TIPMIP``
   * - ``experiment_id``
     - one of the 23 ``esm-up2p0*`` experiments

TIPMIP experiments
------------------

The experiment names encode the ramp-up/stabilise/ramp-down protocol. All of
them branch, directly or indirectly, from ``esm-up2p0``:

.. list-table::
   :header-rows: 1
   :widths: 45 25 30

   * - ``experiment_id``
     - Branches from
     - Meaning
   * - ``esm-up2p0``
     - ``esm-piControl``
     - the 2 %/yr ramp-up itself
   * - ``esm-up2p0-gwl1p5``, ``-gwl2p0``, ``-gwl3p0``, ``-gwl4p0``, ``-gwl5p0``, ``-gwl6p0``
     - ``esm-up2p0``
     - stabilisation at a global warming level
   * - ``esm-up2p0-gwl2p0-50y-dn2p0`` and similar
     - the matching ``-gwlN`` run
     - ramp-down after N years of stabilisation

List the full set for the version you have installed with:

.. code-block:: bash

   python -c "import json, access_moppy, pathlib; \
   p = pathlib.Path(access_moppy.__file__).parent / 'vocabularies/CMIP6Plus_CVs'; \
   cv = json.loads((p / 'CMIP6Plus_experiment_id.json').read_text())['experiment_id']; \
   print('\n'.join(k for k, v in cv.items() if 'TIPMIP' in str(v.get('activity_id'))))"

Set ``parent_experiment_id`` to whatever the run actually branched from — the
"Branches from" column above is the protocol default, not a guarantee about
your run.

Minimal config
--------------

.. code-block:: yaml

   # tipmip_test.yml

   variables:
     - Amon.tas
     - Amon.pr
     - Omon.thetao
     - Oyr.o2

   cmip_version: CMIP6Plus
   experiment_id: esm-up2p0
   source_id: ACCESS-ESM1-5
   variant_label: r1i1p1f1
   grid_label: gn
   activity_id: TIPMIP

   # source_id is not ACCESS-ESM1-6, so point file discovery at the right model.
   model_id: ACCESS-ESM1-5

   parent_info:
     parent_experiment_id: esm-piControl
     parent_activity_id: CMIP
     parent_source_id: ACCESS-ESM1-5
     parent_variant_label: r1i1p1f1
     parent_time_units: "days since 0001-01-01 00:00:00"
     parent_mip_era: CMIP6Plus
     branch_time_in_child: 0.0
     branch_time_in_parent: 0.0     # replace with the actual branch day
     branch_method: standard

   input_folder: "/g/data/<project>/archive/<run>"
   output_folder: "/scratch/<project>/<user>/cmor/tipmip_test"

   # TIPMIP runs are long — restrict the year range while testing.
   start_year: 111
   end_year: 120

   queue: "normal"
   cpus_per_node: 12
   mem: "64GB"
   jobfs: 100GB
   walltime: "02:00:00"
   scheduler_options: "#PBS -P <project>"
   storage: "gdata/p73+gdata/xp65+scratch/<project>"

   # 3-D ocean fields need noticeably more than the 2-D defaults.
   variable_resources:
     Omon.thetao:
       cpus_per_node: 24
       mem: "190GB"
       walltime: "04:00:00"

   worker_init: |
     module use /g/data/xp65/public/modules
     module load conda/analysis3

Run and monitor it exactly as in :doc:`index`:

.. code-block:: bash

   module use /g/data/xp65/public/modules
   module load conda/analysis3
   moppy-cmorise tipmip_test.yml
   moppy-tui --db /scratch/<project>/<user>/cmor/tipmip_test/cmor_tasks.db

Things that catch people out
----------------------------

**Mixed model components.** TIPMIP batches often cover atmosphere variables
from one configuration and ocean/BGC variables from another. ``source_id`` and
``experiment_id`` are single values per config file, so split those into
separate config files rather than trying to express both in one.

**Long runs.** TIPMIP experiments run for centuries. Use ``start_year`` /
``end_year`` to test on a decade first, and expect to raise ``walltime`` and
``mem`` for 3-D ocean variables when you scale up.

**Annual ocean BGC.** ``Oyr.*`` variables are written as a single file (no time
splitting), so they do not benefit from ``--resume``. If one hits the walltime
it restarts from the beginning.

.. note::

   The bundled ``src/access_moppy/examples/batch_tipmip_config.yml`` is a
   CMIP6-era example and its metadata block predates CMIP6Plus TIPMIP support.
   Use its variable list and resource settings as a reference, but take the
   metadata block from this page.

Next steps
----------

- :doc:`cmip6plus` — the CMIP6Plus vocabulary details this page builds on
- :doc:`/howto/batch_processing` — resource tuning and error recovery
- :doc:`/reference/configuration` — every configuration key
