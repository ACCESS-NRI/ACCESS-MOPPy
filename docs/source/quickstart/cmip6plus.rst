CMIP6Plus quick start
=====================

Minimal batch config for CMORising ACCESS output against the CMIP6Plus
controlled vocabularies.

This page assumes you have read :doc:`index` — it only shows what is specific
to CMIP6Plus.

What makes a run "CMIP6Plus"
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Setting
     - Value
   * - ``cmip_version``
     - ``CMIP6Plus``
   * - ``source_id``
     - ``ACCESS-ESM1-5`` or ``ACCESS-CM2``
   * - variable names
     - ``Amon.tas`` (legacy tables) **or** ``APmon.tas`` (MIP tables)

CMIP6Plus keeps the familiar ``table.variable`` compound names. MOPPy accepts
both table naming schemes and picks the matching table set automatically:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Scheme
     - Example
     - Backed by
   * - Legacy CMIP6 names
     - ``Amon.tas``, ``Omon.tos``
     - CMIP6-style tables + CMIP6Plus CVs
   * - New MIP table names
     - ``APmon.tas``, ``OPmon.tos``
     - ``mip-cmor-tables`` + CMIP6Plus CVs

Pick one scheme and use it consistently within a config.

.. important::

   The CMIP6Plus vocabularies are a **different, smaller** set than CMIP6.
   ``experiment_id`` and ``source_id`` must exist there or the job fails
   immediately with a "not found in controlled vocabularies" error listing the
   valid values. ``ACCESS-ESM1-6`` is not an official CMIP6Plus source entry —
   MOPPy accepts it via a temporary override and warns.

Minimal config
--------------

.. code-block:: yaml

   # cmip6plus_test.yml

   variables:
     - Amon.tas
     - Amon.pr
     - Omon.tos

   cmip_version: CMIP6Plus
   experiment_id: historical
   source_id: ACCESS-ESM1-5
   variant_label: r1i1p1f1
   grid_label: gn
   activity_id: CMIP

   # File discovery follows model_id, which defaults to ACCESS-ESM1-6 in
   # batch mode. Set it to match source_id for any other model.
   model_id: ACCESS-ESM1-5

   parent_info:
     parent_experiment_id: piControl
     parent_activity_id: CMIP
     parent_source_id: ACCESS-ESM1-5
     parent_variant_label: r1i1p1f1
     parent_time_units: "days since 0001-01-01 00:00:00"
     parent_mip_era: CMIP6Plus
     branch_time_in_child: 0.0
     branch_time_in_parent: 0.0
     branch_method: standard

   input_folder: "/g/data/<project>/archive/<run>"
   output_folder: "/scratch/<project>/<user>/cmor/cmip6plus_test"

   queue: "normal"
   cpus_per_node: 12
   mem: "64GB"
   jobfs: 100GB
   walltime: "04:00:00"
   scheduler_options: "#PBS -P <project>"
   storage: "gdata/p73+gdata/xp65+scratch/<project>"

   worker_init: |
     module use /g/data/xp65/public/modules
     module load conda/analysis3

Run and monitor it exactly as in :doc:`index`:

.. code-block:: bash

   module use /g/data/xp65/public/modules
   module load conda/analysis3
   moppy-cmorise cmip6plus_test.yml
   moppy-tui --db /scratch/<project>/<user>/cmor/cmip6plus_test/cmor_tasks.db

The ``model_id`` gotcha
-----------------------

Two different settings are involved when your ``source_id`` is not
``ACCESS-ESM1-6``:

- ``source_id`` selects the **variable mappings** — how a CMIP variable is
  built from native model fields.
- ``model_id`` selects the **file-discovery rules** — where the native files
  live in the archive. In batch mode it defaults to ``ACCESS-ESM1-6``, *not* to
  ``source_id``.

So for an ACCESS-ESM1-5 or ACCESS-CM2 run, set ``model_id`` explicitly. If you
forget, discovery looks in the wrong place and the job fails with no input
files found.

Checking valid vocabulary values
--------------------------------

If you are unsure whether an experiment or source exists in CMIP6Plus:

.. code-block:: bash

   python -c "import json, access_moppy, pathlib; \
   p = pathlib.Path(access_moppy.__file__).parent / 'vocabularies/CMIP6Plus_CVs'; \
   print(sorted(json.loads((p / 'CMIP6Plus_experiment_id.json').read_text())['experiment_id']))"

Swap ``experiment_id`` for ``source_id`` or ``activity_id`` to list those.

Next steps
----------

- :doc:`tipmip` — TIPMIP runs, which are CMIP6Plus with a different activity
- :doc:`/howto/batch_processing` — resource tuning and error recovery
- :doc:`/reference/configuration` — every configuration key
