Command-line reference
======================

ACCESS-MOPPy installs nine command-line tools. This page gives the synopsis,
options, and typical usage for each. Run any command with ``--help`` for the
authoritative, version-specific option list.

.. contents:: Commands
   :local:
   :depth: 1

moppy-cmorise
-------------

Batch CMORisation controller. Reads a batch configuration YAML file,
generates PBS job scripts from templates, submits one job per variable, and
starts a monitor job that tracks progress in a SQLite database.

.. code-block:: text

   moppy-cmorise <config.yml>

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Argument / option
     - Description
   * - ``config.yml``
     - Path to the batch configuration YAML file
       (see :doc:`/reference/configuration`).
   * - ``-h``, ``--help``
     - Show help and exit.
   * - ``--resume``
     - Resume failed variables from the first unfinished time split and reuse
       their existing dated DRS version.
   * - ``--monitor``
     - Internal: run the PBS monitor job. Invoked automatically by the
       launcher — do not call directly.

Examples:

.. code-block:: bash

   moppy-cmorise batch_config.yml
  moppy-cmorise batch_config.yml --resume
   moppy-cmorise /path/to/my_experiment.yml

See :doc:`/howto/batch_processing` for the full workflow.

moppy-dashboard
---------------

Launches the Streamlit web dashboard for monitoring batch CMORisation
progress. Requires ``streamlit`` to be installed. The tracker database is
located via the ``CMOR_TRACKER_DB`` environment variable
(default: ``~/.moppy/db/cmor_tasks.db``).

.. code-block:: bash

   moppy-dashboard          # opens the dashboard in your browser

The dashboard offers status/experiment filters, summary statistics, and a
failed-task table with error messages.

moppy-tui
---------

Terminal (Rich) dashboard for the same tracker database — useful over SSH
where no browser is available.

.. code-block:: text

   moppy-tui [--db DB] [--refresh REFRESH] [--status STATUS]
             [--experiment EXPERIMENT] [--max-rows PAGE_SIZE] [--page PAGE]
             [--max-failures MAX_FAILURES] [--once] [--json] [--no-color]

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Description
   * - ``--db DB``
     - Path to ``cmor_tasks.db`` (default: ``$CMOR_TRACKER_DB`` or
       ``~/.moppy/db/cmor_tasks.db``).
   * - ``--refresh REFRESH``
     - Database poll interval in seconds (default: 5.0). Ignored with
       ``--once``/``--json``.
   * - ``--status STATUS``
     - Comma-separated statuses to include
       (``pending,running,completed,failed``).
   * - ``--experiment EXPERIMENT``
     - Filter by ``experiment_id``.
   * - ``--max-rows``, ``--page-size``
     - Page size for the tasks table (default: 20).
   * - ``--page PAGE``
     - 1-based starting page (only meaningful with ``--once``/``--json``).
   * - ``--max-failures MAX_FAILURES``
     - Rows shown in the failures panel.
   * - ``--once``
     - Render one snapshot and exit (useful for cron / logs).
   * - ``--json``
     - Emit a machine-readable JSON snapshot and exit.
   * - ``--no-color``
     - Disable ANSI colours.

moppy-batch-report
------------------

Exports a batch tracker database to a JSON report, optionally enriched with
the original configuration, PBS logs, and QC data.

.. code-block:: text

   moppy-batch-report --db DB [--output OUTPUT] [--config CONFIG]
                      [--script-dir SCRIPT_DIR]
                      [--stderr-tail-lines N] [--skip-qc]

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Description
   * - ``--db DB``
     - Path to ``cmor_tasks.db`` (required).
   * - ``--output OUTPUT``
     - Report path. Defaults to
       ``<db parent>/moppy_batch_report_<UTC>.json``.
   * - ``--config CONFIG``
     - Original batch configuration path.
   * - ``--script-dir SCRIPT_DIR``
     - Directory containing generated PBS scripts/logs.
   * - ``--stderr-tail-lines N``
     - Number of stderr tail lines to include for failed tasks
       (default: 20).
   * - ``--skip-qc``
     - Skip QC data collection (can also set ``MOPPY_SKIP_QC=1``).

moppy-qc
--------

Runs CMIP7 output quality-control checks against one or more CMORised
NetCDF files.

.. code-block:: bash

   moppy-qc output1.nc output2.nc ...

See :doc:`/howto/qc_validation` for the checks performed and how to
interpret results.

moppy-example-config
--------------------

Prints the bundled example batch configuration, or copies it to a file if a
path is given.

.. code-block:: bash

   moppy-example-config                  # print to stdout
   moppy-example-config my_config.yml    # copy to my_config.yml

moppy-calc-ab-coeffts
---------------------

Legacy utility: computes hybrid-height ``a``/``b`` coefficients from a UM
``vertlevs`` namelist file. Only needed for older model output
(ACCESS-ESM1.5, ACCESS-CM2); ACCESS-ESM1.6 output already contains
correctly transformed values. Requires the optional ``f90nml`` dependency:

.. code-block:: bash

   pip install "access_moppy[atmos-tools]"
   moppy-calc-ab-coeffts /path/to/vertlevs_G3

See :doc:`/explanation/coordinates_and_grids` for background on the
hybrid-height coordinate and typical ``vertlevs`` file locations.

moppy-esmval-prepare
--------------------

CMORises raw ACCESS output referenced by an ESMValTool recipe and writes an
ESMValCore data-source configuration, without invoking ESMValTool.

.. code-block:: text

   moppy-esmval-prepare RECIPE --input-root PATH --cache-dir PATH
                        [--model-id ID] [--config FILE]
                        [--output-config FILE] [--workers N] [--dry-run]
                        [--pattern COMPOUND_NAME:GLOB] [-v]

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Argument / option
     - Description
   * - ``RECIPE``
     - Path to the ESMValTool YAML recipe file.
   * - ``--input-root PATH``
     - Root directory of the raw ACCESS archive
       (e.g. ``/g/data/p73/archive/.../MyRun``).
   * - ``--cache-dir PATH``
     - Directory where CMORised files are written in CMIP DRS structure.
   * - ``--model-id ID``
     - ACCESS-MOPPy model identifier (e.g. ``ACCESS-ESM1-6``).
   * - ``--config FILE``
     - Path to any existing file in your ESMValCore config directory; the
       MOPPy data-source config is written alongside it.
   * - ``--output-config FILE``
     - Where to write the generated data-source config
       (default: ``~/.config/esmvaltool/moppy-esmval-data.yml``).
   * - ``--workers N``
     - Number of parallel CMORisation workers (default: 1).
   * - ``--dry-run``
     - Log what would be done without performing CMORisation.
   * - ``--pattern COMPOUND_NAME:GLOB``
     - Override the raw-file glob for a variable, e.g.
       ``'Amon.tas:/output*/atmosphere/netCDF/*mon.nc'``. Repeatable.
   * - ``-v``, ``--verbose``
     - Enable DEBUG-level logging.

moppy-esmval-run
----------------

Same as ``moppy-esmval-prepare``, then immediately runs
``esmvaltool run RECIPE`` with the generated configuration. Accepts all
``moppy-esmval-prepare`` options plus:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Description
   * - ``--esmvaltool-args ARGS``
     - Extra arguments forwarded verbatim to ``esmvaltool run``
       (quoted string).

Example:

.. code-block:: bash

   moppy-esmval-run my_recipe.yml \
       --input-root /g/data/p73/archive/.../MyRun \
       --cache-dir ~/.cache/moppy-esmval \
       --workers 4

See :doc:`/howto/esmvaltool_integration` for the full workflow.
