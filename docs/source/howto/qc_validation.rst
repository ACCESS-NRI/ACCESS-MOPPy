.. _qc-validation:

CMIP7 QC Validation
===================

This page describes how to run ACCESS-MOPPy output quality-control checks on
CMORised files.

QC is one of three independent checks ACCESS-MOPPy applies to output destined
for publication — see :ref:`publication-qc` for how they fit together:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Check
     - Answers
   * - **Physical range QC** (this page)
     - Are the numbers physically plausible for this variable and experiment?
   * - :ref:`Compliance checking <compliance-check>`
     - Do the metadata, file name and DRS path satisfy CF and the CMIP
       controlled vocabularies?
   * - :ref:`QC diagnostic plots <qc-diagnostic-plots>`
     - Does the field *look* right — spatially, and over time?

Scope
-----

- QC is run on the *CMORised output file*, not the raw model input.
- For ``source_id=ACCESS-ESM1-6``, QC covers all variables present in
  ``ACCESS-ESM1-6_mappings.json`` with generic checks (non-missing values,
  finite values, and units checks where defined).
- Physical ranges for all 293 ACCESS-ESM1-6 mapped variables are defined
  explicitly in the QC configuration, with a default per variable and optional
  experiment-specific overrides (``historical``, ``piControl``, ``ssp*``).
- Rules are loaded from ``access_moppy/resources/qc/cmip7_ranges.yml``.
  :doc:`/reference/qc_ranges` renders every rule in force, and explains how a
  rule is resolved for a given file.

What fails, and what only warns
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 45 15 40

   * - Condition
     - Result
     - Reported as
   * - All values missing, or values containing infinity
     - **Fail**
     - ``ValueError`` / ``FAIL`` on the CLI
   * - Units differ from the mapping or the rule
     - **Fail**
     - ``ValueError`` / ``FAIL`` on the CLI
   * - Observed range outside the allowed range
     - Warn
     - ``warnings.warn`` / ``WARN`` on the CLI

A range violation does not fail the file. The bounds are broad sanity
envelopes, so an excursion is a prompt to look at the field rather than proof
the file is wrong — the observed and allowed ranges are recorded in the batch
report for a human to judge.

Running QC in Notebooks
-----------------------

Use the Python API to validate a CMORised file after ``cmoriser.write()``.

.. code-block:: python

   from access_moppy.qc import validate_cmip7_output

   # Write CMORised output first
   cmoriser.run()
   cmoriser.write()

   # Validate the written file
   output_file = "/path/to/CMIP7/output.nc"
   validate_cmip7_output(output_file)

``validate_cmip7_output`` raises ``ValueError`` on a failing check, and emits a
``UserWarning`` — with the variable, experiment, observed range and allowed
range — when only the physical range is exceeded. It also *returns* the
``ValidationResult`` it computed, so a caller that wants to record the outcome
does not have to re-read the file to get it.

Use ``validate_cmip7_output_detailed`` instead to get a ``ValidationResult``
back rather than an exception:

.. code-block:: python

   from access_moppy.qc.cmip7 import validate_cmip7_output_detailed

   result = validate_cmip7_output_detailed(output_file)
   print(result.passed, result.error, result.warning)
   print(result.observed_min, result.observed_max)
   print(result.allowed_min, result.allowed_max)

.. _release-gates:

What gets recorded in the batch report
--------------------------------------

A CMORised variable is a candidate for publication, not a finished product. A
batch run records the outcome of three checks — the *release gates* — against
each task, so a reader of ``moppy_batch_report.json`` can tell what was
actually verified rather than inferring it from the task not having failed:

.. list-table::
   :header-rows: 1
   :widths: 12 30 58

   * - Gate
     - Recorded in
     - Produced by
   * - ``range``
     - ``tasks[].output_summary.gates.range``
     - :func:`~access_moppy.qc.validate_cmip7_output`, run on every file
       immediately after it is repacked
   * - ``repack``
     - ``tasks[].output_summary.gates.repack``
     - ``cmip7repack``, run in place on every CMIP7 file written
   * - ``wcrp``
     - ``tasks[].compliance``
     - :func:`~access_moppy.qc.enforce_compliance`, when
       ``compliance_check: true``

Each gate records ``pass``, ``warn`` or ``fail``, with the evidence behind it —
for the range gate, the observed and allowed bounds and the units. A variable
that writes several files keeps the **worst** outcome each gate produced across
them, so a single bad split is never averaged away.

.. code-block:: json

   {
     "gates": {
       "range": {
         "result": "warn",
         "check_id": "cmip7_ranges",
         "observed": [-2.1, 34.8],
         "allowed": [-2.0, 34.0],
         "units": "degC",
         "message": "CMIP7 QC range warning for tos in experiment piControl ..."
       },
       "repack": {"result": "pass", "tool": "cmip7repack"}
     }
   }

This costs nothing extra at report time: the results are stamped by the worker
that already computed them, so building the report stays a cheap read of the
task database. It does **not** re-open the output files — that is what
``MOPPY_SKIP_QC`` and the batch monitor's ``skip_qc=True`` avoid, after
re-reading every file inside a 1-CPU monitor job proved able to OOM it.

Running QC from the CLI
-----------------------

ACCESS-MOPPy provides a CLI command:

.. code-block:: bash

   moppy-qc /path/to/file1.nc /path/to/file2.nc

Exit status:

- ``0``: all files passed
- ``1``: one or more files failed

Example output:

.. code-block:: text

   PASS /path/to/file1.nc
   WARN /path/to/file2.nc: Observed range 182.000..329.400 outside allowed 180.000..325.000.
   FAIL /path/to/file3.nc: Expected units 'K', found 'degC'.

To see the bounds a file will be held to — without reading any data — ask the
same CLI for the rules themselves:

.. code-block:: bash

   moppy-qc --show-ranges --variable tas --experiment piControl
   moppy-qc --show-ranges --format json

:doc:`/reference/qc_ranges` documents both forms and lists every rule.

Automatic QC during CMORisation
-------------------------------

For CMIP7 runs, ACCESS-MOPPy automatically validates output in the write path
after writing and repacking the file. In other words, when you call
``cmoriser.write()`` for CMIP7 output, QC is already executed.

Batch Report QC Summary
-----------------------

When running a batch CMORisation, the batch report (``moppy_batch_report_<UTC>.json``)
automatically includes a QC section summarizing validation results for all
CMORised output files:

.. code-block:: json

   {
     "qc": {
       "passed": 42,
       "failed": 2,
       "warned": 1,
       "total": 44,
       "failures": [
         {
           "file": "/output/path/pr.nc",
           "variable_id": "pr",
           "experiment_id": "piControl",
           "error": "Expected units 'kg m-2 s-1', found 'mm/day'."
         }
       ],
       "warnings": [
         {
           "file": "/output/path/tas.nc",
           "variable_id": "tas",
           "experiment_id": "piControl",
           "warning": "Observed range 182.000..329.400 outside allowed 180.000..325.000.",
           "observed_range": [182.0, 329.4],
           "allowed_range": [180.0, 325.0],
           "units": "K"
         }
       ]
     }
   }

Range excursions land in ``warnings`` and still count towards ``passed``;
``failures`` holds the files a check rejected outright.

To disable QC collection during batch report generation, use one of:

.. code-block:: bash

   # Environment variable
   export MOPPY_SKIP_QC=1
   moppy-batch-report --db cmor_tasks.db

   # CLI flag
   moppy-batch-report --db cmor_tasks.db --skip-qc

Or programmatically:

.. code-block:: python

   from access_moppy.batch_report import write_batch_report
   write_batch_report(db_path, skip_qc=True)

.. _qc-diagnostic-plots:

QC Diagnostic Plots
--------------------

Batch runs can generate lightweight visual QC plots for every CMORised output
file — a spatial snapshot of the first timestep, and a two-panel timeseries of
the global mean with min/max shading and standard deviation. Enable them with
``qc_plots: true`` in the batch config. See :ref:`qc-plots-batch` for full
details, including how to overlay a reference ACCESS-ESM1-5 CMIP6 timeseries on
each plot using ``cmip6_comparison_store`` to spot drift against the published
CMIP6 submission.

Regenerating plots with the CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To regenerate (or generate for the first time) QC plots outside of a batch
run, use ``moppy-qc-plots``.  It accepts one or more ``.nc`` files, or
directories to scan recursively.

.. code-block:: bash

   # Single file
   moppy-qc-plots /path/to/tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-201412.nc

   # Whole DRS tree
   moppy-qc-plots /scratch/cmor_output/CMIP7

   # With ACCESS-ESM1-5 CMIP6 comparison overlay
   moppy-qc-plots /scratch/cmor_output/CMIP7 \
       --comparison-store /g/data/cmip6_store \
       --preferred-member r1i1p1f1

   # Custom output directory and parallel workers
   moppy-qc-plots /scratch/cmor_output/CMIP7 \
       --qc-dir /scratch/qc_plots \
       --workers 8

Options:

.. code-block:: text

   paths                   One or more .nc files or directories (scanned recursively).
   --qc-dir DIR            Output directory for PNGs. Defaults to a qc_plots/
                           folder next to each input file.
   --comparison-store PATH Path to an ACCESS-ESM1-5 CMIP6 Parquet timeseries
                           store.  When supplied the matching reference
                           global-mean is overlaid on the timeseries plot.
   --preferred-member MBR  Preferred ensemble member for the overlay
                           (e.g. r1i1p1f1).  Falls back to r1i1p1f1 then
                           lex-first when not set.
   --workers N             Number of parallel worker processes (default: 1).

Exit status is ``0`` when all plots succeeded, ``1`` when any file was skipped
due to an error.

The Python API equivalent is:

.. code-block:: python

   from access_moppy.qc.plots import generate_qc_plots
   from pathlib import Path
   from concurrent.futures import ProcessPoolExecutor

   drs_root = Path("/scratch/cmor_output/CMIP7")
   nc_files = sorted(drs_root.rglob("*.nc"))
   qc_dir = drs_root / "qc_plots"

   with ProcessPoolExecutor(max_workers=8) as executor:
       for nc_file, result in zip(nc_files, executor.map(
           lambda f: generate_qc_plots(f, qc_dir=qc_dir), nc_files
       )):
           print(f"{'OK' if result else 'SKIP'} {nc_file.name}")

.. _qc-extending-rules:

Extending rules
---------------

To add experiment-specific thresholds for a variable, or to override ranges
for newly added variables, edit:

.. code-block:: text

   src/access_moppy/resources/qc/cmip7_ranges.yml

Under the ``variables`` section, each variable has a ``default`` entry and an
optional ``experiments`` map for experiment-specific min/max values. For example:

.. code-block:: yaml

   variables:
     tas:
       units: K
       default:
         min: 180.0
         max: 330.0
       experiments:
         historical:
           min: 180.0
           max: 330.0
         piControl:
           min: 180.0
           max: 325.0

Experiment keys are matched with ``fnmatch``, so ``ssp*`` covers every SSP
experiment and the longest matching pattern wins. Keys left out of an override
keep their default value.

After editing, confirm the rule resolves the way you expect:

.. code-block:: bash

   moppy-qc --show-ranges --variable tas --experiment ssp370

See :doc:`/reference/qc_ranges` for the full resolution order and the complete
set of rules currently in force.
