CMIP7 QC Validation
===================

This page describes how to run ACCESS-MOPPy output quality-control checks on
CMORised files.

Scope
-----

- QC is run on the *CMORised output file*, not the raw model input.
- For ``source_id=ACCESS-ESM1-6``, QC covers all variables present in
  ``ACCESS-ESM1-6_mappings.json`` with generic checks (non-missing values,
  finite values, and units checks where defined).
- Physical ranges for all 293 ACCESS-ESM1-6 mapped variables are defined
  explicitly in the QC configuration with defaults and experiment-specific
  overrides (historical, piControl, ssp*).
- Each variable's physical range is derived from its definition (mapping units)
  and stored as a per-variable rule entry.
- Rules are loaded from: ``access_moppy/resources/qc/cmip7_ranges.yml``.

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

If a check fails, ``validate_cmip7_output`` raises ``ValueError`` with details
about the variable, experiment, observed range, and allowed range.

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
   FAIL /path/to/file2.nc: CMIP7 QC failed for tas in experiment piControl using rule piControl: observed range 182.000..329.400 K is outside allowed range 180.000..325.000 K.

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
       "total": 44,
       "failures": [
         {
           "file": "/output/path/tas.nc",
           "variable_id": "tas",
           "experiment_id": "piControl",
           "error": "Observed range 182.000..329.400 K is outside allowed 180.000..325.000 K.",
           "observed_range": [182.0, 329.4],
           "allowed_range": [180.0, 325.0],
           "units": "K"
         }
       ]
     }
   }

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

Rule structure example:

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
         ssp*:
           min: 180.0
           max: 335.0
