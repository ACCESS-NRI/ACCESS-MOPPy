CMIP7 QC Validation
===================

This page describes how to run ACCESS-MOPPy output quality-control checks on
CMORised files.

Scope
-----

- QC is run on the *CMORised output file*, not the raw model input.
- The current implementation includes CMIP7 physical-range checks for
  ``tas``.
- Experiment-aware rules are loaded from:
  ``access_moppy/resources/qc/cmip7_ranges.yml``.

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

Extending rules
---------------

To add more variables or experiment-specific thresholds, update:

.. code-block:: text

   src/access_moppy/resources/qc/cmip7_ranges.yml

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
