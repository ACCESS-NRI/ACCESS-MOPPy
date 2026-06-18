=====================================
Testing CMORisation on Actual Files
=====================================

This guide covers comprehensive testing of the CMORisation pipeline using actual model output files.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

The ACCESS-MOPPy test suite includes integration tests that process real data files through the full CMORisation pipeline. These tests validate that variables are correctly transformed to CMIP6/CMIP7 standards.

**Key Features:**

- **Variable-level granularity**: Test individual variables, not just entire tables
- **Multiple frequencies**: Support for monthly, daily, 3-hourly, and 6-hourly data
- **CMIP6 & CMIP7**: Test both standards with automatic table mapping
- **Compliance validation**: Verify output against CMOR standards using PrePARE or WCRP compliance-checker
- **Flexible filtering**: Run tests for specific tables, variables, or frequencies

Setting Up Test Data
====================

External Data Root
------------------

Set the ``ACCESS_MOPPY_DATA_ROOT`` environment variable to point to your test data directory:

.. code-block:: bash

    export ACCESS_MOPPY_DATA_ROOT=/path/to/test/data

Expected Directory Structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The test data should follow this structure:

.. code-block:: text

    ACCESS_MOPPY_DATA_ROOT/
    ├── output1/
    │   ├── atmosphere/netCDF/
    │   │   ├── *_mon.nc          # Monthly atmospheric data
    │   │   ├── *_dai.nc          # Daily atmospheric data
    │   │   ├── *_3hr.nc          # 3-hourly atmospheric data
    │   │   └── *_6hr.nc          # 6-hourly atmospheric data
    │   ├── ocean/
    │   │   ├── *-variable-1monthly-mean*.nc  # Monthly ocean (CMIP6 convention)
    │   │   └── *-variable-1mon-mean*.nc      # Monthly ocean (alternate convention)
    │   └── ice/
    │       └── iceh-1monthly-mean_*.nc       # Sea ice monthly data
    ├── output2/
    └── output3/

The test discovery looks for files matching these patterns in any ``output*`` subdirectory, so you can have multiple ensemble members or runs.

Running Tests
=============

Before Running Tests
--------------------

1. Set up the pixi environment:

   .. code-block:: bash

       cd /path/to/ACCESS-MOPPy
       pixi shell -e dev

2. Set the data root (if not already in your shell):

   .. code-block:: bash

       export ACCESS_MOPPY_DATA_ROOT=/home/romain/PROJECTS/CMIP7_Test_data/esm-historical

Basic Test Execution
---------------------

**Run all CMORisation tests:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -v

This runs 372 tests: 240 CMIP6 + 132 CMIP7 across all supported tables and variables.

**Run with minimal output:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -q

**Show test collection only (no execution):**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py --collect-only -q

Advanced Filtering
==================

Filter by Variable Name
-----------------------

Test a specific variable across all frequencies:

.. code-block:: bash

    # Test 'tas' in all tables
    pytest tests/integration/test_full_cmorisation.py -k "tas" -v

    # Test temperature variables only
    pytest tests/integration/test_full_cmorisation.py -k "tas or tasa or tasmin or tasmax" -v

    # Test 'tos' (ocean temperature)
    pytest tests/integration/test_full_cmorisation.py -k "tos" -v

Filter by Table
---------------

Test all variables in a specific table:

.. code-block:: bash

    # Test all Amon (monthly atmosphere) variables
    pytest tests/integration/test_full_cmorisation.py -k "Amon" -v

    # Test all ocean variables
    pytest tests/integration/test_full_cmorisation.py -k "Omon" -v

    # Test all sea ice variables
    pytest tests/integration/test_full_cmorisation.py -k "SImon" -v

    # Test daily frequency only
    pytest tests/integration/test_full_cmorisation.py -k "day" -v

Combined Filtering
-------------------

**Test tas in Amon table only:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "Amon and tas" -v

**Test ocean variables but exclude SImon:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "Omon or SImon or ocean" -v

**Test a specific variable in multiple frequencies:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "pr" -v
    # Tests: Amon-pr, CFmon-pr, day-pr, 6hrPlev-pr, etc.

Filter by CMIP Version
----------------------

**Test CMIP6 only (no CMIP7):**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "not cmip7" -v

**Test CMIP7 only:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "cmip7" -v

    # Specific CMIP7 variable
    pytest tests/integration/test_full_cmorisation.py -k "cmip7 and tas" -v

Test Granularity Examples
--------------------------

.. code-block:: bash

    # Single variable test (most specific)
    pytest tests/integration/test_full_cmorisation.py::TestFullCMORIntegration::test_cmorisation_variable -k "Amon-ACCESS" -v
    # Note: Use the full test ID with the parametrized variable name

    # All variables in Amon
    pytest tests/integration/test_full_cmorisation.py -k "Amon" -v

    # All monthly variables (Amon, Lmon, AERmon, etc.)
    pytest tests/integration/test_full_cmorisation.py -k "mon" -v

    # All atmospheric variables
    pytest tests/integration/test_full_cmorisation.py -k "Amon or day or 3hr or 6hr or Eday or CFday" -v

Understanding Test Output
==========================

Successful Test
---------------

.. code-block:: text

    tests/integration/test_full_cmorisation.py::TestFullCMORIntegration::test_cmorisation_variable[Amon-ACCESS-ESM1-6-CMIP6_Amon.json-CMIP6-tas] PASSED [100%]

**What this means:**
- Variable ``tas`` in table ``Amon`` was successfully CMORised
- Output files were created and validated

Skipped Test
-----------

.. code-block:: text

    tests/integration/test_full_cmorisation.py::TestFullCMORIntegration::test_cmorisation_variable[Omon-...] SKIPPED
    Reason: Ocean data directory not available; set ACCESS_MOPPY_DATA_ROOT

**Solutions:**
1. Set ``ACCESS_MOPPY_DATA_ROOT`` to a directory containing ocean files
2. Ensure ocean files exist at ``$ACCESS_MOPPY_DATA_ROOT/output*/ocean/``

Failed Test
-----------

.. code-block:: text

    tests/integration/test_full_cmorisation.py::TestFullCMORIntegration::test_cmorisation_variable[Amon-...-tas] FAILED
    AssertionError: No output files found for tas in /tmp/cmor_output_Amon_tas

**Common causes:**
- Input data file is missing or malformed
- CMORiser encountered a mapping error
- Output file wasn't created for other reasons

**Debugging:**
1. Check the full error message with ``-v`` flag
2. Verify input files exist: ``ls $ACCESS_MOPPY_DATA_ROOT/output*/atmosphere/netCDF/``
3. Check CMORiser logs if available

Compliance Validation
=====================

Default Behavior (PrePARE)
--------------------------

By default, tests validate output using PrePARE:

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" -v

PrePARE validates:
- CMOR table compliance
- Dimensions and coordinates
- NetCDF structure
- Metadata

Using WCRP Compliance Checker
-----------------------------

To use the WCRP compliance-checker instead (more strict):

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" \
        --compliance-tool=wcrp -v

**Requirements:**
- WCRP compliance-checker must be installed
- cc-plugin-wcrp must be installed

**Verify availability:**

.. code-block:: bash

    compliance-checker --list-tests

Should show ``wcrp_cmip6:1.0`` in the output.

Skipping Compliance Validation
------------------------------

To test CMORisation without compliance validation:

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -k "Amon" \
        --compliance-tool=none -v

**Note:** Some tests automatically skip compliance validation:
- ``Omon`` (ocean) - non-standard grid structures
- ``Ofx`` (ocean fixed fields) - fixed variables

Practical Testing Workflows
============================

Scenario 1: Test a New Variable
--------------------------------

You've added a new variable mapping and want to verify it works:

.. code-block:: bash

    # 1. Run just that variable across all tables
    pytest tests/integration/test_full_cmorisation.py -k "mynewvar" -v

    # 2. If it passes, run with CMIP7 too
    pytest tests/integration/test_full_cmorisation.py -k "mynewvar and cmip7" -v

    # 3. Verify compliance output
    pytest tests/integration/test_full_cmorisation.py -k "Amon-mynewvar" -v -s

Scenario 2: Validate a Frequency Tier
--------------------------------------

You want to ensure all daily variables work correctly:

.. code-block:: bash

    # Test all daily (day) variables
    pytest tests/integration/test_full_cmorisation.py -k "day" -v --tb=short

    # Count how many pass/fail
    pytest tests/integration/test_full_cmorisation.py -k "day" -v --tb=no -q

Scenario 3: Quality Check Before Release
-----------------------------------------

Run comprehensive tests to ensure everything works:

.. code-block:: bash

    # Test CMIP6 only (stable baseline)
    pytest tests/integration/test_full_cmorisation.py -k "not cmip7" \
        --tb=short -v 2>&1 | tee test_results.log

    # Check for any failures
    grep -E "(FAILED|ERROR)" test_results.log

    # If clean, test CMIP7
    pytest tests/integration/test_full_cmorisation.py -k "cmip7" \
        --tb=short -v 2>&1 | tee test_results_cmip7.log

Scenario 4: Debug a Single Variable
-----------------------------------

A test is failing and you need detailed information:

.. code-block:: bash

    # Run with full traceback and print statements
    pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" -vv -s --tb=long

    # Show which input files were discovered
    pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" -vv -s

Interpreting Variable Names
============================

Test IDs follow the pattern: ``TableName-VariableName`` or ``TableName-VariableName-cmip7``

Examples:

.. list-table::
   :header-rows: 1

   * - Test ID
     - Meaning
     - Frequency
   * - ``Amon-tas``
     - Temperature in Amon (monthly atmosphere)
     - Monthly
   * - ``day-pr``
     - Precipitation in daily table
     - Daily
   * - ``ocean-tos-cmip7``
     - Ocean temperature (CMIP7 atmos variant)
     - Monthly
   * - ``SImon-sithick``
     - Sea ice thickness
     - Monthly
   * - ``6hrPlev-ta``
     - Air temperature on pressure levels
     - 6-hourly
   * - ``Ofx-areacello``
     - Ocean grid cell area (fixed)
     - Fixed (no time)

CMIP7 Table Mapping
~~~~~~~~~~~~~~~~~~~

CMIP7 tests use these table mappings internally:

- ``atmos`` → CMIP6 ``Amon`` (monthly atmospheric)
- ``ocean`` → CMIP6 ``Omon`` (monthly ocean)
- ``seaIce`` → CMIP6 ``SImon`` (monthly sea ice)
- ``aerosol`` → CMIP6 ``AERmon`` (monthly aerosol)

When you run a CMIP7 test, it processes using the CMIP6 table but validates against CMIP7 standards.

Troubleshooting
===============

No Tests Collected
------------------

**Error:**
.. code-block:: text

    collected 0 items

**Causes & Solutions:**

1. **Wrong path:**
   
   .. code-block:: bash

       # Wrong
       pytest test_full_cmorisation.py
       
       # Correct
       pytest tests/integration/test_full_cmorisation.py

2. **Pixi environment not activated:**

   .. code-block:: bash

       pixi shell -e dev
       pytest tests/integration/test_full_cmorisation.py

3. **Invalid -k filter:**

   .. code-block:: bash

       # Wrong
       pytest tests/integration/test_full_cmorisation.py -k "Amon\-tas"
       
       # Correct
       pytest tests/integration/test_full_cmorisation.py -k "Amon and tas"

All Tests Skipped
------------------

**Error:**
.. code-block:: text

    372 skipped, 1 warning in X.XXs
    Reason: Ocean data directory not available; set ACCESS_MOPPY_DATA_ROOT

**Solution:**

.. code-block:: bash

    # Check current setting
    echo $ACCESS_MOPPY_DATA_ROOT
    
    # Set it
    export ACCESS_MOPPY_DATA_ROOT=/home/romain/PROJECTS/CMIP7_Test_data/esm-historical
    
    # Verify path exists
    ls $ACCESS_MOPPY_DATA_ROOT/output*/atmosphere/netCDF/ | head -5

Input Files Not Found
---------------------

**Error:**
.. code-block:: text

    Required input files not available for Omon.tos; set ACCESS_MOPPY_DATA_ROOT

**Solutions:**

1. **Verify data root:**

   .. code-block:: bash

       ls $ACCESS_MOPPY_DATA_ROOT/output*/ocean/

2. **Check filename conventions:**

   Ocean files should match:
   - ``*-variable-1monthly-mean*.nc`` (preferred)
   - ``*-variable-1mon-mean*.nc`` (alternate)

3. **List available files:**

   .. code-block:: bash

       find $ACCESS_MOPPY_DATA_ROOT -name "*.nc" | grep ocean | head -10

Compliance Validation Failures
------------------------------

**Error:**
.. code-block:: text

    WCRP compliance validation failed mandatory checks:
    - check_dimensions: ...

**Solutions:**

1. **Review the specific check:**

   Run with verbose output to see full details:

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" \
           -vv --tb=long

2. **Compare against known good output:**

   Check if the issue is with your data or the CMORiser:

   .. code-block:: bash

       # Run with PrePARE instead
       pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" \
           --compliance-tool=prepare -v

3. **Inspect the output file:**

   .. code-block:: bash

       ncdump -h /tmp/cmor_output_Amon_tas/tas_Amon_*.nc | head -50

Running Tests in Batch
======================

Run Multiple Variables Sequentially
-----------------------------------

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py \
        -k "tas or pr or hus or huss" \
        -v --tb=short

    # Output counts
    pytest tests/integration/test_full_cmorisation.py \
        -k "tas or pr or hus or huss" \
        -q --tb=no

Test Monthly Data Only
----------------------

.. code-block:: bash

    # Run all monthly frequency tables
    pytest tests/integration/test_full_cmorisation.py \
        -k "Amon or Lmon or AERmon or Omon or SImon or CFmon or Emon or Ofx" \
        -v --tb=short

    # More concise - use 'mon'
    pytest tests/integration/test_full_cmorisation.py \
        -k "mon or Emon or Ofx" \
        -v

Subset of Variables by Category
--------------------------------

**Atmospheric dynamics:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py \
        -k "Amon and (ua or va or ta or ps or zg)" -v

**Radiation:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py \
        -k "Amon and (rsds or rlds or rsus or rlus)" -v

**Water cycle:**

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py \
        -k "Amon and (pr or prc or prw or evspsbl)" -v

Performance Considerations
==========================

Test Execution Time
-------------------

- **Single variable test**: ~1-2 seconds
- **All Amon variables**: ~30-60 seconds
- **All tables (CMIP6)**: ~3-5 minutes
- **All tables (CMIP6 + CMIP7)**: ~5-8 minutes

Reduce Execution Time
~~~~~~~~~~~~~~~~~~~~~

1. **Test specific table only:**

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py -k "Amon" -v
       # Instead of running all 372 tests

2. **Skip compliance validation (faster):**

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py \
           --compliance-tool=none -q

3. **Use parallel execution (requires pytest-xdist):**

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py -n auto -v

Parallel Execution
------------------

To install pytest-xdist:

.. code-block:: bash

    pixi add -e dev pytest-xdist

Run tests in parallel:

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -n auto -v

Run with 4 workers:

.. code-block:: bash

    pytest tests/integration/test_full_cmorisation.py -n 4 -v

Tips & Best Practices
=====================

1. **Always set ACCESS_MOPPY_DATA_ROOT before testing**

   Add to your shell profile (``.bashrc``, ``.zshrc``) to avoid repetition:

   .. code-block:: bash

       export ACCESS_MOPPY_DATA_ROOT=/home/romain/PROJECTS/CMIP7_Test_data/esm-historical

2. **Start with a single variable test**

   When debugging, narrow the scope:

   .. code-block:: bash

       # Good for debugging
       pytest tests/integration/test_full_cmorisation.py -k "Amon-tas" -vv -s

3. **Use --tb=short for cleaner output**

   Reduces verbosity while keeping error context:

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py -k "Amon" --tb=short

4. **Save test results to file**

   For later analysis:

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py -v \
           2>&1 | tee test_results_$(date +%Y%m%d_%H%M%S).log

5. **Test CMIP6 before CMIP7**

   CMIP6 is more stable; use it as baseline:

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py \
           -k "not cmip7" -v
       
       # If passing, test CMIP7
       pytest tests/integration/test_full_cmorisation.py \
           -k "cmip7" -v

6. **Use collection-only to verify filter**

   Before running expensive tests:

   .. code-block:: bash

       pytest tests/integration/test_full_cmorisation.py \
           -k "Amon and (tas or pr)" --collect-only -q
       # Verify you get the tests you expect

Additional Resources
====================

- :doc:`getting_started` - Basic CMORisation usage
- :doc:`mapping_reference` - Variable mapping details
- :doc:`compliance_testing` - Compliance validation
- `PrePARE Documentation <https://pypi.org/project/PrePARE/>`_
- `CMOR Standards <https://cmor.llnl.gov/>`_

For help or issues, refer to the project README or open an issue on GitHub.
