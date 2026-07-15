Getting Started
===============

Welcome to the ACCESS-MOPPy Getting Started guide!

This section will walk you through the initial setup and basic usage of ACCESS-MOPPy, a tool designed to post-process ACCESS model output and produce CMIP-compliant datasets. You’ll learn how to configure your environment, prepare your data, and run the CMORisation workflow using both the Python API and Dask for scalable processing.

.. contents:: Table of Contents
   :local:
   :depth: 2

Set up configuration
--------------------

When you first import `access_moppy` in a Python environment, the package will automatically create a `user.yml` file in your home directory (`~/.moppy/user.yml`).
During this initial setup, you will be prompted to provide some basic information, including:
- Your name
- Your email address
- Your work organization
- Your ORCID

This information is stored in `user.yml` and will be used as global attributes in the files generated during the CMORisation process. This ensures that each CMORised file includes metadata identifying who performed the CMORisation, allowing us to track data provenance and follow up with the responsible person if needed.

Dask support
------------

ACCESS-MOPPy supports Dask for parallel processing, which can significantly speed up the CMORisation workflow, especially when working with large datasets. To use Dask with ACCESS-MOPPy, you can create a Dask client to manage distributed computation. This allows you to take advantage of multiple CPU cores or even a cluster of machines, depending on your setup.

.. code-block:: python

   import dask.distributed as dask
   client = dask.Client(threads_per_worker=1)
   client

Data selection
--------------

``ACCESS_ESM_CMORiser`` offers two ways to supply input data:

**Option 1 — Auto-discovery (recommended):** pass the root directory of your
payu archive via ``input_folder``.  ACCESS-MOPPy reads the variable's mapping
entry and the model's ``file_discovery`` configuration to locate the relevant
files automatically — no manual ``glob`` required.

.. code-block:: python

   # Point to the archive root; ACCESS-MOPPy finds the files for you.
   archive_root = "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/historical/MyRun"

Optionally, restrict the search to a specific time window:

.. code-block:: python

   # Only files within 1900–1950 will be included.
   archive_root = "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/historical/MyRun"
   start_year, end_year = 1900, 1950

**Option 2 — Explicit file list:** collect the files yourself and pass them
via ``input_data``.  This is useful when the auto-discovery pattern does not
match your archive layout, or when you want fine-grained control.

.. code-block:: python

   import glob
   files = glob.glob("../../Test_data/esm1-6/atmosphere/aiihca.pa-0961*_mon.nc")

Parent experiment information
-----------------------------

In CMIP workflows, providing parent experiment information is required for proper data provenance and traceability. This metadata describes the relationship between your experiment and its parent (for example, a historical run branching from a piControl simulation), and is essential for CMIP data publication and compliance.

However, for some applications—such as when using ACCESS-MOPPy to interact with evaluation frameworks like [ESMValTool](https://www.esmvaltool.org/) or [ILAMB](https://www.ilamb.org/)—strict CMIP compliance is not always necessary. In these cases, you may choose to skip providing parent experiment information to simplify the workflow.

If you choose to skip this step, ACCESS-MOPPy will issue a warning to let you know that, if you write the output to disk, the resulting file may not be compatible with CMIP requirements for publication. This flexibility allows you to use ACCESS-MOPPy for rapid evaluation and prototyping, while still supporting full CMIP compliance when needed.

.. code-block:: python

   parent_experiment_config = {
       "parent_experiment_id": "piControl",
       "parent_activity_id": "CMIP",
       "parent_source_id": "ACCESS-ESM1-5",
       "parent_variant_label": "r1i1p1f1",
       "parent_time_units": "days since 0001-01-01 00:00:00",
       "parent_mip_era": "CMIP6",
       "branch_time_in_child": 0.0,
       "branch_time_in_parent": 54786.0,
       "branch_method": "standard"
   }

Set up the CMORiser for CMORisation
------------------------------------

To begin the CMORisation process, you need to create an instance of the `ACCESS_ESM_CMORiser` class. This class requires several key parameters, including the input data and metadata describing your experiment.

A crucial parameter is the `compound_name`, which should be specified using the full CMIP convention: `table.variable` (for example, `Amon.rsds`). This format uniquely identifies the variable, its frequency (e.g., monthly, daily), and the associated CMIP table, ensuring that all requirements for grids and metadata are correctly handled. Using the full compound name helps avoid ambiguity and guarantees that the CMORiser applies the correct standards for each variable.

You can also provide additional metadata such as `experiment_id`, `source_id`, `variant_label`, and `grid_label` to ensure your output is CMIP-compliant. Optionally, you may include parent experiment information for full provenance tracking.

Using ``input_folder`` (auto-discovery):

.. code-block:: python

   from access_moppy import ACCESS_ESM_CMORiser

   cmoriser = ACCESS_ESM_CMORiser(
       input_folder=archive_root,   # archive root — files discovered automatically
       start_year=1900,             # optional: restrict to 1900–1950
       end_year=1950,
       compound_name="Amon.rsds",
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP6",        # Optional, default is CMIP6
       parent_info=parent_experiment_config,  # Optional
   )

Or, using an explicit file list (``input_data``):

.. code-block:: python

   from access_moppy import ACCESS_ESM_CMORiser

   cmoriser = ACCESS_ESM_CMORiser(
       input_data=files,
       compound_name="Amon.rsds",
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP6",        # Optional, default is CMIP6
       parent_info=parent_experiment_config,  # Optional
   )

Choosing CMIP6 vs CMIP6Plus vs CMIP7
-------------------------------------

From a user point of view, vocabulary selection is controlled by the ``cmip_version`` argument in ``ACCESS_ESM_CMORiser``:

- ``cmip_version="CMIP6"`` (default) uses CMIP6 controlled vocabularies
- ``cmip_version="CMIP6Plus"`` uses CMIP6Plus controlled vocabularies
- ``cmip_version="CMIP7"`` uses CMIP7 controlled vocabularies

.. code-block:: python

   from access_moppy import ACCESS_ESM_CMORiser

   # CMIP6 (default)
   cmip6_cmoriser = ACCESS_ESM_CMORiser(
       input_data=files,
       compound_name="Amon.rsds",
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP6",
   )

   # CMIP6Plus
   cmip6plus_cmoriser = ACCESS_ESM_CMORiser(
       input_data=files,
       compound_name="Amon.rsds",
       experiment_id="historical",
       source_id="ACCESS-CM2",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP6Plus",
   )

Use ``source_id``, ``experiment_id``, and other metadata values that exist in the selected controlled vocabulary set. CMIP6 and CMIP6Plus entries are not always identical.

Exploring Variable Mappings
---------------------------

ACCESS-MOPPy provides an enhanced variable mapping display that helps you understand how your raw model variables are mapped to CMIP-compliant variables. The `variable_mapping` attribute provides a rich, interactive display in Jupyter notebooks that shows:

- Variable metadata (CF standard names, units, dimensions)
- Mapping completeness and validation status
- Model-specific mapping information
- Easy-to-read tabular format with color coding

.. code-block:: python

   # Display the variable mapping with enhanced formatting
   cmoriser.variable_mapping

The variable mapping display shows:

- **Variable Name**: The CMIP variable name (e.g., rsds - surface downwelling shortwave flux)
- **CF Standard Name**: The Climate and Forecast conventions standard name
- **Units**: Expected units for the CMIP-compliant variable
- **Dimensions**: How the data dimensions map between raw and CMIP formats
- **Model Info**: Shows the ACCESS model version used for this mapping

You can also access the raw mapping data programmatically:

.. code-block:: python

   # Access the raw mapping dictionary if needed for programmatic use
   print("Variable:", list(cmoriser.variable_mapping.keys()))
   print("CF Standard Name:", cmoriser.variable_mapping['rsds']['CF standard Name'])
   print("Units:", cmoriser.variable_mapping['rsds']['units'])
   print("Compound name:", cmoriser.variable_mapping.compound_name)
   print("Model ID:", cmoriser.variable_mapping.model_id)

The VariableMapping class acts as both a dictionary-like interface for programmatic access and provides rich visual feedback in Jupyter environments to help users understand and validate their variable mappings before processing.

Running the CMORiser
--------------------

To start the CMORisation process, simply call the `run()` method on your `cmoriser` instance as shown below. This step may take some time, especially if you are processing a large number of files.

We recommend using the [dask-labextension](https://github.com/dask/dask-labextension) with JupyterLab to monitor the progress of your computation. The extension provides a convenient dashboard to track task progress and resource usage directly within your notebook interface.

.. code-block:: python

   cmoriser.run()

In-memory processing with xarray and Dask
-----------------------------------------

The CMORisation workflow processes data entirely in memory using `xarray` and Dask. This approach enables efficient parallel computation and flexible data manipulation, but requires that your system has enough memory to handle the size of your dataset.

Once the CMORisation is complete, you can access the resulting dataset by calling the `to_dataset()` method on your `cmoriser` instance. The returned object is a standard xarray dataset, which means you can slice, analyze, or further process the data using familiar xarray operations.

.. code-block:: python

   ds = cmoriser.to_dataset()
   ds

Writing the output to a NetCDF file
-----------------------------------

To save your CMORised data to disk, use the `write()` method of the `cmoriser` instance. This will create a NetCDF file with all attributes set according to the CMIP Controlled Vocabulary, ensuring compliance with CMIP metadata standards.

After writing the file, we recommend validating it using [PrePARE](https://github.com/PCMDI/cmor/tree/master/PrePARE), a tool provided by PCMDI to check the conformity of CMIP files. PrePARE will help you identify any issues with metadata or file structure before publication or further analysis.

.. code-block:: python

   cmoriser.write()

File splitting
~~~~~~~~~~~~~~

By default (``split_years="auto"``), ``write()`` splits the output into
separate files per time chunk following CMIP/ESGF publication conventions.
This keeps individual files to a manageable size and allows users to retrieve
only the years they need.

.. list-table:: Default chunk sizes per output frequency
   :widths: 25 25
   :header-rows: 1

   * - Frequency
     - Years per file
   * - ``1hr``, ``3hr``, ``6hr``
     - 1
   * - ``day``
     - 5
   * - ``mon``
     - 10
   * - ``yr``, ``fx``
     - single file (no split)

For example, a historical run from 1850 to 2014 with daily precipitation
would produce files like::

    pr_day_ACCESS-ESM1-6_historical_r1i1p1f1_gn_18500101-18541231.nc
    pr_day_ACCESS-ESM1-6_historical_r1i1p1f1_gn_18550101-18591231.nc
    ...
    pr_day_ACCESS-ESM1-6_historical_r1i1p1f1_gn_20100101-20141231.nc

The filename time range is derived from the actual first and last timestamps
in each file, so it is always correct even when the experiment does not start
or end on a chunk boundary.

To override the default splitting behaviour, pass ``split_years`` when
constructing the CMORiser:

.. code-block:: python

   from access_moppy import ACCESS_ESM_CMORiser

   cmoriser = ACCESS_ESM_CMORiser(
       input_data=files,
       compound_name="Aday.pr",
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       output_path="/path/to/output",
       split_years=None,   # single file for the whole run
   )
   cmoriser.run()
   cmoriser.write()

Valid values for ``split_years``:

- ``"auto"`` *(default)* — use the CMIP defaults from
  :data:`access_moppy.DEFAULT_CHUNK_YEARS`.
- ``None`` — write the entire time series to one file.
- positive integer — explicit chunk length that overrides the defaults for
  all frequencies (e.g. ``split_years=1`` for annual files).

The default chunk sizes are also importable for inspection:

.. code-block:: python

   from access_moppy import DEFAULT_CHUNK_YEARS
   print(DEFAULT_CHUNK_YEARS)
   # {'1hr': 1, '3hr': 1, '6hr': 1, 'day': 5, 'mon': 10, 'yr': None, 'fx': None}

Running output QC checks
------------------------

ACCESS-MOPPy includes CMIP7 output QC checks (currently including physical
range checks for ``tas``). You can run QC from notebooks or the command line.

Notebook/API usage:

.. code-block:: python

   from access_moppy.qc import validate_cmip7_output

   output_file = "/path/to/CMIP7/output.nc"
   validate_cmip7_output(output_file)

CLI usage:

.. code-block:: bash

   moppy-qc /path/to/output.nc

See :doc:`qc_validation` for complete examples and rule configuration details.

CMIP7 Support with Full Branded Names
======================================

ACCESS-MOPPy also supports the new CMIP7 compound name format, which uses full branded names instead of the table.variable format used in CMIP6. CMIP7 introduces a more descriptive naming convention that includes detailed information about the data processing and grid specifications.

The CMIP7 format follows the pattern: ``realm.variable.operation.frequency.domain`` (e.g., ``atmos.rsds.tavg-u-hxy-u.mon.GLB``)

This provides more explicit information about:

- **Realm**: Model component (atmos, ocean, land, etc.)
- **Variable**: The physical quantity
- **Operation**: Temporal and spatial processing applied
- **Frequency**: Data output frequency
- **Domain**: Spatial domain specification

Using CMIP7 Compound Names
---------------------------

Here's how to use CMIP7 compound names with ACCESS-MOPPy:

.. code-block:: python

   # Example: CMIP7 compound name for surface downwelling shortwave radiation
   cmip7_cmoriser = ACCESS_ESM_CMORiser(
       input_data=files,  # Can reuse the same atmospheric files
       compound_name="atmos.rsds.tavg-u-hxy-u.mon.GLB",  # CMIP7 full branded name
       experiment_id="piControl-spinup",
       source_id="ACCESS-ESM1-6",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP7"  # Explicit CMIP7 support
   )

   # Display the CMIP7 variable mapping
   cmip7_cmoriser.variable_mapping

CMIP6 vs CMIP7 Compound Name Comparison
----------------------------------------

The table below shows the key differences between CMIP6 and CMIP7 compound name formats:

.. list-table:: CMIP6 vs CMIP7 Compound Names
   :widths: 25 35 40
   :header-rows: 1

   * - Aspect
     - CMIP6 Format
     - CMIP7 Format
   * - **Structure**
     - ``table.variable``
     - ``realm.variable.operation.frequency.domain``
   * - **Example**
     - ``Amon.tas``
     - ``atmos.rsds.tavg-u-hxy-u.mon.GLB``
   * - **Information**
     - Table and variable only
     - Detailed processing and grid info
   * - **Length**
     - Compact
     - More descriptive

**CMIP7 Compound Name Breakdown:**

- ``atmos``: Atmospheric realm
- ``tas``: Near-surface air temperature
- ``tavg-h2m-hxy-u``: Time-averaged, 2-meter height, horizontal grid, unstructured
- ``mon``: Monthly frequency
- ``glb``: Global domain

ACCESS-MOPPy automatically handles the mapping between these formats and the underlying model variables.

----

Batch Processing with PBS
=========================

For large-scale CMORisation workflows on PBS-based HPC systems such as NCI
Gadi, ACCESS-MOPPy provides a dedicated batch-processing workflow based on
``moppy-cmorise``.

The detailed guide now lives in :doc:`batch_processing`, which avoids
duplicating the same configuration reference in two places.

Use :doc:`batch_processing` for:

- the full YAML configuration reference
- PBS resource and ``worker_init`` examples
- Streamlit and ``moppy-tui`` monitoring workflows
- tracker database, logs, and JSON batch report details
- troubleshooting and performance tuning guidance

If you want an end-to-end worked example rather than the reference guide, see
:doc:`CMORise_ILAMB_workflow`, which shows a real multi-variable batch setup
for ACCESS-ESM1-6.

If your immediate goal is to help with CMIP7 FastTrack baseline coverage for
ACCESS-ESM1-6 on NCI Gadi, see :doc:`cmip7_fasttrack_baseline` for a more
task-focused contribution guide.

The minimum batch workflow is:

.. code-block:: bash

   moppy-cmorise batch_config.yml

and then monitor progress with either:

.. code-block:: bash

   moppy-tui --db <output_folder>/cmor_tasks.db

or the Streamlit dashboard started by ``moppy-cmorise``.
