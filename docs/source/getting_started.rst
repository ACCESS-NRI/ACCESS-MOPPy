Getting Started
===============

Welcome to the ACCESS-MOPPeR Getting Started guide!

This section will walk you through the initial setup and basic usage of ACCESS-MOPPeR, a tool designed to post-process ACCESS model output and produce CMIP-compliant datasets. You’ll learn how to configure your environment, prepare your data, and run the CMORisation workflow using both the Python API and Dask for scalable processing.

.. contents:: Table of Contents
   :local:
   :depth: 2

Set up configuration
--------------------

When you first import `access_mopper` in a Python environment, the package will automatically create a `user.yml` file in your home directory (`~/.mopper/user.yml`).
During this initial setup, you will be prompted to provide some basic information, including:
- Your name
- Your email address
- Your work organization
- Your ORCID

This information is stored in `user.yml` and will be used as global attributes in the files generated during the CMORisation process. This ensures that each CMORised file includes metadata identifying who performed the CMORisation, allowing us to track data provenance and follow up with the responsible person if needed.

Dask support
------------

ACCESS-MOPPeR supports Dask for parallel processing, which can significantly speed up the CMORisation workflow, especially when working with large datasets. To use Dask with ACCESS-MOPPeR, you can create a Dask client to manage distributed computation. This allows you to take advantage of multiple CPU cores or even a cluster of machines, depending on your setup.

.. code-block:: python

   import dask.distributed as dask
   client = dask.Client(threads_per_worker=1)
   client

Data selection
--------------

The `ACCESS_ESM_CMORiser` class (described in detail below) takes as input a list of paths to NetCDF files containing the raw model output variables to be CMORised. The CMORiser does **not** assume any specific folder structure, DRS (Data Reference Syntax), or file naming convention. It is intentionally left to the user to ensure that the provided files contain the original variables required for CMORisation.

This design is intentional: ACCESS-NRI plans to integrate ACCESS-MOPPeR into extended workflows that leverage the [ACCESS-NRI Intake Catalog](https://github.com/ACCESS-NRI/access-nri-intake-catalog) or evaluation frameworks such as [ESMValTool](https://www.esmvaltool.org/) and [ILAMB](https://www.ilamb.org/). By decoupling file selection from the CMORiser, ACCESS-MOPPeR can be flexibly used in a variety of data processing and evaluation pipelines.

.. code-block:: python

   import glob
   files = glob.glob("../../Test_data/esm1-6/atmosphere/aiihca.pa-0961*_mon.nc")

Parent experiment information
----------------------------

In CMIP workflows, providing parent experiment information is required for proper data provenance and traceability. This metadata describes the relationship between your experiment and its parent (for example, a historical run branching from a piControl simulation), and is essential for CMIP data publication and compliance.

However, for some applications—such as when using ACCESS-MOPPeR to interact with evaluation frameworks like [ESMValTool](https://www.esmvaltool.org/) or [ILAMB](https://www.ilamb.org/)—strict CMIP compliance is not always necessary. In these cases, you may choose to skip providing parent experiment information to simplify the workflow.

If you choose to skip this step, ACCESS-MOPPeR will issue a warning to let you know that, if you write the output to disk, the resulting file may not be compatible with CMIP requirements for publication. This flexibility allows you to use ACCESS-MOPPeR for rapid evaluation and prototyping, while still supporting full CMIP compliance when needed.

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
-----------------------------------

To begin the CMORisation process, you need to create an instance of the `ACCESS_ESM_CMORiser` class. This class requires several key parameters, including the list of input NetCDF files and metadata describing your experiment.

A crucial parameter is the `compound_name`, which should be specified using the full CMIP convention: `table.variable` (for example, `Amon.rsds`). This format uniquely identifies the variable, its frequency (e.g., monthly, daily), and the associated CMIP table, ensuring that all requirements for grids and metadata are correctly handled. Using the full compound name helps avoid ambiguity and guarantees that the CMORiser applies the correct standards for each variable.

You can also provide additional metadata such as `experiment_id`, `source_id`, `variant_label`, and `grid_label` to ensure your output is CMIP-compliant. Optionally, you may include parent experiment information for full provenance tracking.

.. code-block:: python

   from access_mopper import ACCESS_ESM_CMORiser

   cmoriser = ACCESS_ESM_CMORiser(
       input_paths=files,
       compound_name="Amon.rsds",
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       parent_info=parent_experiment_config # <-- This is optional, can be skipped if not needed
   )

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

----

Batch Processing with PBS
=========================

For large-scale CMORisation workflows, ACCESS-MOPPeR provides a batch processing system designed for PBS-based HPC environments like NCI Gadi. This system allows you to process multiple variables in parallel, each running as a separate PBS job with its own Dask cluster.

Configuration File
------------------

Create a YAML configuration file specifying your batch processing parameters:

.. code-block:: yaml

   # batch_config.yml
   # List of variables to process
   variables:
     - Amon.pr
     - Omon.tos
     - Amon.tauu
     - Amon.ts
     - Omon.zos

   # CMIP6 metadata
   experiment_id: piControl
   source_id: ACCESS-ESM1-5
   variant_label: r1i1p1f1
   grid_label: gn
   activity_id: CMIP

   # Input and output paths
   input_folder: "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/spinup/JuneSpinUp-JuneSpinUp-bfaa9c5b"
   output_folder: "/scratch/tm70/rb5533/mopper_output"

   # File patterns for each variable (relative to input_folder)
   file_patterns:
     Amon.pr: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"
     Omon.tos: "output[0-4][0-9][0-9]/ocean/ocean-2d-surface_temp-1monthly-mean*.nc"
     Amon.tauu: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"
     Amon.ts: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"
     Omon.zos: "output[0-4][0-9][0-9]/ocean/ocean-2d-sea_level-1monthly-mean*.nc"

   # PBS job configuration
   queue: normal
   cpus_per_node: 14
   mem: 32GB
   jobfs: 100GB
   walltime: "02:00:00"
   scheduler_options: "#PBS -P tm70"
   storage: "gdata/p73+gdata/tm70+scratch/tm70"

   # Environment setup for each job
   worker_init: |
     source /g/data/tm70/rb5533/miniforge3/bin/activate
     conda activate esmvaltool_dev

   # Optional: Wait for all jobs to complete before exiting
   wait_for_completion: false

Running Batch CMORisation
--------------------------

Submit your batch job using the command-line interface:

.. code-block:: bash

   mopper-cmorise batch_config.yml

This command will:

1. **Initialize a tracking database** in your output directory to monitor job progress
2. **Start a Streamlit dashboard** at http://localhost:8501 for real-time monitoring
3. **Create and submit PBS jobs** for each variable in your configuration
4. **Generate job scripts** in a local `cmor_job_scripts/` directory

Monitoring Progress
-------------------

The batch system includes several monitoring tools:

**Streamlit Dashboard**
   A web-based dashboard automatically starts at http://localhost:8501, showing:

   - Real-time status of all CMORisation tasks
   - Progress tracking (pending, running, completed, failed)
   - Filtering options by status and experiment
   - Task completion times and error logs

**Command Line Monitoring**
   Monitor PBS jobs directly:

   .. code-block:: bash

      # Check job status
      qstat -u $USER

      # Monitor specific jobs (job IDs provided by mopper-cmorise)
      qstat 12345678 12345679 12345680

**Database Tracking**
   The system maintains an SQLite database at `{output_folder}/cmor_tasks.db` that tracks:

   - Task status for each variable
   - Start and completion times
   - Error messages for failed tasks
   - Experiment metadata

File Organization
-----------------

The batch system organizes files as follows:

.. code-block:: text

   your_work_directory/
   ├── batch_config.yml                    # Your configuration file
   ├── cmor_job_scripts/                   # Generated PBS and Python scripts
   │   ├── cmor_Amon_pr.sh                 # PBS script for Amon.pr
   │   ├── cmor_Amon_pr.py                 # Python script for Amon.pr
   │   ├── cmor_Amon_pr.out                # Job stdout
   │   ├── cmor_Amon_pr.err                # Job stderr
   │   └── ...
   └── output_folder/                      # Your specified output directory
       ├── cmor_tasks.db                   # Progress tracking database
       └── CMIP6/                          # CMORised output files (if drs_root specified)
           └── CMIP/
               └── ACCESS-NRI/
                   └── ACCESS-ESM1-5/
                       └── ...

Configuration Options
----------------------

**Required Parameters:**

- ``variables``: List of variables to process (format: ``table.variable``)
- ``experiment_id``, ``source_id``, ``variant_label``, ``grid_label``: CMIP6 metadata
- ``input_folder``: Root directory containing input files
- ``output_folder``: Directory for CMORised output

**File Pattern Mapping:**

- ``file_patterns``: Dictionary mapping variables to glob patterns (relative to ``input_folder``)

**PBS Configuration:**

- ``queue``: PBS queue name (default: "normal")
- ``cpus_per_node``: Number of CPUs per job (default: 4)
- ``mem``: Memory per job (default: "16GB")
- ``jobfs``: Local scratch space (optional)
- ``walltime``: Job time limit (default: "01:00:00")
- ``scheduler_options``: Additional PBS directives
- ``storage``: Required storage systems

**Environment Setup:**

- ``worker_init``: Shell commands to set up the environment in each job

**Optional Parameters:**

- ``activity_id``: CMIP activity (default: derived from experiment)
- ``drs_root``: Enable CMIP6 DRS directory structure
- ``wait_for_completion``: Wait for all jobs before exiting (default: false)

Best Practices
--------------

**Resource Planning:**
   - Use ``jobfs`` for large datasets to improve I/O performance
   - Adjust ``cpus_per_node`` and ``mem`` based on your data size
   - Set appropriate ``walltime`` based on dataset complexity

**File Access:**
   - Ensure ``input_folder`` and ``output_folder`` are on shared filesystems
   - Use relative paths in ``file_patterns`` for portability
   - Test file patterns with a small subset first

**Error Handling:**
   - Monitor the dashboard for failed jobs
   - Check job stderr files in ``cmor_job_scripts/`` for detailed error messages
   - Failed jobs can be resubmitted by running ``mopper-cmorise`` again

**Example PBS Configuration for NCI Gadi:**

.. code-block:: yaml

   queue: normal
   cpus_per_node: 16
   mem: 64GB
   jobfs: 200GB
   walltime: "04:00:00"
   scheduler_options: "#PBS -P tm70"
   storage: "gdata/p73+gdata/tm70+scratch/tm70"
   worker_init: |
     module load netcdf/4.7.4
     source /g/data/tm70/rb5533/miniforge3/bin/activate
     conda activate esmvaltool_dev

Troubleshooting
---------------

**Common Issues:**

1. **Database access errors**: Ensure ``output_folder`` is on a shared filesystem accessible from compute nodes

2. **File not found errors**: Verify ``file_patterns`` match your actual file structure using ``ls`` or ``find``

3. **Memory errors**: Increase ``mem`` or reduce ``cpus_per_node`` for memory-intensive variables

4. **Environment errors**: Check ``worker_init`` commands work on compute nodes

5. **Permission errors**: Ensure write access to ``output_folder`` and job script directory

For advanced usage and troubleshooting, see the example configuration at ``src/access_mopper/examples/batch_config.yml``
