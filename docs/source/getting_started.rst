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

For more details and advanced usage, see the [Getting Started notebook](../notebooks/Getting_started.ipynb).
