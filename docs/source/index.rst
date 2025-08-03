.. ACCESS-MOPPeR documentation master file, created by
   sphinx-quickstart on Wed Apr  2 14:45:51 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ACCESS-MOPPeR Documentation
===========================

ACCESS Model Output Post-Processor (ACCESS-MOPPeR) v2.1.0a (Alpha Version)
---------------------------------------------------------------------------

ACCESS-MOPPeR is a CMORisation tool designed to post-process ACCESS model output. This version represents a significant rewrite of the original MOPPeR, focusing on usability and flexibility. It introduces a user-friendly Python API that can be integrated into Jupyter notebooks and other workflows.

ACCESS-MOPPeR allows for targeted CMORisation of individual variables and is specifically designed to support the ACCESS-ESM1.6 configuration prepared for CMIP7 FastTrack. Ocean variable support remains limited in this alpha release.

**Key Features**
- Improved usability and extensibility
- Python API for integration into notebooks and scripts
- Flexible CMORisation of specific variables
- Tailored for ACCESS-ESM1.6 and CMIP7 FastTrack
- Cross-platform compatibility (not limited to NCI Gadi)
- Dask-enabled for scalable processing
- **Batch processing system for HPC environments**
- **Real-time monitoring with web dashboard**

**Current Limitations**
- Alpha version: intended for evaluation only, not recommended for data publication
- Ocean variable support is limited

> **⚠️ Variable Mapping Under Review**
>
> The mapping of ACCESS variables to CMIP6 and CMIP7 equivalents is under review. Some derived variables may not be available or may require further verification. Please submit an issue if you notice any major problems or missing variables.

**Background**

ACCESS-MOPPeR v2 is a complete rewrite of the original APP4 and MOPPeR frameworks. Unlike previous versions, it does **not** depend on CMOR; instead, it leverages modern Python libraries such as **xarray** and **dask** for efficient processing of NetCDF files. This approach streamlines the workflow, improves flexibility, and enhances integration with contemporary data science tools.

While retaining the core concepts of "custom" and "cmip" modes, ACCESS-MOPPeR v2 unifies these workflows within a single configuration file, focusing on usability and extensibility for current and future CMIP projects.

----

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   batch_processing

----

Installation
------------

ACCESS-MOPPeR requires Python >= 3.11 and the following packages:

- numpy
- pandas
- xarray
- netCDF4
- cftime
- dask
- pyyaml
- tqdm
- requests

Install dependencies and the package with:

.. code-block:: bash

   pip install numpy pandas xarray netCDF4 cftime dask pyyaml tqdm requests
   pip install .

For development and testing:

.. code-block:: bash

   pip install pytest pytest-cov ruff

----

Testing
-------

To run tests:

.. code-block:: bash

   pytest

----

License
-------

ACCESS-MOPPeR is licensed under the Apache-2.0 License.

----

Contact
-------

Author: Romain Beucher
Email: romain.beucher@anu.edu.au
