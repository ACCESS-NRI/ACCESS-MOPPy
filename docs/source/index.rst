.. ACCESS-MOPPy documentation master file, created by
   sphinx-quickstart on Wed Apr  2 14:45:51 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: ../images/Moppy_logo.png
   :align: center
   :width: 300px
   :alt: MOPPy Logo

ACCESS-MOPPy Documentation
===========================

ACCESS-MOPPy (Model Output Post-Processor)
-------------------------------------------------

ACCESS-MOPPy is a CMORisation tool designed to post-process ACCESS model output. This version represents a significant rewrite of the original MOPPy, focusing on usability and flexibility. It introduces a user-friendly Python API that can be integrated into Jupyter notebooks and other workflows.

ACCESS-MOPPy allows for targeted CMORisation of individual variables and is specifically designed to support the ACCESS-ESM1.6 configuration prepared for CMIP7 FastTrack. Ocean variable support remains limited in this alpha release.

**Key Features**
- Improved usability and extensibility
- Python API for integration into notebooks and scripts
- **Enhanced variable mapping display with rich Jupyter notebook interface**
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

ACCESS-MOPPy is a complete rewrite of the original APP4 and MOPPeR frameworks. Unlike previous versions, it does **not** depend on CMOR; instead, it leverages modern Python libraries such as **xarray** and **dask** for efficient processing of NetCDF files. This approach streamlines the workflow, improves flexibility, and enhances integration with contemporary data science tools.

While retaining the core concepts of "custom" and "cmip" modes, ACCESS-MOPPy unifies these workflows within a single configuration file, focusing on usability and extensibility for current and future CMIP projects.

----

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   batch_processing

----

Installation
------------

ACCESS-MOPPy requires Python >= 3.11 and the following packages:

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

Legacy model utilities
----------------------

ACCESS-MOPPy officially targets ACCESS-ESM1.6 and later models.  For users
who need to work with older output (ACCESS-ESM1.5, ACCESS-CM2), the
``access_moppy.legacy_utilities`` sub-package provides helper scripts that
are not part of the main CMORisation pipeline.

``moppy-calc-ab-coeffts`` — Hybrid-height b coefficient calculator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The UM (Unified Model) atmosphere uses a hybrid-height vertical coordinate
whose orography-following ``b`` coefficients must be computed from the raw η
(eta) values in the ``vertlevs`` namelist file via a quadratic formula::

    b(k) = (1 − η(k) / η_etadot)²

ACCESS-ESM1.5 and ACCESS-CM2 CMIP6 output incorrectly stored the raw η
values directly as ``sigma_theta``, omitting this transformation.
ACCESS-ESM1.6 output already contains the correctly transformed values, so
no correction is needed for officially-supported data.

The utility can be invoked from the command line:

.. code-block:: bash

   # install the optional f90nml dependency first
   pip install "access_moppy[atmos-tools]"

   moppy-calc-ab-coeffts /path/to/vertlevs_G3

Or in Python:

.. code-block:: python

   from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

   a_theta, b_theta, a_rho, b_rho = calc_ab("/path/to/vertlevs_G3")

Typical ``vertlevs`` file locations:

- ESM1.5 / ESM1.6: ``/g/data/vk83/configurations/inputs/access-esm1p5/share/atmosphere/grids/resolution_independent/2020.05.19/vertlevs_G3``
- CM2 / CM2.1: ``~access/umdir/vn10.6/ctldata/vert/vertlevs_L85_50t_35s_85km``

See also: `Martin Dix's original script <https://gist.github.com/MartinDix/14d6ab8fa6997c18f5bf5456d22756d5>`_,
and the discussion in `issue #164 <https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/164>`_.

----

License
-------

ACCESS-MOPPy is licensed under the Apache-2.0 License.

----

Contact
-------

Author: Romain Beucher
Email: romain.beucher@anu.edu.au
