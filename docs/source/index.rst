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

ACCESS-MOPPy allows for targeted CMORisation of individual variables and is specifically designed to support the ACCESS-ESM1.6 configuration prepared for CMIP7 FastTrack. It supports atmosphere, land, ocean, and sea-ice CMORisation workflows, with dedicated handling for ocean variables.

.. tip::

   **New to CMORisation?** Watch
   `CMORising ACCESS output for CMIP7 FastTrack
   <https://youtu.be/fYn5j5LflAg>`_ — a 10 minute narrated walkthrough of the
   whole Gadi workflow, from loading the environment to watching the batch
   run. It follows the :doc:`quickstart/index` step for step.

**Key Features**

- Improved usability and extensibility
- Python API for integration into notebooks and scripts
- **Enhanced variable mapping display with rich Jupyter notebook interface**
- Flexible CMORisation of specific variables
- Tailored for ACCESS-ESM1.6 and CMIP7 FastTrack
- Cross-platform compatibility (not limited to NCI Gadi)
- Dask-enabled for scalable processing
- **Batch processing system for HPC environments**
- **Real-time monitoring with web dashboard and a terminal dashboard (`moppy-tui`)**

**Current Status**

- ACCESS-MOPPy is close to stable for supported ACCESS CMORisation workflows.
- Ocean variables are supported, with resource guidance available for larger 3D fields.

.. warning::

   **Variable Mapping Under Review** — the mapping of ACCESS variables to
   CMIP6 and CMIP7 equivalents is under review. Some derived variables may
   not be available or may require further verification. Please submit an
   issue if you notice any major problems or missing variables.

**Background**

ACCESS-MOPPy is a complete rewrite of the original APP4 and MOPPeR frameworks. Unlike previous versions, it does **not** depend on CMOR; instead, it leverages modern Python libraries such as **xarray** and **dask** for efficient processing of NetCDF files. This approach streamlines the workflow, improves flexibility, and enhances integration with contemporary data science tools.

While retaining the core concepts of "custom" and "cmip" modes, ACCESS-MOPPy unifies these workflows within a single configuration file, focusing on usability and extensibility for current and future CMIP projects.

.. _publication-qc:

From native ACCESS output to publishable CMIP7
-----------------------------------------------

For CMIP7 Fast Track, ACCESS-MOPPy takes raw ACCESS model output — UM fields
files, MOM and CICE history — and produces files that are ready to publish:
CMORised, checked against physical ranges, validated against the CMIP
controlled vocabularies, and plotted for a human to look at. The four stages
below run in that order, and every one of them is available both from the
Python API and from a batch run on NCI Gadi.

.. container:: moppy-cards

   .. container:: moppy-card moppy-card-cmorise

      .. rst-class:: moppy-stage

      Stage 1

      .. rubric:: CMORise native output
         :class: moppy-card-title

      Reads ACCESS-ESM1.6 atmosphere, land, ocean and sea-ice output directly
      and writes CMIP7 files — branded variable names, CMIP7 global
      attributes, DRS paths and file names. No CMOR library: the rewrite is
      built on **xarray** and **dask**, so it runs in a notebook or across
      hundreds of PBS jobs.

      :doc:`Fast Track quick start <quickstart/cmip7_fasttrack>` ·
      :doc:`baseline runs <howto/cmip7_fasttrack_baseline>` ·
      :doc:`batch processing <howto/batch_processing>`

   .. container:: moppy-card moppy-card-ranges

      .. rst-class:: moppy-stage

      Stage 2

      .. rubric:: Check the physical range
         :class: moppy-card-title

      Every CMIP7 file written is checked against a per-variable physical
      envelope — 293 ACCESS-ESM1-6 variables, with experiment-specific
      overrides — plus units, missing-value and finite-value checks. Bounds
      are broad on purpose: they catch a unit, sign or conversion error
      without rejecting a plausible extreme.

      :doc:`Every rule, rendered <reference/qc_ranges>` ·
      :doc:`running the checks <howto/qc_validation>` ·
      ``moppy-qc --show-ranges --format json``

   .. container:: moppy-card moppy-card-compliance

      .. rst-class:: moppy-stage

      Stage 3

      .. rubric:: WCRP compliance checker
         :class: moppy-card-title

      Runs the CF suite (``cf:1.11``) and the WCRP CMIP suite
      (``wcrp_cmip7:1.0``, backed by ``esgvoc``) on the first file each
      variable publishes — metadata, controlled-vocabulary values, DRS path
      and file name. A failure stops that variable before any further file is
      written, and the JSON report is kept either way.

      :ref:`Enabling it in a batch run <compliance-check>` ·
      :doc:`checker backends <development/compliance_testing>`

   .. container:: moppy-card moppy-card-plots

      .. rst-class:: moppy-stage

      Stage 4

      .. rubric:: QC diagnostic plots
         :class: moppy-card-title

      Two PNGs per output file: a spatial snapshot of the first timestep, and
      a timeseries of the global mean with min/max shading and standard
      deviation. A published ACCESS-ESM1-5 CMIP6 series can be overlaid on the
      timeseries, so drift against the previous submission is visible at a
      glance.

      :ref:`Plots from a batch run <qc-plots-batch>` ·
      :ref:`regenerating them <qc-diagnostic-plots>` ·
      ``moppy-qc-plots``

Nothing here is optional extra work at publication time: stages 2 and 4 run
inside the CMORisation job, and stage 3 is one line of batch configuration.
The batch report (``moppy_batch_report_<UTC>.json``) collects the results of
all three so a whole experiment can be signed off from a single file.

----

Which guide do I need?
-----------------------

ACCESS-MOPPy covers a few distinct use cases. Find yours below.

.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - I want to...
     - Start here
     - Use case
   * - Run my first CMORisation on NCI Gadi, end to end
     - :doc:`quickstart/index`
     - **Start here** — config, run, monitor in four steps
   * - Watch someone run one before I try it myself
     - :ref:`Quick start video <quickstart-video>`
     - 10 minute narrated walkthrough of the four steps
   * - Get the minimal config for CMIP7 FastTrack, CMIP6Plus, or TIPMIP
     - :doc:`quickstart/cmip7_fasttrack`, :doc:`quickstart/cmip6plus`,
       :doc:`quickstart/tipmip`
     - Project-specific metadata blocks
   * - Drive ACCESS-MOPPy from Python in a Jupyter notebook
     - :doc:`tutorials/notebooks/Getting_started`
     - Executable walkthrough — atmosphere, ocean, and land examples
   * - CMORise one or two variables interactively to see how it works
     - :doc:`tutorials/getting_started`
     - First-time evaluation, learning the API
   * - Look up a class, method, or function signature
     - :doc:`reference/api/access_moppy/index`
     - API reference, generated from the source
   * - Run a production CMORisation of hundreds of variables on NCI Gadi
     - :doc:`howto/batch_processing`
     - Full-experiment CMIP7 FastTrack submission
   * - Get ACCESS-ESM1.6 CMIP7 FastTrack output specifically
     - :doc:`howto/cmip7_fasttrack_baseline`
     - CMIP7 FastTrack baseline runs
   * - Feed CMORised output into ESMValTool evaluation recipes
     - :doc:`howto/esmvaltool_integration`
     - Model evaluation with ESMValTool
   * - Benchmark land output against observations with ILAMB
     - :doc:`howto/cmorise_ilamb_workflow`
     - Land model benchmarking
   * - Check that output meets CMIP/CF compliance rules
     - :doc:`howto/qc_validation`
     - Quality control before publication or sharing
   * - Look up the physical range a variable is checked against
     - :doc:`reference/qc_ranges`
     - Every QC range rule, rendered and filterable
   * - Work with older ACCESS-ESM1.5 or ACCESS-CM2 output
     - :doc:`reference/cli` (``moppy-calc-ab-coeffts``)
     - Legacy model support
   * - Install ACCESS-MOPPy or fix a setup/runtime problem
     - :doc:`howto/installation`, :doc:`howto/troubleshooting`
     - Setup and troubleshooting

----

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart/index
   tutorials/index
   howto/index
   explanation/index
   reference/index
   development/index

----

License
-------

ACCESS-MOPPy is licensed under the Apache-2.0 License.

----

Contact
-------

Author: Romain Beucher
Email: romain.beucher@anu.edu.au
