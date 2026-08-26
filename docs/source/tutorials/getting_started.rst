Getting Started
===============

Welcome to the ACCESS-MOPPy Getting Started guide!

ACCESS-MOPPy is driven from a Python API, so a CMORisation is an ordinary
object you build, run, inspect, and write — from a script, or interactively in
a Jupyter notebook. This page covers the one-time setup and the parts of the
API you are most likely to need to look up.

.. tip::

   **Prefer to run it?** :doc:`notebooks/Getting_started` is the executable
   version of this walkthrough, with worked atmosphere, ocean, and land
   examples and real output. This page is the companion reference.

   If you want to run a production batch on NCI Gadi instead of working
   variable by variable, go to :doc:`/quickstart/index`.

.. contents:: Table of Contents
   :local:
   :depth: 2

Set up configuration
--------------------

When you first import ``access_moppy`` in a Python environment, the package
automatically creates a ``user.yml`` file in your home directory
(``~/.moppy/user.yml``). During this initial setup you are prompted for some
basic information:

- Your name
- Your email address
- Your work organization
- Your ORCID

This information is stored in ``user.yml`` and is used as global attributes in
the files generated during CMORisation. It ensures that each CMORised file
records who performed the CMORisation, so that data provenance can be tracked
and the responsible person followed up with if needed.

The workflow at a glance
------------------------

A complete CMORisation is four calls:

.. code-block:: python

   from access_moppy import ACCESS_ESM_CMORiser

   cmoriser = ACCESS_ESM_CMORiser(
       input_folder="/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/historical/MyRun",
       start_year=1900,             # optional: restrict the time range
       end_year=1950,
       compound_name="Amon.rsds",   # table.variable
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP6",        # optional, default is CMIP6
       parent_info=parent_experiment_config,  # optional
   )

   cmoriser.variable_mapping   # inspect how the raw variable is mapped
   cmoriser.run()              # do the work (lazily, via xarray + dask)
   ds = cmoriser.to_dataset()  # get an xarray Dataset back
   cmoriser.write()            # write CMIP-compliant NetCDF to disk

The ``compound_name`` is the key argument. Give it as the full CMIP
``table.variable`` pair (for example ``Amon.rsds``) rather than the bare
variable name: the table identifies the frequency and the grid/metadata
requirements that apply, so the CMORiser can pick the right standards without
ambiguity. See `CMIP7 branded compound names`_ for the CMIP7 form.

Supplying input data
~~~~~~~~~~~~~~~~~~~~

``ACCESS_ESM_CMORiser`` accepts input in two ways:

``input_folder`` (recommended)
   Pass the root directory of your payu archive. ACCESS-MOPPy reads the
   variable's mapping entry and the model's ``file_discovery`` configuration to
   locate the relevant files automatically — no manual ``glob`` required.
   ``start_year`` and ``end_year`` optionally restrict the search, filtering on
   the year parsed from each filename without opening any files.

``input_data``
   Pass an explicit list of file paths. Useful when the auto-discovery pattern
   does not match your archive layout, or when you want fine-grained control —
   for example the well-known static ocean grid files used by ``Ofx``
   variables.

The discovery helpers are also importable on their own, which is handy for
checking what an archive contains before you commit to a run:

.. code-block:: python

   from access_moppy.file_discovery import discover_files, discover_year_range

   discover_files("/path/to/archive", "Amon.rsds")        # -> list of paths
   discover_year_range("/path/to/archive", "Amon.rsds")   # -> (first, last)

:doc:`notebooks/Getting_started` works through both approaches for atmosphere,
ocean, and land variables.

Inspecting variable mappings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``cmoriser.variable_mapping`` shows how your raw model variables map to
CMIP-compliant ones — CF standard names, units, dimensions, mapping
completeness, and the ACCESS model version the mapping came from. In a Jupyter
notebook it renders as a colour-coded table; elsewhere it behaves as an
ordinary dictionary:

.. code-block:: python

   print("Variable:", list(cmoriser.variable_mapping.keys()))
   print("CF Standard Name:", cmoriser.variable_mapping['rsds']['CF standard Name'])
   print("Units:", cmoriser.variable_mapping['rsds']['units'])
   print("Compound name:", cmoriser.variable_mapping.compound_name)
   print("Model ID:", cmoriser.variable_mapping.model_id)

Use it to validate a mapping before spending compute on it. See
:doc:`/reference/mapping_reference` for the mapping files themselves.

Dask support
------------

ACCESS-MOPPy processes data in memory using ``xarray`` and Dask, which lets you
take advantage of multiple CPU cores or a whole cluster. Create a client before
you run:

.. code-block:: python

   import dask.distributed as dask
   client = dask.Client(threads_per_worker=1)
   client

We recommend the `dask-labextension <https://github.com/dask/dask-labextension>`__
with JupyterLab to monitor progress: it provides a dashboard for task progress
and resource usage directly in your notebook interface.

Because processing happens in memory, your system needs enough of it to hold
the data you are working on. ``to_dataset()`` returns a standard xarray
Dataset, so you can slice, analyse, or further process the result with familiar
xarray operations before — or instead of — writing it out.

Parent experiment information
-----------------------------

In CMIP workflows, parent experiment information is required for provenance and
traceability. This metadata describes the relationship between your experiment
and its parent (for example, a historical run branching from a piControl
simulation), and is essential for CMIP data publication and compliance.

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

For some applications — such as feeding evaluation frameworks like
`ESMValTool <https://www.esmvaltool.org/>`__ or `ILAMB <https://www.ilamb.org/>`__
— strict CMIP compliance is not necessary, and you may skip this step. If you
do, ACCESS-MOPPy warns you that files written to disk may not be suitable for
CMIP publication. This flexibility lets you use ACCESS-MOPPy for rapid
evaluation and prototyping while still supporting full compliance when needed.

Choosing CMIP6, CMIP6Plus, or CMIP7
-----------------------------------

Vocabulary selection is controlled by the ``cmip_version`` argument:

- ``cmip_version="CMIP6"`` *(default)* — CMIP6 controlled vocabularies
- ``cmip_version="CMIP6Plus"`` — CMIP6Plus controlled vocabularies
- ``cmip_version="CMIP7"`` — CMIP7 controlled vocabularies

Use ``source_id``, ``experiment_id``, and other metadata values that exist in
the selected controlled vocabulary set. CMIP6 and CMIP6Plus entries are not
always identical — for example, ``ACCESS-CM2`` is registered in CMIP6Plus.

CMIP7 branded compound names
----------------------------

CMIP7 introduces a more descriptive naming convention that encodes the
processing and grid specification in the compound name itself, following the
pattern ``realm.variable.operation.frequency.domain``:

.. code-block:: python

   cmip7_cmoriser = ACCESS_ESM_CMORiser(
       input_data=files,
       compound_name="atmos.rsds.tavg-u-hxy-u.mon.GLB",  # CMIP7 branded name
       experiment_id="piControl-spinup",
       source_id="ACCESS-ESM1-6",
       variant_label="r1i1p1f1",
       grid_label="gn",
       activity_id="CMIP",
       cmip_version="CMIP7",
   )

.. list-table:: CMIP6 vs CMIP7 compound names
   :widths: 25 35 40
   :header-rows: 1

   * - Aspect
     - CMIP6 format
     - CMIP7 format
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

Reading ``atmos.tas.tavg-h2m-hxy-u.mon.GLB`` piece by piece:

- ``atmos`` — atmospheric realm
- ``tas`` — near-surface air temperature
- ``tavg-h2m-hxy-u`` — time-averaged, 2-metre height, horizontal grid, unstructured
- ``mon`` — monthly frequency
- ``GLB`` — global domain

ACCESS-MOPPy handles the mapping between these formats and the underlying model
variables automatically, so with ``cmip_version="CMIP7"`` you may pass either a
branded name or the CMIP6-style ``table.variable`` form.

File splitting
--------------

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

See :doc:`/howto/qc_validation` for complete examples and rule configuration
details.

After writing a file we also recommend validating it with
`PrePARE <https://github.com/PCMDI/cmor/tree/master/PrePARE>`__, the PCMDI tool
that checks CMIP files for conformity, before publication or further analysis.

Scaling up: batch processing
----------------------------

For large-scale CMORisation on PBS-based HPC systems such as NCI Gadi,
ACCESS-MOPPy provides a dedicated batch workflow based on ``moppy-cmorise``.
The minimum version is:

.. code-block:: bash

   moppy-cmorise batch_config.yml

and then monitoring progress with either:

.. code-block:: bash

   moppy-tui --db <output_folder>/cmor_tasks.db

or the Streamlit dashboard started by ``moppy-cmorise``.

Where to go next
----------------

- :doc:`/howto/batch_processing` — the full YAML configuration reference, PBS
  resource and ``worker_init`` examples, monitoring, tracker database and log
  details, and performance tuning.
- :doc:`/howto/cmorise_ilamb_workflow` — an end-to-end worked example of a real
  multi-variable batch setup for ACCESS-ESM1-6.
- :doc:`/howto/cmip7_fasttrack_baseline` — a task-focused guide if your goal is
  contributing to CMIP7 FastTrack baseline coverage for ACCESS-ESM1-6 on Gadi.
- :doc:`/reference/api/access_moppy/index` — the complete API reference,
  generated from the source.
