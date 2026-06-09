Quality Control
================

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
--------

The ``access_moppy.qc`` package provides a lightweight, extensible framework for
running Quality Control (QC) checks on CMORised ACCESS-ESM output.  It is
intentionally **separate from the CMORisation pipeline** so that checks can be
run at any time — immediately after CMORisation, in a batch over existing files,
or interactively in a notebook.

The checks are organised into two tiers, both derived from the ESM1.6
pre-publication QC document:

**Technical checks**
   Verify that the output is correctly CMORised: variable names, units,
   cell_methods, required CMIP6 global attributes, time-axis integrity,
   and coordinate ranges.

**Science checks**
   Verify that the physical values are plausible: per-variable min/max
   thresholds, non-negative constraints, and global-mean climatological ranges.

All checks return one of four statuses:

.. list-table::
   :header-rows: 1
   :widths: 10 90

   * - Status
     - Meaning
   * - ``PASS``
     - The check completed successfully and found no issues.
   * - ``WARN``
     - A potential issue was found that warrants human review but is not a
       hard error (e.g. global mean slightly outside climatological bounds).
   * - ``FAIL``
     - A definite error was found (e.g. required attribute missing, time not
       monotonic, units mismatch).
   * - ``SKIP``
     - The check could not be applied to this dataset (e.g. no ``vocab``
       provided, variable absent, no time dimension).

Quick start
-----------

Run all built-in checks on an existing CMORised file:

.. code-block:: python

   import xarray as xr
   from access_moppy import ACCESS_ESM_CMORiser
   from access_moppy.qc import QCRunner

   # Option 1: run QC on an existing file
   ds = xr.open_dataset("tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc")
   report = QCRunner().run(ds, context={"cmor_name": "tas"})
   print(report.summary())

   # Option 2: run QC immediately after CMORisation (full vocab context)
   with ACCESS_ESM_CMORiser(
       input_data=files,
       compound_name="Amon.tas",
       experiment_id="historical",
       source_id="ACCESS-ESM1-5",
       variant_label="r1i1p1f1",
       grid_label="gn",
   ) as cmoriser:
       cmoriser.run(write_output=True)
       report = QCRunner().run(
           cmoriser.to_dataset(),
           context={"vocab": cmoriser.cmoriser.vocab, "cmor_name": "tas"},
       )

   print(report)

The ``context`` dictionary
--------------------------

Checks receive two arguments: the xarray ``Dataset`` and a ``context`` dict.
The following keys are recognised:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Key
     - Description
   * - ``"cmor_name"``
     - The CMIP variable short name, e.g. ``"tas"``.  Required by most checks.
   * - ``"vocab"``
     - A vocabulary instance
       (:class:`~access_moppy.vocabulary_processors.CMIP6Vocabulary` etc.).
       When present, enables checks that compare against the CMOR table
       (units, cell_methods, valid_min/valid_max).
   * - ``"compound_name"``
     - E.g. ``"Amon.tas"``.  Used to derive ``cmor_name`` when the latter is
       absent.

Checks that need ``vocab`` and do not receive one return ``SKIP`` rather than
failing, so the runner is always safe to call without a full context.

Reading the report
------------------

.. code-block:: python

   report = QCRunner().run(ds, context={"cmor_name": "tas"})

   # Human-readable summary to stdout
   print(report.summary())

   # Access filtered lists
   for result in report.failures:
       print(result.check_name, result.message)

   for result in report.warnings:
       print(result.check_name, result.message, result.details)

   # Persist to JSON for automated pipelines
   report.to_json("qc_report.json")

   # Convert to plain dict (e.g. for custom serialisation)
   d = report.to_dict()
   print(d["overall_status"])  # "pass" | "warn" | "fail" | "skip"

Built-in checks
---------------

Technical: metadata
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Check name
     - Severity
     - What it checks
   * - ``metadata.required_global_attributes``
     - FAIL
     - All required CMIP6 global attributes are present in ``ds.attrs``.
       Uses the vocabulary's ``get_required_attribute_names()`` when available,
       or a built-in fallback list otherwise.
   * - ``metadata.variable_name``
     - FAIL
     - ``variable_id`` global attribute equals the expected CMOR variable name.
   * - ``metadata.units``
     - FAIL
     - ``units`` attribute on the data variable matches the CMOR table.
       Requires ``vocab`` in context.
   * - ``metadata.cell_methods``
     - WARN
     - ``cell_methods`` attribute matches the CMOR table.
       Requires ``vocab`` in context.

Technical: data integrity
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Check name
     - Severity
     - What it checks
   * - ``data_integrity.valid_range``
     - WARN
     - All values lie within ``valid_min`` / ``valid_max`` from the CMOR table.
       Requires ``vocab``.
   * - ``data_integrity.missing_fraction``
     - FAIL / WARN
     - Fraction of missing/fill values.  FAIL if > 95 %, WARN if > 50 %.
   * - ``data_integrity.fill_value_consistency``
     - FAIL / WARN
     - ``_FillValue`` and ``missing_value`` are present and equal.

Technical: temporal
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Check name
     - Severity
     - What it checks
   * - ``temporal.time_monotonicity``
     - FAIL
     - Time coordinate is strictly monotonically increasing.
   * - ``temporal.time_duplicates``
     - FAIL
     - No duplicate time values.
   * - ``temporal.time_bounds_present``
     - WARN / FAIL
     - ``time_bnds`` is present, has shape ``(time, 2)``, and lower ≤ upper
       for every step.

Technical: spatial
~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Check name
     - Severity
     - What it checks
   * - ``spatial.lat_range``
     - FAIL
     - Latitude values within ``[-90, 90]``.
   * - ``spatial.lon_range``
     - FAIL
     - Longitude values within ``[-180, 360]``.
   * - ``spatial.coordinate_bounds``
     - WARN
     - ``lat_bnds`` and ``lon_bnds`` are present when the variable has lat/lon
       dimensions.

Science: global statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~

These checks are derived from the ESM1.6 pre-publication QC document.  All
science checks issue **WARN** rather than FAIL — the intent is to flag values
for human review, not to block output.

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Check name
     - Severity
     - What it checks
   * - ``global_stats.min_max_range``
     - WARN
     - Actual global min/max against known per-variable physical bounds
       (e.g. surface temperature 180–340 K, precipitation ≥ 0).
   * - ``global_stats.non_negative``
     - WARN
     - Variables that must be non-negative (``pr``, ``rsdt``, ``siconc``,
       ``gpp``, …) contain no negative values beyond floating-point noise.
   * - ``global_stats.global_mean_range``
     - WARN
     - Global spatial mean within expected climatological bounds
       (e.g. global-mean surface temperature 270–295 K).

The thresholds are defined in
``access_moppy/qc/checks/global_stats.py::_VARIABLE_THRESHOLDS``.
Adding a new variable requires one dict entry — no other changes needed.

Running a subset of checks
--------------------------

.. code-block:: python

   from access_moppy.qc import QCRunner
   from access_moppy.qc.base import get_checks

   # Run only temporal checks
   temporal_checks = get_checks([
       "temporal.time_monotonicity",
       "temporal.time_duplicates",
       "temporal.time_bounds_present",
   ])
   report = QCRunner(checks=temporal_checks).run(ds, context={"cmor_name": "tas"})

Adding a custom check
---------------------

Subclass :class:`~access_moppy.qc.base.QCCheck`, set a unique ``name``, implement
``run()``, and register the instance.  The check is then automatically included
in any ``QCRunner()`` call with no explicit check list.

.. code-block:: python

   from access_moppy.qc import QCCheck, register_check, QCRunner

   class TOAEnergyBalanceCheck(QCCheck):
       """Global-mean TOA net radiation N = rsdt − rsut − rlut must be near zero."""

       name = "science.toa_energy_balance"
       _picontrol_tolerance_wm2 = 1.0  # W m-2

       def run(self, ds, context):
           required = {"rsdt", "rsut", "rlut"}
           if not required.issubset(ds.data_vars):
               return self._skip(f"Requires {required}; dataset has {set(ds.data_vars)}")

           N = (ds["rsdt"] - ds["rsut"] - ds["rlut"]).mean().item()
           if abs(N) > self._picontrol_tolerance_wm2:
               return self._warn(
                   f"TOA energy imbalance |N| = {N:.3f} W m-2 "
                   f"(threshold ±{self._picontrol_tolerance_wm2} W m-2)",
                   toa_imbalance_wm2=N,
               )
           return self._pass(f"TOA energy balance N = {N:.3f} W m-2")

   register_check(TOAEnergyBalanceCheck())

   # Now it runs automatically with any QCRunner()
   report = QCRunner().run(ds, context={"cmor_name": "rsdt"})

Batch QC over a directory of files
-----------------------------------

.. code-block:: python

   import glob
   import xarray as xr
   from access_moppy.qc import QCRunner

   runner = QCRunner()
   reports = {}

   for path in glob.glob("output/**/*.nc", recursive=True):
       ds = xr.open_dataset(path)
       cmor_name = ds.attrs.get("variable_id", "unknown")
       reports[path] = runner.run(ds, context={"cmor_name": cmor_name})
       ds.close()

   # Print a summary table
   for path, report in reports.items():
       status = report.overall_status.value.upper()
       print(f"[{status}] {path}")

   # Save all results to JSON
   import json
   all_results = {p: r.to_dict() for p, r in reports.items()}
   with open("batch_qc_report.json", "w") as f:
       json.dump(all_results, f, indent=2, default=str)
