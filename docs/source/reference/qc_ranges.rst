.. _qc-physical-ranges:

Physical range rules
====================

Every CMIP7 file ACCESS-MOPPy writes is checked against a physical range for
its variable: a broad sanity envelope meant to catch a unit, sign or
conversion mistake before the file reaches the ESGF, while still allowing
plausible model extremes. This page is the rendered form of those rules —
the same data the check itself reads.

.. seealso::

   :doc:`/howto/qc_validation` runs the checks;
   :ref:`qc-range-rendering` below shows how to get this table on the command
   line or as JSON.

Where the rules live
--------------------

.. code-block:: text

   src/access_moppy/resources/qc/cmip7_ranges.yml

Each variable carries a ``units`` string, a ``default`` block with ``min`` and
``max``, and an optional ``experiments`` map holding per-experiment overrides:

.. code-block:: yaml

   variables:
     tas:
       units: K
       default:
         min: 180.0
         max: 330.0
       experiments:
         piControl:
           max: 325.0

How a rule is resolved
----------------------

For a file with global attributes ``variable_id`` and ``experiment_id``:

1. The ``default`` block for ``variable_id`` is taken as the starting point.
2. If ``experiments`` holds an exact match for ``experiment_id``, that block is
   merged over the default. Keys not restated in the override — ``min`` in the
   ``tas``/``piControl`` example above — keep their default value.
3. Otherwise the experiment keys are treated as ``fnmatch`` patterns, so
   ``ssp*`` covers every SSP experiment. The longest matching pattern wins.
4. The rule's ``units`` must match the file's ``units`` attribute. A mismatch
   is a **failure**, not a warning, because it means the range being applied is
   not the range the data is in.
5. The observed minimum and maximum of the variable — computed once, with
   missing-value sentinels masked — are compared against the resolved bounds.

A range violation is reported as a **warning**: the file still passes QC, and
the observed and allowed ranges are recorded in the batch report so a human can
judge it. Only structural problems fail a file outright — all values missing,
values containing infinity, or a units mismatch.

Variables with no rule are not range-checked. Variables present in the
ACCESS-ESM1-6 mapping still receive the generic checks (non-missing, finite,
declared units).

.. _qc-range-rendering:

Rendering the rules
-------------------

``moppy-qc --show-ranges`` prints the rules without touching any data, so you
can see the bounds a file will be held to before you run anything:

.. code-block:: bash

   # Every variable, as a table
   moppy-qc --show-ranges

   # Two variables, resolved for one experiment
   moppy-qc --show-ranges --variable tas --variable pr --experiment piControl

.. code-block:: text

   variable  units       min  max  rule
   --------  ----------  ---  ---  ---------
   tas       K           180  325  piControl
   pr        kg m-2 s-1  0    0.1  default

   2 variable(s) from access_moppy/resources/qc/cmip7_ranges.yml
   resolved for experiment_id=piControl

The ``rule`` column names the block the bounds came from: ``default``, or the
experiment key that overrode it.

Add ``--format json`` for the machine-readable form — the JSON that carries the
ranges, ready to pipe into ``jq`` or attach to a data-quality record:

.. code-block:: bash

   moppy-qc --show-ranges --variable tas --experiment piControl --format json

.. code-block:: json

   {
     "experiment_id": "piControl",
     "source": "access_moppy/resources/qc/cmip7_ranges.yml",
     "variable_count": 1,
     "variables": {
       "tas": {
         "units": "K",
         "default": {"min": 180.0, "max": 330.0},
         "experiments": {"piControl": {"max": 325.0}},
         "resolved": {
           "units": "K",
           "min": 180.0,
           "max": 325.0,
           "rule": "piControl"
         }
       }
     }
   }

The ``resolved`` block is only present when ``--experiment`` is given.

The same payload is available from Python:

.. code-block:: python

   from access_moppy.qc import export_range_rules, format_range_rules_table

   rules = export_range_rules(variables=["tas"], experiment_id="piControl")
   print(rules["variables"]["tas"]["resolved"])
   print(format_range_rules_table(rules))

Rules in force
--------------

The table below is generated from ``cmip7_ranges.yml`` when the documentation
is built, so it always shows the bounds this version of ACCESS-MOPPy applies.
Type in the box to filter by variable name, unit or experiment.

.. include:: /_generated/qc_ranges_table.rst

Changing a rule
---------------

Bounds are deliberately broad. If a variable trips its range for a physical
reason — a warmer scenario, a different experiment — add or widen an
experiment override rather than loosening the default, and open a pull request
so every ACCESS-MOPPy user gets the corrected rule. See
:ref:`qc-extending-rules` for the editing workflow.
