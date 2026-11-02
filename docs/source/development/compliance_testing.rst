CMOR Output Compliance Testing
================================

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
--------

The integration test suite in ``tests/integration/test_full_cmorisation.py`` validates that
CMORised output files produced by ACCESS-MOPPy conform to CMIP6 metadata standards. After each
variable is processed, the test runs a compliance checker against the output ``.nc`` file.

Two compliance backends are supported:

- **PrePARE** — the historical default, now deprecated and no longer actively maintained.
- **WCRP compliance-checker** (``cc-plugin-wcrp``) — the recommended replacement, backed by
  the ``esgvoc`` vocabulary server and actively developed by the WCRP community.

The backend is chosen at test runtime via a command-line option (see below). PrePARE remains
the default for backwards compatibility, but new development should target the WCRP checker.

How the tests work
------------------

The main integration test is ``TestFullCMORIntegration.test_full_cmorisation_all_variables``,
parametrised over every table listed in ``CMOR_TABLES`` (``Amon``, ``Lmon``, ``Omon``, etc.).
For each table the test:

1. Loads the set of mapped variables for that table.
2. Selects appropriate input files based on the temporal frequency of the table
   (monthly, daily, 6-hourly, 3-hourly).
3. Runs ``ACCESS_ESM_CMORiser`` to produce a CMIP-compliant ``.nc`` file.
4. Calls ``_validate_output_compliance()`` with the chosen backend.

Ocean (``Omon``) and fixed-field (``Ofx``) variables are excluded from compliance
validation because their non-standard grid structures are not fully supported by either
checker.

The ``compliance_validation_tool`` session fixture (defined in ``tests/conftest.py``)
carries the selected backend name through the entire test session.

.. code-block:: python

   # tests/conftest.py — relevant excerpt
   def pytest_addoption(parser):
       parser.addoption(
           "--validation-tool",
           action="store",
            default="wcrp",
           choices=("prepare", "wcrp"),
           help="...",
       )

   @pytest.fixture(scope="session")
   def compliance_validation_tool(pytestconfig) -> str:
       return pytestconfig.getoption("validation_tool")

Default backend: WCRP compliance-checker (``cc-plugin-wcrp``)
--------------------------------------------------------------

The `WCRP compliance-checker plugin <https://github.com/WCRP-CMIP/cc-plugin-wcrp>`_ wraps the
`compliance-checker <https://github.com/ioos/compliance-checker>`_ framework and validates
files against CMIP6 (and future CMIP7) controlled vocabularies sourced from `esgvoc`.

The checker is invoked as::

   compliance-checker --test wcrp_cmip6:1.0 --format json --output <report>.json <output_file.nc>

The report is parsed from JSON and only *mandatory* checks (weight ≥ 3) cause the test to
fail. Known checker issues can be suppressed via two module-level constants in
``test_full_cmorisation.py``:

.. code-block:: python

   KNOWN_WCRP_CHECKER_EXCLUSIONS: set[str] = set()          # check names to skip entirely
   KNOWN_WCRP_CHECKER_MSG_EXCLUSIONS: tuple[str, ...] = ()  # substrings that suppress a check

The suite name is chosen from the CMIP family under test: ``wcrp_cmip6:1.0`` for
CMIP6, ``wcrp_cmip6plus:1.0`` for CMIP6Plus, and ``wcrp_cmip7:1.0`` for CMIP7.
Use ``compliance-checker --list-tests`` to see all available suites on your system.

If ``compliance-checker`` or the suite required for the CMIP family under test is not
available, the test automatically skips rather than fails, so the default is safe even
in environments where the checker is not installed.

If the checker is installed but the local ``esgvoc`` universe database has not been
initialized yet, the test also skips with a message telling you to run::

   esgvoc use universe@latest

Legacy backend: PrePARE (deprecated)
------------------------------------

`PrePARE <https://github.com/PCMDI/PrePARE>`_ checks CMIP6 compliance by comparing variable
attributes (units, standard name, cell methods, …) against CMIP6 CMOR tables.

.. warning::
   PrePARE is no longer actively maintained. The upstream repository has been archived and the
   tool does **not** support CMIP7 vocabularies.

When PrePARE is invoked explicitly, the test calls::

   PrePARE --variable <cmor_name> --table-path <table_dir> <output_file.nc>

If the ``PrePARE`` executable is not found the test is automatically skipped with
``pytest.skip``.

Installing the WCRP checker
----------------------------

.. code-block:: bash

   pip install compliance-checker cc-plugin-wcrp

Verify the installation::

   compliance-checker --list-tests | grep wcrp

You should see at least one line resembling ``- wcrp_cmip6:1.0``.

.. note::
   ``cc-plugin-wcrp`` connects to the ``esgvoc`` vocabulary service at runtime when
   performing certain checks. Ensure outbound network access is available in your CI
   environment or pre-cache the required vocabularies.

Switching to the WCRP backend
------------------------------

Pass ``--validation-tool wcrp`` on the pytest command line:

.. code-block:: bash

   pytest tests/integration/ \
       -m "integration and not slow" \
       --validation-tool wcrp

For the full slow suite:

.. code-block:: bash

   pytest tests/integration/ \
       -m "integration" \
       --validation-tool wcrp \
       --timeout 600

If ``compliance-checker`` or the ``wcrp_cmip6:1.0`` suite is not available the test
automatically skips rather than fails, so it is safe to always pass ``--validation-tool wcrp``
in environments where the checker may not be installed.

Making the WCRP backend the permanent default
---------------------------------------------

Once the WCRP checker is reliably available in your CI environment, change the ``default``
value of the ``--validation-tool`` option in ``tests/conftest.py``:

.. code-block:: python

   parser.addoption(
       "--validation-tool",
       action="store",
         default="wcrp",
       choices=("prepare", "wcrp"),
       ...
   )

   This repository now uses that configuration.

Suppressing known checker failures
------------------------------------

When a new WCRP check fails for a known reason (e.g. a limitation in ACCESS-MOPPy output
that is under active development), add the check name or a distinctive substring of its
message to the appropriate exclusion set at the top of ``test_full_cmorisation.py``:

.. code-block:: python

   # Suppress checks by exact name
   KNOWN_WCRP_CHECKER_EXCLUSIONS: set[str] = {
       "check_name_to_ignore",
   }

   # Suppress checks whose message contains any of these substrings
   KNOWN_WCRP_CHECKER_MSG_EXCLUSIONS: tuple[str, ...] = (
       "partial substring that identifies a known issue",
   )

Keep entries in these sets minimal and document the reason in an inline comment so they can
be removed once the underlying issue is resolved.

Compliance test markers
-----------------------

All integration tests use the ``integration`` and ``slow`` pytest markers.  Run them
selectively:

.. code-block:: bash

   # Only fast integration tests (skips slow marker)
   pytest tests/integration/ -m "integration and not slow"

   # All integration tests including slow ones
   pytest tests/integration/ -m "integration"

   # Only unit tests
   pytest tests/unit/ -m "unit"

See ``pytest.ini`` for the full list of registered markers.


Optional CMIP7 variable comments
--------------------------------

CMIP7 CMOR tables intentionally leave variable comments blank while the upstream
project homogenises descriptions. MOPPy can supplement them from an already
installed ESGVOC ``cmip7/branded_variable`` record. This is an optional, read-only
lookup; MOPPy does not install, update, or download vocabulary data. Without
ESGVOC or a usable local database, processing continues with a warning.

The lookup verifies both the exact branded identifier and membership of the full
compound product (realm, variable, branding, frequency and region). Only one
unique nonempty ``comment`` is accepted. Multiple alternatives are not ordered
preferences: they can describe different frequencies or hemispheres. Missing
comments, old schemas without compound identities, and ambiguous alternatives
remain optional findings. Root-variable descriptions are not substituted.
Existing producer comments are retained and supplemented without duplication.
Existing nonempty CMOR-table comments take precedence over registry lookup.

Known contradictory descriptions are withheld by exact text, so corrected
future records can be used: ``fNup`` must not be described as nitrogen fixation;
non-daily ``tossq`` must not be described as a daily average; and the current
sea-ice ``siflfwbot`` comment contradicts itself about the transfer direction.
These exclusions do not change calculations, signs, intervals, or masks.

For reproducibility, set ``ESGVOC_HOME`` to the pinned local vocabulary and
``ESGVOC_OFFLINE=true`` before starting MOPPy; record the vocabulary versions
alongside the producer and checker environments. ESGVOC 6.1.0 and 7.2.0 give the same
selection using CMIP7 2.5.4 and Universe 3.4.3. In the 117-product issue #767
selection, 85 comments are usable and 32 are withheld: 28 have alternatives,
three have the contradictions above, and monthly ``rsntds`` is absent from the
record's compound list (only its yearly product is listed). Among the original
113 completed products, the corresponding counts are 82 and 31. These are
metadata-selection counts, not a claim that every optional check passes.

The valid ``phalf`` hybrid-height ``computed_standard_name=altitude`` is retained.
A vocabulary that cannot verify it does not establish that the attribute is
scientifically wrong. See `MOPPy issue #823
<https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/823>`_ for validation and
remaining vocabulary questions.
