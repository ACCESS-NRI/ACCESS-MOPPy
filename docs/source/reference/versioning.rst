Versioning and compatibility
============================

ACCESS-MOPPy version numbers come from git tags of the form ``moppy-vX.Y.Z``,
read at build time by ``versioneer``. The installed version is available as
``access_moppy.__version__`` and from ``pip show access_moppy``.

The version scheme
------------------

Releases follow semantic versioning:

``MAJOR``
   Incremented for a change that requires you to alter a working setup: a
   command or option removed or renamed, a batch configuration key that no
   longer means what it did, or a Python entry point that changes shape.

``MINOR``
   New commands, new configuration keys, new mappings and variables, and
   behaviour that only adds to what a working setup already does.

``PATCH``
   Bug fixes, including fixes that change the *contents* of output files where
   the previous contents were wrong — see the note on output below.

A suffix marks a pre-release: ``1.9.0rc1`` is a release candidate for
``1.9.0``, and ``1.7.20b`` was a beta. Pre-releases are published to PyPI and
to the ``rc`` label of the ``accessnri`` conda channel, but they are not added
to the ``analysis3`` environment on Gadi and ``pip`` will not install them
unless you ask:

.. code-block:: bash

   pip install --pre access_moppy

What the compatibility promise covers
-------------------------------------

From 1.8.0 onwards, the following will not change incompatibly within a major
version:

* **The command-line tools.** The ``moppy-*`` commands listed in
  :doc:`/reference/cli`, their arguments, and their documented options.
* **The batch configuration schema.** The keys documented in
  :doc:`/reference/configuration`. New keys may be added, and a key may gain
  new accepted values, but an existing configuration file will keep working
  and keep meaning the same thing.
* **The documented Python API.** What is exported from the ``access_moppy``
  top level — currently ``ACCESS_ESM_CMORiser``, ``DEFAULT_CHUNK_YEARS`` and
  ``__version__`` — together with the public methods of ``ACCESS_ESM_CMORiser``
  described in :doc:`/reference/api/access_moppy/index`.

What it does not cover
----------------------

* **Module internals.** Anything not reachable from the three surfaces above,
  including names prefixed with an underscore, the layout of the
  ``access_moppy`` submodules, and helper functions in ``utilities``. These
  move between releases without notice.
* **The generated CMOR scripts and job scripts.** The templates under
  ``templates/`` are an implementation detail of a batch run, not a stable
  interface for editing by hand.
* **The bundled controlled vocabularies.** ``CMIP6_CVs``, ``CMIP6Plus_CVs``,
  ``mip_cmor_tables`` and ``cmip7-cmor-tables`` are vendored upstream
  repositories and track their own releases. A MOPPy patch release may update
  them, which can change which variables are available, what their metadata
  says, and what passes compliance checking.
* **The tracker database schema.** The SQLite database behind
  ``moppy-tui`` and ``moppy-dashboard`` is internal state, not a reporting
  interface; read ``moppy_batch_report.json`` instead.

A note on output files
----------------------

The compatibility promise is about the *interface*, not about byte-identical
output. ACCESS-MOPPy exists to produce files that satisfy CMIP compliance
requirements, and those requirements are set upstream. When a compliance fix
lands, output written by the new version will differ from output written by
the old one — that is the fix working. Such changes ship in patch releases,
are listed in ``CHANGELOG.rst``, and are the reason to record the MOPPy
version alongside a published dataset.

If a released version wrote output you have already published and a later
release corrects it, the changelog entry says so explicitly.
