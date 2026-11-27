Develop and deploy MOPPy Studio
===============================

Studio lives in ``apps/studio/`` as the independently versioned
``access-moppy-studio`` package. Its beta version is ``0.1.0b1``. The root
``access_moppy`` installation and release workflow do not install or publish it.
User instructions are in :doc:`../howto/studio`.

Development environment
-----------------------

From the repository root:

.. code-block:: bash

   cd apps/studio
   python3 -m venv .venv
   .venv/bin/python -m pip install -e '.[dev]'
   .venv/bin/python -m pytest -q
   .venv/bin/ruff check .
   .venv/bin/ruff format --check .
   .venv/bin/moppy-studio

Open the printed localhost URL and enter the printed session token. Editing
configs works locally. Runtime preflight and PBS submission require Gadi and
accessible input paths. The development server binds to loopback.

``python -m build`` produces a standalone wheel and source distribution. Web
assets are packaged under ``moppy_studio/web`` so an installed wheel works outside
the checkout. The ``are`` extra adds Jupyter Server and its proxy; ``dev`` adds
the test and lint tools. Studio CI checks both the installed wheel and source
tests without installing the scientific MOPPy stack.

Architecture and compatibility
------------------------------

* ``app.py`` exposes the authenticated API and static web assets.
* ``configuration.py`` preserves advanced configuration values and checks the
  fields interpolated by native shell templates.
* ``service.py`` owns saved reviews, one-time submission and reconciliation.
  An atomic directory claim prevents duplicate submission across Lustre clients.
  Derived run states are returned without overwriting submission records.
* ``runtime.py`` launches ``bridge.py`` in the selected analysis3 environment.
  The bridge renders MOPPy's native monitor template and initialises tracking
  before the first submission. The web process does not import scientific code.
* ``probes.py`` validates the read-only subset of draft configuration.
  ``discovery.py`` runs in analysis3, queries the official CMIP7 Data Request API,
  resolves installed model mappings and samples NetCDF headers. It uses the
  native table-specific resolver, pattern builder, filename-year parser and
  optional-formula-input rules. Changes to these interfaces need adapter checks.
* ``monitor.py`` reads SQLite without migrating or modifying it. Once a batch is
  submitted, native MOPPy owns the database and workers.
* ``are.py`` provides the authenticated Jupyter proxy used by the module launcher.

The beta bridge uses MOPPy's ``compute_monitor_walltime``, monitor template and
``TaskTracker`` interfaces. Changes to these interfaces or the tracking schema
must be checked against Studio. A supported core batch API can replace this
adapter in a follow-up; this addition does not refactor the scientific engine.

The ARE launcher requires an ARE-generated hashed password and base URL. It uses
an authenticated Unix-socket backend, overrides the internal session-token header
and enforces XSRF on mutations. Only the initial password POST has the exception
needed for ARE's login form. Personal Jupyter extensions are not loaded. The
integration test exercises actual login, proxying, assets and API mutations.

Install or update the shared ARE application
--------------------------------------------

Run deployment as the account that owns the shared installation. Copy the
contents of ``apps/studio/`` to ``/g/data/xp65/public/apps/moppy-studio``, excluding
development environments, caches and build products. Preserve the existing
``.are-venv`` during updates. Then run:

.. code-block:: bash

   cd /g/data/xp65/public/apps/moppy-studio
   bash deploy/install.sh

The installer loads ``python3/3.11.7``, installs ``.[are]`` into ``.are-venv`` and
installs the modulefile at ``/g/data/xp65/public/modules/moppy-studio``. The module
puts the Jupyter wrapper on PATH; the wrapper invokes the installed package.
Restart ARE sessions after updating. Record the deployed commit and package
version so the deployment can be reproduced or rolled back.

The installation is independent of analysis3 and the other Studio applications.
Do not install the web dependencies into analysis3. To use a different shared
application path, update the modulefile and module installation destination.

Validation before deployment
----------------------------

The optional browser regression check exercises config import, variable selection,
resource overrides, YAML round-trips, downloads and review requests against a local
Studio server. It intercepts job review and submission, so no PBS jobs are launched.
With Node.js and Playwright available, start Studio locally and run in another
terminal:

.. code-block:: bash

   cd apps/studio
   npm install --no-save --package-lock=false playwright
   npx playwright install chromium
   STUDIO_URL=http://127.0.0.1:8767/ STUDIO_TOKEN='<printed session token>' \
     node tests/browser_builder.cjs
   STUDIO_URL=http://127.0.0.1:8767/ STUDIO_TOKEN='<printed session token>' \
     node tests/browser_request.cjs

``CHROMIUM_PATH`` optionally selects an existing Chromium executable.
``STUDIO_SCREENSHOT`` optionally saves a desktop screenshot. The local server and
test must share a filesystem for the load-by-path check.

``browser_request.cjs`` intercepts scientific probes and checks checklist merging,
mapping/input filters, conservative selection, report export, mobile layout and
discarding stale responses. To verify the actual scientific adapter in analysis3,
run ``python tests/native_discovery_probe.py`` from ``apps/studio``. It queries
the pinned official release, verifies an offline bridge query, creates temporary
NetCDF fixtures, checks year filtering, table restrictions and optional inputs,
and confirms the user's API configuration is unchanged. It submits no jobs.

Run the package tests, including the real Jupyter login/proxy test, and inspect
the light/dark interface at desktop and narrow widths. For changes affecting
submission or the bridge, run a bounded scientific pilot under an allocated
project: a few variables, one year, fresh scratch output and explicit storage.
Inspect worker and monitor exit codes, time coverage, units, metadata and native
QC/compliance results. Submission by itself is not a successful scientific test.

The initial pilot processed daily ``tas``, ``pr`` and ``tos`` for 1850 through
Studio. All three workers and the monitor exited zero; each output had 365 time
steps and passed the configured CF/CMIP7 checks. No production output was copied
or changed. This verifies that workflow, not all variable mappings or experiments.
