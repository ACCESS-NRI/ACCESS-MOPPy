Releasing ACCESS-MOPPy
======================

This guide is for maintainers creating an ACCESS-MOPPy release and for anyone
following up the automatic ``analysis3`` environment update in
``ACCESS-Analysis-Conda``.

How releases are versioned
--------------------------

ACCESS-MOPPy uses ``versioneer`` with the git tag prefix ``moppy-v``. The
release version is taken from the tag itself, so there is no version string to
edit in the source tree before a release.

Examples of valid release tags:

.. code-block:: text

   moppy-v1.8.0
   moppy-v1.8.1
   moppy-v1.9.0rc1

What the numbers mean, and what the project promises not to break within a
major version, is documented for users in :doc:`/reference/versioning`. Read it
before choosing between a patch and a minor bump.

Final releases and pre-releases
-------------------------------

A tag whose version is plain digits and dots — ``moppy-v1.8.0`` — is a final
release. Anything else — ``moppy-v1.9.0rc1``, and the ``a``/``b`` suffixes used
throughout the 1.0 to 1.7 series — is a PEP 440 pre-release, and the CD
workflow treats it differently:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * -
     - Final release
     - Pre-release
   * - PyPI
     - published
     - published, but ``pip`` installs it only with ``--pre``
   * - ``accessnri`` conda channel
     - uploaded to the ``main`` label
     - uploaded to the ``rc`` label
   * - ``analysis3`` environment
     - updated automatically
     - not updated

This is why every release up to ``moppy-v1.7.20b`` was invisible to a plain
``pip install access_moppy``: the whole history was pre-releases. From
``1.8.0`` onwards, tag without a suffix unless you specifically want a
candidate.

Before you tag
--------------

1. Make sure the release commit is on the branch you want to release from and
   that all intended PRs are merged.
2. Update ``CHANGELOG.rst`` with a new top entry for the version you are about
   to tag. The changelog in this repository is the maintainer-written release
   summary; GitHub Releases can point back to it.
3. Check the vocabulary submodules are in the state you want to ship. Run
   ``git submodule update --init --recursive`` so the checked-out submodules
   match the commits the release commit records — ``git submodule status``
   marks a mismatch with a leading ``+``. The CD workflow explicitly verifies
   that the packaged wheel and sdist contain the bundled vocabulary content,
   but it cannot tell that the content is the revision you meant.
4. Run the validation you consider appropriate for the release. At minimum,
   make sure the CI for the release commit is green.
5. Confirm the worktree does not contain accidental local-only changes that
   should not be part of the tagged commit.

Creating the release
--------------------

Create an annotated tag on the release commit, then push it to GitHub:

.. code-block:: bash

   git tag -a moppy-v1.8.0 -m "ACCESS-MOPPy 1.8.0"
   git push origin moppy-v1.8.0

Pushing a tag matching ``moppy-v*`` triggers ``.github/workflows/cd.yml``.

What the CD workflow does
-------------------------

The release workflow has three jobs:

1. ``pypi`` builds the sdist and wheel, checks that the bundled vocabulary
   directories are present in both artifacts, and publishes the package to
   PyPI.
2. ``conda`` builds the conda packages ``access-moppy`` and
   ``access-moppy-esmval`` from the tagged source and uploads them to the
   ``accessnri`` Anaconda channel.
3. ``update_analysis3`` waits for ``access-moppy-esmval`` to appear in the
   channel, updates the ``analysis3`` environment in
   ``ACCESS-NRI/ACCESS-Analysis-Conda``, and opens a pull request there. This
   job is skipped for a pre-release.

After tagging, watch the ``CD`` workflow in GitHub Actions and confirm that the
jobs finish successfully — all three for a final release, the first two for a
candidate.

Releasing a candidate first
---------------------------

For a release that changes something users depend on, or when the release
machinery itself has changed, tag a candidate before the final version:

.. code-block:: bash

   git tag -a moppy-v1.9.0rc1 -m "ACCESS-MOPPy 1.9.0rc1"
   git push origin moppy-v1.9.0rc1

The candidate reaches PyPI and the conda ``rc`` label without touching
``analysis3``, so it can be installed and exercised on Gadi:

.. code-block:: bash

   pip install --pre access_moppy==1.9.0rc1
   # or
   conda install -c accessnri/label/rc access-moppy

When the candidate is good, tag the same commit with the final version and push
that tag. The candidate is not deleted; it stays on PyPI as a pre-release and
is simply never installed by default.

Checking the released artifacts
-------------------------------

After the workflow completes, check that:

1. PyPI shows the new ``access_moppy`` version.
2. Anaconda shows the new ``access-moppy`` and ``access-moppy-esmval``
   packages under the ``accessnri`` channel, on the ``main`` label for a final
   release or the ``rc`` label for a candidate.
3. For a final release, a PR was opened in
   ``ACCESS-NRI/ACCESS-Analysis-Conda`` with a title like
   ``Bump access-moppy to <version> in analysis3 env``.

How the ``analysis3`` update works
----------------------------------

The final release job updates ``ACCESS-Analysis-Conda`` automatically.

It does the following:

1. Derives the version from the workflow input or the tagged release.
2. Waits until ``accessnri::access-moppy-esmval==<version>`` is visible via
   the Anaconda API.
3. Clones ``ACCESS-NRI/ACCESS-Analysis-Conda``.
4. Creates or reuses a branch named ``update/access-moppy-<version>``.
5. Updates the existing ``access-moppy-esmval = { version = "...", ... }``
   dependency entry in ``environments/analysis3/pixi.toml`` to the released
   version.
6. Runs ``pixi run rebuild-env`` in ``environments/analysis3``.
7. Commits the regenerated ``pixi.toml``, ``pixi.lock``, ``solved.json``, and
   ``environment.yml`` files.
8. Pushes the branch and opens a PR if one does not already exist.

Manual ``analysis3`` update
---------------------------

If the package release succeeded but the ``analysis3`` update did not, make the
same change manually in ``ACCESS-NRI/ACCESS-Analysis-Conda``.

From a local clone of that repository:

.. code-block:: bash

   git checkout -b update/access-moppy-<version> origin/main

Edit ``environments/analysis3/pixi.toml`` so the existing
``access-moppy-esmval`` dependency points to ``==<version>``, then rebuild the
environment metadata:

.. code-block:: bash

   cd environments/analysis3
   pixi run rebuild-env

Commit the regenerated files and open a PR:

.. code-block:: bash

   git add environments/analysis3/pixi.toml \
           environments/analysis3/pixi.lock \
           environments/analysis3/solved.json \
           environments/analysis3/environment.yml
   git commit -m "analysis3: bump access-moppy to <version>"
   git push -u origin update/access-moppy-<version>

Then open the PR in GitHub against ``ACCESS-NRI/ACCESS-Analysis-Conda:main``.

Troubleshooting
---------------

If the release tag does not start with ``moppy-v``, the CD workflow will not
run automatically.

If ``update_analysis3`` was skipped, check the version the ``pypi`` job
reported. The job runs only when the version is plain digits and dots, so a
tag such as ``moppy-v1.8.0b`` or ``moppy-v1.8.0rc1`` skips it by design.

If the ``conda`` job succeeds but ``update_analysis3`` times out waiting for
the package, check that the uploaded version exists for
``accessnri::access-moppy-esmval`` rather than only for ``access-moppy``.

If the ``analysis3`` PR already exists, the workflow reuses the branch and does
not open a duplicate PR.
