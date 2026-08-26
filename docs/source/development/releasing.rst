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

   moppy-v1.7.19
   moppy-v1.7.19b
   moppy-v2.0.0rc1

Before you tag
--------------

1. Make sure the release commit is on the branch you want to release from and
   that all intended PRs are merged.
2. Update ``CHANGELOG.rst`` with a new top entry for the version you are about
   to tag. The changelog in this repository is the maintainer-written release
   summary; GitHub Releases can point back to it.
3. Check the vocabulary submodules are in the state you want to ship. The CD
   workflow explicitly verifies that the packaged wheel and sdist contain the
   bundled vocabulary content.
4. Run the validation you consider appropriate for the release. At minimum,
   make sure the CI for the release commit is green.
5. Confirm the worktree does not contain accidental local-only changes that
   should not be part of the tagged commit.

Creating the release
--------------------

Create an annotated tag on the release commit, then push it to GitHub:

.. code-block:: bash

   git tag -a moppy-v1.7.19b -m "ACCESS-MOPPy 1.7.19b"
   git push origin moppy-v1.7.19b

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
   ``ACCESS-NRI/ACCESS-Analysis-Conda``, and opens a pull request there.

After tagging, watch the ``CD`` workflow in GitHub Actions and confirm that all
three jobs finish successfully.

Checking the released artifacts
-------------------------------

After the workflow completes, check that:

1. PyPI shows the new ``access_moppy`` version.
2. Anaconda shows the new ``access-moppy`` and ``access-moppy-esmval``
   packages under the ``accessnri`` channel.
3. A PR was opened in ``ACCESS-NRI/ACCESS-Analysis-Conda`` with a title like
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

If the ``conda`` job succeeds but ``update_analysis3`` times out waiting for
the package, check that the uploaded version exists for
``accessnri::access-moppy-esmval`` rather than only for ``access-moppy``.

If the ``analysis3`` PR already exists, the workflow reuses the branch and does
not open a duplicate PR.
