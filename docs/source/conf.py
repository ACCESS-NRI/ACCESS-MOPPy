# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------
import os
import shutil
import sys
from importlib import metadata as importlib_metadata
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

# -- Tutorial notebooks ------------------------------------------------------
# The tutorial notebooks live in notebooks/ at the repository root so they can
# be run straight from a checkout.  Sphinx can only include documents that sit
# under docs/source, so the ones we publish are copied into
# tutorials/notebooks/ at build time.  The copies are generated, never
# committed (see .gitignore).
#
# Tutorial_CMORise_ILAMB_Variables.ipynb is deliberately not published here: it
# is a batch/CLI walkthrough rather than a Python API tutorial, and it is
# already covered by howto/cmorise_ilamb_workflow.
PUBLISHED_NOTEBOOKS = [
    "Getting_started.ipynb",
    "Tutorial_CM3.ipynb",
    "Tutorial1_CMORisation_ENSO_Recipes.ipynb",
    "Tutorial_ESMValTool_Integration.ipynb",
]

_notebook_dst = Path(__file__).parent / "tutorials" / "notebooks"
_notebook_dst.mkdir(parents=True, exist_ok=True)
for _notebook in PUBLISHED_NOTEBOOKS:
    # Fails loudly if a published notebook is renamed or removed.
    shutil.copyfile(project_root / "notebooks" / _notebook, _notebook_dst / _notebook)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "ACCESS-MOPPy"
copyright = "2025, ACCESS-NRI"
author = "Romain Beucher, ACCESS-NRI"


def _get_docs_version() -> str:
    """Return the version for the currently built package/tree."""
    try:
        return importlib_metadata.version("access_moppy")
    except importlib_metadata.PackageNotFoundError:
        from access_moppy import _version

        return _version.get_versions()["version"]


release = _get_docs_version()
version = release.split("+", 1)[0]

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx_autodoc_typehints",
    "myst_parser",
    "nbsphinx",
    "autoapi.extension",
]

# -- sphinx-autoapi -----------------------------------------------------------
# Generates reference/api/** from the package source at build time, so the
# API reference is never hand-maintained or committed (see docs/.gitignore).
autoapi_type = "python"
autoapi_dirs = [str(project_root / "src")]
autoapi_root = "reference/api"
autoapi_add_toctree_entry = False
autoapi_keep_files = False
autoapi_member_order = "bysource"
# vocabularies/ vendors third-party CMOR-table/CV tooling as git submodules;
# it is not part of access_moppy's own API surface.
autoapi_ignore = [
    "*/vocabularies/*",
    "*/__pycache__/*",
]
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
]

# -- nbsphinx -----------------------------------------------------------------
# The tutorial notebooks read ACCESS archives on NCI Gadi, which the
# documentation builder cannot reach, so render the outputs stored in each
# notebook rather than re-executing the cells.
nbsphinx_execute = "never"
nbsphinx_allow_errors = True

_NOTEBOOK_SOURCE_URL = "https://github.com/ACCESS-NRI/ACCESS-MOPPy/blob/main/notebooks"

nbsphinx_prolog = f"""
{{% set nbname = env.docname.split("/")[-1] + ".ipynb" %}}

.. note::

   This page is rendered from the Jupyter notebook ``notebooks/{{{{ nbname }}}}``
   in the ACCESS-MOPPy repository.  The outputs below were stored the last time
   the notebook was run; cells are not re-executed when the documentation is
   built.  `Open the notebook on GitHub
   <{_NOTEBOOK_SOURCE_URL}/{{{{ nbname }}}}>`__ to run it yourself.
"""

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "**.ipynb_checkpoints",
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "canonical_url": "",
    "logo_only": False,
    "display_version": True,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False,
    "style_nav_header_background": "#2980B9",
    "collapse_navigation": True,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "includehidden": True,
    "titles_only": False,
}

# -- Extension configuration -------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False

autodoc_typehints = "description"
autoclass_content = "both"
autodoc_member_order = "bysource"

# Intersphinx mapping
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "xarray": ("https://docs.xarray.dev/en/stable/", None),
    "dask": ("https://docs.dask.org/en/stable/", None),
}

# MyST parser settings
myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "substitution",
    "tasklist",
]

# Mock imports for Read the Docs
on_rtd = os.environ.get("READTHEDOCS", None) == "True"
if on_rtd:
    autodoc_mock_imports = [
        "netCDF4",
        "cftime",
        "parsl",
    ]
else:
    autodoc_mock_imports = []
