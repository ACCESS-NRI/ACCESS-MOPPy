"""
access_moppy.esmval
===================

Integration layer between ACCESS-MOPPy and ESMValCore / ESMValTool.

This subpackage allows ACCESS-ESM1.6 raw model output to be CMORised
on-the-fly as an automatic pre-processing step before ESMValTool runs a
recipe.  It does **not** modify ESMValCore or ESMValTool source code.

Typical usage
-------------
::

    # Command-line (all-in-one):
    moppy-esmval-run my_recipe.yml

    # Two-step (e.g. pre-cache on HPC, then run on login node):
    moppy-esmval-prepare my_recipe.yml
    esmvaltool run my_recipe.yml

    # Registered as an esmvaltool sub-command (after package install):
    esmvaltool cmorise my_recipe.yml
    esmvaltool run    my_recipe.yml

Public API
----------
- :class:`~access_moppy.esmval.recipe_reader.RecipeReader`
- :class:`~access_moppy.esmval.variable_mapper.VariableIndex`
- :class:`~access_moppy.esmval.file_finder.RawFileFinder`
- :class:`~access_moppy.esmval.orchestrator.CMORiseOrchestrator`
- :func:`~access_moppy.esmval.config_gen.write_esmval_config`
"""

from access_moppy.esmval.orchestrator import CMORiseOrchestrator
from access_moppy.esmval.recipe_reader import CMORTask, RecipeReader
from access_moppy.esmval.variable_mapper import VariableIndex

__all__ = [
    "CMORiseOrchestrator",
    "CMORTask",
    "RecipeReader",
    "VariableIndex",
]
