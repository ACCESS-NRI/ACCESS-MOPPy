Tutorials
=========

Step-by-step lessons for driving ACCESS-MOPPy from Python. Start here if you
are new to the tool, or if you want to CMORise a few variables interactively
before committing to a full batch run.

The notebook pages below are rendered from the executable notebooks in the
``notebooks/`` directory of the repository — clone it and open them in
JupyterLab to follow along. Most read ACCESS archives on **NCI Gadi**, so
you will need access to the relevant project directories (and, for the larger
runs, an ARE session with enough memory); adjust the archive paths at the top
of each notebook to point at your own output.

.. toctree::
   :maxdepth: 2
   :caption: Python API

   getting_started
   notebooks/Getting_started

.. toctree::
   :maxdepth: 2
   :caption: Worked examples

   notebooks/Tutorial1_CMORisation_ENSO_Recipes
   notebooks/Tutorial_CM3
   notebooks/Tutorial_ESMValTool_Integration

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Notebook
     - What it covers
   * - :doc:`notebooks/Getting_started`
     - The full API walkthrough: file discovery, building an
       ``ACCESS_ESM_CMORiser``, inspecting variable mappings, and running,
       inspecting, and writing the result. Includes atmosphere, ocean, and
       land examples.
   * - :doc:`notebooks/Tutorial1_CMORisation_ENSO_Recipes`
     - CMORising the ACCESS-ESM1.6 spin-up for the ACCESS-NRI ENSO Recipes.
   * - :doc:`notebooks/Tutorial_CM3`
     - CMORising ACCESS-CM3 output.
   * - :doc:`notebooks/Tutorial_ESMValTool_Integration`
     - Using the ``access_moppy.esmval`` subpackage to CMORise on the fly for
       an ESMValTool recipe. See also :doc:`/howto/esmvaltool_integration`.

For the complete list of classes and functions, see the
:doc:`API reference </reference/api/access_moppy/index>`.
