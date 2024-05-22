.. mopper documentation master file
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ACCESS MOPPeR - A Model Output Post-Processor for the ACCESS climate model
==========================================================================

MOPPeR processes the raw ACCESS climate model output to produce CMIP style post-processed output using CMOR3.
MOPPeR is developed by the Centre of Excellence for Climate Extremes CMS team and is distributed via the ACCESS-NRI conda channel and github.
ACCESS-MOPPeR is based on the `APP4 tool <https://zenodo.org/records/7703469>`_.

Respect to the APP4 tool, MOPPeR is:

- python3 based;
- uses latest CMOR version;
- has an integrated tool to help generating mapping for new model versions;
- has more customisable options.


.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :titlesonly:

   overview.rst
   gettingstarted.rst
   CMOR3 <cmor.rst>
   customising.rst
   Mapping <mopdb_command.rst>
   cmip.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
