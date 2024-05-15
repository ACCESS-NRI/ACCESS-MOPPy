ACCESS MOPPeR - A Model Output Post-Processor for the ACCESS climate model
===============================================


MOPPeR processes the raw ACCESS climate model output to produce CMIP style post-processed output using CMOR3.
MOPPeR is developed by the Centre of Excellence for Climate Extremes CMS team and is distributed via the ACCESS-NRI conda channel and github.
ACCESS-MOPPeR is based on the `APP4 tool <https://zenodo.org/records/7703469>`_.

Respect to the APP4 tool, MOPPeR is:

- python3 based;
- uses latest CMOR version;
- has an integrated tool to help generating mapping for new model versions;
- has more customisable options.

Commands
********

The ACCESS-MOPPeR includes two distinct modules `mopper` and `mopdb`

MOPPER
------ 

This is the module used to setup and run the files processing as a PBS job.

- **setup**  sets up the working environment and the PBS job
- **run**    executes the processing

MOPDB
-----

This module is used to manage the mapping of raw output to CMIP style variables.

- **varlist**  creates an initial list of variables and attributes based on actual files
- **template** uses the above list to generate a template of mappings to use in the processing
- **cmor**     populates the database cmor variables table
- **map**      populates the database mappings table
- **check**    checks a variable list against the cmor database table to individuate variables without a definition
- **table**    creates a CMOR style table based on a variable list
- **del**      selects and removes records from database tables based on constraints passed as input 



