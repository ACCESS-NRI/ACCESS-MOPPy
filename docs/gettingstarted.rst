ACCESS MOPPeR - A Model Output Post-Processor for the ACCESS climate model
===============================================


MOPPeR processes the raw ACCESS climate model output to produce CMIP style post-processed output using CMOR3.
MOPPeR is developed by the Centre of Excellence for Climate Extremes CMS team and is distributed via the ACCESS-NRI conda channel and github.
ACCESS-MOPPeR is based on the [APP4 tool](https://zenodo.org/records/7703469).

Respect to the APP4 tool, MOPPeR is:

- python3 based;
- uses latest CMOR version;
- has an integrated tool to help generating mapping for new model versions;
- has more customisable options.

Commands:

The ACCESS-MOPPeR includes two distinct modules `mopper` and `mopdb`

MOPPER
------ 
This is the module used to setup and run the files processing as a PBS job.
- **setup**  setup the working environment and the PBS job
- **run**  execute the processing

MOPDB
-----

This module is used to manage the mapping of raw output to CMIP style variables.

- **varlist** create an initial list of variables and attributes based on actual files
- **template** uses the above list to egenrate a template of mappings to use in the processing
- **cmor** populates the database cmor varaibles table
- **map** populates the database mappings table
- **check** check a variable list against the cmor database table to individuate variables without a definition
- **table** create a CMOR style table based on a variable list



