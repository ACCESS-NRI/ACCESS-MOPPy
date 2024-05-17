Install
-------

You can install the latest version of `mopper` directly from conda (accessnri channel)::

   conda install -c accessnri mopper 

If you want to install an unstable version or a different branch:

    * git clone 
    * git checkout <branch-name>   (if installing a a different branch from master)
    * cd mopper 
    * pip install ./ 
      use --user flag if you want to install it in ~/.local

Working on NCI server
---------------------

MOPPeR is pre-installed into a Conda environment at NCI. Load it with::

    module use /g/data3/hh5/public/modules
    module load conda/analysis3-unstable

.. note::
   You need to be a member of the hh5 project to load the modules.
   
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
