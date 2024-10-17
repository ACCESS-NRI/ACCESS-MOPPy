MOPPeR workflow
~~~~~~~~~~~~~~~
A more detailed overview of the workflow going on when calling mop.

setup
^^^^^

* Reads from configuration file: output file attributes, paths (input data, working dir, ancillary files), queue job settings, variables to process 
* Defines and creates output paths
* Updates CV json file if necessary
* Selects variables and corresponding mappings based on table and constraints passed in config file
* Produces mop_var_selection.yaml file with variables matched for each table
* Creates/updates database filelist table to list files to create
* Finalises configuration and save in new yaml file
* Writes job executable file and submits (optional) to queue

run
^^^

* Reads from mopper.db list of files to create
* Sets up the concurrent future pool executor and submits each file db list db table as a process.
* Each process:
  1. Sets up variable log file
  2. Sets up CMOR dataset, tables and axis
  3. Extracts or calculates variable
  4. Writes to file using CMOR3
* When all processes are completed results are returned to log files and status is updated in filelist database table

