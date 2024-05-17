MOPPeR workflow
---------------

mop
~~~
-c/--cfile  experiment configuration file (yaml) the `run` sub-command takes the updated version produced by `setup`
--debug show debug info

The `mop` command has two actions `setup` and `run`, which will go from a configuration file to running the post-processing and having a formatted model output which is CF compliant.

* Sets up the main log file
* calls sub-command: setup or run

setup
~~~~~
-d/--dbname database file to write filelist to. This is optional if passed the database is updated if not a new mopper.db file is created (to be implemented)

* Reads from configuration file: output file attributes, paths (input data, working dir, ancillary files), queue job settings, variables to process 
* Defines and creates output paths
* Updates CV json file if necessary
* Selects variables and corresponding mappings based on table and constraints passed in config file
* Creates/updates database filelist table to list files to create
* Finalises configuration and save in new yaml file
* Writes job executable file and submits (optional) to queue

run
~~~

-d/--dbname database file to read filelist from. This is optional by default expects mopper.db (to be implemented)

* Reads from mopper.db list of files to create
* Sets up the concurrent future pool executor and submits each file db list db table as a process.
* Each process:
  1. Sets up variable log file
  2. Sets up CMOR dataset, tables and axis
  3. Extracts or calculates variable
  4. Writes to file using CMOR3
* When all processes are completed results are returned to log files and status is updated in filelist database table

