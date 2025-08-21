Working directory and output
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `mop setup` command generates the working and output directory based on the yaml configuration file passed as argument.

The directory path is determined by the `output` field. This can be a path or if `default` is set to:
 
  /scratch/<project-id>/<user-id>/MOPPER_output/<exp>/

where `exp` is also defined in the configuration file.

.. note::

  `mop setup` also produces the `map_var_selection.yaml` file which includes lists of matched variables for each table.
  This can be used as a list of variables to select by passing it in a configuration files as the `varlist` field.
  It can be useful to run first `mop setup` with `tables: all` to see which variables can be matched across all available tables and then rerun it using mop_var_selection.yaml as a varlist after refining the selection. 

This folder will contain the following files:


* **mopper.db**

  A database with a `filelist` table where each row represents a file to produce

  .. dropdown:: columns

    * infile - path + filename pattern for input files
    * filepath - expected output filepath
    * filename - expected output filename
    * vin - one or more input variables
    * variable_id - cmor name for variable
    * ctable - cmor table containing variable definition
    * frequency - output variable frequency
    * realm - output variable realm
    * timeshot - cell_methods value for time: point, mean, sum, max, min
    * axes - The cmor names of the axes used in variable definition
    * tstart - datetime stamp for time range start
    * tend - datetime stamp for time range end
    * sel_start - datetime stamp to use for input file selection (start)
    * sel_end - datetime stamp to use for input file selection (end)
    * status - file status: unprocessed, processed, processing_failed, ... Files are post-processed only if status "unprocessed"
    * file_size - estimated uncompressed file size in MB
    * exp_id - experiment id
    * calculation - string representing the calculation to perform, as it will be evaluated by python "eval" (optional)
    * resample - if input data has to be resample the timestep to be used by resample (optional)
    * in_units - units for main input variable
    * positive - "up" or "down" if attribute present in variable definition (optional)
    * cfname - CF conventions standard_name if available
    * source_id - model id
    * access_version - model version
    * json_file_path - filepath for CMOR json experiment file
    * reference_date - reference date to use for time axis
    * version - version label for output

* **mopper_job.sh**

  The PBS job to submit to the queue to run the post-processing.

  .. dropdown:: Example

    | #!/bin/bash
    | #PBS -P v45
    | #PBS -q hugemem
    | #PBS -l storage=gdata/xp65+gdata/ua8+scratch/ly62+scratch/v45+gdata/v45
    | #PBS -l ncpus=24,walltime=12:00:00,mem=768GB,wd
    | #PBS -j oe
    | #PBS -o /scratch/v45/pxp581/MOPPER_output/ashwed1980/job_output.OU
    | #PBS -N mopper_ashwed1980
    |
    | # the code assumes you are running this on gadi and have access to the xp65 project modules
    | # if this is not the case make sure you have loaded alternative python modules
    | # see https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR/blob/main/requirements.txt
    | # for a list of packages
    |
    | module use /g/data/xp65/public/modules
    | module load conda/analysis3
    | source mopper_env/bin/activate # if using conda option
    |
    | cd /g/data/ua8/Working/packages/ACCESS-MOPPeR
    | mop  run -c ashwed1980_config.yaml # --debug (uncomment to run in debug mode)
    | echo 'APP completed for exp ashwed1980.'

* **experiment-id.json**

  The json experiment file needed by CMOR to create the files

* **maps/**  

  A folder containing one json file for each CMOR table used, each file contains the mappings for all selected variables.

* **tables/**  

  A folder containing one json file for each CMOR table used, each file contains the CMOR definition for all selected variables.

* **mopper_log.txt**  

  A log file capturing messages from the main `run` process

* **cmor_logs/**

  A folder containing a log for each file created with cmor logging messages.

* **variable_logs/** 

  A folder containing a log for each file created, detailing the processing steps and, if run in debug mode, debug messages.

* **update_db.py**  

  A basic python code to update file status in the mopper.db database after a run

