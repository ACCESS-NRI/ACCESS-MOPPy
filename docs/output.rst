Working directory and output
============================

The `mop setup` command generate the working and output directory based on the yaml configuration file passed as argument.

The directory path is determined by the `output` field. This can be a path or if `default` is set to:
 
  /scratch/<project-id>/<user-id>/MOPPER_output/<exp>/

where `exp` is also defined in the configuration file.

This folder will contain the following files:

.. dropdown::  experiment-id.json

    The json experiment file needed by CMOR to create the files

.. dropdown:: mopper.db 

    A database with a `filelist` table where each row has the 
    Filelist table contains the following columns:

    * infile - path+ filename pattern for input files
    * filepath - expected output filepath
    * filename - expected output filename
    * vin - one or more input variables
    * variable_id - cmor name for variable
    * ctable - cmor table containing variable definition
    * frequency - output variable frequency
    * realm - output variable realm
    * timeshot - cell_methods value for time: point, mean, sum, max, min 
    * tstart -
    * tend -
    * sel_start -
    * sel_end - 
    * status - file status: unprocessed, processed, processing_failed, ...
    * file_size - estimated uncompressed file size in (bytes?)
    * exp_id - experiment id
    * calculation - string representing the calculation to perform, as it will be evaluated by python "eval" if not calculation is empty
    * resample - if input data has to be resample the timestep to be used by resample, otherwise is empty
    * in_units - units for main input variable
    * positive - 
    * cfname - CF conventions standard_name if available
    * source_id - model id
    * access_version - model version
    * json_file_path - filepath for CMOR json experiment file
    * reference_date - reference date to use for time axis
    * version - version label for output

.. dropdown:: mopper_job.sh  

    The PBS job to submit to the queue to run the post-processing.

    Example:

    #!/bin/bash
    #PBS -P v45
    #PBS -q hugemem
    #PBS -l storage=gdata/hh5+gdata/ua8+scratch/ly62+scratch/v45+gdata/v45
    #PBS -l ncpus=24,walltime=12:00:00,mem=768GB,wd
    #PBS -j oe
    #PBS -o /scratch/v45/pxp581/MOPPER_output/ashwed1980/job_output.OU
    #PBS -N mopper_ashwed1980
    
    module use /g/data/hh5/public/modules
    module load conda/analysis3-23.04

    cd /g/data/ua8/Working/packages/ACCESS-MOPPeR
    python mopper.py  -i ashwed1980_config.yaml run
    echo 'APP completed for exp ashwed1980.'

.. dropdown:: mopper_log.txt  

    A log file capturing messages from the main `run` process

.. dropdown::  update_db.py  

    A basic python code to update file status in the mopper.db database after a run

.. dropdown:: maps  

    A folder containing one json file for each CMOR table used, each file contains the mappings for all selected variables.

.. dropdown:: tables  

    A folder containing one json file for each CMOR table used, each file contains the CMOR definition for all selected variables.

.. dropdown:: cmor_logs

    A folder containing a log for cmor generated messages for each file created

.. dropdown:: variable_logs 

    A folder containing a log for each file created, detailing the processing steps, and if run in debug mode, debug messages.

