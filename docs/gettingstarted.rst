Starting with MOPPeR
====================

A typical workflow to post-process an ACCESS or UM model output requires three steps.

Step1: get a list of variables from the raw output
--------------------------------------------------

     *mopdb varlist -i <path-to-raw-output> -d <date-pattern>*

`mopdb varlist` will output one or more `csv` files with a detailed list of variables, one list for each pattern of output files.

.. code-block:: console

   $ mopdb varlist -i /scratch/../exp -d 20120101
   Opened database ~/.local/lib/python3.10/site-packages/data/access.db successfully
   Variable list for ocean_scalar.nc- successfully written
   Variable list for ocean_month.nc- successfully written
   Variable list for ocean_daily.nc- successfully written

.. csv-table:: Example of varlist output 
   :file: varlist_example.csv
   :delim: ;

The <date-pattern> argument is used to reduce the number of files to check. The tool will recognise anyway a repeated pattern and only add a list of variable for the same pattern once.

 
Step2: create a template for a mapping file
-------------------------------------------

   *mopdb template -i <varlist.csv> -v <access-version> -a <alias>*

.. code-block:: console 

   $ mopdb template -f ocean.csv -v OM2 -a ocnmon
   Opened database ~/.local/lib/python3.10/site-packages/data/access.db successfully
   Derived variables: {'msftyrho', 'msftmrho', 'hfds', 'msftmz', 'msftyz'}
   Changing advectsweby-CM2_mon units from Watts/m^2 to W m-2
   Changing areacello-CMIP6_Ofx units from m^2 to m2
   Variable difvho-CM2_Omon not found in cmor table

`mopdb template` takes as input:
 * the output/s of `varlist` - To get one template for the all variable concatenate the output on `varlist` into one file first.
 * the access version to use as preferred
 * an optional alias, if omitted the varlist filename will be used. Based on the example: `map_ocnmon.csv` or `map_ocean.csv` if omitted.

It produces a csv file with a list of all the variables from raw output mapped to cmip style variables. These mappings also take into account the frequency and include variables that can be potentially calculated with the listed fields. The console output lists these, as shown above.
 
This file should be considered only a template (hence the name) as the possible matches depends on the mappings available in the `access.db` database. This is distributed with the repository or an alternative custom database can be passed with the `--dbname` option.
The mappings can be different between different version and/or configurations of the model. And the database doesn't necessarily contain all the possible combinations.

Starting with version 0.6 the list includes matches based on the standard_name, as these rows often list more than one option per field, it's important to either edit or remove these rows before using the mapping file. 
The :doc:`Customing section <customising>` covers what to do for an experiment using a new configuration which is substantially different from the ones which are available.

.. warning:: 
   Always check that the resulting template is mapping the variables correctly. This is particularly true for derived variables. Comment lines are inserted to give some information on what assumptions were done for each group of mappings.


Step3: Set up the working environment 
-------------------------------------

   *mop -c <conf_exp.yaml> setup*

.. code-block:: console 

   $ mop -c exp_conf.yaml setup
   Simulation to process: cy286
   Setting environment and creating working directory
   Output directory '/scratch/v45/pxp581/MOPPER_output/cy286' exists.
   Delete and continue? [Y,n]
   Y
   Preparing job_files directory...
   Creating variable maps in directory '/scratch/v45/pxp581/MOPPER_output/cy286/maps'

   CMIP6_Omon:
   could not find match for CMIP6_Omon-msftbarot-mon check variables defined in mappings
       Found 22 variables

   CMIP6_Emon:
       Found 3 variables

   CM2_mon:
       Found 2 variables
   creating & using database: /scratch/v45/pxp581/MOPPER_output/cy286/mopper.db
   Opened database /scratch/v45/pxp581/MOPPER_output/cy286/mopper.db successfully
   Found experiment: cy286
   Number of rows in filelist: 27
   Estimated total files size before compression is: 7.9506173729896545 GB
   number of files to create: 27
   number of cpus to be used: 24
   total amount of memory to be used: 768GB
   app job script: /scratch/v45/pxp581/MOPPER_output/cy286/mopper_job.sh
   Exporting config data to yaml file


`mop setup` takes as input a yaml configuration file which contains all the information necessary to post-process the data. The repository two templates which can be modified by the user: ACDD_conf.yaml and CMIP6_conf.yaml to get a CMIP6 compliant output. It is divided into 2 sections:

cmor
^^^^
This part contains all the file paths information for input files, mapping file, custom cmor tables if they exists and where the output should be saved. It's also how a user can control the queue jobs settings and which variables will be processed.

.. dropdown:: Example 

  .. literalinclude:: cmor_conf.yaml
    :language: yaml

attributes
^^^^^^^^^^
The second part is used to define the global attributes to add to every file. CMOR uses a controlled vocabulary file to list required attributes (see ..). We provide the official CMIP6 and a custom made controlled vocabulary as part of the repository data. Hence, we created two templates one for `CMIP6 compliant files <https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR/blob/main/src/data/cmor_tables/CMIP6_CV.json>`_, the other for `ACDD compliant files <https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR/blob/main/src/data/cmor_tables/ACDD_CV.json>`_. 
The ACDD conventions help producing reasonably well-documented files when a specific standard is not required, they are also the convetions requested by NCI to publish data as part of their collection.
While the CMIP6 file should be followed exactly, the ACDD template is just including a minimum number of required attributes, any other attribute deem necessary can always be added.

.. dropdown:: Example 

  .. literalinclude:: attr_conf.yaml
    :language: yaml

.. note::
   These two configurations are based on CMOR Controlled Vocabularies currently available with the repository. 
   A user can define and set their own CV and then modify the configuration yaml file correspondingly. However, CMOR still had some hardcoded attributes that cannot be bypassed, see the :doc:`CMOR3 section <cmor>` for more information.


Running the post-processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~

`mop setup` sets up the working environment by default in 

.. code-block:: bash

   /scratch/<project>/<userid>/MOPPeR-Output/

This includes the mopper_job.sh job to submit to the queue.  
If `test` is set to False in the configuration file, the job is automatically submitted. 

.. note::
   `mop run` is used to execute the post-processing and it is called in mopper_job.sh. 
   It takes a final experiment configuration yaml file generated in the same setup step to finalise the run settings. This file will contain all necessary information (including more details added by the tool itself) and can be kept for provenance and reproducibility.

.. include:: mop_workflow.rst

.. include:: output.rst
