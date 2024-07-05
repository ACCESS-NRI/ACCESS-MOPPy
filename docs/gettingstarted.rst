Starting with MOPPeR
====================

A typical workflow to post-process an ACCESS or UM model output requires three steps.

 
Step1: create a template for a mapping file
-------------------------------------------

   *mopdb template -f <path-to-model-output> -v <access-version> -a <alias>*

.. code-block:: console 

   $ mopdb template -f /scratch/.../exp1/atmos -m 095101 -v CM2 -a exp1
   Opened database /home/581/pxp581/.local/lib/python3.10/site-packages/data/access.db successfully
   Found more than 1 definition for fld_s16i222:
   [('psl', 'AUS2200', 'AUS2200_A10min', '10minPt'), ('psl', 'AUS2200', 'AUS2200_A1hr', '1hr')]
   Using psl from AUS2200_A10min
   Variable list for cw323a.pm successfully written
   Opened database /home/581/pxp581/.local/lib/python3.10/site-packages/data/access.db successfully
   Derived variables: {'treeFracBdlEvg', 'grassFracC4', 'shrubFrac', 'prc', 'mrsfl', 'landCoverFrac', 'mmrbc', 'mmrso4', 'theta24', 'sftgif', 'treeFracNdlEvg', 'snw', 'rtmt', 'nwdFracLut', 'sifllatstop', 'prw', 'mrfso', 'rlus', 'mrsll', 'baresoilFrac', 'c4PftFrac', 'wetlandFrac', 'mrro', 'c3PftFrac', 'treeFracBdlDcd', 'od550lt1aer', 'treeFracNdlDcd', 'residualFrac', 'wetss', 'sbl', 'vegFrac', 'rsus', 'cropFrac', 'mmrdust', 'grassFrac', 'mmrss', 'od550aer', 'hus24', 'dryss', 'fracLut', 'mrlso', 'mc', 'od440aer', 'grassFracC3', 'nep', 'mmroa', 'cropFracC3', 'snm', 'agesno'}
   Changing cl-CMIP6_Amon units from 1 to %
   Changing cli-CMIP6_Amon units from 1 to kg kg-1
   Changing clt-CMIP6_Amon units from 1 to %
   Changing clw-CMIP6_Amon units from 1 to kg kg-1
   Variable husuvgrid-CM2_mon not found in cmor table
   ...

`mopdb template` takes as input:
 * -f/--fpath : the path to the model output
 * -m/--match : used to identify files' patterns. The tool will only add a list of variables for the same pattern once.
 * -v/--version : the access version to use as preferred mapping. ESM1.5, CM2, OM2 and AUS2200 are currently available.
 * -a/--alias : an optional alias, if omitted default names will be used for the output files. 

Alternatively a list of variables can be created separately using the *varlist* command and this can be passed directly to template using the *fpath* option.

   *mopdb template -f <varlist.csv> -v <access-version> -a <alias>*

It produces a csv file with a list of all the variables from raw output mapped to cmip style variables. These mappings also take into account the frequency and include variables that can be potentially calculated with the listed fields. The console output lists these, as shown above.
 
This file should be considered only a template (hence the name) as the possible matches depends on the mappings available in the `access.db` database. This is distributed with the repository or an alternative custom database can be passed with the `--dbname` option.
The mappings can be different between different version and/or configurations of the model. And the database doesn't necessarily contain all the possible combinations.

Starting with version 0.6 the list includes matches based on the standard_name, as these rows often list more than one option per field, it's important to either edit or remove these rows before using the mapping file. 
The :doc:`Customing section <customising>` covers what to do for an experiment using a new configuration which is substantially different from the ones which are available.
It also provides an intermediate varlist_<alias>.csv file that shows the information derived directly from the files. This can be useful to debug in case of issues with the mapping. This file is checked before the mapping step to make sure the tool has detected sensible frequency and realm, if the check fails the mapping won't proceed but the varlist file can be edited appropriately.

.. warning:: 
   Always check that the resulting template is mapping the variables correctly. This is particularly true for derived variables. Comment lines are inserted to give some information on what assumptions were done for each group of mappings.
   The se


Step2: Set up the working environment 
-------------------------------------

   *mop -c <conf_exp.yaml> setup*

.. code-block:: console 
https://climate-cms.org/posts/2023-05-31-vscode-are.html
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


.. _conf-file:

The `mop setup` command takes as input a yaml configuration file which contains all the information necessary to post-process the data. The repository two templates which can be modified by the user: ACDD_conf.yaml and CMIP6_conf.yaml to get a CMIP6 compliant output. It is divided into 2 sections:

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
