Starting with MOPPeR
====================

A typical workflow to post-process an ACCESS or UM model output requires three steps.

Step1: get a list of variables from the raw output
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   mopdb varlist -i <path-to-raw-output> -d <date-pattern>
   mopdb varlist -i /scratch/.. -d 20120101 

`mopdb varlist` will output one or more `csv` files with a detailed list of variables, one list for each pattern of output files.
Here is an example of varlist output:

.. literalinclude:: varlist_example.csv
  :language: csv

The <date-pattern> argument is used to reduce the number of files to check. The tool will recognise anyway a repeated pattern and only add a list of variable for the same pattern once.

 
Step2: create a template for a mapping file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   mopdb template -i <varlist.csv> -v <access-version> -a <alias>
   mopdb varlist -i myexperiment.csv -v AUS2200 - a exp22 

`mopdb template` takes as input:
 * the output/s of `varlist` - To get one template for the all variable concatenate the output on `varlist` into one file first.
 * the access version to use as preferred
 * an optional alias if omitted the varlist filename will be used. Based on the example: `map_exp22.csv` or `map_varlist.csv` if omitted.

The output is one csv file with a list of all the variables from raw output mapped to cmip style variables. The mappings also take into account the frequency and include variables that can be potentially calculated with the listed fields. 
This file should be considered only a template (hence the name) as the tool will try to match the raw output to the mappings stored in the access.db database distributed with the repository or an alternative custom database.
The mappings can be different between different version and/or configurations of the model. And the database doesn't necessarily contain all the possible combinations.
In particular, from version 0.6 a list of mappings matched by standard_name is added, as these rows often list more than one option per field, it's important to either edit or remove these rows before using the mapping file. 
To see more on what to do should your experiment use a new configuration which is substantially different from what is available see relevant .... 

.. warning:: 
   Always check that the resulting template is mapping the variables correctly. This is particularly true for derived variables. Comment lines are inserted to give some information on what assumptions were done for each group of mappings.


Step3: Set up the working environment 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   mop -i <conf_exp.yaml> setup
   mopdb  -i conf_flood22.yaml setup 

`mop setup` takes as input a yaml configuration file for the experiment based on the provided ACDD_conf.yaml for custom mode and CMIP6_conf.yaml for CMIP6 mode.

Configuring the post-processing
===============================

The configuration yaml file passed to `mop setup` contains all the information needed by the tool to post-process the data.
This is in most cases the only file a user might have to modify. It is divided into 2 section

cmor
----

this part contains all the file path information, for exsmple where the inpout files are, where the output will be saved, the paths for extra cmor table and mapping etc.

.. literalinclude:: cmor_conf.yaml
  :language: yaml

attributes
----------
The second part is used to define the global attributes to add to every file.
As in most cases people are post-processing the data to share it we created two templates one for CMIP6 compliant files, the other for ACDD compliant files. You can use this last if you aren't following any specific convention but want to make sure you are producing reasonably well-documented files.
While the CMIP6 file should be followed exactly, the ACDD template is just including a a minimum number of attributes, you can always add any other attribute you deem necessary.

.. literalinclude:: attr_conf.yaml
  :language: yaml


.. note::
   These two configurations are based on CMOR Controlled Vocabularies currently available with the repository. 
   A user can define and set their own CV and then modify the configuration yaml file correspondingly. However, CMOR still had some hardcoded attributes that cannot be bypassed, see the `CMOR section <Understanding the CMOR3 structure>`_ for more information.


`mop setup` sets up the working environment by default in 

.. code-block:: bash

   /scratch/<project>/<userid>/MOPPeR-Output/

This includes the mopper_job.sh job to submit to the queue.  
If `test` is set to False in the configuration file, the job is automatically submitted. 

.. note::
   `mop run` is used to execute the post-processing and it is called in mopper_job.sh. 
   It takes a final experiment configuration yaml file generated in the same setup step to finalise the run settings. This file will contain all necessary information (including more details added by the tool itself) and can be kept for provenance and/or re-used to repeat the same process.

