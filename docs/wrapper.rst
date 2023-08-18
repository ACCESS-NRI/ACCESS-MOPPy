The wrapper
===========

The MOPPeR tool needs to know how to map the model output to the desired cmor variable definitions.

The mappings are passed to the tool via a master_<exp>.csv file.
This file contains the minimum information needed by the wrapper to then work out the filenames, what calculations need to happen if a variable should be resample to a different frequency, which tables to pass to CMOR, the file size etc.

The mapping master table will depend on the model ocnfiguration. If a user is running a series of experiments using a model configuration already available then they don't need to do anything.

If someone is running a different model configuration or using a new grid then aspecific mapping table eneds to be created. (see how to ...)

{As most configurations will still output similar variables, we created a tool that reads already available mappings and variable defintions from a database.

More information is added to the master table by the mopper wrapper, this will then save a copy of the final master table in the mopper output directory together with a database. The database will contained the file_master table which has a lists of the output file to be created, each row for one file containing all the information necessary to produce the file itself.


