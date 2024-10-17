# [ACCESS Model Output Post-Processor (MOPPeR)](https://access-mopper.readthedocs.io/en/latest)
[![Read the docs](https://readthedocs.org/projects/access-mopper/badge/?version=latest)](https://access-mopper.readthedocs.io/en/latest/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.13841181.svg)](https://doi.org/10.5281/zenodo.13841181)

This code is derived from the [APP4](https://doi.org/10.5281/zenodo.7703469), initially created by Peter Uhe for CMIP5, and further developed for CMIP6-era by Chloe Mackallah from CSIRO, O&A Aspendale.

---

The MOPPeR is a CMORisation tool designed to post-process [ACCESS](https://research.csiro.au/access/) model output. The original APP4 main use was to produce [ESGF](https://esgf-node.llnl.gov/)-compliant formats, primarily for publication to [CMIP6](https://www.wcrp-climate.org/wgcm-cmip/wgcm-cmip6). The code was originally built for CMIP5, and was further developed for CMIP6-era activities.  
It used [CMOR3](https://cmor.llnl.gov/) and files created with the [CMIP6 data request](https://github.com/cmip6dr/dreqPy) to generate CF-compliant files according to the [CMIP6 data standards](https://docs.google.com/document/d/1os9rZ11U0ajY7F8FWtgU4B49KcB59aFlBVGfLC4ahXs/edit).The APP4 also had a custom mode option to allow users to post-process output without strict adherence to the ESGF standards. MOPPeR was developed to extend the custom mode as much as it is allowed by the CMOR tool, it can be used to produce CMIP6 compliant data but other standards can also be defined.

CMOR uses Controlled Vocabularies as metadata constraints, with [CMIP6_CV.json](https://cmor.llnl.gov/mydoc_cmor3_CV/) being the main one. This has an effect on how the data is written in the files, variables' names, directory structure, filenames, and global attributes. The APP4 also relied on mapping files to match the raw model output to CMOR defined variables. To make this approach more flexible we introduced a new tool `mopdb` that helps the users create their own mapping and handling CMOR tables definitions.
 
Designed for use on ACCESS model output that has been archived using the [ACCESS Archiver tool](https://github.com/ACCESS-Hive/ACCESS-Archiver).

Although we retained a differentiation between `custom` and `cmip` mode the main workflow is the same and `mode` is now only another field in the main  configuration file.

See [MOPPeR ReadtheDocs](https://access-mopper.readthedocs.io/en/stable/) for the full documentation.

### Install

You can install the latest version of `mopper` directly from conda (accessnri channel)::

   conda install -c coecms mopper

If you want to install an unstable version or a different branch:

    * git clone
    * git checkout <branch-name>   (if installing a a different branch from master)
    * cd mopper
    * pip install ./
      use --user flag if you want to install it in ~/.local

#### Working on the NCI server

MOPPeR is pre-installed into a Conda environment at NCI. Load it with::

    module use /g/data3/hh5/public/modules
    module load conda/analysis3-unstable

  NB. You need to be a member of the hh5 project to load the modules.
