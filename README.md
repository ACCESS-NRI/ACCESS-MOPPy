# [ACCESS Model Output Post-Processor (MOPPeR)](https://access-mopper.readthedocs.io/en/latest)
[![Read the docs](https://readthedocs.org/projects/access-mopper/badge/?version=latest)](https://access-mopper.readthedocs.io/en/latest/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14010850.svg)](https://doi.org/10.5281/zenodo.14010850)

This code is derived from the [APP4](https://doi.org/10.5281/zenodo.7703469), initially created by Peter Uhe for CMIP5, and further developed for CMIP6-era by Chloe Mackallah from CSIRO, O&A Aspendale.

---
# ACCESS-NRI Support for ACCESS-MOPPeR

With the conclusion of the ARC Centre of Excellence for Climate Extremes (CLEX), support for ACCESS-MOPPeR is transitioning to ACCESS-NRI.

ACCESS-NRI is committed to providing continuity and will maintain the current version of ACCESS-MOPPeR, ensuring that it remains up to date and usable. Current support will include:

- Bug fixes
- Documentation
- An up-to-date dependency stack
- As time allows, development of new documentation and training examples for users

Community contributions are encouraged, including bug reports, pull requests, and suggestions for new features.

While no new development is currently planned, ACCESS-NRI recognises the value of ACCESS-MOPPeR, particularly for future CMIP7 submissions, and will be actively working with the community to prioritise future work (and resourcing).

ACCESS-NRI acknowledges the outstanding contributions made by the CLEX CMS team in developing and supporting this tool, from the original APP4 (Chloe Mackallah @chloemackallah ) to MOPPeR (Paola Petrelli @Paola-CMS and Sam Green @sam.green). Their work has laid a solid foundation for ACCESS-MOPPeR's continued success.

ACCESS-NRI remains dedicated to making data standardisation easier for users, ensuring that the tool continues to meet the evolving needs of the ACCESS community.

For any questions or support requests, please see [ACCESS-Support](https://aus01.safelinks.protection.outlook.com/?url=https%3A%2F%2Faccess-hive.org.au%2Fabout%2Fuser_support%2F&data=05%7C02%7Cromain.beucher%40anu.edu.au%7C3180d100195141b372b908dd101292ad%7Ce37d725cab5c46249ae5f0533e486437%7C0%7C0%7C638684394182362350%7CUnknown%7CTWFpbGZsb3d8eyJFbXB0eU1hcGkiOnRydWUsIlYiOiIwLjAuMDAwMCIsIlAiOiJXaW4zMiIsIkFOIjoiTWFpbCIsIldUIjoyfQ%3D%3D%7C0%7C%7C%7C&sdata=6TSq%2Fs88OQOXOgvCjDRFuX1NHC1QQj2OUBLmMWQ5O2s%3D&reserved=0)

---

## What is MOPPeR?

The MOPPeR is a CMORisation tool designed to post-process [ACCESS](https://research.csiro.au/access/) model output. The original APP4 main use was to produce [ESGF](https://esgf-node.llnl.gov/)-compliant formats, primarily for publication to [CMIP6](https://www.wcrp-climate.org/wgcm-cmip/wgcm-cmip6). The code was originally built for CMIP5, and was further developed for CMIP6-era activities.  
It used [CMOR3](https://cmor.llnl.gov/) and files created with the [CMIP6 data request](https://github.com/cmip6dr/dreqPy) to generate CF-compliant files according to the [CMIP6 data standards](https://docs.google.com/document/d/1os9rZ11U0ajY7F8FWtgU4B49KcB59aFlBVGfLC4ahXs/edit).The APP4 also had a custom mode option to allow users to post-process output without strict adherence to the ESGF standards. MOPPeR was developed to extend the custom mode as much as it is allowed by the CMOR tool, it can be used to produce CMIP6 compliant data but other standards can also be defined.

CMOR uses Controlled Vocabularies as metadata constraints, with [CMIP6_CV.json](https://cmor.llnl.gov/mydoc_cmor3_CV/) being the main one. This has an effect on how the data is written in the files, variables' names, directory structure, filenames, and global attributes. The APP4 also relied on mapping files to match the raw model output to CMOR defined variables. To make this approach more flexible we introduced a new tool `mopdb` that helps the users create their own mapping and handling CMOR tables definitions.
 
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
