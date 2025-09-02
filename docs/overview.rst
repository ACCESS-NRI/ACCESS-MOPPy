Install
=======

ACCESS-MOPPeR is available at NCI on the hh5 conda environments.
As these will be soon phased out you will have to create your own custom conda environment or local installation.
We reccommed to use the ACCESS-NRI conda envs (xp65) as base, as these will have most of the requirements available. 

To create a custom conda environment and install mopper following these steps:

1. module load conda/analysis3
2. export PYTHONNOUSERSITE=1
3. python -m venv mopper_env --system-site-packages
4. source  <path-to-mopper-env>/mopper_env/bin/activate
5. pip install git+https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR@main
 
The source command will activate the conda env you just created.
Any time you want to use the tool in a new session repeat the first and third steps.

The `pip` command above will install from the main branch, you can also install from a different branch.

.. warning::
  If copying and pasting be careful to re-type the hyphens before 'system-site-packages'
