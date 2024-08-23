Install
=======

We are planning to release ACCESS-MOPPeR in conda soon and then it will be available at NCI on our conda environments.
In the meantime, you can icreate a custom conda environment and install mopper following these steps:

1. module load conda/analysis3
2. python -m venv mopper_env --system-site-packages
3. source  <path-to-mopper-env>/mopper_env/bin/activate
4. pip install git+https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR@main
 
The source command will activate the conda env you just created.
Any time you want to use the tool in a new session repeat the first and third steps.

The `pip` command above will install from the main branch, you can also indicate a different branch.

