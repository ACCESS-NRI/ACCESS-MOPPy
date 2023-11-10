-------
Install
-------

    You can install the latest version of `mopper` directly from conda (accessnri channel)

    conda install -c accessnri mopper 

    If you want to install an unstable version or a different branch:

    * git clone 
    * git checkout <branch-name>   (if installing a a different branch from master)
    * cd mopper 
    * python setup.py install or pip install ./ 
      use --user with either othe commands if you want to install it in ~/.local

---------------------
Working on NCI server
---------------------

MOPPeR is pre-installed into a Conda environment at NCI. Load it with::

    module use /g/data3/hh5/public/modules
    module load conda/analysis3-unstable

:: note::
   You need to be a member of the hh5 project to load the modules.
   
