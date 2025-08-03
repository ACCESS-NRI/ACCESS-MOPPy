Batch Processing Guide
======================

ACCESS-MOPPeR includes a comprehensive batch processing system designed for High Performance Computing (HPC) environments using PBS job schedulers. This system enables efficient parallel processing of multiple variables, each running as an independent PBS job with dedicated resources.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
--------

The batch processing system provides several key advantages for large-scale CMORisation workflows:

- **Parallel Processing**: Multiple variables processed simultaneously as separate PBS jobs
- **Resource Management**: Fine-grained control over CPU, memory, and storage allocation
- **Progress Tracking**: Real-time monitoring through web dashboard and database logging
- **Error Recovery**: Failed jobs can be easily identified and resubmitted
- **Scalability**: Handles workflows from single variables to hundreds of variables

Architecture
------------

The batch system consists of several components:

1. **Main Controller** (``mopper-cmorise``): Orchestrates job submission and monitoring
2. **Job Scripts**: Generated PBS scripts with embedded Python processing code
3. **Tracking Database**: SQLite database maintaining job status and history
4. **Web Dashboard**: Streamlit-based real-time monitoring interface
5. **Worker Jobs**: Individual PBS jobs processing specific variables

System Requirements
-------------------

**Software Requirements:**
- Python >= 3.11 with ACCESS-MOPPeR installed
- PBS Pro job scheduler
- Shared filesystem accessible from login and compute nodes

**Recommended Hardware:**
- Login node: 4+ GB RAM for dashboard and job management
- Compute nodes: 16+ GB RAM per job (variable-dependent)
- Fast shared storage (e.g., Lustre, GPFS) for input/output data

**Network Requirements:**
- Compute nodes must access shared filesystems
- Login node network access for dashboard (port 8501)

Configuration Reference
-----------------------

Complete configuration file specification:

.. code-block:: yaml

   # Required: Variables to process
   variables:
     - Amon.pr
     - Omon.tos
     - Amon.tas

   # Required: CMIP6 metadata
   experiment_id: "piControl"
   source_id: "ACCESS-ESM1-5"
   variant_label: "r1i1p1f1"
   grid_label: "gn"
   activity_id: "CMIP"

   # Required: File locations
   input_folder: "/g/data/project/model_output"
   output_folder: "/scratch/project/cmor_output"

   # Required: File pattern mapping
   file_patterns:
     Amon.pr: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"
     Omon.tos: "output[0-4][0-9][0-9]/ocean/*temp*.nc"
     Amon.tas: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"

   # PBS Resource Configuration
   queue: "normal"                    # PBS queue name
   cpus_per_node: 16                  # CPUs per job
   mem: "32GB"                        # Memory per job
   jobfs: "100GB"                     # Local scratch space (optional)
   walltime: "02:00:00"              # Maximum runtime
   scheduler_options: "#PBS -P tm70"  # Additional PBS directives
   storage: "gdata/p73+scratch/tm70"  # Required storage systems

   # Environment Setup
   worker_init: |
     module load netcdf/4.7.4
     source /path/to/conda/bin/activate
     conda activate mopper_env

   # Optional Settings
   drs_root: "/scratch/project/cmor_output/CMIP6"  # Enable DRS structure
   wait_for_completion: false         # Wait for all jobs before exit
   database_path: "/custom/db/path"   # Custom database location

Advanced Usage
--------------

**Custom Environment Setup**

For complex software environments:

.. code-block:: yaml

   worker_init: |
     # Load required modules
     module purge
     module load intel-compiler/2021.4.0
     module load netcdf/4.7.4
     module load hdf5/1.12.1

     # Activate conda environment
     source /g/data/tm70/software/miniconda3/bin/activate
     conda activate access_mopper_env

     # Set environment variables
     export TMPDIR=$PBS_JOBFS
     export OMP_NUM_THREADS=1

**Dynamic Resource Allocation**

Different variables may require different resources:

.. code-block:: yaml

   # Base configuration
   cpus_per_node: 8
   mem: "16GB"

   # Variable-specific overrides (future feature)
   variable_resources:
     Omon.thetao:  # 3D ocean temperature requires more resources
       cpus_per_node: 32
       mem: "128GB"
       walltime: "06:00:00"

Performance Optimization
------------------------

**I/O Optimization**

1. **Use jobfs for temporary files**:

   .. code-block:: yaml

      jobfs: "200GB"  # Provides fast local SSD storage

2. **Optimize file patterns** to minimize file scanning:

   .. code-block:: yaml

      # Good: Specific pattern
      file_patterns:
        Amon.pr: "output[0-4][0-9][0-9]/atmosphere/netCDF/*pr*_mon.nc"

      # Avoid: Overly broad patterns
      file_patterns:
        Amon.pr: "**/*.nc"  # Scans entire directory tree

**Memory Management**

1. **Match memory to data size**:
   - Atmosphere monthly: 16-32GB typically sufficient
   - Ocean 3D variables: 64-128GB may be required
   - Daily data: Increase memory proportionally

2. **Use chunking for large datasets**:
   The system automatically configures Dask chunking, but you can influence this through resource allocation.

**Parallelization Strategy**

1. **Balance job count vs. resources**:
   - More jobs: Faster completion, higher scheduler overhead
   - Fewer jobs: Lower overhead, potential resource waste

2. **Group related variables** (future feature):
   Process compatible variables together to reduce job count.

Monitoring and Debugging
------------------------

**Dashboard Features**

The Streamlit dashboard provides:

- **Status Overview**: Color-coded job status (pending, running, completed, failed)
- **Progress Tracking**: Job start/completion times
- **Error Reporting**: Direct access to error messages
- **Filtering**: Filter by status, experiment, or time period
- **Refresh Control**: Automatic updates with configurable intervals

**Log File Analysis**

Each job produces detailed logs:

.. code-block:: bash

   cmor_job_scripts/
   ├── cmor_Amon_pr.out    # Standard output
   ├── cmor_Amon_pr.err    # Standard error
   └── cmor_Amon_pr.sh     # Generated PBS script

**Database Queries**

Direct database access for advanced monitoring:

.. code-block:: python

   import sqlite3
   import pandas as pd

   # Connect to tracking database
   conn = sqlite3.connect('/scratch/project/cmor_output/cmor_tasks.db')

   # Query job status
   df = pd.read_sql_query("""
       SELECT variable, status, start_time, end_time,
              (julianday(end_time) - julianday(start_time)) * 24 as hours
       FROM cmor_tasks
       WHERE status = 'completed'
       ORDER BY hours DESC
   """, conn)

   print("Longest running jobs:")
   print(df.head())

**Common Issues and Solutions**

1. **Jobs stuck in queue**:
   - Check resource availability: ``qstat -q``
   - Verify project allocation: ``nci_account -P project``
   - Reduce resource requirements temporarily

2. **File access errors**:
   - Verify shared filesystem mounts on compute nodes
   - Check file permissions and ownership
   - Test file patterns manually: ``ls -la pattern``

3. **Memory errors**:
   - Increase ``mem`` parameter
   - Reduce ``cpus_per_node`` to allocate more memory per core
   - Use ``jobfs`` for temporary storage

4. **Environment errors**:
   - Test ``worker_init`` commands on compute nodes
   - Check module availability: ``module avail``
   - Verify conda environment exists

Error Recovery
--------------

**Resubmitting Failed Jobs**

The system is designed for easy recovery:

.. code-block:: bash

   # Rerun the same configuration
   mopper-cmorise batch_config.yml

   # The system will:
   # 1. Skip completed jobs automatically
   # 2. Resubmit only failed or pending jobs
   # 3. Maintain the same tracking database

**Manual Intervention**

For specific failures:

.. code-block:: bash

   # Check specific job logs
   cat cmor_job_scripts/cmor_Amon_pr.err

   # Edit and resubmit individual job
   qsub cmor_job_scripts/cmor_Amon_pr.sh

**Database Cleanup**

Reset job status if needed:

.. code-block:: python

   import sqlite3

   conn = sqlite3.connect('/scratch/project/cmor_output/cmor_tasks.db')

   # Reset failed jobs to pending
   conn.execute("""
       UPDATE cmor_tasks
       SET status = 'pending', start_time = NULL, end_time = NULL
       WHERE status = 'failed'
   """)
   conn.commit()

Best Practices
--------------

**Project Organization**

1. **Use descriptive configuration names**:

   .. code-block:: bash

      batch_config_historical_r1i1p1f1.yml
      batch_config_picontrol_atmosphere_only.yml

2. **Maintain configuration version control**:

   .. code-block:: bash

      git add batch_config.yml
      git commit -m "Add CMORisation config for historical experiment"

**Resource Planning**

1. **Start with conservative estimates**:
   - Begin with smaller jobs to test resource requirements
   - Scale up based on actual usage patterns
   - Monitor efficiency through dashboard

2. **Consider data locality**:
   - Place output near input data when possible
   - Use scratch filesystems for temporary data
   - Clean up intermediate files promptly

**Quality Assurance**

1. **Validate small subsets first**:

   .. code-block:: yaml

      # Test configuration with limited data
      variables:
        - Amon.pr  # Single variable first

      file_patterns:
        Amon.pr: "output001/atmosphere/netCDF/*mon.nc"  # Limited time range

2. **Use PrePARE for validation**:

   .. code-block:: bash

      # Validate output files
      PrePARE /scratch/project/cmor_output/*.nc

Integration Examples
-------------------

**With ESMValTool**

.. code-block:: yaml

   # ESMValTool recipe using CMORised output
   projects:
     CMIP6:
       root_path: /scratch/project/cmor_output/CMIP6

**With Intake Catalog**

.. code-block:: python

   import intake

   # Create catalog of CMORised data
   catalog = intake.open_catalog('/scratch/project/cmor_output/catalog.yml')
   ds = catalog.ACCESS_ESM1_5.piControl.Amon.pr.to_dask()

Future Enhancements
------------------

Planned improvements include:

- **Variable-specific resource allocation**
- **Automatic retry logic for transient failures**
- **Integration with workflow management systems (Snakemake, Nextflow)**
- **Support for additional schedulers (SLURM, SGE)**
- **Enhanced monitoring with metrics and alerts**
- **Automatic output validation with PrePARE**

For the most current information and feature requests, see the ACCESS-MOPPeR GitHub repository.
