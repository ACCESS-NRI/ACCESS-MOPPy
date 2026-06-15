Batch Processing Guide
======================

ACCESS-MOPPy includes a comprehensive batch processing system designed for High Performance Computing (HPC) environments using PBS job schedulers. This system enables efficient parallel processing of multiple variables, each running as an independent PBS job with dedicated resources.

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

1. **Main Controller** (``moppy-cmorise``): Orchestrates job submission and monitoring
2. **Job Scripts**: Generated PBS scripts with embedded Python processing code
3. **Tracking Database**: SQLite database maintaining job status and history
4. **Web Dashboard**: Streamlit-based real-time monitoring interface
5. **Worker Jobs**: Individual PBS jobs processing specific variables

System Requirements
-------------------

**Software Requirements:**
- Python >= 3.11 with ACCESS-MOPPy installed
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

   # Required: CMIP metadata
   experiment_id: "piControl"
   source_id: "ACCESS-ESM1-6"
   variant_label: "r1i1p1f1"
   grid_label: "gn"
   activity_id: "CMIP"
   cmip_version: "CMIP7"

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
     conda activate moppy_env

   # Optional Settings
   drs_root: "/scratch/project/cmor_output/CMIP7"  # Enable DRS structure
   script_dir: "PATH-TO-SCRIPTS"  # Custom directory for generated scripts
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
     conda activate access_moppy_env

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

**Web Dashboard (Streamlit)**

The Streamlit dashboard provides:

- **Status Overview**: Color-coded job status (pending, running, completed, failed)
- **Progress Tracking**: Job start/completion times
- **Error Reporting**: Direct access to error messages
- **Filtering**: Filter by status, experiment, or time period
- **Refresh Control**: Automatic updates with configurable intervals

It binds to ``http://localhost:8501`` on the host where ``moppy-cmorise`` is
invoked.  When that host is a Gadi login node, reaching it from a laptop
requires either an SSH local port forward (``ssh -L 8501:localhost:8501 ...``)
or an `ARE <https://are.nci.org.au>`_ session in which the browser already
runs alongside the dashboard.  Pin a specific login node (``gadi-login-04`` …)
so the tunnel target matches the dashboard host.

**Terminal Dashboard (moppy-tui)**

For environments where opening a browser to the login node is awkward —
typically a plain SSH session into Gadi — ACCESS-MOPPy ships an alternative
``rich``-based terminal dashboard reading the same SQLite tracker DB.

**On NCI Gadi (recommended):** the ``conda/analysis3`` module already
includes ``access_moppy`` and its ``rich`` dependency, so ``moppy-tui`` is
available immediately after loading the module — no ``pip install`` needed:

.. code-block:: bash

   module use /g/data/xp65/public/modules
   module load conda/analysis3

   # start the dashboard (auto-refresh, interactive paging)
   moppy-tui --db /scratch/<project>/cmor_output/cmor_tasks.db

   # or pick up the path from the environment (set by moppy-cmorise too)
   export CMOR_TRACKER_DB=/scratch/<project>/cmor_output/cmor_tasks.db
   moppy-tui

**Other environments:** install the optional ``tui`` extra, which pulls in
``rich``:

.. code-block:: bash

   pip install "access_moppy[tui]"
   moppy-tui --db <output_folder>/cmor_tasks.db

The tracker database is on Lustre (``/scratch`` or ``/g/data``), so
``moppy-tui`` works equally well from a login node, an ARE Jupyter terminal,
or a tmux session inside an interactive PBS job — no port forwarding, no
browser.

**Key features:**

- **Same data source as the web dashboard** — both can run side-by-side.
- **Live mode** with auto-refresh and interactive paging
  (``j/k`` / ``↓/↑`` move one row; ``n/p`` / ``Space/b`` / ``PgDn/PgUp``
  move one page; ``g/G`` jump to top/bottom; ``r`` forces a re-read;
  ``q`` / ``Ctrl-C`` quit).
- **Progress bar with ETA** computed from average completed-task duration.
- **Per-row duration** (live for running tasks).
- **Failure panel** with truncated error messages for the most recent
  failed tasks.

**Sample output (live mode):**

.. code-block:: text

   ╭──────────────────────────── ACCESS-MOPPy CMORisation Monitor ────────────────────────────╮
   │ DB: /scratch/tm70/yz9299/cmor_output/cmor_tasks.db    refreshed: 2026-05-14 01:15:10     │
   ╰──────────────────────────────────────────────────────────────────────────────────────────╯
   ╭──────────────────────────────────────── Progress ────────────────────────────────────────╮
   │ ━━━━━━━━━━━━━━━━   40.0%   completed 6 / 15   ETA 01:11:14                               │
   ╰──────────────────────────────────────────────────────────────────────────────────────────╯
   ╭──────────────────────────────────────── Summary ─────────────────────────────────────────╮
   │   running 3   pending 4   failed 2   completed 6                                         │
   ╰──────────────────────────────────────────────────────────────────────────────────────────╯
   ╭──────────────────────────────────── Tasks 1-10 of 15 ────────────────────────────────────╮
   │ ┏━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓ │
   │ ┃  # ┃ Variable          ┃ Experiment   ┃ Status     ┃ Started               ┃ Duration┃ │
   │ ┡━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩ │
   │ │  1 │ Omon.so           │ piControl    │ running    │ 2026-05-13T11:45:00   │ 13:30:10│ │
   │ │  2 │ Omon.sos          │ piControl    │ running    │ 2026-05-13T11:45:00   │ 13:30:10│ │
   │ │  3 │ Omon.thetao       │ piControl    │ running    │ 2026-05-13T11:45:00   │ 13:30:10│ │
   │ │  4 │ Lmon.mrso         │ piControl    │ pending    │ —                     │        —│ │
   │ │  5 │ Omon.mlotst       │ piControl    │ pending    │ —                     │        —│ │
   │ │  6 │ SImon.siconc      │ piControl    │ pending    │ —                     │        —│ │
   │ │  7 │ SImon.sitemptop   │ piControl    │ pending    │ —                     │        —│ │
   │ │  8 │ Lmon.mrro         │ piControl    │ failed     │ 2026-05-13T12:00:00   │ 00:00:45│ │
   │ │  9 │ SImon.sithick     │ piControl    │ failed     │ 2026-05-13T12:00:00   │ 00:01:30│ │
   │ │ 10 │ Amon.pr           │ piControl    │ completed  │ 2026-05-13T12:00:00   │ 00:07:10│ │
   │ └────┴───────────────────┴──────────────┴────────────┴───────────────────────┴─────────┘ │
   ╰──────────────────────────────────────────────────────────────────────────────────────────╯
   ╭──────────────────────────────────── Recent failures ─────────────────────────────────────╮
   │ ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ │
   │ ┃ Variable      ┃ Experiment ┃ Error                                                   ┃ │
   │ ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩ │
   │ │ SImon.sithick │ piControl  │ KeyError: 'hi_m' not found in input files; check        │ │
   │ │               │            │ 'model_variables' in the mapping.                       │ │
   │ │ Lmon.mrro     │ piControl  │ ValueError: Unsupported calculation type 'foo' for      │ │
   │ │               │            │ 'Lmon.mrro'.                                            │ │
   │ └───────────────┴────────────┴─────────────────────────────────────────────────────────┘ │
   ╰──────────────────────────────────────────────────────────────────────────────────────────╯
   ╭──────────────────────────────────────────────────────────────────────────────────────────╮
   │   j/↓ down  k/↑ up  n/Space pgDn  p/b pgUp  g top  G bottom  r refresh  q quit           │
   ╰──────────────────────────────────────────────────────────────────────────────────────────╯

In a real terminal the status column is colour-coded (``running`` cyan,
``pending`` dim, ``failed`` red, ``completed`` green) and the progress bar
fills with the theme accent colour.  After filtering, the tasks-panel title
changes to make the DB total explicit, e.g.
``Tasks 1-2 of 2 filtered (DB total 15)``.

**Useful flags:**

.. code-block:: bash

   # status / experiment filters
   moppy-tui --status failed,running --experiment piControl

   # custom page size (default 20)
   moppy-tui --page-size 40

   # one-shot snapshot for cron / email / logs
   moppy-tui --once --page 2 --page-size 20

   # machine-readable JSON snapshot for jq / scripts
   moppy-tui --json | jq '.summary'

   # durable batch coordination report from an existing tracker DB
   moppy-batch-report --db <output_folder>/cmor_tasks.db

   # write the report somewhere explicit
   moppy-batch-report --db <output_folder>/cmor_tasks.db --output batch_report.json

   # disable colour for log capture
   moppy-tui --once --no-color | tee progress.log

The ``--once`` and ``--json`` modes never block on stdin, so they are safe
in pipelines and cron jobs.

**Durable JSON coordination report**

When the batch monitor finalises, ACCESS-MOPPy writes a durable coordination
report next to the tracker database:

.. code-block:: text

   <output_folder>/moppy_batch_report.json

The SQLite database remains the source of truth for coordination; the JSON
report is a schema-versioned export for after-the-fact completion checks,
provenance capture, and later loading into dashboards or databases.  It
contains summary counts, final success/terminal-state flags, monitor metadata,
per-task status/timing/PBS job IDs, log paths, and bounded failure details.
When PBS history is still available, each task also includes a filtered
``pbs`` object with Payu-style scheduler provenance such as final job state,
exit status, queue/project, timestamps, requested resources, and resources
used.  ACCESS-MOPPy deliberately does not dump unbounded PBS fields such as
submit arguments or stdout/stderr content; reports can still contain NCI
project names, hostnames, job IDs, and filesystem paths, so treat them as
operational provenance rather than public artefacts.

Existing tracker databases can be exported manually:

.. code-block:: bash

   moppy-batch-report --db <output_folder>/cmor_tasks.db


**When to use which dashboard:**

- *Web dashboard* — collaborative monitoring, rich filtering on a desktop
  browser, ARE-friendly.
- *Terminal dashboard* — quick checks from any SSH session, scripted
  monitoring (``--once``/``--json``), environments where the Streamlit
  process gets killed by the login-node process reaper.

**Log File Analysis**

Each job produces detailed logs:

.. code-block:: bash

   cmor_job_scripts/
   ├── Amon_pr/
   │   ├── cmor_Amon_pr.sh    ← PBS job script
   │   ├── cmor_Amon_pr.py    ← Python CMORisation script
   │   ├── cmor_Amon_pr.out   ← PBS stdout (written at runtime)
   │   └── cmor_Amon_pr.err   ← PBS stderr (written at runtime)
   ├── Omon_tos/
   │   ├── cmor_Omon_tos.sh
   │   ├── cmor_Omon_tos.py
   │   ├── cmor_Omon_tos.out
   │   └── cmor_Omon_tos.err
   └── ...

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
   moppy-cmorise batch_config.yml

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

For the most current information and feature requests, see the ACCESS-MOPPy GitHub repository.
