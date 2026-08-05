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

How it works
------------

At a high level, one ``moppy-cmorise`` invocation turns a config file into a
tracked, self-recovering batch of PBS jobs — one per variable — that keeps
running even after you log out:

.. code-block:: text

   ┌──────────────────────────────────────────────────────────────┐
   │ 1. You write batch_config.yml                                │
   │    (which variables to CMORise + PBS resources)               │
   └──────────────────────────────────────────────────────────────┘
                                  │  moppy-cmorise batch_config.yml
                                  ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ 2. Login node -- moppy-cmorise                                │
   │    - records every variable in a tracking database            │
   │    - submits ONE PBS "monitor" job                             │
   │    - exits immediately -- safe to log out now                 │
   └──────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ 3. Monitor job (runs on a compute node)                       │
   │    - submits one PBS worker job per variable                  │
   │      (all run in parallel)                                    │
   │    - watches the database, retries jobs that fail             │
   │      or die without reporting (e.g. out of memory)            │
   └──────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ 4. Worker jobs -- one per variable, in parallel               │
   │      tas . pr . tos . siconc . ...                            │
   │    - load raw ACCESS output                                   │
   │    - CMORise the variable                                     │
   │    - write CMIP-compliant NetCDF file(s)                      │
   │    - record success / failure in the database                │
   └──────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ 5. You check progress at any time:                            │
   │      moppy-tui | moppy-dashboard | moppy-batch-report         │
   └──────────────────────────────────────────────────────────────┘

Because the monitor job — not your login shell — owns the whole batch, a
dropped SSH connection or a laptop going to sleep never stops a run in
progress. And because every variable's status lives in the tracking
database rather than in memory, re-running the same command later
(see :ref:`resubmitting-failed-jobs`) always picks up exactly where the
batch left off.

Resuming a partially written variable
-------------------------------------

Pass ``--resume`` when resubmitting, or set ``resume: true`` in the batch
configuration, to reuse completed time-split files after a worker reaches its
PBS walltime. For example: ``moppy-cmorise batch_config.yml --resume``. MOPPy checks
the existing NetCDF files for the same variable, experiment, source, and
variant. It resumes discovery at the first unfinished split and writes the
remaining files into the original dated DRS version directory. A readable
file that does not reach its expected ``split_years`` boundary is ignored and
rewritten.

MOPPy records a hidden completion marker only after each split has passed
post-write processing and publication. For output created by older MOPPy
versions without these markers, resume mode conservatively rewrites the newest
split because it may be the file interrupted by PBS.

Resume mode requires time-split output. Variables configured with
``split_years: null`` can only be skipped when their single output file covers
the complete requested period.

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

   # Optional: model_id selects the mapping file used for auto file-discovery.
   # Defaults to "ACCESS-ESM1.6" when omitted.
   # model_id: ACCESS-ESM1.6

   # Optional: Explicit file patterns per variable.
   # When omitted, MOPPy discovers files automatically from the
   # file_discovery configuration embedded in the model mapping JSON.
   # Provide explicit patterns only to override the defaults — for example
   # to restrict to a subset of output folders or to handle non-standard layouts.
   #
   # file_patterns:
   #   Amon.pr:  "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"
   #   Omon.tos: "output[0-4][0-9][0-9]/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc"

   # PBS Resource Configuration
   queue: "normal"                    # PBS queue name
   cpus_per_node: 16                  # CPUs per job
   mem: "32GB"                        # Memory per job
   jobfs: "100GB"                     # Local scratch space (optional)
   # use_jobfs_staging: true          # Write, repack, validate, and generate the
                                       # first QC snapshot on $PBS_JOBFS, then move
                                       # completed artifacts to final output/DRS.
                                       # Requires 'jobfs' to hold the largest output.
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

   # QC diagnostic plots (default: false)
   # When true, generates two PNGs per output file in <output_folder>/qc_plots/:
   #   <stem>_snapshot.png   – spatial map of the first timestep
   #   <stem>_timeseries.png – per-timestep mean/min/max and std dev
   # Requires matplotlib + pyarrow: pip install "access_moppy[qc-plots]"
   qc_plots: false

   # Optional: path to an external ACCESS-ESM1-5 CMIP6 Parquet timeseries store.
   # When set (and qc_plots is true), the matching reference global-mean series
   # is overlaid on each timeseries PNG for visual comparison.
   # cmip6_comparison_store: /path/to/ACCESS-ESM1-5_CMIP6_Timeseries

   # Optional: preferred ensemble member for the comparison overlay.
   # Defaults to r1i1p1f1 when available, otherwise lexicographically first.
   # preferred_cmip6_member: r1i1p1f1

QC Diagnostic Plots
-------------------

Setting ``qc_plots: true`` in the batch config generates lightweight visual
quality-control plots for every CMORised output file, mirroring the diagnostic
capability that was available in APP4.

**What is generated**

For each ``.nc`` file written during CMORisation, two PNGs are placed in
``<output_folder>/qc_plots/``:

``<stem>_snapshot.png``
   A spatial map (``imshow``) of the first available timestep, averaged over
   any level or pressure dimension.  For ``fx`` (time-independent) variables
   the sole frame is used.

``<stem>_timeseries.png``
   A two-panel figure showing, at each timestep, the global mean with
   min/max shading (top panel) and the standard deviation (bottom panel),
   computed across all non-time spatial dimensions.  Skipped for ``fx``
   variables and files containing only a single timestep.

**Installation**

``matplotlib`` is an optional dependency.  Install it alongside ACCESS-MOPPy:

.. code-block:: bash

   pip install "access_moppy[qc-plots]"

**Usage**

Add ``qc_plots: true`` to your batch config:

.. code-block:: yaml

   qc_plots: true

Plots are generated inside the worker PBS job immediately after the output
file is written, so no additional pass over the data is needed.  Any plot
failure emits a warning to the job's stderr log but never aborts the
CMORisation step.

**ACCESS-ESM1-5 comparison overlay**

You can overlay a reference global-mean timeseries from a pre-built
ACCESS-ESM1-5 CMIP6 Parquet store onto the timeseries panel of each QC plot.
This makes it easy to spot systematic biases or drifts relative to the
published CMIP6 submission at a glance.

To enable the overlay, set ``cmip6_comparison_store`` in the batch config:

.. code-block:: yaml

   qc_plots: true
   cmip6_comparison_store: /path/to/ACCESS-ESM1-5_CMIP6_Timeseries

   # Optional: pick a specific ensemble member (default: r1i1p1f1)
   # preferred_cmip6_member: r1i1p1f1

The store is matched by ``variable``, ``table_id``, ``experiment_id``, and
``grid_label``.  The timeseries panel uses actual dates on the X-axis when the
overlay is active so both series share a common time reference.  If no match
is found in the store the plot is produced normally without an overlay — no
error or warning is raised.

The store must contain a ``catalog.csv`` (or ``catalog.parquet``) and a
``timeseries/`` directory of Hive-partitioned Parquet files.  Reading Parquet
files requires ``pyarrow``, which is included in the ``qc-plots`` extra:

.. code-block:: bash

   pip install "access_moppy[qc-plots]"

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

      jobfs: "200GB"  # Requests local NVMe scratch, sized for the job

   Requesting ``jobfs`` on its own only allocates the local scratch space and
   makes it available (as ``$PBS_JOBFS``) to the job; it does not, by itself,
   change where output is written. To write, repack, validate, and generate the
   first QC snapshot on ``$PBS_JOBFS``, also set
   ``use_jobfs_staging: true`` (see the sample config above). Completed NetCDF
   and snapshot PNG files are moved to the final location. For split output,
   one full-period timeseries is generated after all splits are published. This
   avoids rewriting the shared copy during CMIP7 repacking and reduces validation
   contention at the cost of a final copy step, so size ``jobfs`` comfortably
   above the largest expected output file.

   For many independent jobs publishing to Lustre at once, bound only the final
   transfers with a shared slot directory:

   .. code-block:: yaml

      publication_lock_dir: "/scratch/<project>/<user>/cmor/.publication_slots"
      max_concurrent_publications: 12
      publication_jitter_seconds: 120
      publication_stale_seconds: 86400
      max_inflight_jobs: 100
      monitor_poll_interval: 300

   Every experiment that should share the limit must use the same
   ``publication_lock_dir``. ``publication_stale_seconds`` should be longer
   than the largest worker walltime so an active transfer is never reclaimed.

2. **Prefer auto-discovery over manual patterns** when possible:

   Auto-discovery builds focused glob patterns from the variable's
   ``model_variables`` list and the component-level config in the mapping
   JSON, so it is already tuned to the expected file layout.  Only add an
   explicit ``file_patterns`` entry when you need to narrow the set of
   output folders (e.g. for a time-range subset) or when dealing with
   a non-standard folder layout.

   .. code-block:: yaml

      # Restrict to specific folders — manual override
      file_patterns:
        Amon.pr: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"

      # Avoid: Overly broad patterns scan the entire tree
      file_patterns:
        Amon.pr: "**/*.nc"

**Memory Management**

1. **Match memory to data size**:
   - Atmosphere monthly: 16-32GB typically sufficient
   - Ocean 3D variables: 64-128GB may be required
   - Daily data: Increase memory proportionally

2. **Use chunking for large datasets**:
   The system automatically configures Dask chunking, but you can influence this through resource allocation.

3. **Pipeline computation ahead of NetCDF writes**:
   ``write_prefetch`` controls how many bounded Dask slices are computed ahead
   of the serial NetCDF writer. It defaults to ``4``; use ``1`` to disable
   prefetching. Larger values can improve worker utilisation when reads or
   derivations dominate, but retain more completed slices in distributed
   memory. Dask worker sizing accounts for ``write_prefetch`` and
   ``max_chunk_size_mb`` when their combined write window exceeds the defaults.
   If the requested PBS memory cannot provide one suitably sized worker, the
   job fails before starting the cluster. Disabling chunking uses a conservative
   28GB per-worker floor because the write is no longer memory-bounded.

4. **Understand how the number of Dask workers is chosen**:
   Each job gets ``n_workers = min(cpus_per_node, effective_mem // per_worker_floor_gb)``,
   with one thread per worker — netCDF4/HDF5 reads serialise on a global lock
   within a process, so extra threads add no read throughput and only tie up
   more resident memory per worker. This means a job is either **CPU-bound**
   (``cpus_per_node`` is the smaller number — raising ``mem`` alone won't add
   workers) or **memory-bound** (``mem // per_worker_floor_gb`` is smaller —
   raising ``cpus_per_node`` alone won't add workers). Check which applies
   before tuning: a batch report's ``worker_memory.n_workers`` next to the
   job's requested ``ncpus`` tells you immediately (equal means CPU-bound).

   ``per_worker_floor_gb`` itself comes from probing one input file and
   classifying the variable as light/medium/heavy (overridable via
   ``MOPPY_WORKER_GB_LIGHT``/``MOPPY_WORKER_GB_MEDIUM``/``MOPPY_WORKER_GB_HEAVY``,
   set in ``worker_init``). That probe is a proxy — it estimates from how much
   a variable *reads*, which can be wrong for variables whose peak memory
   comes from *computation* instead (vertical interpolation to pressure
   levels, for example, can need more memory than its output file size would
   suggest).

   For a variable that has already been run at least once, measured reality
   is better evidence than that guess. Set ``MOPPY_WORKER_MEMORY_HISTORY`` to
   a shared, group-writable file path in ``worker_init`` to enable a small
   calibration cache, keyed by ``(model_id, variable)``:

   .. code-block:: yaml

      worker_init: |
        module use /g/data/xp65/public/modules
        module load conda/analysis3-latest

        export MOPPY_WORKER_MEMORY_HISTORY=/g/data/xp65/public/apps/moppy_cache/worker_memory_history.json
        # export MOPPY_WORKER_MEMORY_SAFETY_FACTOR=1.5  # default; margin over the measured peak

   Once a variable has run, later runs of it (any experiment, same model) are
   sized from its measured peak RSS instead of the file-size guess. This is
   opt-in and safe by default:

   - Unset (the default) reproduces today's behaviour exactly; no file is
     ever created or read.
   - A missing directory, unwritable path, or corrupt cache file is treated
     as "no history yet" and silently falls back to the file-probe heuristic
     — it can never fail a job.
   - Entries are gated on scale: each one also records how many input files
     it was measured on, and is only trusted for a run processing at most
     that many (± a small tolerance). A short sanity-check run (e.g. a
     ``_one_year.yml`` config) can therefore never under-size a much longer
     production run of the same variable — and a later small run can only
     add an observation, never lower an already-established floor.

   The directory needs creating once, with permissions that let every user
   submitting jobs under the relevant PBS project write to it (e.g. a
   project-group-owned directory with the setgid bit set, so new files
   inherit group ownership).

   To decide *how much* to raise ``cpus_per_node``/``mem`` for a variable
   that's CPU- or memory-bound, ``scripts/recommend_worker_scaling.py`` reads
   a batch report and recommends values sized off each job's measured
   ``peak_rss_mb`` rather than whatever tier it happened to be assigned:

   .. code-block:: bash

      python scripts/recommend_worker_scaling.py report.json --target-workers 8
      python scripts/recommend_worker_scaling.py report.json --pattern ocean --add-workers 3

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

   # list failed variables only (JSON, scriptable)
   moppy-tui --status failed --json | jq -r '.tasks[].variable_id'

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

   <output_folder>/moppy_batch_report_<UTC>.json

The filename is stamped with the finalisation time in UTC (for example
``moppy_batch_report_20260707T143022Z.json``) so that repeated runs under the
same output folder do not overwrite each other's reports.

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

.. _resubmitting-failed-jobs:

Error Recovery
--------------

Re-running after a completed batch is safe and idempotent — the tracking
database preserves state across invocations so the recovery workflows below
all share the same ``moppy-cmorise`` command.

**Re-run only failed variables**

After a batch finishes, every variable is either ``completed`` or ``failed``
(the monitor marks any variable that never left ``pending`` as ``failed``
during its finalisation sweep). Re-running the same configuration automatically
skips completed variables and resubmits only the failed ones:

.. code-block:: bash

   moppy-cmorise batch_config.yml

No extra flags are required. The existing ``cmor_tasks.db`` in
``output_folder`` is reused and completed rows are left untouched.

**Re-run only missing variables (extending the config)**

If you add new variables to the ``variables`` list in your config file and
re-run, only those new entries are submitted. Variables already in the
database (whether ``completed`` or ``failed``) are handled by the normal
skip/re-run rules above:

.. code-block:: yaml

   # batch_config.yml — add the new entries to the existing list
   variables:
     - Amon.tas     # already completed — will be skipped
     - Amon.pr      # already failed   — will be resubmitted
     - Amon.huss    # brand new        — will be submitted as pending

.. code-block:: bash

   moppy-cmorise batch_config.yml   # picks up only Amon.huss (+ Amon.pr)

New variables are inserted as ``pending``; existing rows remain unchanged.

**Re-run a specific variable**

Use ``--rerun-variable`` to reset one or more variables back to pending and
resubmit them — even if they previously completed. All other variables are
unaffected:

.. code-block:: bash

   # Re-run a single variable
   moppy-cmorise batch_config.yml --rerun-variable Amon.tas

   # Re-run several at once
   moppy-cmorise batch_config.yml --rerun-variable Amon.tas Amon.pr

The variable name(s) must appear in the config file's ``variables`` list.

**Run only a specific subset of variables**

Use ``--variable`` to limit a run to a specific subset of variables from the
config, ignoring all others. This is useful for targeted first-runs or
debugging a single variable without affecting the rest of the batch:

.. code-block:: bash

   # Run only Amon.tas
   moppy-cmorise batch_config.yml --variable Amon.tas

   # Run only a handful of variables
   moppy-cmorise batch_config.yml --variable Amon.tas Amon.pr Omon.tos

Only the listed variables are added to the tracking database for this
invocation; variables not listed are neither inserted nor touched. The
variable name(s) must appear in the config file's ``variables`` list.

.. note::

   ``--variable`` can be combined with ``--rerun-variable`` to limit the run
   to a subset *and* force-reset one or more of those variables that may have
   already completed:

   .. code-block:: bash

      moppy-cmorise batch_config.yml --variable Amon.tas --rerun-variable Amon.tas

**Force re-run everything**

``--force`` resets every variable in the config (including completed ones) to
pending before the monitor starts, effectively re-running the whole batch from
scratch:

.. code-block:: bash

   moppy-cmorise batch_config.yml --force

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
--------------------

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
-------------------

Planned improvements include:

- **Variable-specific resource allocation**
- **Automatic retry logic for transient failures**
- **Integration with workflow management systems (Snakemake, Nextflow)**
- **Support for additional schedulers (SLURM, SGE)**
- **Enhanced monitoring with metrics and alerts**
- **Automatic output validation with PrePARE**

For the most current information and feature requests, see the ACCESS-MOPPy GitHub repository.
