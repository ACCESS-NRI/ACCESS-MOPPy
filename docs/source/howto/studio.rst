MOPPy Studio on ARE (Beta)
==========================

MOPPy Studio prepares batch configurations, submits native MOPPy PBS monitor
jobs and displays variable progress and logs. It uses the same model mappings,
scientific processing and tracking database as :doc:`batch_processing`.

.. note::

   **Beta — in development.** Studio's interface and supported workflow may
   change. Start with a few variables and a short period, then inspect the
   outputs before scaling up. The header identifies the beta on every page;
   the footer gives the Studio version. A run separately shows its MOPPy version.

Launch a session
----------------

You need an NCI account, an allocated compute project and access to the projects
holding your input data, output directory and software. The shared installation
is at ``/g/data/xp65/public/apps/moppy-studio``. If the module is unavailable,
contact its maintainer or follow :doc:`../development/studio`.

Open `ARE <https://are.nci.org.au>`_ and create a **JupyterLab** session:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Setting
   * - Compute project
     - Your allocated project
   * - CPUs / memory
     - 1 CPU / 4 GB for the web application
   * - Module directories
     - ``/g/data/xp65/public/modules``
   * - Modules
     - ``python3/3.11.7 moppy-studio``
   * - Storage
     - ``gdata/xp65`` plus every input, output and auxiliary-data project

For example, inputs on p73 and outputs on iq82 scratch require
``gdata/xp65+gdata/p73+scratch/iq82``. Include storage containing symlink targets
as well. These permissions must also be present in the batch's storage setting.

Select **Open JupyterLab** when the session starts. The module opens Studio
through ARE's password login. This uses the JupyterLab launch form; it does not
add a separate dashboard tile. Load only one Studio module per session.

The ARE session hosts the interface. CMORisation workers request their own PBS
resources and continue after the session ends.

Load or build a batch
---------------------

1. Open **Create a batch**. Enter an existing Gadi ``batch_config.yml`` path and
   select **Load path**, upload a local YAML file, or use **New configuration**.
   Loading a file does not modify it.
2. Set the model, CMIP version, experiment, variant and activity. Expand
   **Parent experiment metadata** to check the branch lineage.
3. Set the native model input directory and a **new output directory**, for
   example ``/scratch/PROJECT/USER/moppy/test-1850``. Scripts, logs, the tracking
   database and scientific outputs belong to this batch directory.
4. Choose a small variable subset and set both first and last year for a test.
   Use table identifiers such as ``Amon.tas`` for CMIP6, or compound identifiers
   such as ``atmos.tas.tavg-h2m-hxy-u.day.glb`` for CMIP7.
5. Set the compute project, queue, per-worker CPUs, memory, jobfs and walltime.
   **Concurrent variable jobs** limits the number of jobs MOPPy has in flight.
   Add all required storage mounts.
6. Check the scientific runtime and **Worker environment & advanced settings**.
   Imported ``worker_init`` is executable shell setup and is preserved for review.

Use **YAML editor** for file patterns, per-variable resources, compliance options,
chunking and fields that are not exposed in the builder. Switching editors
preserves supported advanced values but normalises comments and formatting.
**Download YAML** exports the current configuration without submitting anything.

For a bounded test, choose a new scratch output folder and restrict input paths
or file patterns to the intended period. Loading a production config can retain
its output paths and custom environment hooks: review these before submission.
See :doc:`../reference/configuration` for MOPPy's scientific settings.

Review and submit
-----------------

Select **Check & review batch**. Studio checks configuration structure, accessible
paths and the installed MOPPy interface, then saves a plan containing the exact
YAML and native monitor script. No job is submitted at this stage.

Review the project, storage, output path, metadata, variable list, resources and
worker setup. The dialog shows the resolved MOPPy version, dated analysis3 module
and monitor walltime. Studio-generated worker setup is pinned to that module;
custom imported setup remains as supplied and can select a different environment.

Tick the review checkbox and select **Submit to PBS**. Studio submits the monitor
once and displays its job ID. MOPPy then creates and schedules the variable jobs.
The review is immutable: change the configuration and create a new review if the
settings need correction before submission.

Follow progress and logs
------------------------

Open **Runs & progress** and select a batch. The view shows completed, running,
pending and failed variables, monitor status and per-variable PBS IDs. The selected
batch refreshes every 15 seconds while the page is visible. The percentage counts
completed variables; it does not estimate elapsed work or remaining time.

Select a variable's **Inspect** button and choose stdout or stderr under
**Log stream**. PBS may stage these logs only when the worker ends. Expand
**Task metadata, output summaries & compliance** for native output records and
compliance reports when available. Completion alone is not a substitute for
scientific QC; see :doc:`qc_validation`.

For a batch launched outside Studio, enter its output directory or
``cmor_tasks.db`` path under **Attach an existing batch**. Attachment and
subsequent monitoring are read-only.

Saved reviews and run records live in ``~/.local/state/moppy-studio-are`` and
remain available in later sessions under the same account. Scientific outputs
stay in the selected output directory. Scratch storage is not archival storage.

Understand recovery states
--------------------------

* **Submission uncertain / submitting:** submission may have reached PBS. Select
  **Find accepted job in PBS** to look for the owned job with the saved unique name
  and output path. Studio never repeats ``qsub`` automatically. No match is not
  proof that submission failed; inspect PBS history before starting equivalent work.
* **Preparation failed:** the monitor was not submitted. Inspect the error and
  create a new review with a fresh output directory.
* **Submission failed:** PBS rejected submission. Correct the reported issue and
  prepare a fresh plan.
* **Unknown:** PBS history is unavailable. Check the recorded job IDs and logs.
* **Tracking unavailable:** the database cannot currently be read. Check storage
  access and refresh; Studio does not repair or migrate a running database.
* **Incomplete / failed / completed with errors:** inspect the task errors and
  worker logs. A monitor exit code of zero does not guarantee every variable passed.

Current beta limits
-------------------

The beta supports paths and batch configurations. Intake catalog discovery,
in-app cancellation, retrying and appending to existing batches are not included.
Use MOPPy/PBS tools for recovery operations.

Every new submission requires a fresh output folder. ``script_dir`` must equal
that folder; an optional ``drs_root`` must be inside it. Paths used by native shell
templates cannot contain spaces or shell metacharacters. ``scheduler_options``
is limited to the project directive; resource overrides support queue, CPUs,
memory, jobfs and walltime. Mount suggestions inspect top-level paths, not every
embedded auxiliary path or symlink.

Preflight does not establish full temporal coverage, complete variable mappings,
vocabulary correctness or scientific correctness. The adapter accepts the native
MOPPy 1.8.x and 1.9.x interfaces; the initial scientific pilot used MOPPy 1.8.5 in
``conda/analysis3-26.10``. Other runtime versions need adapter verification.

Report problems through `ACCESS-MOPPy issues
<https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues>`_, including the Studio version,
MOPPy version, operation and relevant error or job ID. Remove credentials and
private configuration details from any shared logs.
