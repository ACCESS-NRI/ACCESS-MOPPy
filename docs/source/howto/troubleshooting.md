# Troubleshooting and FAQ

Common problems and how to resolve them. If your issue is not listed here,
please [open an issue](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues).

## Installation and environment

### `ModuleNotFoundError: No module named 'streamlit'` when running `moppy-dashboard`

The web dashboard requires `streamlit`, which is not a core dependency:
`pip install streamlit`. On Gadi, use `moppy-tui` instead — it needs no
browser and works in any SSH session.

### `ImportError: The 'f90nml' package is required` from `moppy-calc-ab-coeffts`

Install the optional extra: `pip install "access_moppy[atmos-tools]"`.

### On Gadi: a system Python module shadows the environment

If you have `module load python3/...` in your shell startup, its
`PYTHONPATH` can pollute conda/pixi environments. Run `module purge` (then
re-load only what you need, e.g. `conda/analysis3`) before using
ACCESS-MOPPy.

## Finding input data

### `KeyError: '<variable>' not found in input files`

The raw model variable listed under `model_variables` in the mapping JSON is
not present in the discovered files. Check that:

- `input_folder` points at the archive root (the directory containing
  `output000`, `output001`, …), not an individual output folder;
- the variable exists at the requested frequency in your run configuration;
- if you supplied `file_patterns`, the glob actually matches files
  containing that variable (`ls` the pattern manually).

### Auto-discovery finds no files

- Verify `model_id` matches your model (default is `ACCESS-ESM1.6`).
- Verify `start_year`/`end_year` overlap the years actually present.
- As a fallback, pass an explicit file list via `input_data`, or a
  per-variable glob via `file_patterns` in batch configs.

## Batch processing on PBS

### Jobs stuck in the queue

- Check queue availability: `qstat -q`.
- Verify your project allocation: `nci_account -P <project>`.
- Ensure `storage` lists every `/g/data` and `/scratch` project your job
  reads or writes — missing mounts cause silent file-not-found failures on
  compute nodes.

### Jobs fail immediately with environment errors

Test the `worker_init` block by running its commands manually in an
interactive PBS job (`qsub -I`). Common causes: module not available on
compute nodes, conda environment path wrong, or missing
`module use /g/data/xp65/public/modules`.

### Memory errors (job killed, `MemoryError`)

- Increase `mem` in the batch config, or add a `variable_resources`
  override for the affected variable.
- 3D ocean variables (e.g. `Omon.thetao`) typically need 64–128 GB.
- Use `jobfs` for temporary storage.
- Check a batch report's `worker_memory` for the variable: if
  `n_workers` equals the job's requested `ncpus`, it's CPU-bound and more
  `mem` won't help — raise `cpus_per_node` instead (or vice versa). See
  {doc}`/howto/batch_processing` (Performance Optimization → Memory
  Management) for how worker count is derived, and consider enabling
  `MOPPY_WORKER_MEMORY_HISTORY` so future runs of the same variable size
  off its actual measured peak instead of a file-size guess.

### Re-running after failures

Just run `moppy-cmorise` again with the same config: completed tasks are
skipped and failed/pending tasks are resubmitted. Inspect
`<script_dir>/<Table_var>/cmor_<Table_var>.err` for the failure cause first.

## Output and compliance

### Warning about missing parent experiment information

`parent_info` is required for CMIP publication but optional for evaluation
workflows (ESMValTool, ILAMB). Provide the block described in
{doc}`/reference/configuration` when you intend to publish.

### QC failure: observed range outside allowed range

The CMORised values violate the physical range rules in
`src/access_moppy/resources/qc/cmip7_ranges.yml`. Either the data is genuinely
wrong (check units and the mapping), or the rule needs an
experiment-specific override — see {doc}`/howto/qc_validation`.

### Output files are split into many pieces / one huge file

File splitting is controlled by `split_years` (default `"auto"`: 10 years
per file for monthly, 5 for daily, 1 for sub-daily). Pass `split_years=None`
for a single file or an integer for a fixed chunk length. See
{doc}`/reference/configuration`.

## Monitoring

### The Streamlit dashboard is unreachable from my laptop

The dashboard binds to `localhost:8501` on the login node. Either create an
SSH tunnel (`ssh -L 8501:localhost:8501 gadi-login-04.nci.org.au`, pinning
the same login node where `moppy-cmorise` runs) or use an
[ARE](https://are.nci.org.au) session. Simplest alternative: `moppy-tui`.

### `moppy-tui` shows no tasks

Point it at the right database: `moppy-tui --db <output_folder>/cmor_tasks.db`,
or export `CMOR_TRACKER_DB`.
