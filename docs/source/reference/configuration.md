# Configuration reference

ACCESS-MOPPy is configured in three places:

1. **`~/.moppy/user.yml`** — per-user provenance metadata, created on first
   import.
2. **`ACCESS_ESM_CMORiser` keyword arguments** — for interactive / scripted
   Python use.
3. **Batch configuration YAML** — consumed by `moppy-cmorise` for PBS batch
   runs.

## User configuration (`~/.moppy/user.yml`)

Created interactively the first time you import `access_moppy`. It records
your name, email, organisation, and ORCID, which are written as global
attributes into every CMORised file for provenance tracking.

## Python API: `ACCESS_ESM_CMORiser` parameters

See the generated {py:class}`API reference <access_moppy.driver.ACCESS_ESM_CMORiser>`
for the authoritative signature.

| Parameter | Type / default | Description |
|---|---|---|
| `input_data` | path, list of paths, `xr.Dataset`, `xr.DataArray`, or `None` | Data to CMORise. May be omitted for internally generated variables or when using `input_folder` auto-discovery. |
| `input_folder` | path, `None` | Root of a payu archive. ACCESS-MOPPy discovers input files automatically using the model mapping's `file_discovery` configuration. |
| `start_year`, `end_year` | int, `None` | Restrict auto-discovery to a year range (filename-based, no file I/O). |
| `compound_name` | str (required) | CMIP6-style `table.variable` (e.g. `"Amon.tas"`) or CMIP7 branded name (e.g. `"atmos.rsds.tavg-u-hxy-u.mon.GLB"`). |
| `experiment_id` | str (required) | CMIP experiment ID, e.g. `"historical"`. Must exist in the selected CV. |
| `source_id` | str (required) | CMIP source ID, e.g. `"ACCESS-ESM1-6"`. |
| `variant_label` | str (required) | e.g. `"r1i1p1f1"`. |
| `grid_label` | str, `None` | e.g. `"gn"`. |
| `cmip_version` | `"CMIP6"` (default), `"CMIP6Plus"`, `"CMIP7"` | Selects the controlled vocabulary and CMOR table set. |
| `activity_id` | str, `None` | e.g. `"CMIP"`. |
| `output_path` | path, default `"."` | Where `write()` places output files. |
| `drs_root` | path, `None` | If set, output is organised in the CMIP DRS directory structure under this root. |
| `parent_info` | dict, `None` | Parent experiment metadata (see below). Omitting it triggers a warning: output may not be publication-compliant. |
| `model_id` | str, `None` | Selects the model mapping file (default `"ACCESS-ESM1.6"`). |
| `validate_frequency` | bool, `True` | Check that input data frequency matches the target table. |
| `enable_resampling` | bool, `True` | Allow temporal resampling when input frequency differs. |
| `resampling_method` | str, `"auto"` | Resampling method selection. |
| `enable_chunking` | bool, `False` | Enable explicit dataset chunking. |
| `split_years` | `"auto"` (default), `None`, or int | Output file splitting policy (see below). |

### `parent_info` block

Required for CMIP publication; optional for evaluation workflows
(ESMValTool, ILAMB).

```python
parent_info = {
    "parent_experiment_id": "piControl",
    "parent_activity_id": "CMIP",
    "parent_source_id": "ACCESS-ESM1-5",
    "parent_variant_label": "r1i1p1f1",
    "parent_time_units": "days since 0001-01-01 00:00:00",
    "parent_mip_era": "CMIP6",
    "branch_time_in_child": 0.0,
    "branch_time_in_parent": 54786.0,
    "branch_method": "standard",
}
```

### `split_years` values

| Value | Behaviour |
|---|---|
| `"auto"` (default) | Split per CMIP/ESGF conventions: 1 year per file for sub-daily, 5 for daily, 10 for monthly; single file for yearly/fixed. |
| `None` | Write the entire time series to one file. |
| positive int | Explicit chunk length in years for all frequencies. |

The defaults are importable as `access_moppy.DEFAULT_CHUNK_YEARS`.

## Batch configuration YAML (`moppy-cmorise`)

Generate a starting point with `moppy-example-config my_config.yml`.

### Required keys

| Key | Description |
|---|---|
| `variables` | List of compound names to process (one PBS job each), e.g. `- Amon.pr`. |
| `experiment_id`, `source_id`, `variant_label`, `grid_label`, `activity_id` | CMIP metadata, as for the Python API. |
| `input_folder` | Root directory of the raw model archive. |
| `output_folder` | Where CMORised output, the tracker database, and reports are written. |

### Optional keys

| Key | Default | Description |
|---|---|---|
| `cmip_version` | `CMIP6` | `CMIP6`, `CMIP6Plus`, or `CMIP7`. |
| `model_id` | `ACCESS-ESM1.6` | Mapping file used for auto file-discovery. |
| `parent_info` | package defaults | Parent experiment block (same keys as the Python API). |
| `file_patterns` | auto-discovery | Per-variable glob overrides, e.g. `Amon.pr: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"`. Only needed for non-standard layouts or to restrict folders. |
| `drs_root` | unset | Organise output in CMIP DRS structure under this root. |
| `script_dir` | auto | Directory for generated PBS scripts and logs. |
| `wait_for_completion` | `false` | Block until all jobs finish before exiting. |
| `resume` | `false` | Reuse contiguous completed time splits, continue from the first unfinished year, and retain the existing dated DRS version. |
| `source_partition_years` | unset | Opt-in pre-loading partition size for monthly or daily variables with a direct mapping. Each partition builds an independent Dask graph; the value must be a positive multiple of the resolved `split_years`. Silently skipped for other frequencies, self-contained, or non-direct mappings. |
| `compliance_check` | `false` | Run the `cf:1.11` and WCRP compliance checkers on the first file a variable writes. Failures stop that variable before the remaining source partitions are CMORised. Pair with `source_partition_years` so the check happens after a few years rather than after the whole time series. Requires the `compliance-checker` executable in the job environment. |
| `compliance_check_min_weight` | `3` | Lowest checker weight treated as a failure. `3` covers mandatory checks only; `1` fails on every finding. |
| `compliance_check_suite` | derived from `cmip_version` | Explicit list of checker suites, e.g. `[cf:1.11, wcrp_cmip7:1.0]`. Left unset, CMIP7 gets `[cf:1.11, wcrp_cmip7:1.0]`, CMIP6 gets `[cf:1.11, wcrp_cmip6:1.0]`, CMIP6Plus gets `[cf:1.11, wcrp_cmip6plus:1.0]`. |
| `max_inflight_jobs` | unset | Maximum variable jobs submitted by one monitor at a time. A finished job opens a slot for the next variable. |
| `monitor_poll_interval` | `30` | Seconds between aggregate PBS status requests for active workers. Use a longer interval, such as `300`, for large batches. |
| `publication_lock_dir` | unset | Shared directory containing publication slots. Set this to the same Lustre path across related experiments. |
| `max_concurrent_publications` | unset | Maximum simultaneous staged-file moves using `publication_lock_dir`. |
| `publication_jitter_seconds` | `0` | Maximum random delay before acquiring a publication slot. |
| `publication_stale_seconds` | `86400` | Age after which an abandoned publication slot can be recovered. This should exceed the longest worker walltime. |
| `database_path` | `<output_folder>/cmor_tasks.db` | Custom tracker database location. |

### Compliance check

```yaml
source_partition_years: 10   # write the first output after 10 years of input
compliance_check: true       # validate that first file before continuing
compliance_check_min_weight: 3
```

The checker runs once per variable, on the first file written by the first
source partition, after that file has been published to its DRS location. The
default pairs two suites in a single invocation, because they do not overlap:

| Suite | Covers |
|---|---|
| `cf:1.11` | The CF conventions themselves — data types, coordinate systems, cell methods, bounds semantics, `standard_name` validity. |
| `wcrp_cmip6:1.0` / `wcrp_cmip6plus:1.0` / `wcrp_cmip7:1.0`, chosen by `cmip_version` | CMIP specifics — required global attributes and their CV values, DRS directory and filename, variable registry, chunking, time-axis consistency. |

Override the pair with `compliance_check_suite` to run any suites the local
`compliance-checker --list-tests` offers. The default is derived from
`cmip_version` rather than hardcoded, so a CMIP6 batch is never validated
against the CMIP7 vocabulary:

```yaml
compliance_check_suite:
  - cf:1.11
  - wcrp_cmip7:1.0
```

A JSON report holding both sections is always kept — pass or fail — next to
the generated worker script, in
`<script_dir>/logs/<variable>/compliance_<file>.json`. An `access_moppy` block
in that report records the suites, the enforced weight, and the esgvoc
vocabulary version applied, because the suite name pins the checker plugin and
not the vocabulary: `wcrp_cmip7:1.0` validates against whichever CMIP7 CV the
local esgvoc database holds (`1.2.16` in `conda/analysis3-26.07`).

WCRP checks that fail because the esgvoc vocabulary database is unavailable
are reported as a warning but never abort the variable, so an incomplete
checker environment cannot take down a whole batch. Install it with
`esgvoc install` to enable those checks.

`compliance_check_min_weight: 1` is rarely usable: CMIP output declares
`Conventions = "CF-1.7 CMIP-6.2"`, which `cf:1.11` reports as a weight-2
finding for every file.

When the file fails, it is renamed to `<file>.nc.compliance_failed` and the
variable is marked failed in the tracker. The file is kept for inspection, and
because it no longer matches the published DRS layout a later `resume` run
cannot mistake it for a completed split.

### PBS resource keys

| Key | Example | Description |
|---|---|---|
| `queue` | `"normal"` | PBS queue name. |
| `cpus_per_node` | `14` | CPUs per job. |
| `mem` | `"32GB"` | Memory per job. |
| `jobfs` | `"100GB"` | Local scratch space. |
| `walltime` | `"02:00:00"` | Maximum runtime per job. |
| `scheduler_options` | `"#PBS -P tm70"` | Extra PBS directives (project, etc.). |
| `storage` | `"gdata/p73+scratch/tm70"` | NCI storage mounts required by jobs. |
| `worker_init` | multi-line string | Shell commands run at job start (module loads, conda activation). Also where you export Dask worker-sizing env vars such as `MOPPY_WORKER_MEMORY_HISTORY` — see {doc}`/howto/batch_processing`. |

### Per-variable resource overrides

```yaml
variable_resources:
  Omon.thetao:          # 3D ocean variables need more resources
    cpus_per_node: 28
    mem: 128GB
    walltime: "06:00:00"
  day.pr:               # daily data needs more memory
    mem: 64GB
    walltime: "04:00:00"
```

### Complete example

```yaml
variables:
  - Amon.pr
  - Omon.tos

experiment_id: piControl
source_id: ACCESS-ESM1-6
variant_label: r1i1p1f1
grid_label: gn
activity_id: CMIP
cmip_version: CMIP7

input_folder: "/g/data/project/model_output"
output_folder: "/scratch/project/cmor_output"

queue: "normal"
cpus_per_node: 16
mem: "32GB"
walltime: "02:00:00"
scheduler_options: "#PBS -P tm70"
storage: "gdata/p73+scratch/tm70"

worker_init: |
  module use /g/data/xp65/public/modules
  module load conda/analysis3
```

See {doc}`/howto/batch_processing` for tuning guidance, monitoring, and
error recovery.
