# ILAMB Evaluation Workflow: CMORisation with Batch Processing

This guide covers the end-to-end workflow for preparing ACCESS-ESM1-6 model output
for evaluation with [ILAMB (International Land Model Benchmarking)](https://www.ilamb.org/).
The workflow uses ACCESS-MOPPy's batch processing system to CMORise multiple land,
atmosphere, and biogeochemistry variables in parallel on NCI's Gadi HPC.

:::{note}
ACCESS-MOPPy currently injects a temporary CMIP7 controlled-vocabulary override for
`ACCESS-ESM1-6` so CMIP7 workflows can use the real `source_id` before the bundled
CMIP7 CV snapshot is updated. This shim is intended to be removed once the official
CMIP7 CVs include `ACCESS-ESM1-6`.
:::

---

## Overview

ILAMB evaluates land surface model performance against observational benchmarks.
It expects variables in CF-compliant NetCDF format with standard CMIP names and units.
ACCESS-MOPPy handles the conversion (CMORisation) from raw ACCESS-ESM1-6 output to
this format, with each variable submitted as an independent PBS job.

**Experiment:** `historical-02` (ACCESS-ESM1-6 production run)
**Variables covered:** 22 variables across Emon, Lmon, and Amon CMIP tables

:::{note}
ILAMB does not require strict CMIP6 publication compliance. CMORisation here is
used to standardise variable names, units, and metadata — not for data submission.
:::

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| NCI project access | `p73`, `xp65`|
| Software | `conda/analysis3-26.04` via `xp65` modules |
| Scheduler | PBS Pro (Gadi) |

> **Note:** `tm70` appears in the example paths throughout this guide (e.g. `/scratch/tm70/$USER/…`) but is **not** a required prerequisite. Substitute your own project code where appropriate.

---

## Variables

The workflow processes 22 variables organised by CMIP table. Variables marked
with `*` require **daily** input files; all others use monthly input.

### Emon — Monthly ecosystem diagnostics

| Variable | Long name |
|----------|-----------|
| `cSoil` | Carbon mass in soil pool |
| `fBNF` | Biological nitrogen fixation |

### Lmon — Monthly land surface

| Variable | Long name |
|----------|-----------|
| `cVeg` | Carbon mass in vegetation |
| `gpp` | Gross primary production |
| `lai` | Leaf area index |
| `nbp` | Net biome production |
| `ra` | Plant respiration |
| `rh` | Heterotrophic respiration |
| `tsl` | Temperature of soil layers |
| `mrro` | Total runoff |

### Amon — Monthly atmosphere / surface energy balance

| Variable | Long name |
|----------|-----------|
| `evspsbl` | Evaporation |
| `hfls` | Surface upward latent heat flux |
| `hfss` | Surface upward sensible heat flux |
| `hurs` | Near-surface relative humidity |
| `pr` | Precipitation |
| `rlds` | Surface downwelling longwave radiation |
| `rlus` | Surface upwelling longwave radiation |
| `rsds` | Surface downwelling shortwave radiation |
| `rsus` | Surface upwelling shortwave radiation |
| `tas` | Near-surface air temperature |
| `tasmax` * | Daily maximum near-surface air temperature |
| `tasmin` * | Daily minimum near-surface air temperature |

> `tasmax` and `tasmin` are Amon-table variables but are derived from **daily**
> ACCESS output (`*dai.nc`). Their file patterns differ from the other Amon variables.

### Omon — Ocean (pending)

| Variable | Status |
|----------|--------|
| `hfds` (Downward heat flux at sea surface) | Pending — ocean file pattern not yet confirmed |

---

## Batch Configuration

Save the following as `ilamb_variables_cmorise.yml`. Update `output_folder`
before running.

```yaml
# Batch CMORisation configuration — Feb26-PI-CNP-concentrations (ACCESS-ESM1-6)
# Run with: python -m access_moppy.batch_cmoriser batch_config_Feb26_PI_CNP.yml

# Variables to process (one PBS job per variable)
variables:
  # --- Emon ---
  - Emon.cSoil
  - Emon.fBNF
  # --- Lmon ---
  - Lmon.cVeg
  - Lmon.gpp
  - Lmon.lai
  - Lmon.nbp
  - Lmon.ra
  - Lmon.rh
  - Lmon.tsl
  - Lmon.mrro
  # --- Amon ---
  - Amon.evspsbl
  - Amon.hfls
  - Amon.hfss
  - Amon.hurs
  - Amon.pr
  - Amon.rlds
  - Amon.rlus
  - Amon.rsds
  - Amon.rsus
  - Amon.tasmax
  - Amon.tasmin
  - Amon.tas
  # --- Omon ---
  # Omon.hfds  # TODO: add ocean file pattern once known

# CMIP7 metadata
experiment_id: historical
source_id: ACCESS-ESM1-6
variant_label: r1i1p1f1
grid_label: gn
activity_id: CMIP
cmip_version: CMIP7

# Input and output paths
input_folder: "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/historical-02"
output_folder: "YOUR_OUTPUT_PATH"

# When true, after all variables finish the monitor automatically creates an
# <output_folder>/ilamb_input/ directory of <variable_id>.nc symlinks pointing
# at the CMORised files — ready to use as ILAMB model input (see
# "Preparing ILAMB-Ready Files" below). Default: false.
ilamb_input_format: true

# File patterns (relative to input_folder)
# All atmosphere/land variables share the same pattern
file_patterns:
  Emon.cSoil:   "/output1*/atmosphere/netCDF/*mon.nc"
  Emon.fBNF:    "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.cVeg:    "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.gpp:     "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.lai:     "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.nbp:     "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.ra:      "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.rh:      "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.tsl:     "/output1*/atmosphere/netCDF/*mon.nc"
  Lmon.mrro:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.evspsbl: "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.hfls:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.hfss:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.hurs:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.pr:      "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.rlds:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.rlus:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.rsds:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.rsus:    "/output1*/atmosphere/netCDF/*mon.nc"
  Amon.tasmax:  "/output1*/atmosphere/netCDF/*dai.nc"
  Amon.tasmin:  "/output1*/atmosphere/netCDF/*dai.nc"
  Amon.tas:     "/output1*/atmosphere/netCDF/*mon.nc"

# PBS job configuration (defaults for all variables)
queue: "normal"
cpus_per_node: 12
mem: "190GB"
jobfs: 100GB
walltime: "02:00:00"
scheduler_options: "#PBS -P iq82"
storage: "gdata/tm70+gdata/xp65+gdata/p73+scratch/tm70" #Example

# Environment setup
worker_init: |
  module use /g/data/xp65/public/modules
  module load conda/analysis3-26.04

wait_for_completion: false
```

### Key configuration notes

**File patterns**

The pattern `/output1*/atmosphere/netCDF/*mon.nc` uses shell globbing:

- `output1*` — matches all restart chunks beginning with `output1`
  (e.g. `output100`, `output101`, …). Adjust the prefix if your archive uses
  a different naming scheme.
- `*mon.nc` — monthly-frequency files. Daily files (`*dai.nc`) are used only
  for `tasmax` and `tasmin`.

**Resource allocation**

Each job requests `12 CPUs` and `190 GB` of memory. This is sized for
land/atmosphere monthly data at N96 resolution. If you add 3-D ocean variables
(e.g. `Omon.hfds`) you may need per-variable overrides:

```yaml
variable_resources:
  Omon.hfds:
    cpus_per_node: 28
    mem: "190GB"
    walltime: "04:00:00"
```

**`wait_for_completion: false`**

The controller submits all 22 jobs and exits immediately. Jobs run
independently on Gadi. Use the tracking database or PBS commands to monitor
progress (see [Monitoring](#monitoring) below).

---

## Running the Workflow

### 1. Set your output path

Replace `YOUR_OUTPUT_PATH` in the config with a path on scratch, e.g.:

```bash
output_folder: "/scratch/tm70/$USER/ilamb_cmorised/historical-02"
```

### 2. Submit the batch

From a Gadi login node, with ACCESS-MOPPy available in your environment:

```bash
module use /g/data/xp65/public/modules
module load conda/analysis3-26.04

moppy-cmorise ilamb_variables_cmorise.yml
```

This will:
1. Parse and validate the configuration
2. Create a SQLite tracking database at `output_folder/cmor_tasks.db`
3. Generate PBS and Python scripts under `output_folder/cmor_job_scripts/`
4. Submit 22 PBS jobs (one per variable) via `qsub`
5. Print submitted job IDs and exit

---

## Monitoring

### PBS commands

```bash
# List your running jobs
qstat -u $USER

# Check a specific job
qstat -f <job_id>

# Watch overall queue
qstat -q normal
```

### Tracking database

```python
import sqlite3, pandas as pd

conn = sqlite3.connect("/scratch/tm70/$USER/ilamb_cmorised/historical-02/cmor_tasks.db")

df = pd.read_sql_query("""
    SELECT variable, status, start_time, end_time, error_message
    FROM cmor_tasks
    ORDER BY start_time
""", conn)

print(df)
```

Possible `status` values: `pending` → `running` → `completed` / `failed`.

### Log files

Each job writes stdout and stderr under the `cmor_job_scripts/` directory:

```
cmor_job_scripts/
├── cmor_Amon_pr.sh       # Generated PBS script
├── cmor_Amon_pr.py       # Generated Python script
├── cmor_Amon_pr.out      # stdout
└── cmor_Amon_pr.err      # stderr (check here first on failure)
```

---

## Error Recovery

Failed jobs can be resubmitted by re-running the same command:

```bash
moppy-cmorise ilamb_variables_cmorise.yml
```

Completed variables are skipped automatically; only `failed` or `pending`
variables are resubmitted.

To manually reset a failed variable in the database:

```python
conn.execute("""
    UPDATE cmor_tasks
    SET status = 'pending', start_time = NULL, end_time = NULL, error_message = NULL
    WHERE variable = 'Amon.tas' AND status = 'failed'
""")
conn.commit()
```

### Common failures

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `FileNotFoundError` in `.err` | File pattern matches nothing | Check `output1*` prefix against actual archive directory names |
| `MemoryError` / job killed | Data larger than `mem` allocation | Increase `mem` or reduce `cpus_per_node` |
| Job never starts (stuck `Q`) | Insufficient project allocation | Run `nci_account -P iq82` to check SU balance |
| Module not found in job | `worker_init` not sourcing correctly | Test `module use` command interactively on a compute node |
| `tasmax`/`tasmin` empty output | Wrong file pattern | Confirm daily files exist at `*dai.nc`; adjust glob if needed |

---

## Output Structure

`drs_root` defaults to `output_folder`, so a batch run writes a DRS tree rooted
there. With `cmip_version: CMIP7` that is the CMIP7 DRS:

```
output_folder/
├── MIP-DRS7/CMIP7/CMIP/ACCESS-Consortium/ACCESS-ESM1-6/historical/r1i1p1f1/
│   └── glb/mon/tas/tavg-h2m-hxy-u/gn/v20260819/
│       ├── tas_tavg-h2m-hxy-u_mon_glb_gn_ACCESS-ESM1-6_historical_r1i1p1f1_185001-185912.nc
│       ├── tas_tavg-h2m-hxy-u_mon_glb_gn_ACCESS-ESM1-6_historical_r1i1p1f1_186001-186912.nc
│       └── ...
└── cmor_tasks.db
```

The path elements are
`<drs_specs>/<mip_era>/<activity_id>/<institution_id>/<source_id>/<experiment_id>/`
`<variant_label>/<region>/<frequency>/<variable_id>/<branding_suffix>/<grid_label>/<version>`,
with a `latest` symlink beside each version directory. Long runs are split into
several files per variable, so expect many time chunks per variable directory
rather than a single file.

Set `drs_root` to another path to move the tree there, or to an empty string to
write flat files into `output_folder` instead.

---

## Preparing ILAMB-Ready Files

:::{tip}
If you set `ilamb_input_format: true` in the batch configuration (see above), the
monitor already builds the model-input symlinks for you: once all variables
finish, an `<output_folder>/ilamb_input/` directory is created containing one
`<variable_id>.nc` symlink per CMORised file. You can point ILAMB's `MODELS`
entry straight at that directory and skip the manual `create_ilamb_model_symlinks`
step below — you still need the `DATA` observational link (Step 1) to complete the
ILAMB-ROOT layout.
:::

ILAMB requires a specific directory layout called **ILAMB-ROOT**:

```
ILAMB_ROOT/
├── DATA/          → observational reference datasets
└── MODELS/
    └── <model_name>/
        ├── tas.nc → <CMORised file>
        ├── pr.nc  → <CMORised file>
        └── ...
```

Use `create_ilamb_data_tree` to build this layout in one call after all batch
jobs complete:

```python
from access_moppy.utilities import create_ilamb_data_tree

create_ilamb_data_tree(
    output_dir="/scratch/tm70/$USER/ilamb_cmorised/historical-02",
    ilamb_root="/scratch/tm70/$USER/ilamb_root",
    model_name="ACCESS-ESM1-6",
)
```

This call:

1. Creates `ILAMB_ROOT/DATA` as a symlink to the NCI observational dataset
   replica at `/g/data/ct11/access-nri/replicas/ILAMB` (default).
2. Creates `ILAMB_ROOT/MODELS/ACCESS-ESM1-6/` and populates it with
   `<variable>.nc` symlinks pointing at the CMORised files in `output_dir`.

The resulting layout:

```
/scratch/tm70/$USER/ilamb_root/
├── DATA  →  /g/data/ct11/access-nri/replicas/ILAMB
└── MODELS/
    └── ACCESS-ESM1-6/
        ├── pr.nc   →  /scratch/tm70/$USER/ilamb_cmorised/historical-02/pr_Amon_…nc
        ├── tas.nc  →  /scratch/tm70/$USER/ilamb_cmorised/historical-02/tas_Amon_…nc
        ├── gpp.nc  →  /scratch/tm70/$USER/ilamb_cmorised/historical-02/gpp_Lmon_…nc
        └── ...
```

### Optional: custom observational source

To point `DATA` at a different observational archive, pass `obs_source`:

```python
create_ilamb_data_tree(
    output_dir="/scratch/tm70/$USER/ilamb_cmorised/historical-02",
    ilamb_root="/scratch/tm70/$USER/ilamb_root",
    model_name="ACCESS-ESM1-6",
    obs_source="/path/to/custom/obs",
)
```

### Optional: step-by-step control

To create the `DATA` link and model symlinks independently:

```python
from access_moppy.utilities import (
    create_ilamb_observational_symlinks,
    create_ilamb_model_symlinks,
)

# Step 1 – observational data
create_ilamb_observational_symlinks(
    ilamb_root="/scratch/tm70/$USER/ilamb_root",
)

# Step 2 – model output
create_ilamb_model_symlinks(
    output_dir="/scratch/tm70/$USER/ilamb_cmorised/historical-02",
    ilamb_dir="/scratch/tm70/$USER/ilamb_root/MODELS/ACCESS-ESM1-6",
)
```

Set `ILAMB_ROOT` to the root directory when running ILAMB:

```bash
export ILAMB_ROOT=/scratch/tm70/$USER/ilamb_root
```

---

After set-up, run ilamb with following command.

```
module use /g/data/xp65/public/modules
module load conda/analysis3-26.04


export ILAMB_ROOT=YOUR-ILAMB-ROOT
export CARTOPY_DATA_DIR=/g/data/kj13/admin/ILAMB/script_github_ilamb
export BUILD_DIR=YOUR-BUILD-DIR

rm -rf BUILD_DIR
mpiexec -n <NUMBER OF PROCESS> ilamb-run --config <YOUR ILAMB CONFIG FILE> --model_setup <YOUR DATASET SETUP .txt FILE> --regions global --build_dir $BUILD_DIR
```

---

## CMIP7 output

Everything above describes CMIP6 output. CMIP7 needs no extra steps — the same
`ilamb_input_format: true`, `create_ilamb_data_tree` and
`create_ilamb_model_symlinks` calls work on a CMIP7 run — but the symlink names
are derived differently, and two options exist to control what gets linked.

### Why the names differ

CMIP7 collapses several CMIP6 variables onto one physical parameter and tells
them apart with a branding suffix, so the first component of a CMIP7 filename is
not the variable name:

| CMIP7 file | CMIP6 name |
|---|---|
| `tas_tavg-h2m-hxy-u_mon_glb_gn_…` | `tas` |
| `tas_tmaxavg-h2m-hxy-u_mon_glb_gn_…` | `tasmax` |
| `tas_tminavg-h2m-hxy-u_mon_glb_gn_…` | `tasmin` |

The CMIP6 name is recovered by looking up
`(variable_id, branding_suffix, frequency, region)` in
`mappings/cmip7_to_cmip6_compound_name_mapping.json`. Files that do not resolve
are skipped with a warning. CMIP7 layouts — flat or DRS — are detected
automatically; pass `drs_format="cmip7"` to force it.

### Choosing frequency and variables

ILAMB indexes a model directory by the variable name *inside* each file, so it
cannot hold monthly and daily `tas` at once — it would treat them as time chunks
of one series. Only one frequency is linked, `mon` by default, plus every `fx`
field, which ILAMB needs for cell measures. A variable present only at another
frequency is named in a warning rather than dropped silently.

```python
create_ilamb_model_symlinks(
    output_dir, ilamb_dir,
    frequency="mon",
    frequency_overrides={"tasmax": "day"},          # per-variable exception
    variables=["gpp", "nbp", "tas", "areacella"],   # exact list; omit for all
)
```

`variables` uses CMIP6 names, so `"tasmax"` selects the `tas_tmaxavg-…` file.
The list is exact — include `areacella`/`sftlf` if you want them. In a batch
config the same two controls are `ilamb_frequency` and `ilamb_variables`.

### Time-chunked variables

A variable with one source file is linked as `<variable>.nc`; a variable split
across several files gets a `<variable>/` directory of `<variable>_<time_range>.nc`
links. ILAMB walks subdirectories and concatenates the chunks, so both forms
behave the same:

```
ilamb_input/
├── areacella.nc                  → …/areacella_ti-u-hxy-u_fx_…nc
└── tas/
    ├── tas_185001-185912.nc      → …/tas_tavg-h2m-hxy-u_mon_…_185001-185912.nc
    └── …
```

DRS trees carry a `latest` symlink and may hold more than one `v<YYYYMMDD>`
directory; the newest version of each variable is used.
