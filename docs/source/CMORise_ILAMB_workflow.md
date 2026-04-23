# ILAMB Evaluation Workflow: CMORisation with Batch Processing

This guide covers the end-to-end workflow for preparing ACCESS-ESM1-6 model output
for evaluation with [ILAMB (International Land Model Benchmarking)](https://www.ilamb.org/).
The workflow uses ACCESS-MOPPy's batch processing system to CMORise multiple land,
atmosphere, and biogeochemistry variables in parallel on NCI's Gadi HPC.

:::{note}
ACCESS-ESM1-6 is not yet officially registered in the CMIP controlled vocabularies.
As a temporary workaround, we use ACCESS-ESM1-5 as the `source_id` during CMORisation.
This means some outputs may appear labelled as ACCESS-ESM1-5, which can be confusing.
We are aware of this limitation and plan to remove it as soon as ACCESS-ESM1-6 is
validated and added to the CMIP vocabulary.
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
| NCI project access | `p73`, `tm70`|
| Software | `conda/analysis3-26.04` via `xp65` modules |
| Scheduler | PBS Pro (Gadi) |

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

# CMIP6 metadata
experiment_id: historical
source_id: ACCESS-ESM1-5
variant_label: r1i1p1f1
grid_label: gn
activity_id: CMIP

# Input and output paths
input_folder: "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/historical-02"
output_folder: "YOUR_OUTPUT_PATH"

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

When `drs_root` is **not** set (the default for this workflow), all CMORised
files land directly in `output_folder` with CMIP6-standard filenames:

```
output_folder/
├── pr_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
├── tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
├── gpp_Lmon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
├── ...
└── cmor_tasks.db
```

---

## Preparing ILAMB-Ready Files

ILAMB does **not** use CMIP6 DRS directory trees. It expects each variable to
be a single file named `<variable>.nc` inside a flat directory, which is then
set as `ILAMB_ROOT`.

Run the following command after all batch jobs complete. Set `OUTPUT_FOLDER`
to your actual output path, then copy-paste the block as-is:

```bash
OUTPUT_FOLDER="/scratch/tm70/$USER/ilamb_cmorised/historical-02"

mkdir -p "${OUTPUT_FOLDER}/ILAMB_format"
for f in "${OUTPUT_FOLDER}"/*.nc; do
    fname="${f##*/}"
    var="${fname%%_*}"
    ln -sf "../${fname}" "${OUTPUT_FOLDER}/ILAMB_format/${var}.nc" && echo "  ${var}.nc"
done
```

The resulting layout:

```
output_folder/
├── pr_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
├── tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
├── gpp_Lmon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
├── ...
├── cmor_tasks.db
└── ILAMB_format/
    ├── pr.nc     -> ../pr_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
    ├── tas.nc    -> ../tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
    ├── gpp.nc    -> ../gpp_Lmon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc
    └── ...
```

Point ILAMB at the `ILAMB_format` subdirectory in `ilamb-setup.txt`:

```
ACCESS-ESM1_6_historical_Moppy_cmorised, /scratch/tm70/$USER/ilamb_variables/batch-processing2/ilamb_format, CMIP6
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
