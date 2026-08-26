<div align="center">
  <img src="docs/images/Moppy_logo.png" alt="MOPPy Logo" width="300"/>
</div>

# ACCESS-MOPPy (Model Output Post-processor in Python)

[![Documentation Status](https://readthedocs.org/projects/access-moppy/badge/?version=latest)](https://access-moppy.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/access_moppy.svg)](https://badge.fury.io/py/access_moppy)
[![Conda Version](https://img.shields.io/conda/vn/accessnri/access-moppy.svg)](https://anaconda.org/accessnri/access-moppy)
[![DOI](https://zenodo.org/badge/955620939.svg)](https://doi.org/10.5281/zenodo.21385771)

ACCESS-MOPPy is a CMORisation tool designed to post-process ACCESS model output and produce CMIP-compliant datasets.

## Key Features

- **Python API** for integration into notebooks and scripts
- **Batch processing system** for HPC environments with PBS
- **Real-time monitoring** with web-based dashboard
- **Flexible CMORisation** of individual variables
- **Dask-enabled** for scalable parallel processing
- **Cross-platform compatibility** (not limited to NCI Gadi)
- **CMIP6 and CMIP7 FastTrack support**
- **Publication QC**: physical-range checks, the WCRP compliance checker, and
  diagnostic plots — see [below](#from-native-access-output-to-publishable-cmip7)

## From native ACCESS output to publishable CMIP7

For **CMIP7 Fast Track**, ACCESS-MOPPy takes raw ACCESS model output — UM
fields files, MOM and CICE history — and produces files that are ready to
publish: CMORised, checked against physical ranges, validated against the CMIP
controlled vocabularies, and plotted for a human to look at. The four stages
run in that order, from a notebook or from a batch run on NCI Gadi.

![Stage 1](https://img.shields.io/badge/Stage_1-CMORise_native_output-2980b9?style=flat-square)

Reads ACCESS-ESM1.6 atmosphere, land, ocean and sea-ice output directly and
writes CMIP7 files — branded variable names, CMIP7 global attributes, DRS paths
and file names. No CMOR library: the rewrite is built on **xarray** and
**dask**, so the same code runs in a notebook or across hundreds of PBS jobs.

→ [Fast Track quick start](https://access-moppy.readthedocs.io/en/latest/quickstart/cmip7_fasttrack.html)
· [baseline runs](https://access-moppy.readthedocs.io/en/latest/howto/cmip7_fasttrack_baseline.html)
· [batch processing](https://access-moppy.readthedocs.io/en/latest/howto/batch_processing.html)

![Stage 2](https://img.shields.io/badge/Stage_2-Check_the_physical_range-0f7b6c?style=flat-square)

Every CMIP7 file written is checked against a per-variable physical envelope —
293 ACCESS-ESM1-6 variables, with experiment-specific overrides — plus units,
missing-value and finite-value checks. The bounds are broad on purpose: they
catch a unit, sign or conversion error without rejecting a plausible extreme.

The rules themselves are data, and you can read them without touching a file:

```bash
moppy-qc --show-ranges --variable tas --variable pr --experiment piControl
```

```text
variable  units       min  max  rule
--------  ----------  ---  ---  ---------
tas       K           180  325  piControl
pr        kg m-2 s-1  0    0.1  default
```

Add `--format json` for the machine-readable form, ready to pipe into `jq` or
attach to a data-quality record.

→ [Every rule, rendered and filterable](https://access-moppy.readthedocs.io/en/latest/reference/qc_ranges.html)
· [running the checks](https://access-moppy.readthedocs.io/en/latest/howto/qc_validation.html)

![Stage 3](https://img.shields.io/badge/Stage_3-WCRP_compliance_checker-a8580a?style=flat-square)

Runs the CF suite (`cf:1.11`) and the WCRP CMIP suite (`wcrp_cmip7:1.0`, backed
by `esgvoc`) on the first file each variable publishes — metadata,
controlled-vocabulary values, DRS path and file name. A failure stops that
variable before any further file is written, and the JSON report is kept either
way. One line of batch config turns it on:

```yaml
compliance_check: true
```

→ [Enabling it in a batch run](https://access-moppy.readthedocs.io/en/latest/reference/configuration.html#compliance-check)
· [checker backends](https://access-moppy.readthedocs.io/en/latest/development/compliance_testing.html)

![Stage 4](https://img.shields.io/badge/Stage_4-QC_diagnostic_plots-6b3fa0?style=flat-square)

Two PNGs per output file: a spatial snapshot of the first timestep, and a
timeseries of the global mean with min/max shading and standard deviation. A
published ACCESS-ESM1-5 CMIP6 series can be overlaid on the timeseries, so
drift against the previous submission is visible at a glance.

```bash
moppy-qc-plots /scratch/cmor_output/CMIP7 --comparison-store /g/data/cmip6_store
```

→ [Plots from a batch run](https://access-moppy.readthedocs.io/en/latest/howto/batch_processing.html#qc-diagnostic-plots)
· [regenerating them](https://access-moppy.readthedocs.io/en/latest/howto/qc_validation.html#qc-diagnostic-plots)

> [!NOTE]
> Stages 2 and 4 run inside the CMORisation job, and stage 3 is one line of
> batch configuration. The batch report (`moppy_batch_report_<UTC>.json`)
> collects the results of all three, so a whole experiment can be signed off
> from a single file.

## Installation

ACCESS-MOPPy requires Python >= 3.11.

### On NCI Gadi (recommended for ACCESS users)

The `conda/analysis3-latest` environment maintained by ACCESS-NRI already
includes `access_moppy` and its dependencies, so no `pip install` is needed:

```bash
module use /g/data/xp65/public/modules
module load conda/analysis3-latest
```

All command-line tools (`moppy-cmorise`, `moppy-tui`, `moppy-qc`, …) are
available immediately after loading the module. You'll need membership of
the `xp65` NCI project for the module itself, plus whichever projects hold
the model output and CV/table data you're processing. Pin a dated release
(e.g. `conda/analysis3-26.04`) instead of `-latest` if you need a
reproducible environment for a production run.

### From PyPI

```bash
pip install access_moppy
```

### From source

The controlled vocabularies under `src/access_moppy/vocabularies/` are pulled
in as git submodules. If you install from a local clone, initialise them
first, otherwise the CMOR tables/CVs will be missing and imports will fail
with an error like `No module named 'access_moppy.vocabularies.CMIP6_CVs'`:

```bash
git clone --recurse-submodules https://github.com/ACCESS-NRI/ACCESS-MOPPy.git
cd ACCESS-MOPPy
pip install .
```

If you already have a clone without the submodules populated, run:

```bash
git submodule update --init --recursive
pip install .
```

## Quick Start

### Interactive Usage (Python API)

```python
import glob
from access_moppy import ACCESS_ESM_CMORiser

# Select input files
files = glob.glob("/path/to/model/output/*mon.nc")

# Create CMORiser instance
cmoriser = ACCESS_ESM_CMORiser(
    input_data=files,
    compound_name="Amon.pr",  # table.variable format
    experiment_id="historical",
    source_id="ACCESS-ESM1-5",
    variant_label="r1i1p1f1",
    grid_label="gn",
    activity_id="CMIP",
    output_path="/path/to/output"
)

# Run CMORisation
cmoriser.run()
cmoriser.write()
```

### Batch Processing (HPC/PBS)

For large-scale processing on HPC systems:

1. **Create a configuration file** (`batch_config.yml`):

```yaml
variables:
  - Amon.pr
  - Omon.tos
  - Amon.ts

experiment_id: piControl
source_id: ACCESS-ESM1-5
variant_label: r1i1p1f1
grid_label: gn

input_folder: "/g/data/project/model/output"
output_folder: "/scratch/project/cmor_output"

file_patterns:
  Amon.pr: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"
  Omon.tos: "output[0-4][0-9][0-9]/ocean/*temp*.nc"
  Amon.ts: "output[0-4][0-9][0-9]/atmosphere/netCDF/*mon.nc"

# PBS configuration
queue: normal
cpus_per_node: 16
mem: 32GB
walltime: "02:00:00"
scheduler_options: "#PBS -P your_project"
storage: "gdata/project+scratch/project"

worker_init: |
  module load conda
  conda activate your_environment
```

2. **Submit batch job**:

```bash
moppy-cmorise batch_config.yml
```

3. **Monitor progress** at http://localhost:8501

## Batch Processing Features

The batch processing system provides:

- **Parallel execution**: Each variable processed as a separate PBS job
- **Real-time monitoring**: Web dashboard showing job status and progress
- **Automatic tracking**: SQLite database maintains job history and status
- **Error handling**: Failed jobs can be easily identified and resubmitted
- **Resource optimization**: Configurable CPU, memory, and storage requirements
- **Environment management**: Automatic setup of conda/module environments

### Monitoring Tools

- **Streamlit Dashboard**: Real-time web interface at http://localhost:8501
- **Command line**: Use standard PBS commands (`qstat`, `qdel`)
- **Database**: SQLite tracking at `{output_folder}/cmor_tasks.db`
- **Log files**: Individual stdout/stderr for each job

### File Organization

```
work_directory/
├── batch_config.yml          # Your configuration
├── cmor_job_scripts/          # Generated PBS scripts and logs
│   ├── cmor_Amon_pr.sh       # PBS script
│   ├── cmor_Amon_pr.py       # Python processing script
│   ├── cmor_Amon_pr.out      # Job output
│   └── cmor_Amon_pr.err      # Job errors
└── output_folder/
    ├── cmor_tasks.db         # Progress tracking
    └── [CMORised files]      # Final output
```

## Documentation

Full documentation: <https://access-moppy.readthedocs.io>

- **Quick start**: [Run your first CMORisation on Gadi](https://access-moppy.readthedocs.io/en/latest/quickstart/index.html)
- **Tutorials**: [Notebook walkthroughs](https://access-moppy.readthedocs.io/en/latest/tutorials/index.html)
- **How-to guides**: [Batch processing](https://access-moppy.readthedocs.io/en/latest/howto/batch_processing.html), [QC validation](https://access-moppy.readthedocs.io/en/latest/howto/qc_validation.html), [ESMValTool](https://access-moppy.readthedocs.io/en/latest/howto/esmvaltool_integration.html), [ILAMB](https://access-moppy.readthedocs.io/en/latest/howto/cmorise_ilamb_workflow.html)
- **Reference**: [CLI](https://access-moppy.readthedocs.io/en/latest/reference/cli.html), [configuration keys](https://access-moppy.readthedocs.io/en/latest/reference/configuration.html), [QC range rules](https://access-moppy.readthedocs.io/en/latest/reference/qc_ranges.html), [Python API](https://access-moppy.readthedocs.io/en/latest/reference/api/access_moppy/index.html)
- **Example Configuration**: `src/access_moppy/examples/batch_config.yml`

## Test Data Override

Integration and end-to-end tests require an external test-data tree set via
the `ACCESS_MOPPY_DATA_ROOT` environment variable.

- Covered tests: full CMOR integration and end-to-end real-file tests
- No fallback: test-data fixtures in `tests/data/` are not used by these tests
- Requirement: `ACCESS_MOPPY_DATA_ROOT` must point to a valid dataset root
  containing `output*/atmosphere/netCDF`, `output*/ocean`, and `output*/ice`

Example:

```bash
export ACCESS_MOPPY_DATA_ROOT=/path/to/CMIP7_Test_data/esm-historical
pixi run -e dev python -m pytest tests/integration/test_full_cmorisation.py
pixi run -e dev python -m pytest tests/integration/test_cmip7_baseline_cmorisation.py
pixi run -e dev python -m pytest tests/e2e/test_end_to_end.py
```

CMIP7 baseline test note:

- `tests/integration/test_cmip7_baseline_cmorisation.py` runs one case per
  CMIP7 baseline variable listed in
  `src/access_moppy/examples/batch_config_esm1-6_cmip7_baseline.yml`
- By default, this suite checks end-to-end CMORisation success (run/write/output)
- To additionally enforce WCRP compliance-checker validation for this suite,
  set `ACCESS_MOPPY_BASELINE_VALIDATE_WCRP=1`

Example with strict WCRP validation enabled:

```bash
export ACCESS_MOPPY_DATA_ROOT=/path/to/CMIP7_Test_data/esm-historical
export ACCESS_MOPPY_BASELINE_VALIDATE_WCRP=1
pixi run -e dev python -m pytest tests/integration/test_cmip7_baseline_cmorisation.py --validation-tool=wcrp
```

## Current Status

- **Stable project status**: ACCESS-MOPPy is suitable for supported CMORisation workflows and ongoing production-oriented use.
- **Ocean variables**: Ocean variables are supported, including dedicated ocean CMORisers and resource guidance for large 3D variables.
- **Variable mapping**: Mapping coverage continues to be reviewed and improved for CMIP6/CMIP7 compliance.

## Support

- **Issues**: Submit via GitHub Issues
- **Questions**: Contact ACCESS-NRI support
- **Contributions**: Welcome via Pull Requests

## License

ACCESS-MOPPy is licensed under the Apache-2.0 License.
