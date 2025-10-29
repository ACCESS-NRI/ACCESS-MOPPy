<div align="center">
  <img src="docs/images/Moppy_logo.png" alt="MOPPy Logo" width="300"/>
</div>

# ACCESS-MOPPy (Model Output Post-Processor)

[![Documentation Status](https://readthedocs.org/projects/access-moppy/badge/?version=latest)](https://access-moppy.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/access_moppy.svg)](https://badge.fury.io/py/access_moppy)
[![Conda Version](https://img.shields.io/conda/vn/accessnri/access-moppy.svg)](https://anaconda.org/accessnri/access-moppy)

ACCESS-MOPPy is a CMORisation tool designed to post-process ACCESS model output and produce CMIP-compliant datasets.

## Key Features

- **Python API** for integration into notebooks and scripts
- **Batch processing system** for HPC environments with PBS
- **Real-time monitoring** with web-based dashboard
- **Flexible CMORisation** of individual variables
- **Dask-enabled** for scalable parallel processing
- **Cross-platform compatibility** (not limited to NCI Gadi)
- **CMIP6 and CMIP7 FastTrack support**

## Installation

ACCESS-MOPPy requires Python >= 3.11. Install with:

```bash
pip install numpy pandas xarray netCDF4 cftime dask pyyaml tqdm requests streamlit
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
    input_paths=files,
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

- **Getting Started**: `docs/source/getting_started.rst`
- **Example Configuration**: `src/access_moppy/examples/batch_config.yml`
- **API Reference**: [Coming soon]

## Current Limitations

- **Alpha version**: Intended for evaluation only
- **Ocean variables**: Limited support in current release
- **Variable mapping**: Under review for CMIP6/CMIP7 compliance

## Support

- **Issues**: Submit via GitHub Issues
- **Questions**: Contact ACCESS-NRI support
- **Contributions**: Welcome via Pull Requests

## License

ACCESS-MOPPy is licensed under the Apache-2.0 License.
