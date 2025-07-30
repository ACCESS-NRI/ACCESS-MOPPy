# ACCESS Model Output Post-Processor (ACCESS-MOPPeR) v2.1.0a (Alpha Version)

## Overview
ACCESS-MOPPeR v2.1.0a is a CMORisation tool designed to post-process ACCESS model output. This version represents a significant rewrite of the original MOPPeR, focusing on usability rather than raw performance. It introduces a more flexible and user-friendly Python API that can be integrated into Jupyter notebooks and other workflows.

ACCESS-MOPPeR allows for targeted CMORisation of individual variables and is specifically designed to support the ACCESS-ESM1.6 configuration prepared for CMIP7 FastTrack. However, ocean variable support remains limited in this alpha release.

## Key Features
- **Improved Usability**: Designed for ease of use over maximum performance.
- **Python API**: Enables seamless integration into Python-based workflows, including Jupyter notebooks.
- **Flexible CMORisation**: Supports targeted CMORisation of specific variables.
- **ACCESS-ESM1.6 Support**: Tailored for CMIP7 FastTrack simulations.
- **Cross-Platform Compatibility**: Can be run from any computing platform, not limited to NCI Gadi.
- **Dask Enabled**

## Current Limitations
- **Alpha Version**: Intended for evaluation purposes only; not recommended for data publication.

> **⚠️ Variable Mapping Under Review**
>
> We are currently reviewing the mapping of ACCESS variables to their CMIP6 and CMIP7 equivalents. Some variables that require derivation may not be available yet, or their calculation may need further verification.
> **If you notice any major issues or missing variables, please submit an issue!**


## Background
ACCESS-MOPPeR v2 is a complete rewrite of the original APP4 and MOPPeR frameworks. Unlike previous versions, it does **not** depend on CMOR; instead, it leverages modern Python libraries such as **xarray** and **dask** for efficient processing of NETCDF files. This approach streamlines the workflow, improves flexibility, and enhances integration with contemporary data science tools.

While retaining the core concepts of "custom" and "cmip" modes, ACCESS-MOPPeR v2 unifies these workflows within a single configuration file, focusing on usability and extensibility for current and future CMIP projects.

---

## Installation


```sh
pip install numpy pandas xarray netCDF4 cftime dask pyyaml tqdm requests
pip install .
```

---

## Documentation

See the [Getting Started notebook](notebooks/Getting_started.ipynb) and the [docs](docs/) folder for detailed usage and API documentation.

---

## Testing

To run tests:

```sh
pytest
```

---

## License

ACCESS-MOPPeR is licensed under the Apache-2.0 License.

---

## Contact

Author: Romain Beucher
