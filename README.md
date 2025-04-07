# ACCESS-MOPPeR v2.0.0a (Alpha Version)

## Overview
ACCESS-MOPPeR v2.0.0a is a CMORisation tool designed to post-process ACCESS model output. This version represents a significant rewrite of the original MOPPeR, focusing on usability rather than raw performance. It introduces a more flexible and user-friendly Python API that can be integrated into Jupyter notebooks and other workflows.

ACCESS-MOPPeR allows for targeted CMORisation of individual variables and is specifically designed to support the ACCESS-ESM1.6 configuration prepared for CMIP7 FastTrack. However, ocean variable support remains limited in this alpha release.

## Key Features
- **Improved Usability**: Designed for ease of use over maximum performance.
- **Python API**: Enables seamless integration into Python-based workflows, including Jupyter notebooks.
- **Flexible CMORisation**: Supports targeted CMORisation of specific variables.
- **ACCESS-ESM1.6 Support**: Tailored for CMIP7 FastTrack simulations.
- **Cross-Platform Compatibility**: Can be run from any computing platform, not limited to NCI Gadi.
- **Custom Mode Support**: Users can define their own standards beyond CMIP6 compliance.
- **Latest CMOR Version**: Uses the most recent version of CMOR (Climate Model Output Rewriter).

## Current Limitations
- **Alpha Version**: Intended for evaluation purposes only; not recommended for data publication.
- **Limited Ocean Variable Support**: Further development is needed to fully support ocean-related variables.
- **Single-CPU Execution**: Multi-threading and distributed computing optimizations are planned for a future release.

## Background
ACCESS-MOPPeR builds upon the original APP4 and MOPPeR frameworks, which were initially developed for CMIP5 and later extended for CMIP6. These tools leveraged CMOR3 and CMIP6 data request files to produce CF-compliant datasets aligned with ESGF standards. MOPPeR introduced the **mopdb** tool, allowing users to create custom mappings and CMOR table definitions.

This rewrite retains key features of the original MOPPeR while enhancing usability. The differentiation between "custom" and "cmip" modes remains, but both modes now follow a unified workflow defined in a single configuration file.

## Usage
ACCESS-MOPPeR v2.0.0a is best suited for users interested in evaluating outputs from ACCESS-ESM1.6 development releases. Full documentation is not available yet.
Please refer to the [Getting Started Notebook](https://github.com/ACCESS-NRI/ACCESS-MOPPeR/blob/v2/notebooks/Getting_started.ipynb): 

## Future Development
- **Optimized Multi-CPU Execution**: Parallel processing support will be introduced in later versions.
- **Enhanced Ocean Variable Support**: Expansion of CMORisation capabilities for ocean-related data.
- **Expanded CMORisation Standards**: Continued flexibility in defining custom post-processing standards beyond CMIP6.

## Disclaimer
This is an **alpha release** and should not be used for official data publications. Users should expect potential changes in future versions that may affect workflow compatibility.

For feedback or issues, please contribute via the project's repository or contact the development team.
