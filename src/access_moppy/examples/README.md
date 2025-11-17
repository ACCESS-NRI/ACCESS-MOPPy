# ACCESS-MOPPy Examples

This directory contains examples and configuration files for ACCESS-MOPPy.

## Files

- **`batch_config.yml`** - Example batch processing configuration
- **`show_config.py`** - Script to display current configuration
- **`demo_transparent_backend.py`** - Demonstrates the transparent NetCDF4 backend integration

## NetCDF4 Backend Usage

The NetCDF4 backend provides significant performance improvements for direct variable mappings:

### Using ACCESS_ESM_CMORiser (Recommended)

```python
from access_moppy import ACCESS_ESM_CMORiser

# Default behavior (same as before)
cmoriser = ACCESS_ESM_CMORiser(
    input_paths=files,
    compound_name="Amon.tas",
    experiment_id="historical",
    source_id="ACCESS-ESM1-6", 
    variant_label="r1i1p1f1",
    grid_label="gn"
)

# With NetCDF4 backend for performance boost
fast_cmoriser = ACCESS_ESM_CMORiser(
    input_paths=files,
    compound_name="Amon.tas",
    experiment_id="historical",
    source_id="ACCESS-ESM1-6",
    variant_label="r1i1p1f1", 
    grid_label="gn",
    backend="netcdf4"  # 2-10x faster for direct mappings
)
```

### Using Component-Specific CMORisers

```python
from access_moppy.atmosphere import CMIP6_Atmosphere_CMORiser

# Default behavior (same as before)
cmoriser = CMIP6_Atmosphere_CMORiser(
    input_paths=files,
    output_path="output.nc",
    cmip6_vocab=vocab,
    variable_mapping=mappings,
    compound_name="Amon.tas"
)

# With NetCDF4 backend for performance boost
fast_cmoriser = CMIP6_Atmosphere_CMORiser(
    input_paths=files,
    output_path="output.nc", 
    cmip6_vocab=vocab,
    variable_mapping=mappings,
    compound_name="Amon.tas",
    backend="netcdf4"  # 2-10x faster for direct mappings
)
```

The system automatically validates suitability and falls back to xarray for complex cases.