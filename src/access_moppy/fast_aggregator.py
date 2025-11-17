"""
Fast NetCDF aggregation for direct variable mappings.

This module provides a lightweight alternative to xarray for cases where
variables just need to be renamed, concatenated, and have metadata updated
without complex computations or transformations.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import netCDF4 as nc
import numpy as np
from cftime import num2date


class FastNetCDFAggregator:
    """
    Lightweight NetCDF aggregator that bypasses xarray for direct variable mappings.
    
    Optimized for cases where:
    - Variables are renamed 1:1 (no calculations)
    - Files are concatenated along time dimension
    - Only metadata needs updating
    - No complex coordinate transformations needed
    """
    
    def __init__(
        self,
        input_paths: List[str],
        output_path: str,
        variable_mapping: Dict[str, Any],
        cmor_name: str,
        cmip6_vocab: Any,
        chunk_size: int = 1000,  # Time steps per chunk
    ):
        self.input_paths = input_paths
        self.output_path = output_path
        self.mapping = variable_mapping[cmor_name]
        self.cmor_name = cmor_name
        self.vocab = cmip6_vocab
        self.chunk_size = chunk_size
        
        # Get source variable name
        self.source_var = self.mapping["model_variables"][0]
        
        # Prepare dimension mappings
        self.dim_mapping = self.mapping["dimensions"]
        self.reverse_dim_mapping = {v: k for k, v in self.dim_mapping.items()}
        
    def _validate_direct_mapping(self) -> bool:
        """Check if this mapping is suitable for fast aggregation."""
        calc = self.mapping["calculation"]
        
        # Must be direct type
        if calc["type"] != "direct":
            return False
            
        # Must have exactly one source variable
        if len(self.mapping["model_variables"]) != 1:
            return False
            
        # Check if any complex bounds handling is needed
        # For now, skip if bounds are required
        for dim, v in self.vocab.axes.items():
            if v.get("must_have_bounds") == "yes":
                input_dim = None
                for k, val in self.dim_mapping.items():
                    if val == v["out_name"]:
                        input_dim = k
                        break
                if input_dim and f"{input_dim}_bnds" in self._get_first_file_vars():
                    return False  # Has bounds - use xarray
                    
        return True
        
    def _get_first_file_vars(self) -> List[str]:
        """Get variable names from first input file."""
        with nc.Dataset(self.input_paths[0], 'r') as ds:
            return list(ds.variables.keys())
            
    def _get_file_info(self, filepath: str) -> Tuple[int, Dict]:
        """Get time dimension size and global attributes from a file."""
        with nc.Dataset(filepath, 'r') as ds:
            time_size = ds.dimensions['time'].size if 'time' in ds.dimensions else 0
            attrs = {k: getattr(ds, k) for k in ds.ncattrs()}
            
            # Also get time variable info for bounds calculation
            time_info = {}
            if 'time' in ds.variables:
                time_var = ds.variables['time']
                time_info = {
                    'units': getattr(time_var, 'units', ''),
                    'calendar': getattr(time_var, 'calendar', 'standard'),
                    'values': time_var[:].copy()  # Get time values
                }
                
        return time_size, attrs, time_info
        
    def _calculate_output_dimensions(self) -> Dict[str, int]:
        """Calculate dimensions for output file."""
        dims = {}
        total_time = 0
        
        # Get dimensions from first file and sum time dimension
        with nc.Dataset(self.input_paths[0], 'r') as ds:
            for dim_name, dim in ds.dimensions.items():
                if dim_name == 'time':
                    # Calculate total time across all files
                    for filepath in self.input_paths:
                        time_size, _, _ = self._get_file_info(filepath)
                        total_time += time_size
                    dims['time'] = total_time
                else:
                    dims[dim_name] = dim.size
                    
        return dims
        
    def _copy_variable_attributes(self, src_var: nc.Variable, dst_var: nc.Variable):
        """Copy attributes from source to destination variable."""
        for attr in src_var.ncattrs():
            if attr != '_FillValue':  # Skip fill value as it's set during creation
                setattr(dst_var, attr, getattr(src_var, attr))
                
    def _rename_dimensions_in_attrs(self, attrs: Dict, var_name: str) -> Dict:
        """Rename dimensions in variable attributes like bounds."""
        updated_attrs = attrs.copy()
        
        # Handle bounds attribute
        if 'bounds' in updated_attrs:
            bounds_name = updated_attrs['bounds']
            # Check if bounds variable needs renaming
            for old_dim, new_dim in self.dim_mapping.items():
                if bounds_name.startswith(old_dim):
                    updated_attrs['bounds'] = bounds_name.replace(old_dim, new_dim)
                    break
                    
        return updated_attrs
        
    def aggregate(self) -> str:
        """
        Perform fast aggregation and return output file path.
        
        Returns:
            str: Path to the output file
        """
        if not self._validate_direct_mapping():
            raise ValueError(
                f"Mapping for {self.cmor_name} is not suitable for fast aggregation. "
                "Use xarray-based CMORiser instead."
            )
            
        # Calculate output dimensions
        output_dims = self._calculate_output_dimensions()
        
        # Calculate time range for filename
        time_range = self._calculate_time_range()
        
        # Generate CMOR-compliant output path
        output_path = self._generate_cmor_filename(time_range)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Pre-calculate memory requirements and optimize chunk size
        total_time_steps = output_dims.get('time', 1)
        self._optimize_chunk_size(total_time_steps)
        
        with nc.Dataset(output_path, 'w', format='NETCDF4') as out_ds:
            # Create dimensions (rename according to mapping)
            for dim_name, size in output_dims.items():
                out_dim_name = self.dim_mapping.get(dim_name, dim_name)
                if out_dim_name == 'time':
                    out_ds.createDimension(out_dim_name, None)  # Unlimited
                else:
                    out_ds.createDimension(out_dim_name, size)
                    
            # Process each input file and aggregate
            time_offset = 0
            total_files = len(self.input_paths)
            
            for i, filepath in enumerate(self.input_paths):
                progress = (i + 1) / total_files * 100
                print(f"Processing file {i+1}/{total_files} ({progress:.1f}%): {Path(filepath).name}")
                
                with nc.Dataset(filepath, 'r') as in_ds:
                    # Copy global attributes from first file
                    if i == 0:
                        for attr in in_ds.ncattrs():
                            setattr(out_ds, attr, getattr(in_ds, attr))
                            
                    # Get file time info
                    time_size, _, time_info = self._get_file_info(filepath)
                    
                    # Create variables on first file
                    if i == 0:
                        self._create_output_variables(in_ds, out_ds)
                            
                    # Copy time data efficiently
                    if 'time' in in_ds.variables:
                        self._copy_time_data(in_ds, out_ds, time_offset, time_size)
                        
                    # Copy data variable in optimized chunks
                    self._copy_variable_data(in_ds, out_ds, time_offset, time_size)
                        
                    time_offset += time_size
                    
            # Update global attributes with CMOR requirements
            self._update_cmor_attributes(out_ds)
            
        print(f"Fast aggregation complete: {output_path}")
        return str(output_path)
        
    def _optimize_chunk_size(self, total_time_steps: int):
        """Optimize chunk size based on total data size and available memory."""
        # Estimate memory usage and adjust chunk size accordingly
        # This is a simple heuristic - could be made more sophisticated
        
        if total_time_steps > 10000:
            # For very large datasets, use smaller chunks
            self.chunk_size = min(self.chunk_size, 500)
        elif total_time_steps < 100:
            # For small datasets, larger chunks are fine
            self.chunk_size = min(self.chunk_size, total_time_steps)
            
        print(f"Optimized chunk size: {self.chunk_size} time steps")
        
    def _create_output_variables(self, in_ds: nc.Dataset, out_ds: nc.Dataset):
        """Create all output variables and copy their attributes."""
        # Create coordinate variables first
        for var_name in in_ds.variables:
            if var_name in in_ds.dimensions:  # Coordinate variable
                in_var = in_ds.variables[var_name]
                out_var_name = self.dim_mapping.get(var_name, var_name)
                
                # Create dimension-renamed coordinate
                out_var = out_ds.createVariable(
                    out_var_name,
                    in_var.dtype,
                    tuple(self.dim_mapping.get(d, d) for d in in_var.dimensions),
                    fill_value=getattr(in_var, '_FillValue', None)
                )
                self._copy_variable_attributes(in_var, out_var)
                
                # Copy coordinate data (except time, which we'll aggregate)
                if var_name != 'time':
                    out_var[:] = in_var[:]
                    
        # Create main data variable
        in_var = in_ds.variables[self.source_var]
        out_var = out_ds.createVariable(
            self.cmor_name,
            in_var.dtype,
            tuple(self.dim_mapping.get(d, d) for d in in_var.dimensions),
            fill_value=getattr(in_var, '_FillValue', None),
            # Enable compression for better I/O performance
            zlib=True,
            complevel=1,  # Light compression for speed
            shuffle=True,
        )
        
        # Copy and update attributes
        attrs = {attr: getattr(in_var, attr) for attr in in_var.ncattrs() if attr != '_FillValue'}
        attrs = self._rename_dimensions_in_attrs(attrs, self.cmor_name)
        
        for attr, val in attrs.items():
            setattr(out_var, attr, val)
            
    def _copy_time_data(self, in_ds: nc.Dataset, out_ds: nc.Dataset, time_offset: int, time_size: int):
        """Copy time coordinate data efficiently."""
        time_var = in_ds.variables['time']
        out_time_name = self.dim_mapping.get('time', 'time')
        out_time = out_ds.variables[out_time_name]
        
        # Copy time slice
        out_time[time_offset:time_offset + time_size] = time_var[:]
        
    def _copy_variable_data(self, in_ds: nc.Dataset, out_ds: nc.Dataset, time_offset: int, time_size: int):
        """Copy main variable data in optimized chunks."""
        in_var = in_ds.variables[self.source_var]
        out_var = out_ds.variables[self.cmor_name]
        
        # Find time dimension index
        time_dim_idx = None
        for idx, dim in enumerate(in_var.dimensions):
            if dim == 'time':
                time_dim_idx = idx
                break
                
        if time_dim_idx is not None:
            # Copy in chunks along time dimension with progress updates
            chunks_processed = 0
            total_chunks = (time_size + self.chunk_size - 1) // self.chunk_size
            
            for chunk_start in range(0, time_size, self.chunk_size):
                chunk_end = min(chunk_start + self.chunk_size, time_size)
                
                # Create slice objects
                in_slice = [slice(None)] * len(in_var.dimensions)
                in_slice[time_dim_idx] = slice(chunk_start, chunk_end)
                in_slice = tuple(in_slice)
                
                out_slice = [slice(None)] * len(out_var.dimensions)
                out_slice[time_dim_idx] = slice(
                    time_offset + chunk_start, 
                    time_offset + chunk_end
                )
                out_slice = tuple(out_slice)
                
                # Copy chunk
                out_var[out_slice] = in_var[in_slice]
                
                chunks_processed += 1
                if total_chunks > 1:  # Only show progress for multi-chunk operations
                    chunk_progress = chunks_processed / total_chunks * 100
                    print(f"  Chunk {chunks_processed}/{total_chunks} ({chunk_progress:.1f}%)")
        else:
            # No time dimension - copy entire variable
            out_var[:] = in_var[:]
        
    def _calculate_time_range(self) -> str:
        """Calculate time range string for filename from input files."""
        try:
            # Get time info from first and last files
            with nc.Dataset(self.input_paths[0], 'r') as first_ds:
                if 'time' not in first_ds.variables:
                    return "unknown"
                    
                first_time_var = first_ds.variables['time']
                units = getattr(first_time_var, 'units', '')
                calendar = getattr(first_time_var, 'calendar', 'standard')
                
                if len(first_time_var) > 0:
                    first_time = first_time_var[0]
                else:
                    return "unknown"
            
            with nc.Dataset(self.input_paths[-1], 'r') as last_ds:
                if 'time' not in last_ds.variables:
                    return "unknown"
                    
                last_time_var = last_ds.variables['time']
                if len(last_time_var) > 0:
                    last_time = last_time_var[-1]
                else:
                    return "unknown"
            
            # Convert to datetime objects
            first_date = num2date(first_time, units=units, calendar=calendar.lower())
            last_date = num2date(last_time, units=units, calendar=calendar.lower())
            
            # Format as YYYYMM-YYYYMM
            start_str = f"{first_date.year:04d}{first_date.month:02d}"
            end_str = f"{last_date.year:04d}{last_date.month:02d}"
            
            return f"{start_str}-{end_str}"
            
        except Exception as e:
            print(f"Warning: Could not determine time range: {e}")
            return "unknown"
            
    def _generate_cmor_filename(self, time_range: str) -> Path:
        """Generate CMOR-compliant filename."""
        # Get required attributes from vocabulary
        table_id = getattr(self.vocab, 'table_id', 'unknown')
        
        # Default attributes (these should ideally come from configuration)
        source_id = 'ACCESS-ESM1-6'  # Should be configurable
        experiment_id = 'historical'  # Should be configurable 
        variant_label = 'r1i1p1f1'   # Should be configurable
        grid_label = 'gn'            # Should be configurable
        
        # Generate filename: variable_table_source_experiment_variant_grid_timerange.nc
        filename = (f"{self.cmor_name}_{table_id}_{source_id}_{experiment_id}_"
                   f"{variant_label}_{grid_label}_{time_range}.nc")
        
        # Use provided output path or generate from base directory
        if isinstance(self.output_path, str) and self.output_path.endswith('.nc'):
            # Output path is a specific file
            output_path = Path(self.output_path)
            # Replace filename with CMOR-compliant one
            output_path = output_path.parent / filename
        else:
            # Output path is a directory
            output_path = Path(self.output_path) / filename
            
        return output_path

    def _update_cmor_attributes(self, ds: nc.Dataset):
        """Update dataset with CMOR-required global attributes."""
        # Add CMOR-specific attributes based on vocabulary
        variable_info = self.vocab.variable
        
        # Update variable_id to CMOR name
        ds.setncattr('variable_id', self.cmor_name)
        
        # Add table_id if available in vocab
        if hasattr(self.vocab, 'table_id'):
            ds.setncattr('table_id', self.vocab.table_id)
            
        # Add other standard CMOR attributes
        current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        ds.setncattr('creation_date', current_time)
        
        # Add default attributes (these should come from configuration in a real implementation)
        ds.setncattr('source_id', 'ACCESS-ESM1-6')
        ds.setncattr('experiment_id', 'historical') 
        ds.setncattr('variant_label', 'r1i1p1f1')
        ds.setncattr('grid_label', 'gn')
        ds.setncattr('activity_id', 'CMIP')
        ds.setncattr('institution_id', 'CSIRO-ARCCSS')
        
        # Add mip_era
        ds.setncattr('mip_era', 'CMIP6')


def can_use_fast_aggregation(variable_mapping: Dict[str, Any], cmor_name: str) -> bool:
    """
    Determine if a variable mapping can use fast aggregation.
    
    Args:
        variable_mapping: The variable mapping configuration
        cmor_name: The CMOR variable name
        
    Returns:
        bool: True if fast aggregation is suitable
    """
    if cmor_name not in variable_mapping:
        return False
        
    mapping = variable_mapping[cmor_name]
    calc = mapping["calculation"]
    
    # Must be direct type
    if calc["type"] != "direct":
        return False
        
    # Must have exactly one source variable
    if len(mapping["model_variables"]) != 1:
        return False
        
    # Check for complex dimension operations
    # If any dimension mapping involves coordinate transformations beyond simple renaming,
    # we should use xarray
    dimensions = mapping.get("dimensions", {})
    
    # For now, allow simple dimension renaming
    # Future: could add checks for coordinate transformations
    
    return True