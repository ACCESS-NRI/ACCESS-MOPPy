import json
from importlib.resources import as_file, files
from typing import Dict, List, Optional, Union
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import xarray as xr
from cftime import num2date

type_mapping = {
    "real": np.float32,
    "double": np.float64,
    "float": np.float32,
    "int": np.int32,
    "short": np.int16,
    "byte": np.int8,
}


def load_model_mappings(compound_name: str, model_id: str = None) -> Dict:
    """
    Load Mappings for ACCESS models.

    Args:
        compound_name: CMIP6 compound name (e.g., 'Amon.tas')
        model_id: Model identifier. If None, defaults to 'ACCESS-ESM1.6'.

    Returns:
        Dictionary containing variable mappings for the requested compound name.
    """
    _, cmor_name = compound_name.split(".")
    mapping_dir = files("access_moppy.mappings")

    # Default to ACCESS-ESM1.6 if no model_id provided
    if model_id is None:
        model_id = "ACCESS-ESM1.6"

    # Load model-specific consolidated mapping
    model_file = f"{model_id}_mappings.json"

    for entry in mapping_dir.iterdir():
        if entry.name == model_file:
            with as_file(entry) as path:
                with open(path, "r", encoding="utf-8") as f:
                    all_mappings = json.load(f)

                    # Search in component-organized structure
                    for component in ["atmosphere", "land", "ocean", "time_invariant"]:
                        if (
                            component in all_mappings
                            and cmor_name in all_mappings[component]
                        ):
                            return {cmor_name: all_mappings[component][cmor_name]}

                    # Fallback: search in flat "variables" structure (for backward compatibility)
                    variables = all_mappings.get("variables", {})
                    if cmor_name in variables:
                        return {cmor_name: variables[cmor_name]}

    # If model file not found or variable not found, return empty dict
    return {}


class FrequencyMismatchError(ValueError):
    """Raised when input files have inconsistent temporal frequencies."""
    pass


def detect_time_frequency_lazy(
    ds: xr.Dataset, time_coord: str = "time"
) -> Optional[pd.Timedelta]:
    """
    Detect the temporal frequency of a dataset using only the first few time points.
    
    This function works lazily by only loading a small sample of time coordinates
    to infer the frequency without loading the entire time dimension into memory.
    
    Args:
        ds: xarray Dataset with temporal coordinate
        time_coord: name of the time coordinate (default: "time")
        
    Returns:
        pandas Timedelta representing the detected frequency, or None if cannot detect
        
    Raises:
        ValueError: if time coordinate is missing or has insufficient data
    """
    if time_coord not in ds.coords:
        raise ValueError(f"Time coordinate '{time_coord}' not found in dataset")
    
    time_var = ds[time_coord]
    
    # Check if we have at least 2 time points
    if time_var.size < 2:
        raise ValueError(f"Need at least 2 time points to detect frequency, got {time_var.size}")
    
    # Sample first few time points (max 10 to keep it lightweight)
    n_sample = min(10, time_var.size)
    
    # Load only the sample time points - this is the key to keeping it lazy
    time_sample = time_var.isel({time_coord: slice(0, n_sample)}).compute()
    
    # Convert to pandas datetime for easier frequency detection
    try:
        # Handle different time formats
        units = time_var.attrs.get("units")
        calendar = time_var.attrs.get("calendar", "standard")
        
        if units and "since" in units:
            # Convert from numeric time to datetime
            dates = num2date(
                time_sample.values, 
                units=units, 
                calendar=calendar,
                only_use_cftime_datetimes=False
            )
            # Convert to pandas datetime if possible for better frequency inference
            if hasattr(dates[0], 'strftime'):  # Standard datetime
                time_index = pd.to_datetime([d.strftime('%Y-%m-%d %H:%M:%S') for d in dates])
            else:  # cftime datetime
                # For cftime objects, use a more manual approach
                time_diffs = []
                for i in range(1, len(dates)):
                    diff = dates[i] - dates[i-1]
                    # Convert to total seconds
                    total_seconds = diff.days * 86400 + diff.seconds
                    time_diffs.append(total_seconds)
                
                if time_diffs:
                    avg_seconds = np.mean(time_diffs)
                    return pd.Timedelta(seconds=avg_seconds)
                return None
        else:
            # Assume already in datetime format
            time_index = pd.to_datetime(time_sample.values)
            
        # Infer frequency from pandas
        if len(time_index) >= 2:
            freq = pd.infer_freq(time_index)
            if freq:
                return pd.Timedelta(pd.tseries.frequencies.to_offset(freq).delta)
            else:
                # Manual frequency calculation if pandas can't infer
                time_diffs = time_index[1:] - time_index[:-1]
                # Use the most common difference as the frequency
                unique_diffs, counts = np.unique(time_diffs, return_counts=True)
                most_common_diff = unique_diffs[np.argmax(counts)]
                return most_common_diff
                
    except Exception as e:
        warnings.warn(f"Could not detect frequency from time coordinate: {e}")
        return None
        
    return None


def validate_consistent_frequency(
    file_paths: Union[str, List[str]], 
    time_coord: str = "time",
    tolerance_seconds: float = 3600.0  # 1 hour tolerance by default
) -> pd.Timedelta:
    """
    Validate that all input files have consistent temporal frequency.
    
    This function opens each file lazily and detects the frequency using only
    a small sample of time coordinates, ensuring good performance even with
    many large files.
    
    Args:
        file_paths: Path or list of paths to NetCDF files
        time_coord: name of the time coordinate (default: "time")
        tolerance_seconds: tolerance for frequency differences in seconds
        
    Returns:
        pandas Timedelta of the validated consistent frequency
        
    Raises:
        FrequencyMismatchError: if files have inconsistent frequencies
        ValueError: if no files provided or frequency cannot be detected
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    
    if not file_paths:
        raise ValueError("No file paths provided")
    
    frequencies = []
    file_info = []
    
    for file_path in file_paths:
        try:
            # Open file lazily - no data is loaded into memory here
            with xr.open_dataset(file_path, decode_cf=False, chunks={}) as ds:
                freq = detect_time_frequency_lazy(ds, time_coord)
                if freq is not None:
                    frequencies.append(freq)
                    file_info.append((file_path, freq))
                else:
                    warnings.warn(f"Could not detect frequency for file: {file_path}")
                    
        except Exception as e:
            warnings.warn(f"Error processing file {file_path}: {e}")
            continue
    
    if not frequencies:
        raise ValueError("Could not detect frequency from any input files")
    
    # Check consistency
    base_freq = frequencies[0]
    base_seconds = base_freq.total_seconds()
    
    inconsistent_files = []
    
    for file_path, freq in file_info[1:]:  # Skip first file
        freq_seconds = freq.total_seconds()
        diff_seconds = abs(freq_seconds - base_seconds)
        
        if diff_seconds > tolerance_seconds:
            inconsistent_files.append({
                'file': file_path,
                'frequency': freq,
                'expected': base_freq,
                'difference_seconds': diff_seconds
            })
    
    if inconsistent_files:
        error_msg = f"Inconsistent temporal frequencies detected:\n"
        error_msg += f"Expected frequency: {base_freq}\n"
        error_msg += f"Reference file: {file_info[0][0]}\n\n"
        error_msg += "Inconsistent files:\n"
        for info in inconsistent_files:
            error_msg += f"  {info['file']}: {info['frequency']} (diff: {info['difference_seconds']:.1f}s)\n"
        
        raise FrequencyMismatchError(error_msg)
    
    return base_freq
