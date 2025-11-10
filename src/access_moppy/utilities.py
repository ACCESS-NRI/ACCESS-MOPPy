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


class IncompatibleFrequencyError(ValueError):
    """Raised when input frequency cannot be resampled to target CMIP6 frequency."""
    pass


class ResamplingRequiredWarning(UserWarning):
    """Warning when input frequency requires temporal resampling/averaging."""
    pass


def parse_cmip6_table_frequency(compound_name: str) -> pd.Timedelta:
    """
    Parse CMIP6 table frequency from compound name.
    
    Args:
        compound_name: CMIP6 compound name (e.g., 'Amon.tas', '3hr.pr', 'day.tasmax')
        
    Returns:
        pandas Timedelta representing the target CMIP6 frequency
        
    Raises:
        ValueError: if compound name format is invalid or frequency not recognized
    """
    try:
        table_id, _ = compound_name.split(".")
    except ValueError:
        raise ValueError(f"Invalid compound name format: {compound_name}. Expected 'table.variable'")
    
    # Map CMIP6 table IDs to their frequencies
    frequency_mapping = {
        # Common atmospheric tables
        "Amon": pd.Timedelta(days=30),      # Monthly (approximate)
        "Aday": pd.Timedelta(days=1),       # Daily
        "A3hr": pd.Timedelta(hours=3),      # 3-hourly
        "A6hr": pd.Timedelta(hours=6),      # 6-hourly
        "AsubhR": pd.Timedelta(minutes=30), # Sub-hourly
        
        # Ocean tables
        "Omon": pd.Timedelta(days=30),      # Monthly ocean
        "Oday": pd.Timedelta(days=1),       # Daily ocean
        "Oyr": pd.Timedelta(days=365),      # Yearly ocean
        
        # Land tables
        "Lmon": pd.Timedelta(days=30),      # Monthly land
        "Lday": pd.Timedelta(days=1),       # Daily land
        
        # Sea ice tables
        "SImon": pd.Timedelta(days=30),     # Monthly sea ice
        "SIday": pd.Timedelta(days=1),      # Daily sea ice
        
        # Additional frequency tables
        "3hr": pd.Timedelta(hours=3),
        "6hr": pd.Timedelta(hours=6),
        "day": pd.Timedelta(days=1),
        "mon": pd.Timedelta(days=30),
        "yr": pd.Timedelta(days=365),
        
        # CF standard tables
        "CFday": pd.Timedelta(days=1),
        "CFmon": pd.Timedelta(days=30),
        "CF3hr": pd.Timedelta(hours=3),
        "CFsubhr": pd.Timedelta(minutes=30),
        
        # Specialized tables
        "6hrLev": pd.Timedelta(hours=6),
        "6hrPlev": pd.Timedelta(hours=6),
        "6hrPlevPt": pd.Timedelta(hours=6),
    }
    
    if table_id not in frequency_mapping:
        raise ValueError(f"Unknown CMIP6 table ID: {table_id}. Cannot determine target frequency.")
    
    return frequency_mapping[table_id]


def is_frequency_compatible(input_freq: pd.Timedelta, target_freq: pd.Timedelta) -> tuple[bool, str]:
    """
    Check if input frequency is compatible with target CMIP6 frequency.
    
    Compatible means the input frequency is higher (more frequent) than or equal to
    the target frequency, allowing for temporal averaging/resampling.
    
    Args:
        input_freq: Detected frequency of input files
        target_freq: Target CMIP6 frequency from table
        
    Returns:
        tuple of (is_compatible: bool, reason: str)
    """
    input_seconds = input_freq.total_seconds()
    target_seconds = target_freq.total_seconds()
    
    # Allow some tolerance for floating point comparison (1% tolerance)
    tolerance = 0.01
    
    if abs(input_seconds - target_seconds) / target_seconds < tolerance:
        return True, "Frequencies match exactly"
    elif input_seconds < target_seconds:
        # Input is more frequent (higher resolution) - can be averaged down
        ratio = target_seconds / input_seconds
        if ratio == int(ratio):  # Clean integer ratio
            return True, f"Input frequency ({input_freq}) can be averaged to target frequency ({target_freq}) with ratio 1:{int(ratio)}"
        else:
            return True, f"Input frequency ({input_freq}) can be resampled to target frequency ({target_freq}) with ratio 1:{ratio:.2f}"
    else:
        # Input is less frequent (lower resolution) - cannot be upsampled meaningfully
        return False, f"Input frequency ({input_freq}) is lower than target frequency ({target_freq}). Cannot upsample temporal data meaningfully."


def validate_cmip6_frequency_compatibility(
    file_paths: Union[str, List[str]], 
    compound_name: str,
    time_coord: str = "time",
    tolerance_seconds: float = 3600.0,
    interactive: bool = True
) -> tuple[pd.Timedelta, bool]:
    """
    Validate that input files have compatible frequency with CMIP6 target frequency.
    
    This function:
    1. Validates frequency consistency across input files
    2. Parses target frequency from CMIP6 compound name
    3. Checks compatibility and determines if resampling is needed
    4. Optionally prompts user for confirmation when resampling is required
    
    Args:
        file_paths: Path or list of paths to NetCDF files
        compound_name: CMIP6 compound name (e.g., 'Amon.tas')
        time_coord: name of the time coordinate (default: "time")
        tolerance_seconds: tolerance for frequency differences in seconds
        interactive: whether to prompt user when resampling is needed
        
    Returns:
        tuple of (detected_frequency, resampling_required)
        
    Raises:
        FrequencyMismatchError: if files have inconsistent frequencies
        IncompatibleFrequencyError: if input frequency cannot be resampled to target
        ValueError: if compound name is invalid
    """
    # First validate consistency across input files
    detected_freq = validate_consistent_frequency(file_paths, time_coord, tolerance_seconds)
    
    # Parse target frequency from compound name
    try:
        target_freq = parse_cmip6_table_frequency(compound_name)
    except ValueError as e:
        raise ValueError(f"Cannot determine target frequency from compound name '{compound_name}': {e}")
    
    # Check compatibility
    is_compatible, reason = is_frequency_compatible(detected_freq, target_freq)
    
    if not is_compatible:
        raise IncompatibleFrequencyError(
            f"Input files have incompatible temporal frequency for CMIP6 table.\n"
            f"Compound name: {compound_name}\n"
            f"Target frequency: {target_freq}\n" 
            f"Input frequency: {detected_freq}\n"
            f"Reason: {reason}\n\n"
            f"CMIP6 tables require input data with frequency higher than or equal to the target frequency "
            f"to allow proper temporal averaging. You cannot upsample from lower frequency data."
        )
    
    # Determine if resampling is required
    input_seconds = detected_freq.total_seconds() 
    target_seconds = target_freq.total_seconds()
    resampling_required = abs(input_seconds - target_seconds) / target_seconds > 0.01
    
    if resampling_required:
        message = (
            f"⚠️  TEMPORAL RESAMPLING REQUIRED ⚠️\n\n"
            f"CMIP6 table: {compound_name}\n"
            f"Target frequency: {target_freq}\n"
            f"Input frequency: {detected_freq}\n"
            f"Compatibility: {reason}\n\n"
            f"Your input files will be temporally averaged/resampled during CMORisation.\n"
            f"This is a common and valid operation for CMIP6 data preparation.\n"
        )
        
        if interactive:
            print(message)
            response = input("Do you want to continue with temporal resampling? [y/N]: ").strip().lower()
            if response not in ['y', 'yes']:
                raise InterruptedError(
                    "CMORisation aborted by user due to temporal resampling requirement. "
                    "To proceed non-interactively, set interactive=False or validate_frequency=False."
                )
            print("✓ Proceeding with temporal resampling...")
        else:
            # Non-interactive mode - just warn
            warnings.warn(message, ResamplingRequiredWarning, stacklevel=2)
    
    return detected_freq, resampling_required


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
