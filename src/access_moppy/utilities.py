import json
import warnings
from importlib.resources import as_file, files
from typing import Dict, List, Optional, Union

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
        table_id, variable = compound_name.split(".")
    except ValueError:
        raise ValueError(
            f"Invalid compound name format: {compound_name}. Expected 'table.variable'"
        )

    # Validate that both table and variable are non-empty
    if not table_id or not variable:
        raise ValueError(
            f"Invalid compound name format: {compound_name}. Both table and variable must be non-empty."
        )

    # Map CMIP6 table IDs to their frequencies
    frequency_mapping = {
        # Common atmospheric tables
        "Amon": pd.Timedelta(days=30),  # Monthly (approximate)
        "Aday": pd.Timedelta(days=1),  # Daily
        "A3hr": pd.Timedelta(hours=3),  # 3-hourly
        "A6hr": pd.Timedelta(hours=6),  # 6-hourly
        "AsubhR": pd.Timedelta(minutes=30),  # Sub-hourly
        # Ocean tables
        "Omon": pd.Timedelta(days=30),  # Monthly ocean
        "Oday": pd.Timedelta(days=1),  # Daily ocean
        "Oyr": pd.Timedelta(days=365),  # Yearly ocean
        # Land tables
        "Lmon": pd.Timedelta(days=30),  # Monthly land
        "Lday": pd.Timedelta(days=1),  # Daily land
        # Sea ice tables
        "SImon": pd.Timedelta(days=30),  # Monthly sea ice
        "SIday": pd.Timedelta(days=1),  # Daily sea ice
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
        raise ValueError(
            f"Unknown CMIP6 table ID: {table_id}. Cannot determine target frequency."
        )

    return frequency_mapping[table_id]


def is_frequency_compatible(
    input_freq: pd.Timedelta, target_freq: pd.Timedelta
) -> tuple[bool, str]:
    """
    Check if input frequency is compatible with target CMIP6 frequency.

    Compatible means the input frequency is higher (more frequent) than or equal to
    the target frequency, allowing for temporal averaging/resampling.

    Special handling for monthly data: recognizes that calendar months (28-31 days)
    are all compatible with CMIP6 monthly tables (typically 30 days).

    Args:
        input_freq: Detected frequency of input files
        target_freq: Target CMIP6 frequency from table

    Returns:
        tuple of (is_compatible: bool, reason: str)
    """
    input_seconds = input_freq.total_seconds()
    target_seconds = target_freq.total_seconds()

    # Check if both frequencies are in the monthly range (20-35 days)
    monthly_min = 20 * 86400  # 20 days in seconds
    monthly_max = 35 * 86400  # 35 days in seconds

    input_is_monthly = monthly_min <= input_seconds <= monthly_max
    target_is_monthly = monthly_min <= target_seconds <= monthly_max

    if input_is_monthly and target_is_monthly:
        # Both are monthly - calendar month variations are expected and compatible
        input_days = input_seconds / 86400
        target_days = target_seconds / 86400
        return (
            True,
            f"Both frequencies are monthly (input: {input_days:.0f} days, target: {target_days:.0f} days). Calendar month variations are compatible.",
        )

    # Standard frequency compatibility check for non-monthly data
    tolerance = 0.01  # 1% tolerance for floating point comparison

    if abs(input_seconds - target_seconds) / target_seconds < tolerance:
        return True, "Frequencies match exactly"
    elif input_seconds < target_seconds:
        # Input is more frequent (higher resolution) - can be averaged down
        ratio = target_seconds / input_seconds
        if ratio == int(ratio):  # Clean integer ratio
            return (
                True,
                f"Input frequency ({input_freq}) can be averaged to target frequency ({target_freq}) with ratio 1:{int(ratio)}",
            )
        else:
            return (
                True,
                f"Input frequency ({input_freq}) can be resampled to target frequency ({target_freq}) with ratio 1:{ratio:.2f}",
            )
    else:
        # Input is less frequent (lower resolution) - cannot be upsampled meaningfully
        return (
            False,
            f"Input frequency ({input_freq}) is lower than target frequency ({target_freq}). Cannot upsample temporal data meaningfully.",
        )


def _is_monthly_target(compound_name: str) -> bool:
    """Check if CMIP6 compound name represents monthly data."""
    table_id, _ = compound_name.split(".")
    monthly_tables = {"Amon", "Lmon", "Omon", "SImon", "CFmon", "mon"}
    return table_id in monthly_tables


def _detect_frequency_from_concatenated_files(
    file_paths: Union[str, List[str]],
    time_coord: str = "time",
    max_sample_files: int = 10,
) -> pd.Timedelta:
    """
    Efficiently detect frequency using xarray concatenation approach.

    This method uses xr.open_mfdataset() to concatenate files and detect
    frequency from the resulting time coordinate, avoiding individual file processing.

    Args:
        file_paths: Path or list of paths to NetCDF files
        time_coord: name of the time coordinate
        max_sample_files: maximum number of files to sample for detection (for performance)

    Returns:
        Detected frequency as pandas Timedelta
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    # For very large numbers of files, sample a subset for frequency detection
    if len(file_paths) > max_sample_files:
        print(
            f"🚀 Sampling {max_sample_files} files from {len(file_paths)} total for efficient frequency detection"
        )
        # Sample files from beginning, middle, and end to get representative coverage
        sample_indices = list(range(0, min(max_sample_files // 3, len(file_paths))))
        sample_indices.extend(
            list(
                range(
                    len(file_paths) // 2 - max_sample_files // 6,
                    len(file_paths) // 2 + max_sample_files // 6,
                )
            )
        )
        sample_indices.extend(
            list(range(len(file_paths) - max_sample_files // 3, len(file_paths)))
        )
        # Remove duplicates and ensure we don't exceed bounds
        sample_indices = sorted(
            list(set([i for i in sample_indices if 0 <= i < len(file_paths)]))
        )
        sampled_files = [file_paths[i] for i in sample_indices[:max_sample_files]]
    else:
        sampled_files = file_paths

    try:
        print(
            f"📂 Opening {len(sampled_files)} files with xarray multi-file dataset..."
        )

        # Use xr.open_mfdataset for efficient concatenation
        # decode_cf=False keeps it lazy, combine='nested' with concat_dim for proper concatenation
        with xr.open_mfdataset(
            sampled_files,
            decode_cf=False,
            chunks={},
            concat_dim=time_coord,
            combine="nested",
            data_vars="minimal",  # Only load coordinate variables
            coords="minimal",
        ) as mf_ds:
            # Detect frequency from the concatenated time coordinate
            detected_freq = detect_time_frequency_lazy(mf_ds, time_coord)

            if detected_freq is None:
                raise ValueError(
                    "Could not detect frequency from concatenated time coordinate"
                )

            print(f"⚡ Efficiently detected frequency: {detected_freq}")
            return detected_freq

    except Exception as e:
        # Fallback to individual file checking if concatenation fails
        warnings.warn(
            f"Multi-file concatenation failed ({e}), falling back to individual file analysis"
        )
        return _detect_frequency_from_individual_files(sampled_files, time_coord)


def _detect_frequency_from_individual_files(
    file_paths: Union[str, List[str]], time_coord: str = "time"
) -> pd.Timedelta:
    """
    Fallback method: detect frequency from individual files (original approach).

    Used when multi-file concatenation approach fails.
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    frequencies = []
    file_info = []

    print(f"📁 Analyzing {len(file_paths)} files individually...")

    # Detect frequency from each file
    for file_path in file_paths:
        try:
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

    # Return the most common frequency
    from collections import Counter

    freq_counts = Counter(frequencies)
    detected_freq = freq_counts.most_common(1)[0][0]

    print(f"📊 Detected frequency from individual files: {detected_freq}")
    return detected_freq


def _validate_monthly_compatibility(
    file_paths: Union[str, List[str]], time_coord: str = "time"
) -> pd.Timedelta:
    """
    Validate monthly files allowing for calendar month variations (28-31 days).

    Uses efficient concatenation-based approach when possible, with fallback
    to individual file analysis for validation.
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    # First, try efficient frequency detection
    try:
        detected_freq = _detect_frequency_from_concatenated_files(
            file_paths, time_coord
        )

        # Verify this looks like monthly data
        freq_seconds = detected_freq.total_seconds()
        monthly_min = 20 * 86400  # 20 days in seconds
        monthly_max = 35 * 86400  # 35 days in seconds

        if not (monthly_min <= freq_seconds <= monthly_max):
            # If concatenated detection doesn't give monthly range, validate individual files
            print(
                f"⚠️  Concatenated frequency ({detected_freq}) not in monthly range, validating individual files..."
            )
            return _validate_monthly_files_individually(file_paths, time_coord)

        print(
            f"📅 Validated monthly data with calendar variations (detected: {detected_freq})"
        )
        return detected_freq

    except Exception as e:
        warnings.warn(f"Concatenation-based detection failed: {e}")
        return _validate_monthly_files_individually(file_paths, time_coord)


def _validate_monthly_files_individually(
    file_paths: List[str], time_coord: str = "time"
) -> pd.Timedelta:
    """
    Validate monthly files individually (original detailed validation).

    This is used as a fallback when concatenation-based detection fails
    or when we need detailed per-file validation.
    """
    frequencies = []
    file_info = []

    # Detect frequency from each file
    for file_path in file_paths:
        try:
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

    # Check that all frequencies are in the monthly range (20-35 days)
    monthly_min = 20 * 86400  # 20 days in seconds
    monthly_max = 35 * 86400  # 35 days in seconds

    non_monthly_files = []
    for file_path, freq in file_info:
        freq_seconds = freq.total_seconds()
        if not (monthly_min <= freq_seconds <= monthly_max):
            non_monthly_files.append((file_path, freq))

    if non_monthly_files:
        error_msg = "Files do not appear to be monthly data:\n"
        for file_path, freq in non_monthly_files:
            days = freq.total_seconds() / 86400
            error_msg += f"  {file_path}: {freq} ({days:.1f} days)\n"
        error_msg += "\nExpected monthly files should be in range 20-35 days."
        raise FrequencyMismatchError(error_msg)

    # All files are monthly - return a representative monthly frequency
    # Use the most common frequency, or the first one if all different
    from collections import Counter

    freq_counts = Counter(frequencies)
    representative_freq = freq_counts.most_common(1)[0][0]

    print(f"📅 Validated {len(file_info)} monthly files with calendar variations:")
    for file_path, freq in file_info:
        days = freq.total_seconds() / 86400
        print(f"   • {file_path}: {days:.0f} days")
    print(f"📏 Representative monthly frequency: {representative_freq}")

    return representative_freq


def validate_cmip6_frequency_compatibility(
    file_paths: Union[str, List[str]],
    compound_name: str,
    time_coord: str = "time",
    tolerance_seconds: float = None,  # Auto-determined based on detected frequency
    interactive: bool = True,
) -> tuple[pd.Timedelta, bool]:
    """
    Validate that input files have compatible frequency with CMIP6 target frequency.

    This function:
    1. Validates frequency consistency across input files (with special handling for monthly data)
    2. Parses target frequency from CMIP6 compound name
    3. Checks compatibility and determines if resampling is needed
    4. Optionally prompts user for confirmation when resampling is required

    For monthly CMIP6 tables (Amon, Lmon, Omon, etc.), this function recognizes that
    individual monthly files have different calendar lengths (28-31 days) and validates
    them appropriately.

    Args:
        file_paths: Path or list of paths to NetCDF files
        compound_name: CMIP6 compound name (e.g., 'Amon.tas')
        time_coord: name of the time coordinate (default: "time")
        tolerance_seconds: tolerance for frequency differences in seconds.
                          If None (default), automatically determined based on frequency.
        interactive: whether to prompt user when resampling is needed

    Returns:
        tuple of (detected_frequency, resampling_required)

    Raises:
        FrequencyMismatchError: if files have inconsistent frequencies
        IncompatibleFrequencyError: if input frequency cannot be resampled to target
        ValueError: if compound name is invalid
    """
    # Parse target frequency from compound name first to determine validation strategy
    try:
        target_freq = parse_cmip6_table_frequency(compound_name)
    except ValueError as e:
        raise ValueError(
            f"Cannot determine target frequency from compound name '{compound_name}': {e}"
        )

    # Check if this is monthly data
    if _is_monthly_target(compound_name):
        # Use monthly-aware validation that allows calendar variations
        print(
            f"🗓️  Monthly CMIP6 table detected ({compound_name}) - using calendar-aware validation"
        )
        detected_freq = _validate_monthly_compatibility(file_paths, time_coord)
    else:
        # Use standard strict frequency validation for non-monthly data
        print(
            f"⏰ Non-monthly CMIP6 table ({compound_name}) - using strict frequency validation"
        )
        detected_freq = validate_consistent_frequency(
            file_paths, time_coord, tolerance_seconds
        )

    # Parse target frequency from compound name
    try:
        target_freq = parse_cmip6_table_frequency(compound_name)
    except ValueError as e:
        raise ValueError(
            f"Cannot determine target frequency from compound name '{compound_name}': {e}"
        )

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

    # Special handling for monthly data - no resampling needed if both are monthly
    if _is_monthly_target(compound_name):
        # For monthly CMIP6 tables, calendar month variations (28-31 days) are natural
        # and do not require resampling - the data is already at the correct temporal resolution
        resampling_required = False
        print(
            "📅 Monthly data detected - no resampling required (calendar variations are natural)"
        )
    else:
        # For non-monthly data, use standard frequency comparison with 1% tolerance
        resampling_required = (
            abs(input_seconds - target_seconds) / target_seconds > 0.01
        )

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
            response = (
                input("Do you want to continue with temporal resampling? [y/N]: ")
                .strip()
                .lower()
            )
            if response not in ["y", "yes"]:
                raise InterruptedError(
                    "CMORisation aborted by user due to temporal resampling requirement. "
                    "To proceed non-interactively, set interactive=False or validate_frequency=False."
                )
            print("✓ Proceeding with temporal resampling...")
        else:
            # Non-interactive mode - just warn
            warnings.warn(message, ResamplingRequiredWarning, stacklevel=2)

    return detected_freq, resampling_required


def _parse_access_frequency_metadata(frequency_str: str) -> Optional[pd.Timedelta]:
    """
    Parse ACCESS model frequency metadata string to pandas Timedelta.

    ACCESS models use a standardized frequency schema with patterns like:
    - "fx" (fixed/time-invariant)
    - "subhr" (sub-hourly, typically 30 minutes)
    - "Nmin" (N minutes, e.g., "30min")
    - "Nhr" (N hours, e.g., "3hr", "12hr")
    - "Nday" (N days, e.g., "1day", "5day")
    - "Nmon" (N months, e.g., "1mon", "3mon")
    - "Nyr" (N years, e.g., "1yr", "10yr")
    - "Ndec" (N decades, e.g., "1dec")

    Args:
        frequency_str: ACCESS frequency string from global metadata

    Returns:
        pandas Timedelta representing the frequency, or None if cannot parse
    """
    if not isinstance(frequency_str, str):
        return None

    freq = frequency_str.strip().lower()

    try:
        # Handle special cases first
        if freq == "fx":
            # Fixed/time-invariant data - no temporal frequency
            return None
        elif freq == "subhr":
            # Sub-hourly, typically 30 minutes for ACCESS models
            return pd.Timedelta(minutes=30)

        # Parse numeric frequency patterns
        import re

        # Minutes: e.g., "30min", "15min"
        if freq.endswith("min"):
            match = re.match(r"^(\d+)min$", freq)
            if match:
                minutes = int(match.group(1))
                return pd.Timedelta(minutes=minutes)

        # Hours: e.g., "3hr", "6hr", "12hr"
        elif freq.endswith("hr"):
            match = re.match(r"^(\d+)hr$", freq)
            if match:
                hours = int(match.group(1))
                return pd.Timedelta(hours=hours)

        # Days: e.g., "1day", "5day"
        elif freq.endswith("day"):
            match = re.match(r"^(\d+)day$", freq)
            if match:
                days = int(match.group(1))
                return pd.Timedelta(days=days)

        # Months: e.g., "1mon", "3mon" (approximate)
        elif freq.endswith("mon"):
            match = re.match(r"^(\d+)mon$", freq)
            if match:
                months = int(match.group(1))
                # Approximate months as 30.44 days (365.25/12)
                return pd.Timedelta(days=months * 30.44)

        # Years: e.g., "1yr", "5yr" (approximate)
        elif freq.endswith("yr"):
            match = re.match(r"^(\d+)yr$", freq)
            if match:
                years = int(match.group(1))
                # Use 365.25 days per year (accounting for leap years)
                return pd.Timedelta(days=years * 365.25)

        # Decades: e.g., "1dec" (approximate)
        elif freq.endswith("dec"):
            match = re.match(r"^(\d+)dec$", freq)
            if match:
                decades = int(match.group(1))
                # 10 years per decade
                return pd.Timedelta(days=decades * 10 * 365.25)

        return None

    except (ValueError, AttributeError):
        return None


def _detect_frequency_from_access_metadata(ds: xr.Dataset) -> Optional[pd.Timedelta]:
    """
    Detect temporal frequency from ACCESS model global frequency metadata.

    ACCESS models include a standardized 'frequency' global attribute
    that explicitly specifies the temporal sampling frequency.

    Args:
        ds: xarray Dataset with potential ACCESS frequency metadata

    Returns:
        pandas Timedelta representing the detected frequency, or None if not found
    """
    # Check for frequency in global attributes
    frequency_attr = ds.attrs.get("frequency")
    if frequency_attr:
        parsed_freq = _parse_access_frequency_metadata(frequency_attr)
        if parsed_freq is not None:
            return parsed_freq

    # Also check for alternative attribute names that might be used
    alternative_names = [
        "freq",
        "time_frequency",
        "temporal_frequency",
        "sampling_frequency",
    ]
    for attr_name in alternative_names:
        if attr_name in ds.attrs:
            parsed_freq = _parse_access_frequency_metadata(ds.attrs[attr_name])
            if parsed_freq is not None:
                return parsed_freq

    return None


def detect_time_frequency_lazy(
    ds: xr.Dataset, time_coord: str = "time"
) -> Optional[pd.Timedelta]:
    """
    Detect the temporal frequency of a dataset using multiple methods.

    This function works lazily and uses a hierarchical approach to detect frequency
    without loading entire time dimensions into memory.

    Priority order:
    1. ACCESS model frequency metadata (most reliable for ACCESS raw data)
    2. CF-compliant time bounds (most reliable for processed/CMIP6 data)
    3. Time coordinate differences (fallback method)

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

    # Method 1: Try to detect frequency from ACCESS model metadata (highest priority)
    access_freq = _detect_frequency_from_access_metadata(ds)
    if access_freq is not None:
        print(f"🏷️  Detected frequency from ACCESS metadata: {access_freq}")
        return access_freq

    # Method 2: Try to detect frequency from time bounds (CF-compliant approach)
    bounds_freq = _detect_frequency_from_bounds(ds, time_coord)
    if bounds_freq is not None:
        print(f"🎯 Detected frequency from time bounds: {bounds_freq}")
        return bounds_freq

    # Method 3: Fallback to time coordinate differences
    if time_var.size < 1:
        raise ValueError(
            f"Need at least 1 time point to detect frequency, got {time_var.size}"
        )

    # For single time point, we can't detect frequency from differences
    if time_var.size == 1:
        warnings.warn(
            "Only one time point available, no ACCESS metadata, and no time bounds found. "
            "Cannot determine temporal frequency reliably."
        )
        return None

    print("📊 Detecting frequency from time coordinate differences (fallback method)")

    # Sample first few time points (max 10 to keep it lightweight)
    n_sample = min(10, time_var.size)

    # Load only the sample time points - this is the key to keeping it lazy
    time_sample = time_var.isel({time_coord: slice(0, n_sample)}).compute()

    # Convert to pandas datetime for easier frequency detection
    try:
        # Handle different time formats
        units = time_var.attrs.get("units")
        calendar = time_var.attrs.get("calendar", "standard")

        # Check if values are already datetime64 (even if units suggest otherwise)
        if np.issubdtype(time_sample.values.dtype, np.datetime64):
            # Already datetime64 - use directly
            time_index = pd.to_datetime(time_sample.values)
        elif units and "since" in units:
            # Convert from numeric time to datetime
            try:
                dates = num2date(
                    time_sample.values,
                    units=units,
                    calendar=calendar,
                    only_use_cftime_datetimes=False,
                )
                # Convert to pandas datetime if possible for better frequency inference
                if hasattr(dates[0], "strftime"):  # Standard datetime
                    time_index = pd.to_datetime(
                        [d.strftime("%Y-%m-%d %H:%M:%S") for d in dates]
                    )
                else:  # cftime datetime
                    # For cftime objects, use a more manual approach
                    time_diffs = []
                    for i in range(1, len(dates)):
                        diff = dates[i] - dates[i - 1]
                        # Convert to total seconds
                        total_seconds = diff.days * 86400 + diff.seconds
                        time_diffs.append(total_seconds)

                    if time_diffs:
                        avg_seconds = np.mean(time_diffs)
                        return pd.Timedelta(seconds=avg_seconds)
                    return None
            except (ValueError, OverflowError) as e:
                # If numeric conversion fails, try treating as datetime64
                if np.issubdtype(time_sample.values.dtype, np.datetime64):
                    time_index = pd.to_datetime(time_sample.values)
                else:
                    raise e
        else:
            # Assume already in datetime format
            time_index = pd.to_datetime(time_sample.values)

        # Infer frequency from pandas
        if len(time_index) >= 2:
            freq = pd.infer_freq(time_index)
            if freq:
                # Convert frequency string to Timedelta
                try:
                    offset = pd.tseries.frequencies.to_offset(freq)
                    # For some offsets like MonthBegin, we need to estimate the timedelta
                    if hasattr(offset, "delta") and offset.delta is not None:
                        return pd.Timedelta(offset.delta)
                    elif "M" in freq:  # Monthly frequencies
                        # Use actual time differences for monthly data
                        time_diffs = time_index[1:] - time_index[:-1]
                        avg_diff = time_diffs.mean()
                        return pd.Timedelta(avg_diff)
                    elif "Y" in freq:  # Yearly frequencies
                        # Use actual time differences for yearly data
                        time_diffs = time_index[1:] - time_index[:-1]
                        avg_diff = time_diffs.mean()
                        return pd.Timedelta(avg_diff)
                    else:
                        # Try to convert directly for simple frequencies
                        return pd.Timedelta(offset)
                except (ValueError, TypeError):
                    # Fall back to manual calculation
                    pass

            # Manual frequency calculation if pandas can't infer or convert
            time_diffs = time_index[1:] - time_index[:-1]
            # Use the most common difference as the frequency
            unique_diffs, counts = np.unique(time_diffs, return_counts=True)
            most_common_diff = unique_diffs[np.argmax(counts)]
            # Ensure we return a pandas Timedelta, not numpy timedelta64
            return pd.Timedelta(most_common_diff)

    except Exception as e:
        warnings.warn(f"Could not detect frequency from time coordinate: {e}")
        return None

    return None


def _detect_frequency_from_bounds(
    ds: xr.Dataset, time_coord: str = "time"
) -> Optional[pd.Timedelta]:
    """
    Detect temporal frequency from CF-compliant time bounds information.

    This method is preferred because time bounds explicitly define the temporal
    intervals that each time coordinate represents, making frequency detection
    more reliable than inferring from coordinate differences.

    Args:
        ds: xarray Dataset with potential time bounds
        time_coord: name of the time coordinate

    Returns:
        pandas Timedelta representing the detected frequency, or None if no bounds found
    """
    # Common names for time bounds variables
    potential_bounds_names = [
        f"{time_coord}_bnds",  # CF standard
        f"{time_coord}_bounds",  # Alternative spelling
        "time_bnds",  # Common case
        "time_bounds",  # Alternative
        "bounds_time",  # Some models
        f"{time_coord}_bnd",  # Shortened version
    ]

    bounds_var = None
    bounds_name = None

    # Check if time coordinate has bounds attribute pointing to bounds variable
    time_var = ds[time_coord]
    if hasattr(time_var, "bounds") or "bounds" in time_var.attrs:
        bounds_attr = getattr(time_var, "bounds", time_var.attrs.get("bounds"))
        if bounds_attr and bounds_attr in ds.data_vars:
            bounds_var = ds[bounds_attr]
            bounds_name = bounds_attr

    # If not found via bounds attribute, search for common bounds variable names
    if bounds_var is None:
        for name in potential_bounds_names:
            if name in ds.data_vars or name in ds.coords:
                bounds_var = ds[name]
                bounds_name = name
                break

    if bounds_var is None:
        return None

    try:
        # Load only the first bounds entry to keep it lazy
        bounds_sample = bounds_var.isel(
            {bounds_var.dims[0]: slice(0, min(3, bounds_var.sizes[bounds_var.dims[0]]))}
        )
        bounds_sample = bounds_sample.compute()

        # Time bounds should have shape (time, 2) where the last dimension is [start, end]
        if bounds_sample.ndim != 2 or bounds_sample.shape[-1] != 2:
            warnings.warn(
                f"Time bounds variable '{bounds_name}' has unexpected shape: {bounds_sample.shape}"
            )
            return None

        # Get units and calendar from bounds or time coordinate
        units = bounds_var.attrs.get("units") or time_var.attrs.get("units")
        calendar = bounds_var.attrs.get("calendar") or time_var.attrs.get(
            "calendar", "standard"
        )

        if units and "since" in units:
            # Convert bounds to datetime objects
            bounds_dates = num2date(
                bounds_sample.values,
                units=units,
                calendar=calendar,
                only_use_cftime_datetimes=False,
            )

            # Calculate the interval for the first time step
            start_time = bounds_dates[0, 0]  # Start of first interval
            end_time = bounds_dates[0, 1]  # End of first interval

            # Calculate the time difference
            if hasattr(start_time, "total_seconds"):
                # Standard datetime objects
                interval = end_time - start_time
                total_seconds = interval.total_seconds()
            else:
                # cftime objects
                diff = end_time - start_time
                total_seconds = diff.days * 86400 + diff.seconds

            frequency = pd.Timedelta(seconds=total_seconds)

            # Verify consistency with second interval if available
            if bounds_sample.shape[0] > 1:
                start_time2 = bounds_dates[1, 0]
                end_time2 = bounds_dates[1, 1]

                if hasattr(start_time2, "total_seconds"):
                    interval2 = end_time2 - start_time2
                    total_seconds2 = interval2.total_seconds()
                else:
                    diff2 = end_time2 - start_time2
                    total_seconds2 = diff2.days * 86400 + diff2.seconds

                # Check if intervals are consistent (within 5% tolerance)
                if abs(total_seconds - total_seconds2) / total_seconds > 0.05:
                    warnings.warn(
                        f"Inconsistent time intervals detected in bounds: "
                        f"{frequency} vs {pd.Timedelta(seconds=total_seconds2)}"
                    )

            return frequency

        else:
            warnings.warn(
                f"Time bounds variable '{bounds_name}' missing time units information"
            )
            return None

    except Exception as e:
        warnings.warn(f"Error processing time bounds '{bounds_name}': {e}")
        return None


def _determine_smart_tolerance(frequency: pd.Timedelta) -> float:
    """
    Determine appropriate tolerance for frequency validation based on the detected frequency.

    Args:
        frequency: Detected frequency as pandas Timedelta

    Returns:
        Tolerance in seconds
    """
    freq_seconds = frequency.total_seconds()

    # Monthly data: 20-35 days range
    if 20 * 86400 <= freq_seconds <= 35 * 86400:
        # Monthly data can vary from 28 days (Feb) to 31 days (Jan/Mar/May/Jul/Aug/Oct/Dec)
        # Allow up to 4 days difference to accommodate calendar month variations
        return 4 * 86400  # 4 days = 345,600 seconds

    # Weekly data: 6-8 days range
    elif 6 * 86400 <= freq_seconds <= 8 * 86400:
        # Weekly data should be consistent, allow 1 day tolerance
        return 1 * 86400  # 1 day = 86,400 seconds

    # Daily data: 0.8-1.2 days range
    elif 0.8 * 86400 <= freq_seconds <= 1.2 * 86400:
        # Daily data should be very consistent, allow 2 hours tolerance
        return 2 * 3600  # 2 hours = 7,200 seconds

    # Sub-daily data (hourly, 3-hourly, etc.)
    elif freq_seconds < 0.8 * 86400:
        # Sub-daily should be very consistent, allow 1 hour tolerance
        return 3600  # 1 hour = 3,600 seconds

    # Annual or longer data
    elif freq_seconds > 35 * 86400:
        # Yearly data can vary due to leap years, allow 2 days tolerance
        return 2 * 86400  # 2 days = 172,800 seconds

    # Default fallback
    else:
        # Use 5% of the frequency as tolerance, minimum 1 hour
        return max(freq_seconds * 0.05, 3600)


def validate_consistent_frequency(
    file_paths: Union[str, List[str]],
    time_coord: str = "time",
    tolerance_seconds: float = None,  # Auto-determined based on detected frequency
    use_concatenation: bool = True,  # Enable efficient concatenation approach
) -> pd.Timedelta:
    """
    Validate that all input files have consistent temporal frequency.

    Uses efficient concatenation approach when possible, falling back to
    individual file processing for detailed validation when needed.

    Args:
        file_paths: Path or list of paths to NetCDF files
        time_coord: name of the time coordinate (default: "time")
        tolerance_seconds: tolerance for frequency differences in seconds.
                          If None (default), automatically determined based on frequency.
        use_concatenation: whether to use efficient xarray concatenation approach (default: True)

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

    # Try efficient concatenation approach first
    if use_concatenation:
        try:
            detected_freq = _detect_frequency_from_concatenated_files(
                file_paths, time_coord
            )

            # For non-monthly data or when detailed validation is needed,
            # we might still want to validate individual files for consistency
            if tolerance_seconds is not None:
                print(
                    f"🔍 Performing detailed consistency validation with tolerance {tolerance_seconds}s"
                )
                return _validate_frequency_consistency_detailed(
                    file_paths, time_coord, tolerance_seconds, detected_freq
                )

            # Auto-determine tolerance and validate if needed
            auto_tolerance = _determine_smart_tolerance(detected_freq)

            # For monthly data with large tolerance, concatenation result is likely sufficient
            if auto_tolerance >= 86400:  # >= 1 day tolerance (monthly data)
                print(
                    f"📅 Large tolerance detected ({auto_tolerance/86400:.1f} days) - concatenated frequency sufficient"
                )
                return detected_freq

            # For sub-daily data with tight tolerance, do detailed validation
            print(
                f"🔍 Small tolerance ({auto_tolerance}s) - performing detailed validation"
            )
            return _validate_frequency_consistency_detailed(
                file_paths, time_coord, auto_tolerance, detected_freq
            )

        except Exception as e:
            warnings.warn(f"Concatenation-based frequency detection failed: {e}")
            # Fall through to individual file approach

    # Fallback to individual file processing
    return _validate_frequency_consistency_detailed(
        file_paths, time_coord, tolerance_seconds
    )


def _validate_frequency_consistency_detailed(
    file_paths: List[str],
    time_coord: str = "time",
    tolerance_seconds: float = None,
    expected_freq: pd.Timedelta = None,
) -> pd.Timedelta:
    """
    Detailed frequency consistency validation using individual file processing.

    This is the original approach, used as fallback or when detailed validation is needed.
    """
    frequencies = []
    file_info = []

    print(f"📁 Performing detailed frequency validation on {len(file_paths)} files...")

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

    # Use expected frequency if provided, otherwise use first detected frequency
    base_freq = expected_freq if expected_freq is not None else frequencies[0]
    base_seconds = base_freq.total_seconds()

    # Auto-determine tolerance if not provided
    if tolerance_seconds is None:
        tolerance_seconds = _determine_smart_tolerance(base_freq)
        print(
            f"📏 Auto-determined tolerance: {tolerance_seconds/86400:.1f} days ({tolerance_seconds:.0f}s) for frequency ~{base_freq}"
        )

    inconsistent_files = []

    # Check all files against the base frequency
    for file_path, freq in file_info:
        freq_seconds = freq.total_seconds()
        diff_seconds = abs(freq_seconds - base_seconds)

        if diff_seconds > tolerance_seconds:
            inconsistent_files.append(
                {
                    "file": file_path,
                    "frequency": freq,
                    "expected": base_freq,
                    "difference_seconds": diff_seconds,
                }
            )

    if inconsistent_files:
        error_msg = "Inconsistent temporal frequencies detected:\n"
        error_msg += f"Expected frequency: {base_freq}\n"
        if expected_freq is not None:
            error_msg += "(From concatenation analysis)\n"
        else:
            error_msg += f"Reference file: {file_info[0][0]}\n"
        error_msg += (
            f"Tolerance: {tolerance_seconds}s ({tolerance_seconds/86400:.2f} days)\n\n"
        )
        error_msg += "Inconsistent files:\n"
        for info in inconsistent_files:
            error_msg += f"  {info['file']}: {info['frequency']} (diff: {info['difference_seconds']:.1f}s)\n"

        raise FrequencyMismatchError(error_msg)

    print(f"✅ Validated {len(file_info)} files with consistent frequency: {base_freq}")
    return base_freq


def determine_resampling_method(
    variable_name: str, variable_attrs: dict, cmip6_table: str = None
) -> str:
    """
    Determine the appropriate temporal resampling method based on variable characteristics.

    Args:
        variable_name: Name of the variable (e.g., 'tas', 'pr', 'uas')
        variable_attrs: Variable attributes dictionary from xarray
        cmip6_table: CMIP6 table name for additional context

    Returns:
        Resampling method: 'mean', 'sum', 'min', 'max', 'first', 'last'
    """
    # Get variable metadata
    standard_name = variable_attrs.get("standard_name", "").lower()
    long_name = variable_attrs.get("long_name", "").lower()
    units = variable_attrs.get("units", "").lower()
    cell_methods = variable_attrs.get("cell_methods", "").lower()
    variable_lower = variable_name.lower()

    # Check cell_methods for guidance first (highest priority)
    if "time: sum" in cell_methods:
        return "sum"
    elif "time: mean" in cell_methods:
        return "mean"
    elif "time: maximum" in cell_methods:
        return "max"
    elif "time: minimum" in cell_methods:
        return "min"

    # Extreme variables (min/max depending on context)
    if (
        "maximum" in standard_name
        or "maximum" in long_name
        or variable_lower.endswith("max")
        or "tasmax" in variable_lower
    ):
        return "max"
    if (
        "minimum" in standard_name
        or "minimum" in long_name
        or variable_lower.endswith("min")
        or "tasmin" in variable_lower
    ):
        return "min"

    # Precipitation and flux variables (should be summed)
    if any(
        keyword in standard_name or keyword in long_name or keyword in variable_lower
        for keyword in ["precipitation", "flux", "rate"]
    ):
        if "kg m-2 s-1" in units or "kg/m2/s" in units:
            return "sum"  # Convert rate to total

    # Temperature and intensive variables (should be averaged)
    temperature_keywords = ["temperature", "pressure", "density", "concentration"]
    temperature_prefixes = ["tas", "ta", "ps", "psl", "hus", "hur"]

    if any(
        keyword in standard_name or keyword in long_name
        for keyword in temperature_keywords
    ) or any(variable_lower.startswith(prefix) for prefix in temperature_prefixes):
        return "mean"

    # Wind components (vector quantities - should be averaged)
    wind_prefixes = ["uas", "vas", "ua", "va", "wap"]
    if any(variable_lower.startswith(prefix) for prefix in wind_prefixes):
        return "mean"

    # Cloud and radiation variables (typically averaged)
    cloud_keywords = ["cloud", "radiation", "albedo"]
    cloud_prefixes = ["clt", "clw", "cli", "rsdt", "rsut", "rlut", "rsds", "rlds"]

    if any(
        keyword in standard_name or keyword in long_name for keyword in cloud_keywords
    ) or any(variable_lower.startswith(prefix) for prefix in cloud_prefixes):
        return "mean"

    # Default to mean for most variables
    return "mean"


def get_resampling_frequency_string(target_freq: pd.Timedelta) -> str:
    """
    Convert pandas Timedelta to xarray/pandas resampling frequency string.

    Args:
        target_freq: Target frequency as pandas Timedelta

    Returns:
        Frequency string for pandas/xarray resampling (e.g., 'D', 'M', 'Y', '3H')
    """
    total_seconds = target_freq.total_seconds()

    # Map common CMIP6 frequencies to pandas frequency strings
    if total_seconds <= 3600:  # <= 1 hour
        hours = total_seconds / 3600
        if hours == 1:
            return "H"
        else:
            return f"{int(hours)}H"
    elif total_seconds <= 86400:  # <= 1 day
        hours = total_seconds / 3600
        if hours == 24:
            return "D"  # Daily
        elif hours == 12:
            return "12H"
        elif hours == 6:
            return "6H"
        elif hours == 3:
            return "3H"
        else:
            return f"{int(hours)}H"
    elif total_seconds <= 86400 * 31:  # <= ~1 month
        days = total_seconds / 86400
        if 28 <= days <= 31:
            return "M"  # Monthly (end of month)
        else:
            return f"{int(days)}D"
    elif total_seconds <= 86400 * 366:  # <= ~1 year
        days = total_seconds / 86400
        if 360 <= days <= 366:
            return "Y"  # Yearly (end of year)
        else:
            return f"{int(days)}D"
    else:
        # Multi-year or very long periods
        years = total_seconds / (86400 * 365.25)
        return f"{int(years)}Y"


def resample_dataset_temporal(
    ds: xr.Dataset,
    target_freq: pd.Timedelta,
    variable_name: str,
    time_coord: str = "time",
    method: str = "auto",
) -> xr.Dataset:
    """
    Resample dataset to target temporal frequency using lazy xarray/Dask operations.

    Args:
        ds: xarray Dataset to resample
        target_freq: Target frequency as pandas Timedelta
        variable_name: Name of the main variable being processed
        time_coord: Name of the time coordinate
        method: Resampling method ('auto', 'mean', 'sum', 'min', 'max', 'first', 'last')

    Returns:
        Resampled xarray Dataset
    """
    if time_coord not in ds.coords:
        raise ValueError(f"Time coordinate '{time_coord}' not found in dataset")

    # Convert target frequency to resampling string
    freq_str = get_resampling_frequency_string(target_freq)

    print(f"📊 Resampling dataset to {target_freq} using frequency string '{freq_str}'")

    # Use resample approach (more robust than groupby_bins)
    try:
        # Decode time coordinate if needed for resampling
        if "units" in ds[time_coord].attrs and "since" in ds[time_coord].attrs.get(
            "units", ""
        ):
            ds_decoded = xr.decode_cf(ds, decode_times=True)
        else:
            ds_decoded = ds

        # Apply different aggregation methods to different variables
        resampled_vars = {}

        for var_name in ds.data_vars:
            if method == "auto":
                # Automatically determine method based on variable characteristics
                var_method = determine_resampling_method(
                    var_name,
                    ds[var_name].attrs,
                    cmip6_table=None,  # Could be enhanced to use table info
                )
            else:
                var_method = method

            print(f"  • Variable '{var_name}': using '{var_method}' aggregation")

            # Create resampler for this specific variable
            var_resampler = ds_decoded[var_name].resample({time_coord: freq_str})

            # Apply the chosen aggregation method
            if var_method == "mean":
                resampled_vars[var_name] = var_resampler.mean()
            elif var_method == "sum":
                resampled_vars[var_name] = var_resampler.sum()
            elif var_method == "min":
                resampled_vars[var_name] = var_resampler.min()
            elif var_method == "max":
                resampled_vars[var_name] = var_resampler.max()
            elif var_method == "first":
                resampled_vars[var_name] = var_resampler.first()
            elif var_method == "last":
                resampled_vars[var_name] = var_resampler.last()
            else:
                # Default to mean
                resampled_vars[var_name] = var_resampler.mean()

        # Create new dataset with resampled variables
        ds_resampled = xr.Dataset(resampled_vars)

        # Copy coordinates (except time which is already resampled)
        for coord_name in ds.coords:
            if coord_name != time_coord:
                ds_resampled[coord_name] = ds[coord_name]

        # Update attributes
        ds_resampled.attrs = ds.attrs.copy()

        # Update variable attributes and add resampling info
        for var_name in ds_resampled.data_vars:
            ds_resampled[var_name].attrs = ds[var_name].attrs.copy()

            # Update cell_methods to reflect temporal aggregation
            cell_methods = ds_resampled[var_name].attrs.get("cell_methods", "")
            if method == "auto":
                agg_method = determine_resampling_method(var_name, ds[var_name].attrs)
            else:
                agg_method = method

            new_cell_method = f"time: {agg_method}"
            if cell_methods:
                ds_resampled[var_name].attrs["cell_methods"] = (
                    f"{cell_methods} {new_cell_method}"
                )
            else:
                ds_resampled[var_name].attrs["cell_methods"] = new_cell_method

        print(
            f"✓ Successfully resampled dataset from {len(ds[time_coord])} to {len(ds_resampled[time_coord])} time steps"
        )

        return ds_resampled

    except Exception as e:
        raise RuntimeError(f"Failed to resample dataset: {e}")


def validate_and_resample_if_needed(
    ds: xr.Dataset,
    compound_name: str,
    variable_name: str,
    time_coord: str = "time",
    method: str = "auto",
) -> tuple[xr.Dataset, bool]:
    """
    Validate temporal frequency and resample if needed for CMIP6 compatibility.

    Args:
        ds: xarray Dataset to check and potentially resample
        compound_name: CMIP6 compound name (e.g., 'Amon.tas')
        variable_name: Name of the main variable
        time_coord: Name of the time coordinate
        method: Resampling method ('auto' for automatic selection)

    Returns:
        tuple of (dataset, was_resampled)
    """
    # Detect current frequency
    detected_freq = detect_time_frequency_lazy(ds, time_coord)
    if detected_freq is None:
        raise ValueError("Could not detect temporal frequency from dataset")

    # Get target frequency
    target_freq = parse_cmip6_table_frequency(compound_name)

    # Check if resampling is needed
    input_seconds = detected_freq.total_seconds()
    target_seconds = target_freq.total_seconds()

    # Check compatibility first
    is_compatible, reason = is_frequency_compatible(detected_freq, target_freq)
    if not is_compatible:
        raise IncompatibleFrequencyError(f"Cannot resample: {reason}")

    # Check if both frequencies are monthly (special case)
    monthly_min = 20 * 86400  # 20 days in seconds
    monthly_max = 35 * 86400  # 35 days in seconds

    input_is_monthly = monthly_min <= input_seconds <= monthly_max
    target_is_monthly = monthly_min <= target_seconds <= monthly_max

    if input_is_monthly and target_is_monthly:
        # Both are monthly - no resampling needed (calendar variations are natural)
        print(
            f"✓ Both frequencies are monthly (input: {detected_freq}, target: {target_freq}) - no resampling required"
        )
        return ds, False

    # Allow small tolerance for exact matches
    tolerance = 0.01
    if abs(input_seconds - target_seconds) / target_seconds < tolerance:
        print(
            f"✓ Dataset frequency ({detected_freq}) matches target frequency ({target_freq})"
        )
        return ds, False

    print(f"Resampling required: {detected_freq} → {target_freq}")
    print(f"Reason: {reason}")

    # Perform resampling
    ds_resampled = resample_dataset_temporal(
        ds, target_freq, variable_name, time_coord, method
    )

    return ds_resampled, True
