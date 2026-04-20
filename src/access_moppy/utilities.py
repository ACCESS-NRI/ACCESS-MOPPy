import json
import warnings
from datetime import timedelta
from importlib.resources import as_file, files
from pathlib import Path
from typing import Dict, List, Optional, Union

import cftime
import numpy as np
import pandas as pd
import xarray as xr
from cftime import date2num, num2date

# Optional import for CMIP7 data request functionality
try:
    from data_request_api.content import dump_transformation as dt
    from data_request_api.query import data_request as dr

    DATA_REQUEST_API_AVAILABLE = True
except ImportError:
    dt = None
    dr = None
    DATA_REQUEST_API_AVAILABLE = False

_SUBMONTHLY_INPUT_VARIABLES = {"tasmax", "tasmin"}
_MONTHLY_TABLE_IDS = {"Amon", "Lmon", "Omon", "SImon", "CFmon", "mon"}

type_mapping = {
    "real": np.float32,
    "double": np.float64,
    "float": np.float32,
    "int": np.int32,
    "short": np.int16,
    "byte": np.int8,
}


def _get_cmip7_to_cmip6_mapping(cmip7_compound_name: str) -> Optional[str]:
    """
    Get CMIP6 equivalent for a CMIP7 compound name, supporting exact matches and regex patterns.

    Args:
        cmip7_compound_name: CMIP7 compound name or regex pattern

    Returns:
        CMIP6 equivalent compound name, or None if no mapping exists
    """
    import json
    import re

    # Load the CMIP7 to CMIP6 mapping file
    try:
        mapping_dir = files("access_moppy.mappings")
        mapping_file = "cmip7_to_cmip6_compound_name_mapping.json"

        cmip7_to_cmip6_mapping = {}
        for entry in mapping_dir.iterdir():
            if entry.name == mapping_file:
                with as_file(entry) as path:
                    with open(path, "r", encoding="utf-8") as f:
                        cmip7_to_cmip6_mapping = json.load(f)
                break

        if not cmip7_to_cmip6_mapping:
            print(f"❌ CMIP7 to CMIP6 mapping file '{mapping_file}' not found")
            return None

    except Exception as e:
        print(f"❌ Error loading CMIP7 to CMIP6 mapping: {e}")
        return None

    # Check for exact match first (case insensitive)
    for key in cmip7_to_cmip6_mapping.keys():
        if key.lower() == cmip7_compound_name.lower():
            return cmip7_to_cmip6_mapping[key]

    # Check if it's a regex pattern (contains special characters)
    regex_chars = set("*+?[]{}()^$|\\")
    if any(char in cmip7_compound_name for char in regex_chars):
        # Handle as regex pattern
        try:
            pattern = re.compile(cmip7_compound_name, re.IGNORECASE)
            matches = [
                key for key in cmip7_to_cmip6_mapping.keys() if pattern.search(key)
            ]

            if len(matches) == 0:
                print(
                    f"❌ No CMIP7 variables found matching pattern '{cmip7_compound_name}'"
                )
                return None
            elif len(matches) == 1:
                return cmip7_to_cmip6_mapping[matches[0]]
            else:
                print(f"⚠️  Pattern '{cmip7_compound_name}' matches multiple variables:")
                for match in sorted(matches):
                    print(f"  - {match}")
                print("Please specify one exactly.")
                return None

        except re.error as e:
            print(f"❌ Invalid regex pattern '{cmip7_compound_name}': {e}")
            return None

    # Not found
    return None


def load_model_mappings(compound_name: str, model_id: str = None) -> Dict:
    """
    Load Mappings for ACCESS models for CMIP6.

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
                    for component in [
                        "aerosol",
                        "atmosphere",
                        "land",
                        "ocean",
                        "oceanBgchem",
                        "time_invariant",
                        "sea_ice",
                    ]:
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


def get_monthly_ocean_files(
    compound_name: str,
    root_folder: str = "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/spinup/Dec25-PI-control/",
    target_folders: str = "output40[0-9]/ocean/",
    model_id: str = "ACCESS-ESM1.6",
) -> List[str]:
    """
    Find ocean data files for a given CMOR variable.

    This utility function searches for ocean model output files that correspond to a
    specific CMIP variable by using the variable mapping to identify the required
    model variables and then searching for files with the ocean-specific naming pattern.

    Args:
        compound_name: CMOR compound name (e.g., 'Omon.so', 'Ofx.areacello')
        root_folder: Root directory to search for files (default: ACCESS-ESM1.6 Dec spin-up path)
        target_folders: Target folder pattern relative to root_folder (default: output40[0-9]/ocean/)
        model_id: Model identifier for loading mappings (default: ACCESS-ESM1.6)

    Returns:
        List of absolute paths to matching ocean files

    Raises:
        ValueError: If compound_name format is invalid
        FileNotFoundError: If root directory doesn't exist

    Examples:
        >>> # Find ocean salinity files
        >>> files = get_monthly_ocean_files("Omon.so")
        >>> print(f"Found {len(files)} salinity files")

        >>> # Find ocean cell area files (different location)
        >>> area_files = get_monthly_ocean_files(
        ...     "Ofx.areacello",
        ...     target_folders="output401/ocean/"
        ... )
    """
    import glob

    # Validate inputs
    if not compound_name or "." not in compound_name:
        raise ValueError(
            f"Invalid compound_name format: {compound_name}. Expected 'table.variable' format."
        )

    # Extract variable name from compound name
    try:
        table_name, variable_name = compound_name.split(".", 1)
    except ValueError:
        raise ValueError(
            f"Invalid compound_name format: {compound_name}. Expected 'table.variable' format."
        )

    # Check if root folder exists
    root_path = Path(root_folder)
    if not root_path.exists():
        raise FileNotFoundError(f"Root folder does not exist: {root_folder}")

    # Load variable mappings
    try:
        mapping = load_model_mappings(compound_name, model_id)
    except Exception as e:
        warnings.warn(f"Could not load mapping for {compound_name}: {e}")
        return []

    if not mapping:
        warnings.warn(f"No mapping found for {compound_name}")
        return []

    # Get model variables from mapping
    model_variables = mapping[variable_name].get("model_variables", [])
    if not model_variables:
        warnings.warn(f"No model variables found in mapping for {compound_name}")
        return []

    # Search for files
    files_found = []
    search_pattern_base = str(root_path / target_folders)

    for model_variable in model_variables:
        # Ocean files typically have pattern: *-{model_variable}-1monthly-mean*.nc
        # For fixed fields, pattern might be different (e.g., ocean-2d-area_t.nc)
        if table_name == "Ofx":
            # Fixed fields have different naming patterns
            filename_patterns = [
                f"*{model_variable}*.nc",  # General pattern for fixed fields
                f"ocean-*-{model_variable}.nc",  # More specific ocean pattern
            ]
        else:
            # Monthly mean files
            filename_patterns = [f"*-{model_variable}-1monthly-mean*.nc"]

        for filename_pattern in filename_patterns:
            search_pattern = search_pattern_base + "/" + filename_pattern

            try:
                matching_files = glob.glob(search_pattern)
                files_found.extend(matching_files)

                if not matching_files and len(filename_patterns) == 1:
                    # Only warn if no alternatives were tried
                    warnings.warn(
                        f"No files found for model variable '{model_variable}' with pattern: {search_pattern}",
                        UserWarning,
                    )

            except Exception as e:
                warnings.warn(
                    f"Error searching for files with pattern '{search_pattern}': {e}"
                )

    # Remove duplicates and sort
    files_found = sorted(list(set(files_found)))

    if not files_found:
        warnings.warn(
            f"No ocean files found for {compound_name} in {search_pattern_base}"
        )

    return files_found


class VariableMapping:
    """
    A wrapper class for variable mappings that provides enhanced display functionality
    for Jupyter notebooks and better user experience.
    """

    def __init__(self, mapping_dict: Dict, compound_name: str, model_id: str = None):
        self._mapping = mapping_dict
        self.compound_name = compound_name
        self.model_id = model_id or "ACCESS-ESM1.6"
        self.variable_name = (
            compound_name.split(".")[1] if "." in compound_name else compound_name
        )

    def __getitem__(self, key):
        return self._mapping[key]

    def __contains__(self, key):
        return key in self._mapping

    def __iter__(self):
        return iter(self._mapping)

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()

    def items(self):
        return self._mapping.items()

    def get(self, key, default=None):
        return self._mapping.get(key, default)

    @property
    def mapping(self):
        """Access the raw mapping dictionary."""
        return self._mapping

    def __repr__(self):
        if not self._mapping:
            return f"VariableMapping(empty - no mapping found for {self.compound_name})"
        return f"VariableMapping({self.compound_name}, model={self.model_id})"

    def _repr_html_(self):
        """Rich HTML display for Jupyter notebooks (xarray-inspired theme)."""
        if not self._mapping:
            return f"""
            <div style="border: 1px solid #ddd; margin: 10px 0; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; max-width: 800px; display: inline-block;">
                <div style="background: #f7f7f7; border-bottom: 1px solid #ddd; padding: 8px 12px;">
                    <div style="font-weight: bold; color: #666;">❌ Variable Mapping</div>
                    <div style="font-size: 0.9em; color: #999;">No mapping found</div>
                </div>
                <div style="padding: 12px;">
                    <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
                        <tr><td style="padding: 3px 0; color: #666; font-weight: 500;">Compound Name:</td><td style="padding: 3px 0; font-family: monospace;">{self.compound_name}</td></tr>
                        <tr><td style="padding: 3px 0; color: #666; font-weight: 500;">Model:</td><td style="padding: 3px 0; font-family: monospace;">{self.model_id}</td></tr>
                    </table>
                    <div style="margin-top: 8px; font-size: 0.85em; color: #888; font-style: italic;">
                        Variable may not be available for this model or compound name may be incorrect.
                    </div>
                </div>
            </div>
            """

        variable_info = list(self._mapping.values())[
            0
        ]  # Get the first (and typically only) variable

        # Build HTML representation in xarray style
        html = f"""
        <div style="border: 1px solid #ddd; margin: 10px 0; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; max-width: 800px; display: inline-block;">
            <div style="background: #f7f7f7; border-bottom: 1px solid #ddd; padding: 8px 12px;">
                <div style="font-weight: bold; color: #333;">ACCESS-MOPPy Variable Mapping</div>
                <div style="font-size: 0.9em; color: #666; font-family: monospace;">{self.compound_name}</div>
            </div>
            <div style="padding: 12px;">
                <table style="width: 100%; font-size: 0.9em; border-collapse: collapse;">
        """

        # Model info
        html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500; width: 25%;">Model:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #0066cc;">{self.model_id}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Variable:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #0066cc;">{self.variable_name}</td>
                    </tr>
        """

        # CF Standard Name
        if "CF standard Name" in variable_info:
            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">CF Standard Name:</td>
                        <td style="padding: 6px 0; font-family: monospace; font-size: 0.85em;">{variable_info["CF standard Name"]}</td>
                    </tr>
            """

        # Units
        if "units" in variable_info:
            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Units:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #d73027;">{variable_info["units"]}</td>
                    </tr>
            """

        # Dimensions
        if "dimensions" in variable_info:
            dims = variable_info["dimensions"]
            dim_entries = [
                f"<span style='color: #0066cc;'>{k}</span>: <span style='color: #666;'>{v}</span>"
                for k, v in dims.items()
            ]
            dim_str = ", ".join(dim_entries)
            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Dimensions:</td>
                        <td style="padding: 6px 0; font-family: monospace; font-size: 0.85em;">{dim_str}</td>
                    </tr>
            """

        # Model Variables
        if "model_variables" in variable_info:
            model_vars = variable_info["model_variables"]
            if len(model_vars) == 1:
                html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Model Variable:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #0066cc;">{model_vars[0]}</td>
                    </tr>
                """
            else:
                vars_list = "</li><li style='margin: 2px 0;'>".join(model_vars)
                html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500; vertical-align: top;">Model Variables:</td>
                        <td style="padding: 6px 0;">
                            <div style="font-family: monospace; font-size: 0.85em; color: #666;">({len(model_vars)} variables)</div>
                            <div style="max-height: 120px; overflow-y: auto; margin-top: 4px; padding: 6px; background: #f9f9f9; border: 1px solid #eee; border-radius: 3px;">
                                <ul style="margin: 0; padding-left: 15px; font-family: monospace; font-size: 0.8em; color: #0066cc;">
                                    <li style="margin: 2px 0;">{vars_list}</li>
                                </ul>
                            </div>
                        </td>
                    </tr>
                """

        # Calculation/Processing
        if "calculation" in variable_info:
            calc = variable_info["calculation"]
            calc_type = calc.get("type", "unknown")

            # Color code by calculation type (xarray-like)
            type_colors = {
                "direct": "#4caf50",
                "formula": "#ff9800",
                "dataset_function": "#2196f3",
                "internal": "#9c27b0",
            }
            color = type_colors.get(calc_type, "#666")

            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Processing:</td>
                        <td style="padding: 6px 0;">
                            <span style="background: {color}; color: white; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; font-weight: 500;">{calc_type.upper()}</span>
                        </td>
                    </tr>
            """

            # Add formula or operation details
            if calc_type == "direct" and "formula" in calc:
                html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500; padding-left: 20px;">Formula:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #333; font-size: 0.85em;">{calc["formula"]}</td>
                    </tr>
                """
            elif calc_type == "formula" and "operation" in calc:
                html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500; padding-left: 20px;">Operation:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #333; font-size: 0.85em;">{calc["operation"]}</td>
                    </tr>
                """
            elif calc_type == "dataset_function" and "function" in calc:
                html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500; padding-left: 20px;">Function:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #333; font-size: 0.85em;">{calc["function"]}</td>
                    </tr>
                """

        # Z-axis information (for 3D variables)
        if "zaxis" in variable_info:
            zaxis = variable_info["zaxis"]
            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Vertical Coord:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #0066cc;">{zaxis.get("type", "Unknown")}</td>
                    </tr>
            """

        # Positive direction
        if "positive" in variable_info and variable_info["positive"]:
            html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 6px 0; color: #666; font-weight: 500;">Positive:</td>
                        <td style="padding: 6px 0; font-family: monospace; color: #333;">{variable_info["positive"]}</td>
                    </tr>
            """

        html += """
                </table>
            </div>
        </div>
        """
        return html

    def summary(self):
        """Return a brief summary of the mapping."""
        if not self._mapping:
            return f"No mapping found for {self.compound_name} in model {self.model_id}"

        variable_info = list(self._mapping.values())[0]
        model_vars = variable_info.get("model_variables", [])
        calc_type = variable_info.get("calculation", {}).get("type", "unknown")

        return f"{self.compound_name}: {len(model_vars)} model variable(s), {calc_type} processing"

    def to_dict(self):
        """Return the underlying mapping dictionary."""
        return self._mapping


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
    return table_id in _MONTHLY_TABLE_IDS


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
    file_paths: Union[str, List[str]],
    time_coord: str = "time",
    allow_submonthly: bool = False,
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
            return _validate_monthly_files_individually(
                file_paths, time_coord, allow_submonthly=allow_submonthly
            )

        print(
            f"📅 Validated monthly data with calendar variations (detected: {detected_freq})"
        )
        return detected_freq

    except Exception as e:
        warnings.warn(f"Concatenation-based detection failed: {e}")
        return _validate_monthly_files_individually(
            file_paths, time_coord, allow_submonthly=allow_submonthly
        )


def _validate_monthly_files_individually(
    file_paths: List[str], time_coord: str = "time", allow_submonthly: bool = False
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
        # allow_submonthly=True: variables like tasmax/tasmin that aggregate daily→monthly
        if allow_submonthly and all(
            freq.total_seconds() < monthly_min for _, freq in non_monthly_files
        ):
            print(
                "📆 Sub-monthly input detected - valid for monthly aggregation (tasmax/tasmin)"
            )
        else:
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

    # Variables that require sub-monthly (e.g. daily) input for monthly aggregation
    cmor_var = compound_name.split(".")[-1] if compound_name else ""
    allow_submonthly = cmor_var in _SUBMONTHLY_INPUT_VARIABLES
    # Check if this is monthly data
    if _is_monthly_target(compound_name):
        # Use monthly-aware validation that allows calendar variations
        print(
            f"🗓️  Monthly CMIP6 table detected ({compound_name}) - using calendar-aware validation"
        )
        detected_freq = _validate_monthly_compatibility(
            file_paths, time_coord, allow_submonthly=allow_submonthly
        )
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
                    f"📅 Large tolerance detected ({auto_tolerance / 86400:.1f} days) - concatenated frequency sufficient"
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
            f"📏 Auto-determined tolerance: {tolerance_seconds / 86400:.1f} days ({tolerance_seconds:.0f}s) for frequency ~{base_freq}"
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
        error_msg += f"Tolerance: {tolerance_seconds}s ({tolerance_seconds / 86400:.2f} days)\n\n"
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
            return "h"
        else:
            return f"{int(hours)}h"
    elif total_seconds <= 86400:  # <= 1 day
        hours = total_seconds / 3600
        if hours == 24:
            return "D"  # Daily
        elif hours == 12:
            return "12h"
        elif hours == 6:
            return "6h"
        elif hours == 3:
            return "3h"
        else:
            return f"{int(hours)}h"
    elif total_seconds <= 86400 * 31:  # <= ~1 month
        days = total_seconds / 86400
        if 28 <= days <= 31:
            return "ME"  # Monthly (end of month)
        else:
            return f"{int(days)}D"
    elif total_seconds <= 86400 * 366:  # <= ~1 year
        days = total_seconds / 86400
        if 360 <= days <= 366:
            return "YE"  # Yearly (end of year)
        else:
            return f"{int(days)}D"
    else:
        # Multi-year or very long periods
        years = total_seconds / (86400 * 365.25)
        return f"{int(years)}YE"


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


def calculate_time_bounds(
    ds: xr.Dataset, time_coord: str = "time", bnds_name: str = "nv"
) -> xr.DataArray:
    """
    Calculate time bounds from time coordinate for CMIP6 compliance.
    Infers time bounds based on the temporal frequency of the data.
    Supports wide date ranges (0000-2200) using cftime.

    Handles three types of time coordinates:
    1. cftime objects
    2. numpy datetime64 objects
    3. numeric values with 'units' and 'calendar' attributes (converted from cftime)

    Parameters
    ----------
    ds : xr.Dataset
        Dataset containing time coordinate
    time_coord : str, default "time"
        Name of the time coordinate in the dataset
    bnds_name : str, default "nv"
        Name of the bounds dimension. Use "nv" for ocean data (default),
        or "bnds" for atmosphere data

    Returns
    -------
    xr.DataArray
        Time bounds array with shape (time_coord, bnds_name) where second dimension=2

    Raises
    ------
    ValueError
        If time coordinate is missing or cannot infer frequency
    """
    if time_coord not in ds.coords:
        raise ValueError(
            f"Dataset must contain '{time_coord}' coordinate to calculate time bounds"
        )

    time = ds[time_coord]
    n_times = len(time)

    if n_times < 2:
        raise ValueError("Need at least 2 time points to infer time bounds")

    # Get time values and attributes
    time_values = time.values
    calendar = time.attrs.get("calendar", "proleptic_gregorian")
    units = time.attrs.get("units")

    # Determine the type of time coordinate
    if time_values.size > 0:
        first_val = time_values.flat[0]
        is_cftime = isinstance(first_val, cftime.datetime)
        is_numeric_with_units = (
            isinstance(first_val, (int, float, np.integer, np.floating))
            and units is not None
        )
    else:
        raise ValueError(f"Time coordinate '{time_coord}' is empty")

    # Convert numeric+units to cftime for bounds calculation
    original_was_numeric = False
    if is_numeric_with_units:
        original_was_numeric = True
        # Convert numeric values to cftime objects for bounds calculation
        time_values = num2date(
            time_values, units=units, calendar=calendar, only_use_cftime_datetimes=True
        )
        is_cftime = True

    # Try to infer frequency
    freq = _infer_frequency(time_values)

    # Initialize bounds array
    time_bnds = np.empty((n_times, 2), dtype=object if is_cftime else time_values.dtype)

    if freq == "monthly":
        time_bnds = _calculate_monthly_bounds(time_values, calendar, is_cftime)

    elif freq == "daily":
        time_bnds = _calculate_daily_bounds(time_values, calendar, is_cftime)

    elif freq == "yearly":
        time_bnds = _calculate_yearly_bounds(time_values, calendar, is_cftime)

    else:
        time_bnds = _calculate_midpoint_bounds(time_values)

    # Convert bounds back to numeric if original was numeric
    if original_was_numeric:
        time_bnds_numeric = np.empty((n_times, 2), dtype=np.float64)
        time_bnds_numeric[:, 0] = date2num(
            time_bnds[:, 0], units=units, calendar=calendar
        )
        time_bnds_numeric[:, 1] = date2num(
            time_bnds[:, 1], units=units, calendar=calendar
        )
        time_bnds = time_bnds_numeric

    # Build attributes dictionary - start with long_name only
    attrs = {"long_name": "time bounds"}

    # Only add units if present in time coordinate
    if "units" in time.attrs:
        attrs["units"] = time.attrs["units"]

    # Add calendar attribute if present
    if "calendar" in time.attrs:
        attrs["calendar"] = time.attrs["calendar"]
    elif is_cftime and hasattr(time_values[0], "calendar"):
        attrs["calendar"] = time_values[0].calendar

    # Create DataArray with proper dimensions and attributes
    time_bnds_da = xr.DataArray(
        time_bnds,
        dims=[time_coord, bnds_name],
        coords={time_coord: time, bnds_name: np.array([0, 1])},
        attrs=attrs,
    )

    return time_bnds_da


def _infer_frequency(time_values) -> Optional[str]:
    """Infer temporal frequency from time values."""
    if len(time_values) < 2:
        return None

    # Calculate time differences
    if hasattr(time_values[0], "calendar"):  # cftime
        diffs = [
            (time_values[i + 1] - time_values[i]).days
            for i in range(min(10, len(time_values) - 1))
        ]
    else:  # numpy datetime64
        diffs = (
            np.diff(time_values[: min(11, len(time_values))])
            .astype("timedelta64[D]")
            .astype(int)
        )

    avg_diff = np.mean(diffs)

    # Classify frequency based on average difference
    if 28 <= avg_diff <= 31:
        return "monthly"
    elif 0.9 <= avg_diff <= 1.1:
        return "daily"
    elif 365 <= avg_diff <= 366:
        return "yearly"
    else:
        return "irregular"


def _calculate_monthly_bounds(time_values, calendar: str, is_cftime: bool):
    """Calculate bounds for monthly data."""
    n_times = len(time_values)
    bounds = np.empty((n_times, 2), dtype=time_values.dtype)

    if is_cftime:
        actual_calendar = (
            time_values[0].calendar if hasattr(time_values[0], "calendar") else calendar
        )

        for i, t in enumerate(time_values):
            # Start of month - use the actual calendar from time values
            bounds[i, 0] = cftime.datetime(t.year, t.month, 1, calendar=actual_calendar)
            # End of month = start of next month
            if t.month == 12:
                bounds[i, 1] = cftime.datetime(
                    t.year + 1, 1, 1, calendar=actual_calendar
                )
            else:
                bounds[i, 1] = cftime.datetime(
                    t.year, t.month + 1, 1, calendar=actual_calendar
                )
    else:
        # Use numpy datetime64
        for i, t in enumerate(time_values):
            t_dt = np.datetime64(t, "D")
            year = t_dt.astype("datetime64[Y]").astype(int) + 1970
            month = t_dt.astype("datetime64[M]").astype(int) % 12 + 1

            # Start of month
            bounds[i, 0] = np.datetime64(f"{year:04d}-{month:02d}-01")
            # End of month
            if month == 12:
                bounds[i, 1] = np.datetime64(f"{year + 1:04d}-01-01")
            else:
                bounds[i, 1] = np.datetime64(f"{year:04d}-{month + 1:02d}-01")

    return bounds


def _calculate_daily_bounds(time_values, calendar: str, is_cftime: bool):
    """Calculate bounds for daily data."""
    n_times = len(time_values)
    bounds = np.empty((n_times, 2), dtype=time_values.dtype)

    if is_cftime:
        actual_calendar = (
            time_values[0].calendar if hasattr(time_values[0], "calendar") else calendar
        )

        for i, t in enumerate(time_values):
            bounds[i, 0] = cftime.datetime(
                t.year, t.month, t.day, calendar=actual_calendar
            )
            # Add one day
            next_day = t + timedelta(days=1)
            bounds[i, 1] = cftime.datetime(
                next_day.year, next_day.month, next_day.day, calendar=actual_calendar
            )
    else:
        for i, t in enumerate(time_values):
            t_day = np.datetime64(t, "D")
            bounds[i, 0] = t_day
            bounds[i, 1] = t_day + np.timedelta64(1, "D")

    return bounds


def _calculate_yearly_bounds(time_values, calendar: str, is_cftime: bool):
    """Calculate bounds for yearly data."""
    n_times = len(time_values)
    bounds = np.empty((n_times, 2), dtype=time_values.dtype)

    if is_cftime:
        actual_calendar = (
            time_values[0].calendar if hasattr(time_values[0], "calendar") else calendar
        )

        for i, t in enumerate(time_values):
            bounds[i, 0] = cftime.datetime(t.year, 1, 1, calendar=actual_calendar)
            bounds[i, 1] = cftime.datetime(t.year + 1, 1, 1, calendar=actual_calendar)
    else:
        for i, t in enumerate(time_values):
            year = np.datetime64(t, "Y").astype(int) + 1970
            bounds[i, 0] = np.datetime64(f"{year:04d}-01-01")
            bounds[i, 1] = np.datetime64(f"{year + 1:04d}-01-01")

    return bounds


def _calculate_midpoint_bounds(time_values):
    """Calculate bounds using midpoint method for irregular data."""
    n_times = len(time_values)
    bounds = np.empty((n_times, 2), dtype=time_values.dtype)

    # First bound: extrapolate backward
    if hasattr(time_values[0], "calendar"):  # cftime
        dt_first = time_values[1] - time_values[0]
        bounds[0, 0] = time_values[0] - dt_first / 2
        bounds[0, 1] = time_values[0] + (time_values[1] - time_values[0]) / 2
    else:  # numpy datetime64
        dt_first = time_values[1] - time_values[0]
        bounds[0, 0] = time_values[0] - dt_first / 2
        bounds[0, 1] = time_values[0] + (time_values[1] - time_values[0]) / 2

    # Middle bounds: midpoint between adjacent times
    for i in range(1, n_times - 1):
        bounds[i, 0] = time_values[i - 1] + (time_values[i] - time_values[i - 1]) / 2
        bounds[i, 1] = time_values[i] + (time_values[i + 1] - time_values[i]) / 2

    # Last bound: extrapolate forward
    if hasattr(time_values[-1], "calendar"):  # cftime
        dt_last = time_values[-1] - time_values[-2]
        bounds[-1, 0] = time_values[-1] - dt_last / 2
        bounds[-1, 1] = time_values[-1] + dt_last / 2
    else:
        dt_last = time_values[-1] - time_values[-2]
        bounds[-1, 0] = time_values[-1] - dt_last / 2
        bounds[-1, 1] = time_values[-1] + dt_last / 2

    return bounds


def calculate_latitude_bounds(
    ds: xr.Dataset, lat_coord: str = "lat", bnds_name: str = "bnds"
) -> xr.DataArray:
    """
    Calculate latitude bounds for CMIP6 compliance.

    This function calculates the boundaries of each latitude cell by finding
    midpoints between adjacent latitude values. For the first and last cells,
    it extrapolates the boundaries based on the grid spacing.

    Handles both regular (uniform spacing) and irregular latitude grids.
    Ensures all bounds are within the valid latitude range [-90°, 90°].

    Args:
        ds: xarray Dataset containing the latitude coordinate
        lat_coord: Name of the latitude coordinate (default: "lat")
        bnds_name: Name of the bounds dimension (default: "bnds", ocean models use "nv")

    Returns:
        xarray DataArray with dimensions (lat_coord, bnds_name) containing the bounds

    Raises:
        ValueError: If latitude coordinate is missing or has insufficient points
    """
    if lat_coord not in ds.coords:
        raise ValueError(f"Latitude coordinate '{lat_coord}' not found in dataset")

    lat_var = ds[lat_coord]
    lat_values = lat_var.values

    if len(lat_values) < 2:
        raise ValueError(
            f"Need at least 2 latitude points to calculate bounds, got {len(lat_values)}"
        )

    # Initialize bounds array
    bounds = np.zeros((len(lat_values), 2), dtype="float64")

    # Check if latitude grid is regular (uniform spacing)
    lat_diffs = np.diff(lat_values)
    is_regular = np.allclose(lat_diffs, lat_diffs[0], rtol=1e-6)

    if is_regular:
        # Regular grid: use half the grid spacing
        spacing = lat_diffs[0]
        bounds[:, 0] = lat_values - spacing / 2
        bounds[:, 1] = lat_values + spacing / 2

    else:
        # Irregular grid: calculate bounds using midpoints between adjacent points
        for i in range(len(lat_values)):
            if i == 0:
                # First point: extrapolate backward using spacing to next point
                spacing_forward = lat_values[i + 1] - lat_values[i]
                bounds[i, 0] = lat_values[i] - spacing_forward / 2
                bounds[i, 1] = (lat_values[i] + lat_values[i + 1]) / 2
            elif i == len(lat_values) - 1:
                # Last point: extrapolate forward using spacing from previous point
                spacing_backward = lat_values[i] - lat_values[i - 1]
                bounds[i, 0] = (lat_values[i - 1] + lat_values[i]) / 2
                bounds[i, 1] = lat_values[i] + spacing_backward / 2
            else:
                # Middle points: midpoint between adjacent values
                bounds[i, 0] = (lat_values[i - 1] + lat_values[i]) / 2
                bounds[i, 1] = (lat_values[i] + lat_values[i + 1]) / 2

    # Clip bounds to valid latitude range [-90°, 90°]
    bounds = np.clip(bounds, -90.0, 90.0)

    # Verify bounds are monotonic
    if not np.all(bounds[1:, 0] >= bounds[:-1, 1]):
        warnings.warn(
            "Calculated latitude bounds are not monotonic. This may indicate "
            "issues with the input latitude coordinate ordering."
        )

    # Create and return xarray DataArray with specified bnds_name
    return xr.DataArray(
        bounds,
        dims=(lat_coord, bnds_name),
        attrs={},  # No attributes for bounds variables per CMIP6 standards
    )


def calculate_longitude_bounds(
    ds: xr.Dataset, lon_coord: str = "lon", bnds_name: str = "bnds"
) -> xr.DataArray:
    """
    Calculate longitude bounds for CMIP6 compliance.

    This function handles both 0-360° and -180-180° longitude conventions,
    and properly accounts for periodic boundary conditions at the date line.
    It distinguishes between global grids (wrapping around the globe) and
    regional grids (covering only part of the globe).

    Handles both regular (uniform spacing) and irregular longitude grids.

    Args:
        ds: xarray Dataset containing the longitude coordinate
        lon_coord: Name of the longitude coordinate (default: "lon")
        bnds_name: Name of the bounds dimension (default: "bnds", ocean models use "nv")

    Returns:
        xarray DataArray with dimensions (lon_coord, bnds_name) containing the bounds

    Raises:
        ValueError: If longitude coordinate is missing, has insufficient points,
                   or contains values outside expected ranges
    """
    if lon_coord not in ds.coords:
        raise ValueError(f"Longitude coordinate '{lon_coord}' not found in dataset")

    lon_var = ds[lon_coord]
    lon_values = lon_var.values

    if len(lon_values) < 2:
        raise ValueError(
            f"Need at least 2 longitude points to calculate bounds, got {len(lon_values)}"
        )

    # Initialize bounds array
    bounds = np.zeros((len(lon_values), 2), dtype="float64")

    # Detect longitude convention and validate range
    lon_min, lon_max = lon_values.min(), lon_values.max()

    if -1e-6 <= lon_min and lon_max <= 360.0 + 1e-6:
        if lon_max <= 180.0 + 1e-6:
            convention = "-180-180"
        else:
            convention = "0-360"
    else:
        raise ValueError(
            f"Longitude values outside expected range. "
            f"Found: [{lon_min:.2f}, {lon_max:.2f}]. "
            f"Expected: [0, 360] or [-180, 180]"
        )

    # Check if longitude grid is regular (uniform spacing)
    lon_diffs = np.diff(lon_values)
    is_regular = np.allclose(lon_diffs, lon_diffs[0], rtol=1e-6)

    # Determine if this is a global grid (wraps around the Earth)
    is_global = False

    if is_regular:
        spacing = lon_diffs[0]

        # Check if grid is global by comparing total span + one spacing to 360°
        total_span = lon_max - lon_min
        expected_global_span = 360.0 - spacing

        # Also check if the wrap-around spacing matches the regular spacing
        if convention == "0-360":
            wrap_spacing = (lon_values[0] + 360.0) - lon_values[-1]
        else:  # -180-180
            if lon_values[0] < 0 and lon_values[-1] > 0:
                # Grid crosses the date line
                wrap_spacing = (lon_values[0] + 360.0) - lon_values[-1]
            else:
                wrap_spacing = (lon_values[0] + 360.0) - lon_values[-1]

        # Grid is global if:
        # 1. Total span is close to 360° - spacing, OR
        # 2. Wrap-around spacing matches the regular spacing
        is_global = (
            np.abs(total_span - expected_global_span) < abs(spacing) * 0.1
            or np.abs(wrap_spacing - spacing) < abs(spacing) * 0.1
        )

        # Calculate bounds for regular grid
        bounds[:, 0] = lon_values - spacing / 2
        bounds[:, 1] = lon_values + spacing / 2

    else:
        # Irregular grid
        for i in range(len(lon_values)):
            if i == 0:
                # First point: check if grid might be global
                spacing_forward = lon_values[i + 1] - lon_values[i]

                # Calculate potential wrap-around spacing
                if convention == "0-360":
                    wrap_spacing = (lon_values[i] + 360.0) - lon_values[-1]
                else:
                    wrap_spacing = (lon_values[i] + 360.0) - lon_values[-1]

                # If wrap spacing is similar to forward spacing, likely global
                if np.abs(wrap_spacing - spacing_forward) < abs(spacing_forward) * 0.2:
                    is_global = True
                    bounds[i, 0] = lon_values[i] - wrap_spacing / 2
                else:
                    bounds[i, 0] = lon_values[i] - spacing_forward / 2

                bounds[i, 1] = (lon_values[i] + lon_values[i + 1]) / 2

            elif i == len(lon_values) - 1:
                # Last point: use wrap-around if global
                spacing_backward = lon_values[i] - lon_values[i - 1]
                bounds[i, 0] = (lon_values[i - 1] + lon_values[i]) / 2

                if is_global:
                    if convention == "0-360":
                        wrap_spacing = (lon_values[0] + 360.0) - lon_values[i]
                    else:
                        wrap_spacing = (lon_values[0] + 360.0) - lon_values[i]
                    bounds[i, 1] = lon_values[i] + wrap_spacing / 2
                else:
                    bounds[i, 1] = lon_values[i] + spacing_backward / 2

            else:
                # Middle points: midpoint between adjacent values
                bounds[i, 0] = (lon_values[i - 1] + lon_values[i]) / 2
                bounds[i, 1] = (lon_values[i] + lon_values[i + 1]) / 2

    # Normalize bounds to the detected convention
    if convention == "0-360":
        # Ensure all bounds are in [0, 360] range
        bounds = np.where(bounds < 0, bounds + 360, bounds)
        bounds = np.where(bounds > 360, bounds - 360, bounds)
    else:  # -180-180
        # Ensure all bounds are in [-180, 180] range
        bounds = np.where(bounds > 180, bounds - 360, bounds)
        bounds = np.where(bounds < -180, bounds + 360, bounds)

    # Additional check for global grids: ensure continuity at boundaries
    if is_global:
        # For global grids, the last cell's upper bound should connect to first cell's lower bound
        if convention == "0-360":
            # Check if bounds wrap correctly at 0°/360°
            if bounds[-1, 1] > 360:
                bounds[-1, 1] -= 360
            if bounds[0, 0] < 0:
                bounds[0, 0] += 360
        else:  # -180-180
            # Check if bounds wrap correctly at -180°/180°
            if bounds[-1, 1] > 180:
                bounds[-1, 1] -= 360
            if bounds[0, 0] < -180:
                bounds[0, 0] += 360

    # Create and return xarray DataArray with specified bnds_name
    return xr.DataArray(
        bounds,
        dims=(lon_coord, bnds_name),
        attrs={},  # No attributes for bounds variables per CMIP6 standards
    )


def generate_cmip7_to_cmip6_mapping(
    version: str = "latest_stable", output_path: Optional[str] = None
) -> Dict[str, str]:
    """
    Generate a mapping from CMIP7 compound names to CMIP6 compound names.

    This function uses the data_request_api to query the latest CMIP7 data request
    and creates a mapping between CMIP7 and CMIP6 compound names. The mapping
    is optionally saved to a JSON file.

    Note: This function internally calls generate_both_cmip_mappings() for efficiency
    and only returns the forward mapping.

    Args:
        version: Version of the data request to use (default: "latest_stable")
        output_path: Optional path to save the JSON mapping file. If None,
                    saves to the vocabularies directory within the package.

    Returns:
        Dictionary mapping CMIP7 compound names to CMIP6 compound names

    Raises:
        ImportError: If data_request_api package is not available

    Example:
        >>> mapping = generate_cmip7_to_cmip6_mapping()
        >>> print(mapping["Amon.tas"])  # Should return corresponding CMIP6 name
    """
    if not DATA_REQUEST_API_AVAILABLE:
        raise ImportError(
            "data_request_api package is required for generating CMIP7 mappings. "
            "Install it with: pip install CMIP7-data-request-api"
        )

    # Generate both mappings efficiently and return only the forward mapping
    forward_mapping, _ = generate_both_cmip_mappings(
        version=version,
        forward_output_path=output_path,
        reverse_output_path=None,  # Use default path for reverse mapping
    )
    return forward_mapping


def load_cmip7_to_cmip6_mapping(mapping_path: Optional[str] = None) -> Dict[str, str]:
    """
    Load the CMIP7 to CMIP6 compound name mapping from a JSON file.

    Args:
        mapping_path: Optional path to the mapping JSON file. If None,
                     loads from the default location in the vocabularies directory.

    Returns:
        Dictionary mapping CMIP7 compound names to CMIP6 compound names

    Raises:
        FileNotFoundError: If the mapping file doesn't exist
        json.JSONDecodeError: If the mapping file contains invalid JSON
    """
    if mapping_path is None:
        # Default to mappings directory within the package
        import access_moppy

        package_path = Path(access_moppy.__file__).parent
        mappings_path = package_path / "mappings"
        mapping_path = mappings_path / "cmip7_to_cmip6_compound_name_mapping.json"
    else:
        mapping_path = Path(mapping_path)

    if not mapping_path.exists():
        raise FileNotFoundError(
            f"CMIP7 to CMIP6 mapping file not found: {mapping_path}. "
            "Please run generate_cmip7_to_cmip6_mapping() first to create it."
        )

    with open(mapping_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Filter out metadata if present
    mapping = {k: v for k, v in data.items() if not k.startswith("_")}

    print(f"✓ Loaded mapping for {len(mapping)} variables from: {mapping_path}")
    return mapping


def generate_cmip6_to_cmip7_mapping(
    version: str = "latest_stable", output_path: Optional[str] = None
) -> Dict[str, str]:
    """
    Generate a reverse mapping from CMIP6 compound names to CMIP7 compound names.

    This function uses the data_request_api to query the latest CMIP7 data request
    and creates a reverse mapping between CMIP6 and CMIP7 compound names. The mapping
    is optionally saved to a JSON file.

    Note: This function internally calls generate_both_cmip_mappings() for efficiency
    and only returns the reverse mapping.

    Args:
        version: Version of the data request to use (default: "latest_stable")
        output_path: Optional path to save the JSON mapping file. If None,
                    saves to the vocabularies directory within the package.

    Returns:
        Dictionary mapping CMIP6 compound names to CMIP7 compound names

    Raises:
        ImportError: If data_request_api package is not available

    Example:
        >>> reverse_mapping = generate_cmip6_to_cmip7_mapping()
        >>> print(reverse_mapping["AERmon.abs550aer"])  # Should return corresponding CMIP7 name
    """
    if not DATA_REQUEST_API_AVAILABLE:
        raise ImportError(
            "data_request_api package is required for generating CMIP7 mappings. "
            "Install it with: pip install CMIP7-data-request-api"
        )

    # Generate both mappings efficiently and return only the reverse mapping
    _, reverse_mapping = generate_both_cmip_mappings(
        version=version,
        forward_output_path=None,  # Use default path for forward mapping
        reverse_output_path=output_path,
    )
    return reverse_mapping


def load_cmip6_to_cmip7_mapping(mapping_path: Optional[str] = None) -> Dict[str, str]:
    """
    Load the CMIP6 to CMIP7 compound name mapping from a JSON file.

    Args:
        mapping_path: Optional path to the mapping JSON file. If None,
                     loads from the default location in the vocabularies directory.

    Returns:
        Dictionary mapping CMIP6 compound names to CMIP7 compound names

    Raises:
        FileNotFoundError: If the mapping file doesn't exist
        json.JSONDecodeError: If the mapping file contains invalid JSON
    """
    if mapping_path is None:
        # Default to mappings directory within the package
        import access_moppy

        package_path = Path(access_moppy.__file__).parent
        mappings_path = package_path / "mappings"
        mapping_path = mappings_path / "cmip6_to_cmip7_compound_name_mapping.json"
    else:
        mapping_path = Path(mapping_path)

    if not mapping_path.exists():
        raise FileNotFoundError(
            f"CMIP6 to CMIP7 mapping file not found: {mapping_path}. "
            "Please run generate_cmip6_to_cmip7_mapping() first to create it."
        )

    with open(mapping_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Filter out metadata if present
    mapping = {k: v for k, v in data.items() if not k.startswith("_")}

    print(f"✓ Loaded reverse mapping for {len(mapping)} variables from: {mapping_path}")
    return mapping


def get_requested_variables_from_data_request(
    experiment: str = "historical",
    priority: str = "Core",
    variable_name: str = "CMIP6",
    dreq_version: str = "v1.2.2.3",
) -> List[str]:
    """
    Return requested variables for a given experiment and priority from a data request.

    Args:
        experiment: Experiment key under ``requested["experiment"]``.
        priority: Priority class key (for example ``"Core"``).
        variable_name: Variable naming convention to use; must be ``"CMIP6"`` or
            ``"CMIP7"``.
        dreq_version: Data request version string to retrieve.

    Returns:
        A list of requested variable names.

    Raises:
        ValueError: If ``variable_name`` is not ``"CMIP6"`` or ``"CMIP7"``.
        ImportError: If ``DATA_REQUEST_API_AVAILABLE`` is ``False``.

    Example:
        >>> get_requested_variables_from_data_request("historical", "Core")
    """
    if variable_name not in ("CMIP6", "CMIP7"):
        raise ValueError(
            f"Invalid variable_name {variable_name!r}. Must be 'CMIP6' or 'CMIP7'."
        )

    if not DATA_REQUEST_API_AVAILABLE:
        raise ImportError(
            "data_request_api package is required for querying requested variables. "
            "Install it with: pip install CMIP7-data-request-api"
        )

    from data_request_api.content import dreq_content as dc
    from data_request_api.query import dreq_query as dq
    from data_request_api.utilities.config import update_config

    update_config("variable_name", f"{variable_name} Compound Name")

    dc.retrieve(dreq_version)
    dreq_content = dc.load(dreq_version)
    dreq_tables = dq.create_dreq_tables_for_request(
        content=dreq_content,
        dreq_version=dreq_version,
    )
    requested = dq.get_requested_variables(
        content=dreq_tables,
        dreq_version=dreq_version,
        verbose=False,
        check_core_variables=False,
        priority_cutoff=priority.lower(),
    )

    if experiment not in requested.get("experiment", {}):
        raise KeyError(f"Experiment '{experiment}' not found in data request.")

    if priority.capitalize() not in requested["experiment"][experiment]:
        raise KeyError(
            f"Priority '{priority}' not found for experiment '{experiment}' in data request."
        )

    return list(requested["experiment"][experiment][priority.capitalize()])


def get_cmip_mapping_metadata(mapping_type: str = "forward") -> Dict:
    """
    Get metadata about the CMIP7↔CMIP6 compound name mapping files.

    Args:
        mapping_type: Either "forward" (CMIP7→CMIP6) or "reverse" (CMIP6→CMIP7)

    Returns:
        Dictionary containing metadata about the mapping file

    Raises:
        ValueError: If mapping_type is not "forward" or "reverse"
        FileNotFoundError: If the mapping file doesn't exist
    """
    if mapping_type not in ["forward", "reverse"]:
        raise ValueError("mapping_type must be 'forward' or 'reverse'")

    import access_moppy

    package_path = Path(access_moppy.__file__).parent
    mappings_path = package_path / "mappings"

    if mapping_type == "forward":
        mapping_path = mappings_path / "cmip7_to_cmip6_compound_name_mapping.json"
    else:
        mapping_path = mappings_path / "cmip6_to_cmip7_compound_name_mapping.json"

    if not mapping_path.exists():
        raise FileNotFoundError(
            f"Mapping file not found: {mapping_path}. "
            f"Please run generate_both_cmip_mappings() first to create it."
        )

    with open(mapping_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract metadata
    metadata = data.get("_metadata", {})

    if not metadata:
        return {"error": "No metadata found in file"}

    return metadata


def generate_both_cmip_mappings(
    version: str = "latest_stable",
    forward_output_path: Optional[str] = None,
    reverse_output_path: Optional[str] = None,
) -> tuple[Dict[str, str], Dict[str, str]]:
    """
    Generate both forward (CMIP7->CMIP6) and reverse (CMIP6->CMIP7) mappings efficiently.

    This function queries the CMIP7 data request API once and generates both mappings
    from the same data, which is more efficient than calling the individual functions separately.

    Args:
        version: Version of the data request to use (default: "latest_stable")
        forward_output_path: Optional path for CMIP7->CMIP6 mapping JSON file
        reverse_output_path: Optional path for CMIP6->CMIP7 mapping JSON file

    Returns:
        Tuple of (forward_mapping, reverse_mapping) dictionaries

    Raises:
        ImportError: If data_request_api package is not available

    Example:
        >>> forward, reverse = generate_both_cmip_mappings()
        >>> cmip6_name = forward["aerosol.abs550aer.tavg-u-hxy-u.mon.GLB"]
        >>> cmip7_name = reverse[cmip6_name]
    """
    if not DATA_REQUEST_API_AVAILABLE:
        raise ImportError(
            "data_request_api package is required for generating CMIP7 mappings. "
            "Install it with: pip install CMIP7-data-request-api"
        )

    print("Generating both CMIP7<->CMIP6 compound name mappings...")
    print("This may take a moment as it queries the CMIP7 data request API...")

    # Use the latest_stable version of the DR content (default)
    content_dic = dt.get_transformed_content(
        version=version, force_variable_name="CMIP7 Compound Name"
    )

    # Create DataRequest object from the content
    DR = dr.DataRequest.from_separated_inputs(**content_dic)

    # Find all CMIP7 variables
    cmip7_variables = DR.find_variables(operation="any", skip_if_missing=True)

    # Create both mapping dictionaries
    forward_mapping = {}  # CMIP7 -> CMIP6
    reverse_mapping = {}  # CMIP6 -> CMIP7

    for var in cmip7_variables:
        if hasattr(var, "cmip7_compound_name") and hasattr(var, "cmip6_compound_name"):
            if var.cmip7_compound_name and var.cmip6_compound_name:
                cmip7_name = var.cmip7_compound_name.name
                cmip6_name = var.cmip6_compound_name.name

                # Forward mapping (CMIP7 -> CMIP6)
                forward_mapping[cmip7_name] = cmip6_name

                # Reverse mapping (CMIP6 -> CMIP7) - handle potential one-to-many
                if cmip6_name in reverse_mapping:
                    # If CMIP6 name already exists, store as list
                    if isinstance(reverse_mapping[cmip6_name], str):
                        reverse_mapping[cmip6_name] = [
                            reverse_mapping[cmip6_name],
                            cmip7_name,
                        ]
                    else:
                        reverse_mapping[cmip6_name].append(cmip7_name)
                else:
                    reverse_mapping[cmip6_name] = cmip7_name

    # Save forward mapping
    if forward_output_path is None:
        import access_moppy

        package_path = Path(access_moppy.__file__).parent
        mappings_path = package_path / "mappings"
        forward_output_path = (
            mappings_path / "cmip7_to_cmip6_compound_name_mapping.json"
        )
    else:
        forward_output_path = Path(forward_output_path)

    # Create metadata for the JSON files
    from datetime import datetime

    metadata = {
        "_metadata": {
            "description": "CMIP7 to CMIP6 compound name mapping",
            "generated_by": "access_moppy.utilities.generate_both_cmip_mappings()",
            "source": "CMIP7 Data Request API",
            "data_request_version": version,
            "generated_on": datetime.now().isoformat(),
            "total_mappings": len(forward_mapping),
            "usage": "Use access_moppy.utilities.load_cmip7_to_cmip6_mapping() to load this file",
        }
    }

    reverse_metadata = {
        "_metadata": {
            "description": "CMIP6 to CMIP7 compound name mapping (reverse of forward mapping)",
            "generated_by": "access_moppy.utilities.generate_both_cmip_mappings()",
            "source": "CMIP7 Data Request API",
            "data_request_version": version,
            "generated_on": datetime.now().isoformat(),
            "total_mappings": len(reverse_mapping),
            "usage": "Use access_moppy.utilities.load_cmip6_to_cmip7_mapping() to load this file",
            "note": "Some CMIP6 names may map to multiple CMIP7 names (stored as arrays)",
        }
    }

    # Combine metadata with mappings
    forward_data = {**metadata, **forward_mapping}
    reverse_data = {**reverse_metadata, **reverse_mapping}

    forward_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(forward_output_path, "w", encoding="utf-8") as f:
        json.dump(forward_data, f, indent=2, sort_keys=True)

    # Save reverse mapping
    if reverse_output_path is None:
        import access_moppy

        package_path = Path(access_moppy.__file__).parent
        mappings_path = package_path / "mappings"
        reverse_output_path = (
            mappings_path / "cmip6_to_cmip7_compound_name_mapping.json"
        )
    else:
        reverse_output_path = Path(reverse_output_path)

    reverse_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(reverse_output_path, "w", encoding="utf-8") as f:
        json.dump(reverse_data, f, indent=2, sort_keys=True)

    print(f"✓ Generated forward mapping for {len(forward_mapping)} variables")
    print(f"✓ Saved forward mapping to: {forward_output_path}")
    print(f"✓ Generated reverse mapping for {len(reverse_mapping)} variables")
    print(f"✓ Saved reverse mapping to: {reverse_output_path}")

    return forward_mapping, reverse_mapping


def create_ilamb_symlinks(
    output_dir: Union[str, Path],
    ilamb_dir: Union[str, Path],
    drs_format: str = "auto",
    overwrite: bool = False,
) -> Dict[str, Path]:
    """
    Create a flat directory of ``<variable_id>.nc`` symlinks for ILAMB input.

    Scans MOPPY output and creates a symbolic link named ``<variable_id>.nc``
    for each variable found, pointing to the original NetCDF file.  Both
    output formats produced by MOPPY are supported:

    * **flat DRS** – ``.nc`` files written directly into *output_dir* with
      names like ``<variable_id>_<table_id>_…[_<time_range>].nc``.
    * **CMIP6 DRS** – ``.nc`` files nested inside the standard directory
      hierarchy
      ``<mip_era>/<activity_id>/…/<variable_id>/<grid_label>/<version>/``.

    Format auto-detection (``drs_format='auto'``): if ``.nc`` files are
    present directly inside *output_dir* the format is treated as **flat**;
    otherwise it is treated as **cmip6**.

    Args:
        output_dir: Root directory of MOPPY output to scan.
        ilamb_dir: Directory in which symlinks are created (created if absent).
        drs_format: ``'flat'``, ``'cmip6'``, or ``'auto'`` (default).
        overwrite: Replace existing symlinks when ``True``. Default ``False``.

    Returns:
        Mapping of *variable_id* to the :class:`~pathlib.Path` of each
        created symlink.

    Raises:
        FileNotFoundError: If *output_dir* does not exist.
        ValueError: If *drs_format* is not a recognised value, or if multiple
            source files are found for the same variable_id (time-chunked
            output must be concatenated before building ILAMB symlinks).

    Examples:
        >>> # Flat DRS output
        >>> links = create_ilamb_symlinks("/path/to/flat_output", "/path/to/ilamb_input")

        >>> # CMIP6 DRS output
        >>> links = create_ilamb_symlinks(
        ...     "/path/to/drs_root",
        ...     "/path/to/ilamb_input",
        ...     drs_format="cmip6",
        ... )
    """
    output_dir = Path(output_dir).resolve()
    ilamb_dir = Path(ilamb_dir).resolve()

    if not output_dir.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output_dir}")

    valid_formats = ("flat", "cmip6", "auto")
    if drs_format not in valid_formats:
        raise ValueError(
            f"Invalid drs_format {drs_format!r}. Expected one of {valid_formats}."
        )

    if drs_format == "auto":
        drs_format = "flat" if any(output_dir.glob("*.nc")) else "cmip6"

    if drs_format == "flat":
        nc_files = sorted(output_dir.glob("*.nc"))
    else:
        # Recursively find all .nc files, but skip anything already inside ilamb_dir
        # to avoid following previously created symlinks back into the scan.
        ilamb_prefix = str(ilamb_dir) + "/"
        nc_files = sorted(
            f for f in output_dir.rglob("*.nc") if not str(f).startswith(ilamb_prefix)
        )

    if not nc_files:
        warnings.warn(f"No .nc files found in {output_dir} (drs_format={drs_format!r})")
        return {}

    # Group by variable_id (first '_'-delimited component of the stem)
    variable_files: Dict[str, List[Path]] = {}
    for nc_file in nc_files:
        variable_id = nc_file.stem.split("_")[0]
        variable_files.setdefault(variable_id, []).append(nc_file)

    # ILAMB needs exactly one file per variable; time-chunked output must be
    # concatenated first.
    multi_file_vars = {v: fs for v, fs in variable_files.items() if len(fs) > 1}
    if multi_file_vars:
        details = "\n".join(
            f"  {v}:\n" + "\n".join(f"    {f}" for f in sorted(fs))
            for v, fs in sorted(multi_file_vars.items())
        )
        raise ValueError(
            "Multiple source files found for the same variable_id. "
            "Concatenate time-chunked files before creating ILAMB symlinks:\n" + details
        )

    ilamb_dir.mkdir(parents=True, exist_ok=True)

    created: Dict[str, Path] = {}
    for variable_id, (src_file,) in sorted(variable_files.items()):
        link_path = ilamb_dir / f"{variable_id}.nc"

        if link_path.exists() or link_path.is_symlink():
            if overwrite:
                link_path.unlink()
            else:
                warnings.warn(
                    f"Symlink already exists and overwrite=False: {link_path}"
                )
                continue

        link_path.symlink_to(src_file)
        created[variable_id] = link_path

    return created
