"""
Improved ocean file finding function for ACCESS-MOPPy tests.

This module provides utilities to find ocean data files for testing ocean variables.
"""

import glob
from pathlib import Path
from typing import List

from access_moppy.utilities import load_model_mappings

# Default paths for ocean data
ROOT_FOLDER = "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/spinup/Dec25-PI-control/"
TARGET_FOLDERS = "output40[0-9]/ocean/"


def get_monthly_ocean_files(
    compound_name: str,
    root_folder: str = ROOT_FOLDER,
    target_folders: str = TARGET_FOLDERS,
    model_id: str = "ACCESS-ESM1.6",
) -> List[str]:
    """
    Find ocean data files for a given CMOR variable.

    Args:
        compound_name: CMOR compound name (e.g., 'Omon.evs')
        root_folder: Root directory to search for files
        target_folders: Target folder pattern relative to root_folder
        model_id: Model identifier for loading mappings

    Returns:
        List of absolute paths to matching ocean files

    Raises:
        ValueError: If compound_name format is invalid
        FileNotFoundError: If root directory doesn't exist
    """
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
        print(f"Warning: Could not load mapping for {compound_name}: {e}")
        return []

    if not mapping:
        print(f"No mapping found for {compound_name}")
        return []

    # Get model variables from mapping
    model_variables = mapping[variable_name].get("model_variables", [])
    if not model_variables:
        print(f"No model variables found in mapping for {compound_name}")
        return []

    # Search for files
    files_found = []
    search_pattern_base = str(root_path / target_folders)

    for model_variable in model_variables:
        # Ocean files typically have pattern: *-{model_variable}-1monthly-mean*.nc
        filename_pattern = f"*-{model_variable}-1monthly-mean*.nc"
        search_pattern = search_pattern_base + "/" + filename_pattern

        try:
            matching_files = glob.glob(search_pattern)
            files_found.extend(matching_files)

            if not matching_files:
                print(
                    f"No files found for model variable '{model_variable}' with pattern: {search_pattern}"
                )
            else:
                print(
                    f"Found {len(matching_files)} files for model variable '{model_variable}'"
                )

        except Exception as e:
            print(f"Error searching for files with pattern '{search_pattern}': {e}")

    # Remove duplicates and sort
    files_found = sorted(list(set(files_found)))

    if not files_found:
        print(f"No ocean files found for {compound_name} in {search_pattern_base}")
    else:
        print(f"Total files found for {compound_name}: {len(files_found)}")

    return files_found


def check_ocean_data_availability(
    root_folder: str = ROOT_FOLDER, target_folders: str = TARGET_FOLDERS
) -> bool:
    """
    Check if ocean data directory structure exists.

    Args:
        root_folder: Root directory to check
        target_folders: Target folder pattern to check

    Returns:
        True if data directory exists, False otherwise
    """
    root_path = Path(root_folder)
    if not root_path.exists():
        return False

    # Check if any matching ocean folders exist
    search_pattern = str(root_path / target_folders)
    try:
        matching_dirs = glob.glob(search_pattern)
        return len(matching_dirs) > 0
    except Exception:
        return False


def get_available_ocean_variables(
    model_id: str = "ACCESS-ESM1.6", table_name: str = "Omon"
) -> List[str]:
    """
    Get list of available ocean variables for a given table.

    Args:
        model_id: Model identifier
        table_name: CMOR table name (e.g., 'Omon')

    Returns:
        List of available ocean variable names
    """
    try:
        # Import here to avoid circular imports
        import importlib.resources as resources
        import json

        mapping_file = f"{model_id}_mappings.json"

        with (
            resources.files("access_moppy.mappings").joinpath(mapping_file).open() as f
        ):
            all_mappings = json.load(f)

        # Get ocean variables
        if "ocean" in all_mappings:
            ocean_variables = list(all_mappings["ocean"].keys())
            return ocean_variables
        else:
            print(f"No ocean section found in mappings for {model_id}")
            return []

    except Exception as e:
        print(f"Error loading ocean variables: {e}")
        return []


# Example usage
if __name__ == "__main__":
    # Test the function
    test_variables = ["Omon.evs", "Omon.thetao", "Omon.so"]

    for var in test_variables:
        print(f"\nTesting {var}:")
        files = get_monthly_ocean_files(var)
        print(f"Found {len(files)} files")
        if files:
            print(f"First file: {files[0]}")
