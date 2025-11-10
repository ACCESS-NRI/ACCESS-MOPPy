import json
from importlib.resources import as_file, files
from typing import Dict

import numpy as np

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
