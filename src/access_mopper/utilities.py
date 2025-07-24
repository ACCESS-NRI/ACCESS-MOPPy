import json
import re
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


def load_cmip6_mappings(compound_name: str) -> Dict:
    """
    Load all CMIP6 mapping JSON files from the 'mappings' package data directory
    and return a dictionary keyed by CMIP6 table (e.g. 'Amon', 'Lmon', etc).
    """
    mappings = {}

    # Get the Traversable directory inside the package
    mapping_dir = files("access_mopper.mappings")

    # Iterate over all matching JSON files
    for entry in mapping_dir.iterdir():
        if entry.name.startswith("Mappings_CMIP6_") and entry.name.endswith(".json"):
            match = re.match(r"Mappings_CMIP6_(\w+)\.json", entry.name)
            if match:
                table_id = match.group(1)
                # Open the file safely whether zipped or not
                with as_file(entry) as path:
                    with open(path, "r", encoding="utf-8") as f:
                        mappings[table_id] = json.load(f)

    table, cmor_name = compound_name.split(".")
    return mappings.get(table, {})
