"""
Temporary controlled-vocabulary shims for ACCESS models not yet registered in
the upstream CMIP CV bundles.

HOW TO REMOVE A SHIM
--------------------
1. Delete the relevant entry from the dict below (or the whole dict/file once
   all entries are removed).
2. Remove the corresponding import in ``vocabulary_processors.py``.
3. Remove the shim-lookup branches that reference the symbol in
   ``vocabulary_processors.py`` (search for ``_CMIP6_TEMP`` / ``_CMIP7_TEMP``).
4. Run the test suite to confirm nothing breaks.
"""

from typing import Any, Dict

# ---------------------------------------------------------------------------
# CMIP6 / CMIP6Plus shims
# Remove once the upstream CMIP6Plus CV bundle includes the official entry.
# ---------------------------------------------------------------------------

CMIP6_TEMP_SOURCE_WARNED: set[str] = set()

CMIP6_TEMP_SOURCE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "ACCESS-ESM1-6": {
        "activity_participation": [
            "CMIP",
        ],
        "cohort": ["Published"],
        "institution_id": ["CSIRO"],
        "label": "ACCESS-ESM1.6",
        "label_extended": (
            "Australian Community Climate and Earth System Simulator "
            "Earth System Model Version 1.6"
        ),
        "license_info": {
            "exceptions_contact": "@csiro.au <- access_csiro",
            "history": (
                "2019-11-12: initially published under CC BY-SA 4.0; "
                "2022-06-10: relaxed to CC BY 4.0"
            ),
            "id": "CC BY 4.0",
            "license": (
                "Creative Commons Attribution 4.0 International "
                "(CC BY 4.0; https://creativecommons.org/licenses/by/4.0/)"
            ),
            "source_specific_info": "",
            "url": "https://creativecommons.org/licenses/by/4.0/",
        },
        "model_component": {
            "aerosol": {
                "description": "CLASSIC (v1.0)",
                "native_nominal_resolution": "250 km",
            },
            "atmos": {
                "description": (
                    "HadGAM2 (r1.1, N96; 192 x 145 longitude/latitude; "
                    "38 levels; top level 39255 m)"
                ),
                "native_nominal_resolution": "250 km",
            },
            "atmosChem": {
                "description": "none",
                "native_nominal_resolution": "none",
            },
            "land": {
                "description": "CABLE3 (v3)",
                "native_nominal_resolution": "250 km",
            },
            "landIce": {
                "description": "none",
                "native_nominal_resolution": "none",
            },
            "ocean": {
                "description": (
                    "ACCESS-OM2 (MOM5, tripolar primarily 1deg; "
                    "360 x 300 longitude/latitude; 50 levels; "
                    "top grid cell 0-10 m)"
                ),
                "native_nominal_resolution": "100 km",
            },
            "ocnBgchem": {
                "description": "WOMBATlite (same grid as ocean)",
                "native_nominal_resolution": "100 km",
            },
            "seaIce": {
                "description": "CICE5 (same grid as ocean)",
                "native_nominal_resolution": "100 km",
            },
        },
        "release_year": "2026",
        "source_id": "ACCESS-ESM1-6",
    }
}
