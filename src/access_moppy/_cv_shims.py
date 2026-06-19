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

from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# CMIP7 shims
# Remove once the upstream CMIP7 CV bundle includes the official ACCESS
# source and institution registration.
# ---------------------------------------------------------------------------

CMIP7_TEMP_ACCESS_INSTITUTION_ID = "CSIRO"
CMIP7_TEMP_CSIRO_INSTITUTION = "Commonwealth Scientific and Industrial Research Organisation, Aspendale, Victoria 3195, Australia"

CMIP7_TEMP_INSTITUTION_NAMES: Dict[str, str] = {
    "CSIRO": CMIP7_TEMP_CSIRO_INSTITUTION,
}

CMIP7_TEMP_SOURCE_WARNED: set[str] = set()

CMIP7_EXPERIMENT_ALIASES: Dict[str, List[str]] = {
    "piControl": ["picontrol", "esm-picontrol"],
    "piControl-spinup": ["picontrol-spinup", "esm-picontrol-spinup"],
    "picontrol": ["piControl", "esm-picontrol"],
    "picontrol-spinup": ["piControl-spinup", "esm-picontrol-spinup"],
    "esm-picontrol": ["picontrol", "piControl"],
    "esm-picontrol-spinup": ["picontrol-spinup", "piControl-spinup"],
}

CMIP7_TEMP_SOURCE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "ACCESS-ESM1-6": {
        "validation_key": "ACCESS-ESM1-6",
        "ui_label": "ACCESS-ESM1-6",
        "name": "ACCESS-ESM1-6",
        "activity_participation": [
            "CMIP",
            "TIPMIP",
            "TBIMIP",
        ],
        "calendar": ["proleptic-gregorian"],
        "cohort": ["Published"],
        "coupled_components": [
            ["atmosphere", "ocean"],
            ["atmosphere", "sea ice"],
            ["ocean", "sea ice"],
        ],
        "description": (
            "The model is an updated version of ACCESS-ESM1-5, as used for "
            "CMIP6 (Ziehn et al., 2020). Its dynamic components are the UK "
            "Met Office Unified Model atmosphere (v7.3) in a configuration "
            "similar to HadGEM2 including CLASSIC aerosols, with the CABLE "
            "land surface model (v3) including land biogeochemistry. The "
            "atmosphere is coupled to the MOM5 ocean with WOMBATlite "
            "biogeochemistry and CICE5 sea-ice using the OASIS-MCT coupler. "
            "Chemistry is prescribed including monthly mean, zonally-averaged "
            "ozone. Volcano forcing is prescribed as stratospheric optical "
            "depth as monthly means in four equal area latitude bands. "
            "Land-ice is represented by an ice tile in the land surface "
            "model. A grid-cell is either completely ice or ice-free and the "
            "ice tile distribution does not change in time. The main "
            "differences from ACCESS-ESM1.5 include corrections to how "
            "convective momentum transport had been applied in the atmosphere, "
            "a change to ocean albedo to better match observations, a "
            "correction to the passing of ocean carbon fluxes into the "
            "atmosphere, updates to improve energy and water conservation "
            "across the land and atmosphere, inclusion of 3 new plant "
            "functional types (c4 crops and two Australian tree types), "
            "improvements to the treatment of land-use change, updates to "
            "some of the land parameterisations or parameter values, the "
            "inclusion of a pseudo-iceberg scheme for distributing runoff "
            "from Antarctica and Greenland and a substantial update to ocean "
            "biogeochemistry. Many aspects of the model infrastructure have "
            "also been improved to ensure greater provenance, better support "
            "for users and enhanced model throughput. A manuscript describing "
            "ACCESS-ESM1.6 is in preparation (Ziehn et al.)."
        ),
        "dynamic_components": [
            "atmosphere",
            "land surface and subsurface",
            "aerosol",
            "ocean",
            "ocean biogeochemistry",
            "sea ice",
        ],
        "embedded_components": [
            ["aerosol", "atmosphere"],
            ["land surface and subsurface", "atmosphere"],
            ["ocean biogeochemistry", "ocean"],
        ],
        "family": "access-esm",
        "institution_id": [CMIP7_TEMP_ACCESS_INSTITUTION_ID],
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
                "description": "CABLE2.4",
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
                "description": "WOMBAT (same grid as ocean)",
                "native_nominal_resolution": "100 km",
            },
            "seaIce": {
                "description": "CICE4.1 (same grid as ocean)",
                "native_nominal_resolution": "100 km",
            },
        },
        "model_components": [
            "aerosol_classic_h102_v106",
            "atmosphere_um7.3_h102_v106",
            "land_surface_cable3_h102_v105",
            "ocean-biogeochemistry_wombatlite_h109_v109",
            "ocean_mom5_h109_v109",
            "sea_ice_cice5_h109_no-vertical",
        ],
        "omitted_components": [],
        "prescribed_components": [
            "atmospheric chemistry",
            "land ice",
        ],
        "references": ["https://doi.org/10.1071/ES19035"],
        "release_year": "2026",
        "source_id": "ACCESS-ESM1-6",
        "@id": "access-esm1-6",
    }
}

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
