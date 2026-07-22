from typing import Optional

#: Default maximum number of years per output file, keyed by canonical CMIP
#: frequency.  ``None`` means no splitting (single file for the whole run).
#:
#: These values reflect the widely-adopted ESGF/CMIP publication practice of
#: keeping individual files to a manageable size (roughly 2–20 GB) to improve
#: download reliability and allow users to retrieve only the years they need.
#:
#: +-----------+----------------+
#: | Frequency | Years per file |
#: +===========+================+
#: | ``1hr``   | 1              |
#: +-----------+----------------+
#: | ``3hr``   | 1              |
#: +-----------+----------------+
#: | ``6hr``   | 1              |
#: +-----------+----------------+
#: | ``day``   | 5              |
#: +-----------+----------------+
#: | ``mon``   | 10             |
#: +-----------+----------------+
#: | ``yr``    | *no split*     |
#: +-----------+----------------+
#: | ``fx``    | *no split*     |
#: +-----------+----------------+
#:
#: Pass ``split_years="auto"`` (the default) to a CMORiser to apply these
#: defaults, supply a positive integer to override for all frequencies, or
#: ``None`` to write the entire time series into a single file.
DEFAULT_CHUNK_YEARS: dict[str, Optional[int]] = {
    "1hr": 1,
    "3hr": 1,
    "6hr": 1,
    "day": 5,
    "mon": 10,
    "yr": None,
    "fx": None,
}

_default_parent_info = {
    "parent_experiment_id": "piControl",
    "parent_activity_id": "CMIP",
    "parent_source_id": "ACCESS-ESM1-5",
    "parent_variant_label": "r1i1p1f1",
    "parent_time_units": "days since 0001-01-01 00:00:00",
    "parent_mip_era": "CMIP6",
    "branch_time_in_child": 0.0,
    "branch_time_in_parent": 0.0,
    "branch_method": "standard",
}

_default_parent_info_cmip7 = {
    "parent_experiment_id": "piControl",
    "parent_activity_id": "CMIP",
    "parent_source_id": "ACCESS-ESM1-6",
    "parent_variant_label": "r1i1p1f1",
    "parent_time_units": "days since 0001-01-01 00:00:00",
    "parent_mip_era": "CMIP7",
    "branch_time_in_child": 0.0,
    "branch_time_in_parent": 0.0,
    "branch_method": "standard",
}

# CMIP7 does not include model_component / native_nominal_resolution in its
# controlled vocabulary (unlike CMIP6).  This dict provides that information
# for ACCESS source_ids so that nominal_resolution can be written into output
# global attributes.
CMIP7_SOURCE_MODEL_COMPONENTS: dict[str, dict[str, dict[str, str]]] = {
    "ACCESS-ESM1-6": {
        "aerosol": {"native_nominal_resolution": "250 km"},
        "atmos": {"native_nominal_resolution": "250 km"},
        "atmosChem": {"native_nominal_resolution": "none"},
        "land": {"native_nominal_resolution": "250 km"},
        "landIce": {"native_nominal_resolution": "none"},
        "ocean": {"native_nominal_resolution": "100 km"},
        "ocnBgchem": {"native_nominal_resolution": "100 km"},
        "seaIce": {"native_nominal_resolution": "100 km"},
    },
}
