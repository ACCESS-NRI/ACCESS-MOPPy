from typing import Optional

#: Default maximum number of years per output file, keyed by canonical CMIP
#: frequency.  ``None`` means no splitting (single file for the whole run).
#: Users can override these defaults by passing ``split_years=<int>`` to the
#: CMORiser or by setting ``split_years="auto"`` to use these defaults.
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
