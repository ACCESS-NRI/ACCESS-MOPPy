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

# The CMIP7 experiment_id CV (as of CMOR >=3.14.2, which is the first release
# to correctly honour the CV's per-experiment parent_experiment_id rather than
# requiring parent attributes unconditionally) declares piControl and
# esm-piControl as children of a dedicated spin-up experiment, not as root
# experiments:
#
#   "piControl":     parent_experiment_id -> ["piControl-spinup"]
#   "esm-piControl": parent_experiment_id -> ["esm-piControl-spinup"]
#
# ACCESS runs and CMORises these spin-up simulations separately (see the
# CMIP7 "piControl-spinup" example in the getting-started tutorial), so the
# generic ``_default_parent_info_cmip7`` default above (which assumes the
# parent is piControl itself) would be self-referential and wrong when
# applied to piControl/esm-piControl. Use these experiment-specific defaults
# instead.
_default_parent_info_cmip7_picontrol = {
    **_default_parent_info_cmip7,
    "parent_experiment_id": "piControl-spinup",
}

_default_parent_info_cmip7_esm_picontrol = {
    **_default_parent_info_cmip7,
    "parent_experiment_id": "esm-piControl-spinup",
}

#: Per-experiment CMIP7 parent defaults, keyed by lower-cased experiment_id,
#: for experiments whose CV-declared parent is not "piControl".
_default_parent_info_cmip7_by_experiment: dict[str, dict] = {
    "picontrol": _default_parent_info_cmip7_picontrol,
    "esm-picontrol": _default_parent_info_cmip7_esm_picontrol,
}

# CMIP7 does not include model_component / native_nominal_resolution or
# institution_id in its controlled vocabulary (unlike CMIP6).  This dict
# supplements the official CV entry for ACCESS source_ids with the fields
# needed to produce valid global attributes.
CMIP7_SOURCE_SUPPLEMENTS: dict[str, dict] = {
    "ACCESS-ESM1-6": {
        "model_component": {
            "aerosol": {"native_nominal_resolution": "250 km"},
            "atmos": {"native_nominal_resolution": "250 km"},
            "atmosChem": {"native_nominal_resolution": "none"},
            "land": {"native_nominal_resolution": "250 km"},
            "landIce": {"native_nominal_resolution": "none"},
            "ocean": {"native_nominal_resolution": "100 km"},
            "ocnBgchem": {"native_nominal_resolution": "100 km"},
            "seaIce": {"native_nominal_resolution": "100 km"},
        },
    },
}
