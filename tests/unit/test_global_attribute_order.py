"""Unit tests for the order of global attributes in written files.

Files follow CMOR's layout as seen in published ACCESS-ESM1-5 CMIP6 data:
``Conventions`` first, everything else alphabetical, and the tool version,
``tracking_id`` and licence last.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import cftime
import netCDF4 as nc
import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser

# Deliberately scrambled, as the vocabularies build them.
_CMIP7_ATTRS = {
    "tracking_id": "hdl:21.14107/11111111-2222-3333-4444-555555555555",
    "variable_id": "tas",
    "license_id": "CC-BY-4.0",
    "creator_name": "someone",
    "Conventions": "CF-1.12",
    "access_moppy_version": "1.0",
    "parent_experiment_id": "piControl",
    "activity_id": "CMIP",
    "branch_method": "standard",
}

_CMIP7_EXPECTED = [
    "Conventions",
    "activity_id",
    "branch_method",
    "creator_name",
    "history",
    "parent_experiment_id",
    "variable_id",
    "access_moppy_version",
    "tracking_id",
    "license_id",
]


def _cmoriser(attrs):
    times = np.array([cftime.DatetimeGregorian(1850, 1, 15)])
    ds = xr.Dataset(
        {"tas": xr.DataArray(np.ones(1, dtype=np.float32), dims=["time"])},
        coords={
            "time": (
                "time",
                times,
                {"units": "days since 1850-01-01", "calendar": "gregorian"},
            )
        },
        attrs=attrs,
    )
    cmoriser = object.__new__(CMORiser)
    cmoriser.ds = ds
    return cmoriser


@pytest.mark.unit
def test_cmip7_attributes_are_ordered(tmp_path):
    attrs = _cmoriser(dict(_CMIP7_ATTRS))._file_global_attributes()

    assert list(attrs) == _CMIP7_EXPECTED


@pytest.mark.unit
def test_cmip6_licence_is_last(tmp_path):
    attrs = _cmoriser(
        {
            "license": "CC-BY-4.0 ...",
            "tracking_id": "hdl:21.14100/some-uuid",
            "table_id": "Amon",
            "Conventions": "CF-1.7 CMIP-6.2",
            "access_moppy_version": "1.0",
            "experiment_id": "historical",
        }
    )._file_global_attributes()

    assert list(attrs) == [
        "Conventions",
        "experiment_id",
        "history",
        "table_id",
        "access_moppy_version",
        "tracking_id",
        "license",
    ]


@pytest.mark.unit
def test_history_records_moppy_rewrite_in_utc():
    cmoriser = _cmoriser(dict(_CMIP7_ATTRS))
    cmoriser.vocab = MagicMock(mip_era="CMIP7")
    timestamp = datetime(2026, 7, 26, 17, 53, 12, tzinfo=timezone.utc)

    with patch("access_moppy.base.datetime") as mocked_datetime:
        mocked_datetime.now.return_value = timestamp
        attrs = cmoriser._file_global_attributes()

    assert attrs["history"] == (
        "2026-07-26T17:53:12Z ; ACCESS-MOPPy rewrote data to be consistent "
        "with CF-1.12 and CMIP7 data requirements."
    )


@pytest.mark.unit
def test_history_appends_to_existing_provenance():
    existing = "2025-01-02T03:04:05Z ; Source model created data."
    cmoriser = _cmoriser({**_CMIP7_ATTRS, "history": existing})
    cmoriser.vocab = MagicMock(mip_era="CMIP7")

    attrs = cmoriser._file_global_attributes()

    assert attrs["history"].startswith(f"{existing}\n")
    assert attrs["history"].count("ACCESS-MOPPy rewrote data") == 1


@pytest.mark.unit
def test_written_file_keeps_the_order(tmp_path):
    """``_write_single`` must write attributes in the helper's order."""
    cmoriser = _cmoriser(dict(_CMIP7_ATTRS))
    cmoriser.cmor_name = "tas"
    cmoriser.compound_name = "Amon.tas"
    cmoriser.output_path = str(tmp_path)
    cmoriser.drs_root = None
    cmoriser.staging_path = None
    cmoriser.enable_compression = False
    cmoriser.compression_level = 0
    cmoriser.chunker = None
    cmoriser.enable_chunking = False
    cmoriser.split_years = None
    cmoriser.enable_qc_plots = False

    vocab = MagicMock()
    vocab.get_required_attribute_names.return_value = []
    vocab.mip_era = "CMIP6"
    vocab.variable = {"out_name": "tas"}
    vocab.generate_filename.return_value = "tas.nc"
    cmoriser.vocab = vocab

    cmoriser.write()

    with nc.Dataset(tmp_path / "tas.nc") as d:
        assert d.ncattrs() == _CMIP7_EXPECTED
