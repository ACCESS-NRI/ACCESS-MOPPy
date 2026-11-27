"""Per-file tracking IDs for local project output."""

import uuid

import pytest
import xarray as xr

from access_moppy.base import CMORiser


@pytest.mark.unit
def test_uuid_urn_is_regenerated_for_each_written_file():
    writer = CMORiser.__new__(CMORiser)
    original = f"urn:uuid:{uuid.uuid4()}"
    writer.ds = xr.Dataset(attrs={"tracking_id": original, "Conventions": "CF-1.7"})
    first = writer._file_global_attributes()
    second = writer._file_global_attributes()
    assert first["tracking_id"].startswith("urn:uuid:")
    uuid.UUID(first["tracking_id"].removeprefix("urn:uuid:"))
    uuid.UUID(second["tracking_id"].removeprefix("urn:uuid:"))
    assert len({original, first["tracking_id"], second["tracking_id"]}) == 3
    assert writer.ds.attrs["tracking_id"] == original


@pytest.mark.unit
def test_malformed_uuid_urn_is_left_untouched():
    writer = CMORiser.__new__(CMORiser)
    writer.ds = xr.Dataset(attrs={"tracking_id": "urn:uuid:invalid"})
    assert writer._file_global_attributes()["tracking_id"] == "urn:uuid:invalid"
