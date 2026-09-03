"""Unit tests for minting one ``tracking_id`` per written file.

``tracking_id`` identifies a single file, but it was generated once per
CMORiser: ``update_attributes`` runs once, while ``write`` emits one file per
``split_years`` chunk and source partitioning builds a fresh CMORiser per
partition. Every file a partition produced therefore shared one id.
"""

from __future__ import annotations

from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser


def _cmoriser(
    tmp_path, tracking_id="hdl:21.14107/11111111-2222-3333-4444-555555555555"
):
    vocab = Mock()
    vocab.mip_era = "CMIP7"
    vocab.variable = {"out_name": "tas", "units": "K"}
    vocab.get_required_attribute_names = Mock(return_value=[])

    attrs = {"variable_id": "tas", "units": "K"}
    if tracking_id is not None:
        attrs["tracking_id"] = tracking_id

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.asarray([285.0, 286.0], dtype=float),
                dims=["time"],
                attrs={"units": "K"},
                coords={"time": xr.DataArray([0, 1], dims=["time"])},
            )
        },
        attrs=attrs,
    )

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(tmp_path),
        vocab=vocab,
        variable_mapping={"tas": {"dimensions": {"time": "time"}}},
        compound_name="Amon.tas",
    )
    cmoriser.ds = ds
    return cmoriser


@pytest.mark.unit
def test_each_file_gets_its_own_tracking_id(tmp_path):
    """Successive files must not repeat an id.

    One id across a whole variable is what the defect looked like in output:
    120 files of a 20-year run carried 62 distinct ids.
    """
    cmoriser = _cmoriser(tmp_path)

    ids = [cmoriser._file_global_attributes()["tracking_id"] for _ in range(5)]

    assert len(set(ids)) == 5


@pytest.mark.unit
def test_the_handle_prefix_is_preserved(tmp_path):
    """The CMIP6 and CMIP7 handles stay where the vocabulary defines them."""
    for prefix in ("hdl:21.14100", "hdl:21.14107"):
        cmoriser = _cmoriser(tmp_path, tracking_id=f"{prefix}/some-existing-uuid")

        new = cmoriser._file_global_attributes()["tracking_id"]

        assert new.startswith(f"{prefix}/")
        assert new != f"{prefix}/some-existing-uuid"


@pytest.mark.unit
def test_the_dataset_attributes_are_not_mutated(tmp_path):
    """The dataset is reused across split writes, so it must be left alone."""
    original = "hdl:21.14107/11111111-2222-3333-4444-555555555555"
    cmoriser = _cmoriser(tmp_path, tracking_id=original)

    cmoriser._file_global_attributes()

    assert cmoriser.ds.attrs["tracking_id"] == original


@pytest.mark.unit
def test_other_global_attributes_are_carried_through(tmp_path):
    """Only the id changes; everything else reaches the file unaltered."""
    cmoriser = _cmoriser(tmp_path)

    attrs = cmoriser._file_global_attributes()

    assert attrs["variable_id"] == "tas"
    assert set(attrs) == set(cmoriser.ds.attrs)


@pytest.mark.unit
@pytest.mark.parametrize("tracking_id", [None, "", "not-a-handle"])
def test_an_unusable_id_is_left_alone(tmp_path, tracking_id):
    """Without a prefix to carry over there is nothing to mint from."""
    cmoriser = _cmoriser(tmp_path, tracking_id=tracking_id)

    attrs = cmoriser._file_global_attributes()

    assert attrs.get("tracking_id") == tracking_id


class TestSplitWriteMintsIdPerFile:
    """The end of the chain: files on disk, not just the attribute helper.

    Covering only ``_file_global_attributes`` leaves the call site untested —
    ``_write_single`` could go back to reading ``self.ds.attrs`` and every unit
    test above would still pass.
    """

    @pytest.mark.unit
    def test_split_files_carry_distinct_tracking_ids(self, tmp_path):
        from unittest.mock import MagicMock

        import cftime
        import netCDF4 as nc

        from access_moppy.base import CMORiser

        times = np.array(
            [cftime.DatetimeGregorian(y, 1, 15) for y in range(1850, 1854)]
        )
        ds = xr.Dataset(
            {"tas": xr.DataArray(np.ones(len(times), dtype=np.float32), dims=["time"])},
            coords={
                "time": (
                    "time",
                    times,
                    {"units": "days since 1850-01-01", "calendar": "gregorian"},
                )
            },
            attrs={
                "variable_id": "tas",
                "tracking_id": "hdl:21.14107/11111111-2222-3333-4444-555555555555",
            },
        )

        cmoriser = object.__new__(CMORiser)
        cmoriser.ds = ds
        cmoriser.cmor_name = "tas"
        cmoriser.compound_name = "Amon.tas"
        cmoriser.output_path = str(tmp_path)
        cmoriser.drs_root = None
        cmoriser.staging_path = None
        cmoriser.enable_compression = False
        cmoriser.compression_level = 0
        cmoriser.chunker = None
        cmoriser.enable_chunking = False
        cmoriser.split_years = 1
        cmoriser.enable_qc_plots = False

        vocab = MagicMock()
        vocab.get_required_attribute_names.return_value = []
        vocab.mip_era = "CMIP6"
        vocab.variable = {"out_name": "tas"}
        vocab.generate_filename.side_effect = lambda attrs, dataset, cname, cmpd: (
            f"{cname}_{dataset[cname].coords['time'].values[0].year}.nc"
        )
        cmoriser.vocab = vocab

        cmoriser.write()

        assert len(cmoriser.written_files) == 4
        ids = []
        for path in cmoriser.written_files:
            with nc.Dataset(path) as d:
                ids.append(d.getncattr("tracking_id"))
        assert len(set(ids)) == 4, f"one id per file, got {ids}"
        assert all(i.startswith("hdl:21.14107/") for i in ids)
