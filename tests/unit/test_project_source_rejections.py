"""Reject incomplete native manifests and unavailable data before CMORisation."""

import json

import pytest
import xarray as xr

from access_moppy.esmval.data_source import MoppyDataSource


def _entry():
    return {
        "facets": {
            "dataset": "ACCESS-OM3",
            "exp": "om3-real-run",
            "ensemble": "native",
            "mip": "SImon",
            "short_name": "siconc",
            "version": "v1",
            "grid": "native-cice-t",
            "frequency": "mon",
            "timerange": "200001/200002",
        },
        "component": "cice",
        "native_grid": "cice-t",
        "run_id": "om3-real-run",
        "files": ["native.nc"],
        "grid_file": "grid.nc",
        "cmoriser": {
            "experiment_id": "om3-real-run",
            "source_id": "ACCESS-OM3",
            "variant_label": "r1i1p1f1",
        },
    }


def _source(tmp_path, entries, *, schema_version=1):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"schema_version": schema_version, "entries": entries})
    )
    return MoppyDataSource(name="native", project="CMIP6", manifest=manifest)


@pytest.mark.unit
def test_source_rejects_unknown_manifest_schema(tmp_path):
    with pytest.raises(ValueError, match="schema_version 1"):
        _source(tmp_path, [_entry()], schema_version=2)


@pytest.mark.unit
@pytest.mark.parametrize(
    "change,match",
    [
        ("missing_facet", "nonempty identity facets"),
        ("wrong_project", "project must match"),
        ("missing_file", "explicit files list"),
    ],
)
def test_source_rejects_incomplete_or_mislabelled_manifest(tmp_path, change, match):
    entry = _entry()
    if change == "missing_facet":
        entry["facets"].pop("dataset")
    elif change == "wrong_project":
        entry["facets"]["project"] = "CMIP7"
    else:
        entry["files"] = []
    with pytest.raises(ValueError, match=match):
        _source(tmp_path, [entry])


@pytest.mark.unit
@pytest.mark.parametrize("missing", ["native.nc", "grid.nc", "supergrid.nc"])
def test_source_prepare_requires_every_declared_input(tmp_path, missing):
    entry = _entry()
    entry["supergrid_file"] = "supergrid.nc"
    source = _source(tmp_path, [entry])
    for name in ("native.nc", "grid.nc", "supergrid.nc"):
        if name != missing:
            (tmp_path / name).touch()
    element = source.find_data()[0]
    with pytest.raises(FileNotFoundError, match=missing):
        element.prepare()


@pytest.mark.unit
def test_monthly_source_rejects_native_file_without_time_before_cmor_run(tmp_path):
    pytest.importorskip("iris")
    entry = _entry()
    xr.Dataset({"aice_m": (("nj", "ni"), [[0.1]])}).to_netcdf(tmp_path / "native.nc")
    xr.Dataset({"tmask": (("nj", "ni"), [[1]])}).to_netcdf(tmp_path / "grid.nc")
    element = _source(tmp_path, [entry]).find_data()[0]
    with pytest.raises(ValueError, match="Monthly input needs a time coordinate"):
        element.to_iris()
