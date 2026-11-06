"""Manifest discovery and real MOPPy CMORisation through the Core source."""

import hashlib
import json

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from access_moppy.esmval.data_source import MoppyDataSource
from access_moppy.vocabulary_processors import CMIP6Vocabulary


@pytest.fixture
def cice_inputs():
    bounds = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    lat = np.array([[-70.0, -70.0], [-60.0, -60.0]])
    lon = np.array([[0.0, 1.0], [0.0, 1.0]])
    corners_lat = np.array(
        [
            [[-71, -71, -69, -69], [-71, -71, -69, -69]],
            [[-61, -61, -59, -59], [-61, -61, -59, -59]],
        ],
        dtype=float,
    )
    corners_lon = np.array(
        [
            [[-0.5, 0.5, 0.5, -0.5], [0.5, 1.5, 1.5, 0.5]],
            [[-0.5, 0.5, 0.5, -0.5], [0.5, 1.5, 1.5, 0.5]],
        ],
        dtype=float,
    )
    grid = xr.Dataset(
        {
            "TLAT": (
                ("nj", "ni"),
                lat,
                {"units": "degrees_north", "bounds": "TLAT_bnds"},
            ),
            "TLON": (
                ("nj", "ni"),
                lon,
                {"units": "degrees_east", "bounds": "TLON_bnds"},
            ),
            "TLAT_bnds": (("nj", "ni", "nv4"), corners_lat, {"units": "degrees_north"}),
            "TLON_bnds": (("nj", "ni", "nv4"), corners_lon, {"units": "degrees_east"}),
            "tarea": (("nj", "ni"), [[1e4, 2e4], [3e4, 0]], {"units": "cm2"}),
            "tmask": (("nj", "ni"), [[1, 1], [1, 0]]),
        }
    )
    grid = grid.set_coords(["TLAT", "TLON"])
    native = xr.Dataset(
        {
            "aice_m": (
                ("time", "nj", "ni"),
                da.from_array(
                    np.array([[[0.1, 0.2], [0.3, 0.9]], [[0.2, 0.3], [0.4, 0.8]]]),
                    chunks=(1, 2, 2),
                ),
                {"units": "1", "coordinates": "TLON TLAT"},
            ),
            "time_bounds": (("time", "d2"), bounds),
        },
        coords={
            "time": bounds[:, 1],
            "TLAT": (("nj", "ni"), lat, {"units": "degrees_north"}),
            "TLON": (("nj", "ni"), lon, {"units": "degrees_east"}),
        },
    )
    native.time.attrs["bounds"] = "time_bounds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    return native, grid


def entry(project_cv):
    return {
        "facets": dict(
            dataset="ACCESS-OM3",
            exp="om3-real-run",
            ensemble="native",
            mip="SImon",
            short_name="siconc",
            version="v1",
            grid="native-cice-t",
            frequency="mon",
            timerange="200001/200002",
        ),
        "component": "cice",
        "run_id": "om3-real-run",
        "native_grid": "cice-t",
        "files": ["native.nc"],
        "grid_file": "grid.nc",
        "grid_identity_verified": True,
        "grid_reference": "test-static-grid@sha256:known",
        "cmoriser": {
            "experiment_id": "om3-real-run",
            "source_id": "ACCESS-OM3",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
            "cmip_version": "CMIP6",
            "model_id": "ACCESS-OM3",
            "project_cv": project_cv,
        },
    }


def source(tmp_path, entries):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"schema_version": 1, "entries": entries}))
    return MoppyDataSource(name="moppy", project="native_om3", manifest=path)


def write_inputs(tmp_path, cice_inputs):
    native, grid = cice_inputs
    native.to_netcdf(tmp_path / "native.nc")
    grid.to_netcdf(tmp_path / "grid.nc")
    return [tmp_path / "native.nc", tmp_path / "grid.nc"]


def test_manifest_identity_and_time_selection(tmp_path, project_cv):
    s = source(tmp_path, [entry(project_cv)])
    assert (
        len(s.find_data(dataset="ACCESS-*", exp="om3-real-run", timerange="2000/2000"))
        == 1
    )
    assert s.find_data(exp="historical") == []
    assert s.find_data(timerange="2001/2002") == []
    assert s.find_data(project="CMIP6") == []
    assert s.find_data(ensemble=["member-2", "member-3"]) == []
    a = s.find_data()[0]
    a.spec["files"].append("changed.nc")
    assert len(s.find_data()[0].spec["files"]) == 1


@pytest.mark.parametrize(
    "change,match",
    [
        ({"files": []}, "files list"),
        ({"files": ["native.nc", "./native.nc"]}, "Duplicate native"),
        ({"run_id": ""}, "run_id"),
        ({"native_grid": ""}, "native_grid"),
        ({"component": "unknown"}, "component"),
        ({"cmoriser": {}}, "cmoriser identity"),
    ],
)
def test_invalid_manifest_fails_before_loading(tmp_path, project_cv, change, match):
    with pytest.raises(ValueError, match=match):
        source(tmp_path, [{**entry(project_cv), **change}])


def test_duplicate_identity_and_false_frequency_rejected(tmp_path, project_cv):
    with pytest.raises(ValueError, match="Duplicate manifest"):
        source(tmp_path, [entry(project_cv), entry(project_cv)])
    wrong = entry(project_cv)
    wrong["facets"]["frequency"] = "fx"
    with pytest.raises(ValueError, match="fixed field"):
        source(tmp_path, [wrong])
    wrong = entry(project_cv)
    wrong["facets"]["timerange"] = "20000115/20000220"
    with pytest.raises(ValueError, match="whole years or months"):
        source(tmp_path, [wrong])


def test_manifest_rejects_ambiguous_cmoriser_arguments(tmp_path, project_cv):
    for key, value in (
        ("input_data", "other.nc"),
        ("compound_name", "Omon.tos"),
        ("grid_data", "other-grid.nc"),
    ):
        wrong = entry(project_cv)
        wrong["cmoriser"][key] = value
        with pytest.raises(ValueError, match="defined by the manifest"):
            source(tmp_path, [wrong])


@pytest.mark.parametrize(
    "section,key,value,match",
    [
        ("facets", "exp", "different-run", "exp, run_id"),
        ("entry", "run_id", "different-run", "exp, run_id"),
        ("cmoriser", "experiment_id", "different-run", "exp, run_id"),
        ("cmoriser", "source_id", "ACCESS-ESM1-5", "dataset, cmoriser source_id"),
        ("cmoriser", "model_id", "ACCESS-ESM1-5", "dataset, cmoriser source_id"),
    ],
)
def test_manifest_rejects_conflicting_run_or_model_identity(
    tmp_path, project_cv, section, key, value, match
):
    wrong = entry(project_cv)
    target = wrong if section == "entry" else wrong[section]
    target[key] = value
    with pytest.raises(ValueError, match=match):
        source(tmp_path, [wrong])


def test_manifest_rejects_cmor_table_frequency_before_run(
    tmp_path, cice_inputs, project_cv, monkeypatch
):
    from access_moppy.driver import ACCESS_ESM_CMORiser

    write_inputs(tmp_path, cice_inputs)
    wrong = entry(project_cv)
    wrong["facets"]["mip"] = "SIday"

    def unexpected_run(self):
        raise AssertionError("CMORiser.run must not execute for a frequency mismatch")

    monkeypatch.setattr(ACCESS_ESM_CMORiser, "run", unexpected_run)
    element = source(tmp_path, [wrong]).find_data()[0]
    with pytest.raises(ValueError, match="CMOR table frequency"):
        element.to_iris()


def test_core_protocol_runs_real_cmoriser_lazily_and_preserves_files(
    tmp_path,
    cice_inputs,
    project_cv,
    monkeypatch,
):
    pytest.importorskip("esmvalcore")
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    from esmvalcore.io.protocol import DataElement, DataSource

    files = write_inputs(tmp_path, cice_inputs)
    checksums = [hashlib.sha256(path.read_bytes()).hexdigest() for path in files]
    closed = []
    real_open_mfdataset, real_open_dataset = xr.open_mfdataset, xr.open_dataset

    def track_close(dataset, label):
        closer = dataset._close

        def close():
            closed.append(label)
            if closer is not None:
                closer()

        dataset.set_close(close)
        return dataset

    def open_native(*args, **kwargs):
        return track_close(real_open_mfdataset(*args, **kwargs), "native")

    def open_grid(*args, **kwargs):
        dataset = real_open_dataset(*args, **kwargs)
        if str(args[0]) == str(files[1]):
            return track_close(dataset, "grid")
        return dataset

    monkeypatch.setattr(xr, "open_mfdataset", open_native)
    monkeypatch.setattr(xr, "open_dataset", open_grid)
    s = source(tmp_path, [entry(project_cv)])
    element = s.find_data()[0]
    assert isinstance(s, DataSource) and isinstance(element, DataElement)
    cube = element.to_iris()[0]
    assert cube.has_lazy_data()
    assert cube.coord("time").has_bounds()
    assert cube.coord("latitude").has_bounds()
    assert cube.coord("longitude").has_bounds()
    assert cube.coord("latitude").shape == (2, 2)
    assert cube.coord("area_type").points[0] == "sea_ice"
    assert not hasattr(element, "_owners")
    assert closed == ["grid", "native"]
    # A computed value confirms file-backed laziness still works after handles close.
    np.testing.assert_allclose(cube.data[:, 0, 0], [10, 20])
    assert np.ma.getmaskarray(cube.data)[:, 1, 1].all()
    vocab = CMIP6Vocabulary(
        compound_name="SImon.siconc",
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        grid_label="gn",
        project_cv=project_cv,
    )
    official = vocab.cmip_table["variable_entry"]["siconc"]
    assert str(cube.units) == official["units"]
    assert cube.standard_name == official["standard_name"]
    assert element.attributes["experiment_id"] == "om3-real-run"
    assert element.attributes["source_id"] == "ACCESS-OM3"
    assert element.attributes["access_moppy_processing_class"] == "SeaIce_CMORiser"
    assert [
        hashlib.sha256(path.read_bytes()).hexdigest() for path in files
    ] == checksums


def test_manifest_cannot_overstate_available_period(tmp_path, cice_inputs, project_cv):
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    write_inputs(tmp_path, cice_inputs)
    wrong = entry(project_cv)
    wrong["facets"]["timerange"] = "200001/200012"
    element = source(tmp_path, [wrong]).find_data()[0]
    with pytest.raises(ValueError, match="manifest timerange"):
        element.to_iris()
    assert not hasattr(element, "_owners")


def test_manifest_rejects_gapped_month_bounds(tmp_path, cice_inputs, project_cv):
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    native, grid = cice_inputs
    altered = native.copy(deep=True)
    altered["time_bounds"] = (
        ("time", "d2"),
        np.array(
            [["2000-01-01", "2000-02-01"], ["2000-03-01", "2000-04-01"]],
            dtype="datetime64[ns]",
        ),
    )
    altered.to_netcdf(tmp_path / "native.nc")
    grid.to_netcdf(tmp_path / "grid.nc")
    element = source(tmp_path, [entry(project_cv)]).find_data()[0]
    with pytest.raises(ValueError, match="manifest timerange"):
        element.to_iris()


def test_complete_240_months_reach_real_cmoriser(tmp_path, cice_inputs, project_cv):
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    native, grid = cice_inputs
    edges = xr.date_range("1986-01-01", periods=241, freq="MS")
    long_native = xr.Dataset(
        {
            "aice_m": (
                ("time", "nj", "ni"),
                da.full((240, 2, 2), 0.25, chunks=(12, 2, 2)),
                {"units": "1", "coordinates": "TLON TLAT"},
            ),
            "time_bounds": (
                ("time", "d2"),
                np.stack([edges[:-1].values, edges[1:].values], axis=1),
            ),
        },
        coords={"time": edges[1:].values, "TLAT": native.TLAT, "TLON": native.TLON},
    )
    long_native.time.attrs["bounds"] = "time_bounds"
    long_native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    long_native.to_netcdf(tmp_path / "native.nc")
    grid.to_netcdf(tmp_path / "grid.nc")
    full = entry(project_cv)
    full["facets"]["timerange"] = "1986/2005"
    element = source(tmp_path, [full]).find_data()[0]
    cube = element.to_iris()[0]
    assert cube.shape == (240, 2, 2)
    assert cube.has_lazy_data()
    np.testing.assert_allclose(cube.data[[0, -1], 0, 0], [25, 25])
    assert element.attributes["experiment_id"] == "om3-real-run"


def test_lazy_datetime_bounds_with_units_and_no_dtype_convert_to_iris():
    """Composite source merges can leave only units on lazy time bounds."""
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    from access_moppy.iris_utils import dataset_to_iris

    boundaries = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    dataset = xr.Dataset(
        {
            "siconc": (
                ("time",),
                da.from_array([10.0, 20.0], chunks=1),
                {"units": "%"},
            ),
            "time_bnds": (("time", "bnds"), da.from_array(boundaries, chunks=(1, 2))),
        },
        coords={"time": boundaries[:, 0]},
    )
    dataset.time.attrs["bounds"] = "time_bnds"
    dataset.time.encoding.update(units="days since 1900-01-01", calendar="standard")
    dataset.time_bnds.encoding.update(units="days since 1900-01-01", dtype=None)
    original_encoding = dict(dataset.time_bnds.encoding)
    cube = dataset_to_iris(dataset, "siconc")
    assert cube.has_lazy_data()
    assert cube.coord("time").has_bounds()
    np.testing.assert_allclose(cube.data, [10, 20])
    assert dataset.time_bnds.encoding == original_encoding
