"""Public native API and manifest-backed Core input contracts."""

import json

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from access_moppy.esmval.data_source import MoppyDataSource
from access_moppy.evaluation import normalize_native


@pytest.fixture
def cice_inputs():
    bounds = np.array(
        [["2000-01-01", "2000-02-01"], ["2000-02-01", "2000-03-01"]],
        dtype="datetime64[ns]",
    )
    native = xr.Dataset(
        {
            "aice_m": (
                ("time", "nj", "ni"),
                da.from_array(
                    np.array([[[0.1, 0.2], [0.3, 0.9]], [[0.2, 0.3], [0.4, 0.8]]]),
                    chunks=(1, 2, 2),
                ),
                {"units": "1"},
            ),
            "time_bounds": (("time", "d2"), bounds),
        },
        coords={"time": bounds[:, 1]},
    )
    native.time.attrs["bounds"] = "time_bounds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    grid = xr.Dataset(
        {
            "tarea": (("nj", "ni"), [[1e4, 2e4], [3e4, 0]], {"units": "cm2"}),
            "TLAT": (("nj", "ni"), [[-70, -80], [70, 80]], {"units": "degrees_north"}),
            "TLON": (("nj", "ni"), [[0, 10], [0, 10]], {"units": "degrees_east"}),
            "tmask": (("nj", "ni"), [[1, 1], [1, 0]]),
        }
    )
    native = native.assign_coords(TLAT=grid.TLAT, TLON=grid.TLON)
    return native, grid


def entry():
    return {
        "facets": dict(
            dataset="ACCESS-OM3",
            exp="development-test",
            ensemble="member-1",
            mip="SImon",
            short_name="siconc",
            version="v1",
            grid="native-cice-t",
            frequency="mon",
            timerange="200001/200002",
        ),
        "component": "cice",
        "run_id": "development-test",
        "native_grid": "T",
        "files": ["native.nc"],
        "grid_file": "grid.nc",
    }


def source(tmp_path, entries):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"schema_version": 1, "entries": entries}))
    return MoppyDataSource(name="moppy", project="native_om3", manifest=path)


def test_development_identity_works_without_cmip_vocabulary(cice_inputs):
    native, grid = cice_inputs
    before_native, before_grid = native.copy(deep=True), grid.copy(deep=True)
    out = normalize_native(
        native,
        short_name="siconc",
        component="cice",
        grid=grid,
        run_id="unregistered-development-run",
        options={"native_grid": "T"},
    )
    assert out.attrs["evaluation_run_id"] == "unregistered-development-run"
    assert "not CMIP publication" in out.attrs["evaluation_status"]
    assert "experiment_id" not in out.attrs
    assert isinstance(out.siconc.data, da.Array)
    np.testing.assert_allclose(
        (out.siconc / 100 * out.areacello).sum(("nj", "ni")), [1.4, 2.0]
    )
    xr.testing.assert_identical(native, before_native)
    xr.testing.assert_identical(grid, before_grid)


def test_public_api_rejects_unidentified_model_and_pseudo_ph(cice_inputs):
    native, grid = cice_inputs
    kwargs = dict(
        short_name="siconc",
        component="cice",
        grid=grid,
        run_id="development",
        options={"native_grid": "T"},
    )
    with pytest.raises(ValueError, match="model_id"):
        normalize_native(native, model_id="arbitrary-model", **kwargs)
    with pytest.raises(ValueError, match="run_id"):
        normalize_native(native, **{**kwargs, "run_id": ""})
    with pytest.raises(TypeError, match="dictionary"):
        normalize_native(native, **{**kwargs, "options": []})
    with pytest.raises(ValueError, match="allow_pht_estimate"):
        normalize_native(
            native, **{**kwargs, "component": "mom6", "short_name": "phos"}
        )


def test_requested_full_period_rejects_partial_archive(cice_inputs):
    native, grid = cice_inputs
    with pytest.raises(ValueError, match="every month"):
        normalize_native(
            native,
            short_name="siconc",
            component="cice",
            grid=grid,
            run_id="test",
            options={"native_grid": "T", "required_period": [2000, 2000]},
        )


@pytest.mark.parametrize("calendar", ["360_day", "noleap"])
def test_native_calendar_is_preserved(cice_inputs, calendar):
    native, grid = cice_inputs
    edges = xr.date_range(
        "2000-01-01", periods=3, freq="MS", calendar=calendar, use_cftime=True
    )
    native = native.assign_coords(time=edges[1:])
    native["time_bounds"] = (
        ("time", "d2"),
        np.array([edges[:2], edges[1:]], dtype=object).T,
    )
    native.time.attrs["bounds"] = "time_bounds"
    native.time.encoding.update(units="days since 1850-01-01", calendar=calendar)
    result = normalize_native(
        native,
        short_name="siconc",
        component="cice",
        grid=grid,
        run_id="calendar-test",
        options={"native_grid": "T"},
    )
    assert result.time.encoding["calendar"] == calendar
    assert result.time.dt.month.values.tolist() == [1, 2]
    assert result.time.values[0].calendar == calendar


def test_manifest_identity_and_time_selection(tmp_path):
    s = source(tmp_path, [entry()])
    assert (
        len(
            s.find_data(
                dataset="ACCESS-*", exp="development-test", timerange="2000/2000"
            )
        )
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
    ],
)
def test_invalid_manifest_fails_before_loading(tmp_path, change, match):
    with pytest.raises(ValueError, match=match):
        source(tmp_path, [{**entry(), **change}])


def test_duplicate_identity_and_false_frequency_rejected(tmp_path):
    with pytest.raises(ValueError, match="Duplicate manifest"):
        source(tmp_path, [entry(), entry()])
    wrong = entry()
    wrong["facets"]["frequency"] = "fx"
    with pytest.raises(ValueError, match="fixed field"):
        source(tmp_path, [wrong])
    wrong = entry()
    wrong["facets"]["timerange"] = "20000115/20000220"
    with pytest.raises(ValueError, match="whole years or months"):
        source(tmp_path, [wrong])


def test_core_protocol_reads_lazy_cube_and_preserves_input_files(tmp_path, cice_inputs):
    pytest.importorskip("esmvalcore")
    pytest.importorskip("ncdata")
    import hashlib

    from esmvalcore.io.protocol import DataElement, DataSource

    native, grid = cice_inputs
    native.to_netcdf(tmp_path / "native.nc")
    grid.to_netcdf(tmp_path / "grid.nc")
    files = [tmp_path / "native.nc", tmp_path / "grid.nc"]
    checksums = [hashlib.sha256(p.read_bytes()).hexdigest() for p in files]
    s = source(tmp_path, [entry()])
    element = s.find_data()[0]
    assert isinstance(s, DataSource) and isinstance(element, DataElement)
    cube = element.to_iris()[0]
    assert cube.has_lazy_data()
    assert cube.coord("time").has_bounds()
    assert cube.coord("area_type").points[0] == "sea_ice"
    assert cube.coord("latitude").shape == (2, 2)
    # Dataset handles were closed by to_iris; laziness can still reopen them.
    assert not hasattr(element, "_owners")
    assert cube.data.mask[:, 1, 1].all()
    np.testing.assert_allclose(cube.data[:, 0, 0], [10, 20])
    assert element.attributes["evaluation_run_id"] == "development-test"
    assert [hashlib.sha256(p.read_bytes()).hexdigest() for p in files] == checksums


def test_manifest_cannot_overstate_available_period(tmp_path, cice_inputs):
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    native, grid = cice_inputs
    native.to_netcdf(tmp_path / "native.nc")
    grid.to_netcdf(tmp_path / "grid.nc")
    wrong = entry()
    wrong["facets"]["timerange"] = "200001/200012"
    element = source(tmp_path, [wrong]).find_data()[0]
    with pytest.raises(ValueError, match="manifest timerange"):
        element.to_iris()
    assert not hasattr(element, "_owners")
