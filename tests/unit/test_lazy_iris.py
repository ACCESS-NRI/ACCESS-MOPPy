"""Regression checks for lazy and metadata-preserving Iris conversion."""

import dask.array as da
import numpy as np
import pytest
import xarray as xr
from dask import delayed
from dask.callbacks import Callback

from access_moppy.iris_utils import dataset_to_iris


@pytest.mark.unit
def test_dataset_to_iris_keeps_data_lazy_and_preserves_ocean_metadata():
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")

    data_reads = []

    def read_field():
        data_reads.append(True)
        return np.array([[[1.0, 1e20], [0.0, np.nan]]], dtype=np.float32)

    data = da.from_delayed(delayed(read_field)(), shape=(1, 2, 2), dtype=np.float32)
    latitude = np.array([[-30.0, -30.0], [-20.0, -20.0]])
    longitude = np.array([[145.0, 155.0], [145.0, 155.0]])
    lat_bounds = np.stack(
        [latitude - 1, latitude - 1, latitude + 1, latitude + 1], axis=-1
    )
    lon_bounds = np.stack(
        [longitude - 1, longitude + 1, longitude + 1, longitude - 1], axis=-1
    )
    ds = xr.Dataset(
        {
            "tos": (
                ("time", "y", "x"),
                data,
                {"units": "K", "cell_measures": "area: areacello", "_FillValue": 1e20},
            ),
            "latitude": (
                ("y", "x"),
                latitude,
                {
                    "standard_name": "latitude",
                    "units": "degrees_north",
                    "bounds": "vertices_latitude",
                },
            ),
            "longitude": (
                ("y", "x"),
                longitude,
                {
                    "standard_name": "longitude",
                    "units": "degrees_east",
                    "bounds": "vertices_longitude",
                },
            ),
            "vertices_latitude": (("y", "x", "vertices"), lat_bounds),
            "vertices_longitude": (("y", "x", "vertices"), lon_bounds),
            "areacello": (("y", "x"), np.full((2, 2), 1e6), {"units": "m2"}),
        },
        coords={"time": ("time", [0.0], {"units": "days since 2000-01-01"})},
    )
    original_vars = set(ds.data_vars)
    tasks = []
    with Callback(pretask=lambda key, graph, state: tasks.append(key)):
        cube = dataset_to_iris(ds, "tos")
        assert not data_reads
        assert cube.has_lazy_data()
        assert isinstance(cube.core_data(), da.Array)
        assert cube.coord("latitude").bounds.shape == (2, 2, 4)
        np.testing.assert_array_equal(cube.coord("longitude").points, longitude)
        measures = cube.cell_measures()
        assert len(measures) == 1
        assert measures[0].measure == "area"
        np.testing.assert_array_equal(measures[0].data, np.full((2, 2), 1e6))
        assert not data_reads
        result = cube.data

    assert tasks, "Dask callback must observe the deferred computation"
    assert data_reads == [
        True
    ], "The main field is read only when cube.data is requested"
    np.testing.assert_array_equal(
        np.ma.getmaskarray(result), [[[False, True], [False, True]]]
    )
    assert set(ds.data_vars) == original_vars
    assert ds.tos.attrs["_FillValue"] == 1e20
    assert isinstance(ds.tos.data, da.Array)


@pytest.mark.unit
@pytest.mark.parametrize("metadata", ["attrs", "encoding"])
def test_zero_fill_value_is_masked_from_either_metadata_location(metadata):
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    data = xr.DataArray(da.from_array(np.array([0.0, 2.0]), chunks=1), dims="time")
    if metadata == "attrs":
        data.attrs["missing_value"] = 0.0
    else:
        data.encoding["_FillValue"] = 0.0
    cube = dataset_to_iris(xr.Dataset({"tas": data}), "tas")
    assert cube.has_lazy_data()
    np.testing.assert_array_equal(np.ma.getmaskarray(cube.data), [True, False])


@pytest.mark.unit
def test_scalar_string_coordinate_becomes_iris_aux_coord():
    pytest.importorskip("iris")
    pytest.importorskip("ncdata")
    field = xr.DataArray(da.from_array([0.2, 0.4], chunks=1), dims="time")
    ds = xr.Dataset({"siconc": field}).assign_coords(
        type=xr.DataArray(
            "sea_ice",
            attrs={"standard_name": "area_type", "long_name": "Sea Ice area type"},
        )
    )
    cube = dataset_to_iris(ds, "siconc")
    assert cube.has_lazy_data()
    assert cube.coord("area_type").points[0] == "sea_ice"
    assert cube.coord("area_type").var_name == "type"
    assert str(cube.coord("area_type").units) == "unknown"
    assert ds.coords["type"].item() == "sea_ice"
