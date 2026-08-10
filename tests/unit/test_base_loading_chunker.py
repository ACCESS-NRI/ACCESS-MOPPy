"""Additional unit tests for base loading and chunking logic."""

from unittest.mock import Mock

import cftime
import dask.array as da
import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser, DatasetChunker


@pytest.fixture
def mock_vocab():
    vocab = Mock()
    vocab.standardize_missing_values = Mock(side_effect=lambda x, **kwargs: x)
    vocab.get_cmip_missing_value = Mock(return_value=1e20)
    return vocab


@pytest.fixture
def mock_mapping():
    return {
        "CF standard Name": "air_temperature",
        "units": "K",
        "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
        "positive": None,
    }


@pytest.mark.unit
def test_dataset_chunker_calculate_chunk_size_for_time_variable():
    # target is effectively 0, so a single time step already meets it; with
    # the default 128MB max and only 5*4 float32 elements total, everything
    # fits under the max, so all 5 steps batch into one task (see
    # test_dataset_chunker_handles_oversized_time_only_element for the
    # complementary case where the max is also tiny and 1 step is correct).
    chunker = DatasetChunker(target_chunk_size_mb=0.000001)
    var = xr.DataArray(np.ones((5, 4), dtype=np.float32), dims=("time", "x"))

    chunks = chunker.calculate_chunk_size_for_variable(var)

    assert chunks["time"] == 5
    assert chunks["x"] == 4


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"target_chunk_size_mb": 0}, "target_chunk_size_mb must be positive"),
        (
            {"target_chunk_size_mb": 4, "max_chunk_size_mb": 2},
            "max_chunk_size_mb must be greater than or equal",
        ),
    ],
)
def test_dataset_chunker_rejects_invalid_size_bounds(kwargs, message):
    with pytest.raises(ValueError, match=message):
        DatasetChunker(**kwargs)


@pytest.mark.unit
def test_dataset_chunker_handles_oversized_time_only_element():
    chunker = DatasetChunker(
        target_chunk_size_mb=0.0000001,
        max_chunk_size_mb=0.0000001,
    )
    var = xr.DataArray(np.ones(3, dtype=np.float64), dims=("time",))

    chunks = chunker.calculate_chunk_size_for_variable(var)

    assert chunks == {"time": 1}


@pytest.mark.unit
def test_dataset_chunker_bounds_large_spatial_chunks():
    chunker = DatasetChunker(target_chunk_size_mb=4, max_chunk_size_mb=32)
    var = xr.DataArray(
        da.empty((12, 50, 1000, 1000), dtype=np.float32),
        dims=("time", "lev", "j", "i"),
    )

    chunks = chunker.calculate_chunk_size_for_variable(var)
    chunk_bytes = np.dtype(var.dtype).itemsize * np.prod(list(chunks.values()))

    assert chunks["time"] == 1
    assert chunks["lev"] < var.sizes["lev"] or chunks["j"] < var.sizes["j"]
    assert chunk_bytes <= 32 * 1024 * 1024


@pytest.mark.unit
def test_dataset_chunker_preserves_spatial_slab_when_below_maximum():
    chunker = DatasetChunker(target_chunk_size_mb=4, max_chunk_size_mb=32)
    var = xr.DataArray(
        da.empty((12, 10, 100, 100), dtype=np.float32),
        dims=("time", "lev", "j", "i"),
    )

    chunks = chunker.calculate_chunk_size_for_variable(var)

    assert chunks == {"time": 11, "lev": 10, "j": 100, "i": 100}


@pytest.mark.unit
def test_dataset_chunker_batches_multiple_steps_when_one_step_exceeds_target():
    """Regression test: a single time step that already exceeds the target
    (but is well under the max) must batch multiple steps per task instead
    of degenerating to 1 -- this was the atmos.cl bug (38 model levels,
    145x145 grid, float32 -> ~4.04MB/step against a 4MB target / 128MB max,
    previously producing 1 task/month instead of ~31).
    """
    chunker = DatasetChunker(target_chunk_size_mb=4, max_chunk_size_mb=128)
    var = xr.DataArray(
        da.empty((40, 38, 145, 192), dtype=np.float32),
        dims=("time", "lev", "lat", "lon"),
    )

    chunks = chunker.calculate_chunk_size_for_variable(var)
    chunk_bytes = np.dtype(var.dtype).itemsize * np.prod(list(chunks.values()))

    assert chunks["time"] == 31
    assert chunks["lev"] == 38 and chunks["lat"] == 145 and chunks["lon"] == 192
    assert chunk_bytes <= 128 * 1024 * 1024
    # Confirms it's actually using the headroom, not just clamping to 1.
    assert chunks["time"] > 1


@pytest.mark.unit
def test_dataset_chunker_single_step_over_max_still_clamps_to_one():
    """Complementary case: when a single time step alone already exceeds
    the max (not just the target), batching is impossible -- must still
    clamp to 1 time step, same as before this change (the spatial/vertical
    splitting loop then takes over, covered by
    test_dataset_chunker_bounds_large_spatial_chunks).
    """
    chunker = DatasetChunker(target_chunk_size_mb=4, max_chunk_size_mb=32)
    var = xr.DataArray(
        da.empty((12, 50, 1000, 1000), dtype=np.float32),
        dims=("time", "lev", "j", "i"),
    )

    chunks = chunker.calculate_chunk_size_for_variable(var)

    assert chunks["time"] == 1


@pytest.mark.unit
def test_dataset_chunker_rechunk_dataset_skips_non_chunked():
    chunker = DatasetChunker()
    ds = xr.Dataset({"tas": xr.DataArray(np.ones((3, 2)), dims=("time", "x"))})

    out = chunker.rechunk_dataset(ds)

    assert out is ds


@pytest.mark.unit
def test_dataset_chunker_rechunk_dataset_chunked_input():
    # As above: near-0 target + default 128MB max + a tiny total array means
    # everything fits in one task, so "tas" ends up with all 6 time steps in
    # a single chunk rather than being forced down to 1.
    chunker = DatasetChunker(target_chunk_size_mb=0.000001)

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                da.from_array(np.ones((6, 4), dtype=np.float32), chunks=(2, 4)),
                dims=("time", "x"),
            ),
            "time_bnds": xr.DataArray(
                da.from_array(np.arange(12).reshape(6, 2), chunks=(2, 2)),
                dims=("time", "nv"),
            ),
        },
        coords={"time": np.arange(6), "x": np.arange(4)},
    )

    out = chunker.rechunk_dataset(ds)

    assert out is not ds
    assert out["tas"].chunks is not None
    assert out["tas"].chunks[0][0] == 6
    assert out["time_bnds"].chunks[0] == (6,)


@pytest.mark.unit
def test_dataset_chunker_rechunk_dataset_unifies_inconsistent_chunks():
    chunker = DatasetChunker(target_chunk_size_mb=0.000001)

    ds = xr.Dataset(
        {
            "var_a": xr.DataArray(
                da.from_array(np.ones((6, 4), dtype=np.float32), chunks=(2, 4)),
                dims=("yt_ocean", "xt_ocean"),
            ),
            "var_b": xr.DataArray(
                da.from_array(np.ones((6, 4), dtype=np.float32), chunks=(3, 4)),
                dims=("yt_ocean", "xt_ocean"),
            ),
        },
        coords={"yt_ocean": np.arange(6), "xt_ocean": np.arange(4)},
    )

    out = chunker.rechunk_dataset(ds)

    assert out["var_a"].chunks is not None
    assert out["var_b"].chunks is not None
    assert out["var_a"].chunks[0] == (6,)
    assert out["var_b"].chunks[0] == (6,)


@pytest.mark.unit
def test_cmoriser_init_input_data_dataarray_converts_to_dataset(
    mock_vocab, mock_mapping, temp_dir
):
    data = xr.DataArray(np.ones((2, 2)), dims=("time", "lat"), name="tas")

    cmoriser = CMORiser(
        input_data=data,
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
    )

    assert cmoriser.input_is_xarray is True
    assert isinstance(cmoriser.input_dataset, xr.Dataset)
    assert "tas" in cmoriser.input_dataset.data_vars


@pytest.mark.unit
def test_load_dataset_allows_duplicate_non_time_indexes(tmp_path):
    def make_dataset(time, repeated_latitude):
        return xr.Dataset(
            {
                "tau_x": (
                    ("time", "yu_ocean", "xu_ocean"),
                    np.full((1, 3, 2), time),
                )
            },
            coords={
                "time": [time],
                "yu_ocean": [-10.0, repeated_latitude, repeated_latitude],
                "xu_ocean": [0.0, 1.0],
            },
        )

    datasets = [make_dataset(0, 10.0), make_dataset(1, 11.0)]
    with pytest.raises(ValueError, match="index has duplicate values"):
        xr.combine_nested(
            datasets,
            concat_dim="time",
            data_vars="minimal",
            coords="minimal",
            compat="override",
            join="outer",
        )

    input_paths = []
    for index, dataset in enumerate(datasets):
        path = tmp_path / f"tau_x_{index}.nc"
        dataset.to_netcdf(path, format="NETCDF3_64BIT")
        input_paths.append(str(path))

    cmoriser = CMORiser(
        input_data=input_paths,
        output_path=str(tmp_path),
        vocab=Mock(),
        variable_mapping={},
        compound_name="Omon.tauuo",
        enable_chunking=False,
    )
    cmoriser.load_dataset(required_vars={"tau_x"})

    assert "yu_ocean" in cmoriser.ds.coords
    assert "yu_ocean" not in cmoriser.ds.indexes
    assert cmoriser.ds["tau_x"].shape == (2, 3, 2)
    np.testing.assert_array_equal(cmoriser.ds["tau_x"][:, 1, 0], [0, 1])


@pytest.mark.unit
def test_cmoriser_init_with_deprecated_input_paths_warns(
    mock_vocab, mock_mapping, temp_dir
):
    with pytest.warns(DeprecationWarning, match="input_paths"):
        cmoriser = CMORiser(
            input_paths=["file1.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )

    assert cmoriser.input_paths == ["file1.nc"]


@pytest.mark.unit
def test_cmoriser_init_rejects_both_input_params(mock_vocab, mock_mapping, temp_dir):
    with pytest.raises(
        ValueError, match="Cannot specify both 'input_data' and 'input_paths'"
    ):
        CMORiser(
            input_data=xr.Dataset(),
            input_paths=["file1.nc"],
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )


@pytest.mark.unit
def test_cmoriser_init_requires_input(mock_vocab, mock_mapping, temp_dir):
    with pytest.raises(
        ValueError, match="Must specify either 'input_data' or 'input_paths'"
    ):
        CMORiser(
            output_path=str(temp_dir),
            vocab=mock_vocab,
            variable_mapping=mock_mapping,
            compound_name="Amon.tas",
        )


@pytest.mark.unit
def test_cmoriser_init_forwards_maximum_chunk_size(mock_vocab, mock_mapping, temp_dir):
    cmoriser = CMORiser(
        input_data=xr.Dataset(),
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        max_chunk_size_mb=64,
    )

    assert cmoriser.chunker is not None
    assert cmoriser.chunker.max_chunk_size_mb == 64


@pytest.mark.unit
def test_load_dataset_xarray_filters_required_vars_and_warns_missing(
    mock_vocab, mock_mapping, temp_dir
):
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(np.arange(6).reshape(3, 2), dims=("time_0", "lat")),
            "other": xr.DataArray(np.arange(2), dims=("lat",)),
        },
        coords={
            "time_0": np.arange(3),
            "lat": np.array([-10.0, 10.0]),
            "unused": xr.DataArray(np.array([1]), dims=("dummy",)),
        },
    )

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        enable_chunking=False,
    )

    with pytest.warns(UserWarning, match="Some required variables not found"):
        cmoriser.load_dataset(required_vars=["tas", "missing_var"])

    assert "tas" in cmoriser.ds.data_vars
    assert "other" not in cmoriser.ds.data_vars
    assert "time_0" not in cmoriser.ds.dims


@pytest.mark.unit
def test_ensure_numeric_time_coordinates_converts_cftime_without_units(
    mock_vocab, mock_mapping, temp_dir
):
    ds = xr.Dataset(
        coords={
            "time": xr.DataArray(
                [cftime.DatetimeNoLeap(2000, 1, 1), cftime.DatetimeNoLeap(2000, 1, 2)],
                dims=("time",),
            )
        }
    )

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        enable_chunking=False,
    )

    with pytest.warns(UserWarning, match="has no 'units' attribute"):
        out = cmoriser._ensure_numeric_time_coordinates(ds)

    assert np.issubdtype(out["time"].dtype, np.number)
    assert out["time"].attrs["units"] == "days since 0001-01-01"


@pytest.mark.unit
def test_rechunk_dataset_method_handles_disabled_and_no_dataset(
    mock_vocab, mock_mapping, temp_dir, caplog
):
    cmoriser = CMORiser(
        input_paths=["file.nc"],
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        enable_chunking=False,
    )

    import logging

    with caplog.at_level(logging.DEBUG, logger="access_moppy.base"):
        cmoriser.rechunk_dataset()
    assert "Chunking is disabled" in caplog.text


# ==================== DatasetChunker — coordinate rechunking ====================


@pytest.mark.unit
def test_dataset_chunker_rechunk_dataset_splits_coords_and_data_vars():
    """Dask coordinates must land in rechunked_coords, data vars in rechunked_data_vars."""
    chunker = DatasetChunker(target_chunk_size_mb=0.000001)

    data = da.from_array(np.ones((6, 4), dtype=np.float32), chunks=(2, 4))
    time_coord = da.from_array(np.arange(6, dtype=np.float64), chunks=(6,))

    ds = xr.Dataset(
        {"tas": xr.DataArray(data, dims=("time", "x"))},
        coords={"time": xr.DataArray(time_coord, dims=("time",))},
    )

    out = chunker.rechunk_dataset(ds)

    assert "time" in out.coords
    assert "tas" in out.data_vars


# ==================== CMORiser._check_range with dask ====================


@pytest.mark.unit
def test_check_range_dask_warns_for_out_of_range_values(
    mock_vocab, mock_mapping, temp_dir
):
    """_check_range fused-compute path emits a UserWarning when values are out of range."""
    data = da.from_array(np.array([1.0, 2.0, 300.0]), chunks=(3,))
    ds = xr.Dataset({"tas": xr.DataArray(data, dims=("time",))})

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        enable_chunking=False,
    )
    cmoriser.ds = ds

    with pytest.warns(UserWarning, match="above valid_max"):
        cmoriser._check_range("tas", vmin=0.0, vmax=100.0)


@pytest.mark.unit
def test_check_range_dask_passes_for_in_range_values(
    mock_vocab, mock_mapping, temp_dir
):
    """_check_range fused-compute path does not raise when all values are in range."""
    data = da.from_array(np.array([10.0, 20.0, 30.0]), chunks=(3,))
    ds = xr.Dataset({"tas": xr.DataArray(data, dims=("time",))})

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        enable_chunking=False,
    )
    cmoriser.ds = ds

    cmoriser._check_range("tas", vmin=0.0, vmax=100.0)  # Should not raise
