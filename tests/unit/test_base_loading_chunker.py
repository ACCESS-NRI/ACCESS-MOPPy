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
    chunker = DatasetChunker(target_chunk_size_mb=0.000001)
    var = xr.DataArray(np.ones((5, 4), dtype=np.float32), dims=("time", "x"))

    chunks = chunker.calculate_chunk_size_for_variable(var)

    assert chunks["time"] == 1
    assert chunks["x"] == 4


@pytest.mark.unit
def test_dataset_chunker_rechunk_dataset_skips_non_chunked():
    chunker = DatasetChunker()
    ds = xr.Dataset({"tas": xr.DataArray(np.ones((3, 2)), dims=("time", "x"))})

    out = chunker.rechunk_dataset(ds)

    assert out is ds


@pytest.mark.unit
def test_dataset_chunker_rechunk_dataset_chunked_input():
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
    assert out["tas"].chunks[0][0] == 1
    assert out["time_bnds"].chunks[0] == (6,)


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
    mock_vocab, mock_mapping, temp_dir, capsys
):
    cmoriser = CMORiser(
        input_paths=["file.nc"],
        output_path=str(temp_dir),
        vocab=mock_vocab,
        variable_mapping=mock_mapping,
        compound_name="Amon.tas",
        enable_chunking=False,
    )

    cmoriser.rechunk_dataset()
    captured = capsys.readouterr()
    assert "Chunking is disabled" in captured.out
