"""Unit tests for vocabulary processor helper methods."""

from unittest.mock import mock_open, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.vocabulary_processors import CMIP6Vocabulary, VariableNotFoundError


@pytest.fixture
def mock_vocab_data():
    return {
        "experiment_id": {
            "piControl": {
                "experiment": "pre-industrial control",
                "activity_id": ["CMIP"],
            }
        },
        "source_id": {
            "ACCESS-ESM1.6": {
                "label": "ACCESS-ESM1.6",
                "institution_id": ["CSIRO"],
                "license_info": {"id": "CC BY 4.0"},
                "release_year": "2021",
                "model_component": {"atmos": {"description": "UM atmosphere model"}},
            }
        },
        "activity_id": {"CMIP": {}},
    }


@pytest.fixture
def mock_table_data():
    return {
        "Header": {
            "missing_value": "1e20",
            "int_missing_value": "-999",
            "table_id": "Amon",
        },
        "variable_entry": {
            "tas": {
                "frequency": "mon",
                "modeling_realm": "atmos",
                "units": "K",
                "type": "real",
                "dimensions": "longitude latitude time",
            },
            "sftlf": {
                "frequency": "fx",
                "modeling_realm": "land",
                "units": "%",
                "type": "integer",
                "dimensions": "longitude latitude",
            },
        },
    }


@pytest.fixture
def vocabulary_instance(mock_vocab_data, mock_table_data):
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
    ):
        return CMIP6Vocabulary(
            compound_name="Amon.tas",
            experiment_id="piControl",
            source_id="ACCESS-ESM1.6",
            variant_label="r1i2p3f4",
            grid_label="gn",
        )


@pytest.mark.unit
def test_variant_components_valid(vocabulary_instance):
    assert vocabulary_instance.get_variant_components() == {
        "realization_index": 1,
        "initialization_index": 2,
        "physics_index": 3,
        "forcing_index": 4,
    }


@pytest.mark.unit
def test_variant_components_invalid(vocabulary_instance):
    vocabulary_instance.variant_label = "bad_variant"
    with pytest.raises(ValueError, match="Invalid variant_label format"):
        vocabulary_instance.get_variant_components()


@pytest.mark.unit
def test_get_cmip_missing_value_integer_branch(mock_vocab_data, mock_table_data):
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
    ):
        vocab = CMIP6Vocabulary(
            compound_name="Amon.sftlf",
            experiment_id="piControl",
            source_id="ACCESS-ESM1.6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    # _get_variable_entry backfills missing_value; remove it to test integer fallback
    vocab.variable.pop("missing_value", None)
    assert vocab.get_cmip_missing_value() == -999.0


@pytest.mark.unit
def test_normalize_missing_values_to_nan(vocabulary_instance):
    da = xr.DataArray(
        np.array([1.0, -999.0, 2.0]),
        dims=["x"],
        attrs={"missing_value": -999.0, "_FillValue": -999.0},
    )

    result = vocabulary_instance.normalize_missing_values_to_nan(da)

    assert np.isnan(result.values[1])
    assert np.isnan(result.attrs["missing_value"])
    assert np.isnan(result.attrs["_FillValue"])


@pytest.mark.unit
def test_normalize_dataset_missing_values_static_method():
    ds = xr.Dataset(
        {
            "a": xr.DataArray(
                np.array([1.0, -1.0, 3.0]),
                dims=["x"],
                attrs={"missing_value": -1.0, "_FillValue": -1.0},
            ),
            "b": xr.DataArray(np.array([1.0, 2.0, 3.0]), dims=["x"]),
        }
    )

    result = CMIP6Vocabulary.normalize_dataset_missing_values(ds)

    assert np.isnan(result["a"].values[1])
    assert np.isnan(result["a"].attrs["missing_value"])
    assert "missing_value" not in result["b"].attrs


@pytest.mark.unit
def test_get_external_variables_cell_measures_and_heuristics(vocabulary_instance):
    vocabulary_instance.variable = {
        "cell_measures": "area: areacella volume: volcello",
        "cell_methods": "time: mean over areacello",
    }
    vocabulary_instance.cmor_name = "evspsbl"

    external = vocabulary_instance._get_external_variables()

    # Sorted output string
    assert external == "areacella areacello sftlf volcello"


@pytest.mark.unit
def test_get_required_bounds_variables(vocabulary_instance):
    mapping = {
        "tas": {
            "dimensions": {
                "lat_in": "lat",
                "time_in": "time",
            }
        }
    }

    with patch.object(
        vocabulary_instance,
        "_get_axes",
        return_value=(
            {
                "lat": {
                    "out_name": "lat",
                    "must_have_bounds": "yes",
                    "units": "degrees_north",
                },
                "time": {"out_name": "time", "must_have_bounds": "no", "units": "days"},
            },
            {},
        ),
    ):
        required, rename_map = vocabulary_instance._get_required_bounds_variables(
            mapping
        )

    assert rename_map == {"lat_in_bnds": "lat_bnds"}
    assert "lat_bnds" in required
    assert required["lat_bnds"]["out_name"] == "lat"


@pytest.mark.unit
def test_generate_filename_monthly(vocabulary_instance):
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([1.0, 2.0]),
                dims=["time"],
                coords={
                    "time": xr.DataArray(
                        [0, 31],
                        dims=["time"],
                        attrs={
                            "units": "days since 2000-01-01",
                            "calendar": "gregorian",
                        },
                    )
                },
            )
        }
    )
    attrs = {
        "variable_id": "tas",
        "table_id": "Amon",
        "source_id": "ACCESS-ESM1-6",
        "experiment_id": "piControl",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
    }

    filename = vocabulary_instance.generate_filename(attrs, ds, "tas", "Amon.tas")

    assert filename.startswith("tas_Amon_ACCESS-ESM1-6_piControl_r1i1p1f1_gn_")
    assert filename.endswith(".nc")
    assert "_200001-200002.nc" in filename


@pytest.mark.unit
def test_generate_filename_time_independent(vocabulary_instance):
    ds = xr.Dataset({"tas": xr.DataArray(np.array([1.0]), dims=["x"])})
    attrs = {
        "variable_id": "tas",
        "table_id": "fx",
        "source_id": "ACCESS-ESM1-6",
        "experiment_id": "piControl",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
    }

    filename = vocabulary_instance.generate_filename(attrs, ds, "tas", "fx.tas")
    assert filename == "tas_fx_ACCESS-ESM1-6_piControl_r1i1p1f1_gn.nc"


@pytest.mark.unit
def test_get_required_attribute_names(vocabulary_instance):
    mock_json = {"required_global_attributes": ["activity_id", "experiment_id"]}

    mock_file = mock_open(
        read_data='{"required_global_attributes": ["activity_id", "experiment_id"]}'
    )
    with (
        patch("access_moppy.vocabulary_processors.files") as mock_files,
        patch("access_moppy.vocabulary_processors.as_file") as mock_as_file,
        patch("builtins.open", mock_file),
        patch("json.load", return_value=mock_json),
    ):
        mock_cv_file = object()
        mock_files.return_value.__truediv__.return_value = mock_cv_file
        mock_as_file.return_value.__enter__.return_value = "dummy_path"

        attrs = vocabulary_instance.get_required_attribute_names()

    assert attrs == ["activity_id", "experiment_id"]


@pytest.mark.unit
def test_variable_not_found_error_formats_suggestions():
    err = VariableNotFoundError("foo", "Amon", ["Try Amon.bar", "Try day.foo"])
    msg = str(err)

    assert "Variable 'foo' not found in CMIP6 table 'Amon'." in msg
    assert "Try Amon.bar" in msg
    assert "Try day.foo" in msg
