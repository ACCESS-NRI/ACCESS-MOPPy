"""Unit tests for vocabulary processor helper methods."""

from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.vocabulary_processors import (
    CMIP6Vocabulary,
    CMIP7Vocabulary,
    VariableNotFoundError,
)


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
def test_get_required_bounds_variables_z_bounds_factors(vocabulary_instance):
    """z_bounds_factors: factors whose output ends in _bnds are added to rename map."""
    mapping = {
        "zfull": {
            "model_variables": ["zfull"],
            "dimensions": {
                "sigma_theta": "b",  # source_name → out_name
                "theta_level_height": "lev",
            },
        }
    }
    hybrid_axis = {
        "out_name": "lev",
        "z_bounds_factors": "a: lev_bnds b: b_bnds orog: orog",
        "must_have_bounds": "no",
        "units": "m",
        "long_name": "Model level",
    }
    with patch.object(
        vocabulary_instance,
        "_get_axes",
        return_value=({"lev": hybrid_axis}, {}),
    ):
        required, rename_map = vocabulary_instance._get_required_bounds_variables(
            mapping
        )

    # sigma_theta→b, so sigma_theta_bnds→b_bnds must be in the rename map
    assert rename_map.get("sigma_theta_bnds") == "b_bnds"
    assert "b_bnds" in required
    # 'orog' output doesn't end with _bnds → should be skipped
    assert "orog" not in required


@pytest.mark.unit
def test_get_required_bounds_variables_z_bounds_factors_unmatched_skipped(
    vocabulary_instance,
):
    """Factors absent from the dimension mapping produce no entry."""
    mapping = {
        "zfull": {
            "model_variables": ["zfull"],
            "dimensions": {
                "theta_level_height": "lev",  # 'b'/'sigma_theta' NOT present
            },
        }
    }
    hybrid_axis = {
        "out_name": "lev",
        "z_bounds_factors": "b: b_bnds",
        "must_have_bounds": "no",
        "units": "m",
    }
    with patch.object(
        vocabulary_instance,
        "_get_axes",
        return_value=({"lev": hybrid_axis}, {}),
    ):
        required, rename_map = vocabulary_instance._get_required_bounds_variables(
            mapping
        )

    # 'b' not in inverted mapping → nothing added
    assert "b_bnds" not in required
    assert not any(v == "b_bnds" for v in rename_map.values())


@pytest.mark.unit
def test_cmip7_get_required_bounds_variables_z_bounds_factors(cmip7_vocab_instance):
    """CMIP7: z_bounds_factors processing is identical to CMIP6.
    Includes a plain axis (lat) with no z_bounds_factors to cover the continue branch.
    """
    mapping = {
        "zfull": {
            "model_variables": ["zfull"],
            "dimensions": {
                "sigma_theta": "b",
                "theta_level_height": "lev",
            },
        }
    }
    hybrid_axis = {
        "out_name": "lev",
        "z_bounds_factors": "a: lev_bnds b: b_bnds orog: orog",
        "must_have_bounds": "no",
        "units": "m",
    }
    # lat has no z_bounds_factors → exercises the continue branch in the loop
    lat_axis = {"out_name": "lat", "must_have_bounds": "no", "units": "degrees_north"}
    with patch.object(
        cmip7_vocab_instance,
        "_get_axes",
        return_value=({"lev": hybrid_axis, "lat": lat_axis}, {}),
    ):
        required, rename_map = cmip7_vocab_instance._get_required_bounds_variables(
            mapping
        )

    assert rename_map.get("sigma_theta_bnds") == "b_bnds"
    assert "b_bnds" in required


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


_TIME_RANGE_TEMPLATE = {"filename_template": "<variable_id>_<table_id>[_<time_range>]"}
_FILENAME_ATTRS = {
    "variable_id": "tas",
    "table_id": "Amon",
    "source_id": "ACCESS-ESM1-6",
    "experiment_id": "piControl",
    "variant_label": "r1i1p1f1",
    "grid_label": "gn",
}


@pytest.mark.unit
def test_generate_filename_cftime_time_branch(vocabulary_instance):
    """cftime objects (dtype=object) – uses hasattr(.year) branch."""
    cf_time = xr.cftime_range("2020-01", periods=2, freq="MS", calendar="gregorian")
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={"time": cf_time},
            )
        }
    )
    assert ds["tas"].coords["time"].dtype == object  # Confirm cftime dtype

    with patch.object(
        CMIP6Vocabulary, "_load_drs_templates", return_value=_TIME_RANGE_TEMPLATE
    ):
        filename = vocabulary_instance.generate_filename(
            _FILENAME_ATTRS, ds, "tas", "Amon.tas"
        )

    # Monthly format YYYYMM
    assert "202001-202002" in filename


@pytest.mark.unit
def test_generate_filename_datetime64_time_branch(vocabulary_instance):
    """numpy datetime64 time – uses pd.Timestamp branch."""
    dt_time = pd.date_range("2020-01-01", periods=2, freq="MS")
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={"time": dt_time},
            )
        }
    )
    assert np.issubdtype(ds["tas"].coords["time"].dtype, np.datetime64)

    with patch.object(
        CMIP6Vocabulary, "_load_drs_templates", return_value=_TIME_RANGE_TEMPLATE
    ):
        filename = vocabulary_instance.generate_filename(
            _FILENAME_ATTRS, ds, "tas", "Amon.tas"
        )

    assert "202001-202002" in filename


@pytest.mark.unit
def test_generate_filename_numeric_time_branch(vocabulary_instance):
    """Numeric float64 time – uses num2date (else) branch."""
    time_values = np.array([0.0, 31.0], dtype=np.float64)
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={
                    "time": xr.Variable(
                        "time",
                        time_values,
                        attrs={
                            "units": "days since 2020-01-01",
                            "calendar": "standard",
                        },
                    )
                },
            )
        }
    )
    assert ds["tas"].coords["time"].dtype == np.float64

    with patch.object(
        CMIP6Vocabulary, "_load_drs_templates", return_value=_TIME_RANGE_TEMPLATE
    ):
        filename = vocabulary_instance.generate_filename(
            _FILENAME_ATTRS, ds, "tas", "Amon.tas"
        )

    # 0 days since 2020-01-01 = Jan 2020; 31 days = Feb 2020
    assert "202001" in filename
    assert "202002" in filename


@pytest.mark.unit
def test_generate_filename_subdaily_format(vocabulary_instance):
    """Sub-daily table produces YYYYMMDDHHMM format."""
    dt_time = pd.date_range("2020-01-01 00:00", periods=2, freq="3h")
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={"time": dt_time},
            )
        }
    )
    attrs = {**_FILENAME_ATTRS, "table_id": "3hr"}

    with patch.object(
        CMIP6Vocabulary, "_load_drs_templates", return_value=_TIME_RANGE_TEMPLATE
    ):
        filename = vocabulary_instance.generate_filename(attrs, ds, "tas", "3hr.tas")

    # Subdaily: YYYYMMDDHHMM → 202001010000-202001010300
    assert "202001010000" in filename
    assert "202001010300" in filename


@pytest.mark.unit
def test_generate_filename_daily_format(vocabulary_instance):
    """Daily table produces YYYYMMDD format."""
    dt_time = pd.date_range("2020-01-01", periods=2, freq="D")
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={"time": dt_time},
            )
        }
    )
    attrs = {**_FILENAME_ATTRS, "table_id": "day"}

    with patch.object(
        CMIP6Vocabulary, "_load_drs_templates", return_value=_TIME_RANGE_TEMPLATE
    ):
        filename = vocabulary_instance.generate_filename(attrs, ds, "tas", "day.tas")

    # Daily: YYYYMMDD → 20200101-20200102
    assert "20200101" in filename
    assert "20200102" in filename


@pytest.fixture
def cmip7_vocab_instance():
    """Minimal CMIP7Vocabulary instance with all file IO mocked out."""
    mock_cv = {
        "experiment_id": {
            "historical": {
                "experiment": "historical",
                "activity_id": ["CMIP"],
            }
        },
        "source_id": {
            "ACCESS-ESM1-6": {
                "label": "ACCESS-ESM1-6",
                "institution_id": ["CSIRO"],
                "license_info": {"id": "CC BY 4.0"},
                "release_year": "2021",
                "model_component": {"atmos": {"description": "UM"}},
            }
        },
        "activity_id": {"CMIP": {}},
    }
    mock_table = {
        "Header": {"table_id": "Amon"},
        "variable_entry": {
            "tas": {
                "frequency": "mon",
                "units": "K",
                "type": "real",
                "dimensions": "longitude latitude time",
            }
        },
    }
    with (
        patch.object(
            CMIP7Vocabulary,
            "_get_experiment",
            return_value=mock_cv["experiment_id"]["historical"],
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_source",
            return_value=mock_cv["source_id"]["ACCESS-ESM1-6"],
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        return CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )


_CMIP7_ATTRS = {
    "frequency": "mon",
    "region": "glb",
    "grid_label": "gn",
    "source_id": "ACCESS-ESM1-6",
    "experiment_id": "historical",
    "variant_label": "r1i1p1f1",
}


@pytest.mark.unit
def test_cmip7_generate_filename_cftime_time_branch(cmip7_vocab_instance):
    """CMIP7: cftime objects (dtype=object) – uses hasattr(.year) branch."""
    cf_time = xr.cftime_range("2020-01", periods=2, freq="MS", calendar="gregorian")
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]), dims=["time"], coords={"time": cf_time}
            )
        }
    )
    assert ds["tas"].coords["time"].dtype == object

    filename = cmip7_vocab_instance.generate_filename(
        _CMIP7_ATTRS, ds, "tas", "Amon.tas"
    )

    assert "202001" in filename
    assert "202002" in filename


@pytest.mark.unit
def test_cmip7_generate_filename_datetime64_time_branch(cmip7_vocab_instance):
    """CMIP7: numpy datetime64 time – uses pd.Timestamp branch."""
    dt_time = pd.date_range("2020-01-01", periods=2, freq="MS")
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]), dims=["time"], coords={"time": dt_time}
            )
        }
    )
    assert np.issubdtype(ds["tas"].coords["time"].dtype, np.datetime64)

    filename = cmip7_vocab_instance.generate_filename(
        _CMIP7_ATTRS, ds, "tas", "Amon.tas"
    )

    assert "202001" in filename
    assert "202002" in filename


@pytest.mark.unit
def test_cmip7_generate_filename_numeric_time_branch(cmip7_vocab_instance):
    """CMIP7: numeric float64 time – uses num2date (else) branch."""
    time_values = np.array([0.0, 31.0], dtype=np.float64)
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={
                    "time": xr.Variable(
                        "time",
                        time_values,
                        attrs={
                            "units": "days since 2020-01-01",
                            "calendar": "standard",
                        },
                    )
                },
            )
        }
    )
    assert ds["tas"].coords["time"].dtype == np.float64

    filename = cmip7_vocab_instance.generate_filename(
        _CMIP7_ATTRS, ds, "tas", "Amon.tas"
    )

    assert "202001" in filename
    assert "202002" in filename
