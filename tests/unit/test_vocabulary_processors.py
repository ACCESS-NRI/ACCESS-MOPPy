"""Unit tests for vocabulary processor helper methods."""

import warnings
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.vocabulary_processors import (
    _PARENT_ATTRIBUTE_KEYS,
    CMIP6Vocabulary,
    CMIP7Vocabulary,
    VariableNotFoundError,
    _cast_missing_value_to_data_dtype,
    _load_cmor_cvs,
    _remove_parent_attributes,
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
            "ACCESS-ESM1-6": {
                "label": "ACCESS-ESM1-6",
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
            source_id="ACCESS-ESM1-6",
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
@pytest.mark.parametrize(
    "parent_experiment_id", [[], ["none"], ["no parent"], ["piControl-spinup"]]
)
def test_cmip6_root_experiment_omits_parent_attributes(
    mock_vocab_data, mock_table_data, parent_experiment_id
):
    mock_vocab_data["experiment_id"]["piControl"]["parent_experiment_id"] = (
        parent_experiment_id
    )
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
    ):
        vocab = CMIP6Vocabulary(
            compound_name="Amon.tas",
            experiment_id="piControl",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    assert vocab.get_parent_experiment_attrs() == {}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("parent_experiment_id", "expected"),
    [("piControl", True), (["piControl"], True), ("none", False)],
)
def test_cmip6_parent_requirement_handles_cv_shapes(parent_experiment_id, expected):
    vocab = object.__new__(CMIP6Vocabulary)
    vocab.experiment_id = "historical"
    vocab.experiment = {"parent_experiment_id": parent_experiment_id}

    assert vocab.requires_parent_information() is expected


@pytest.mark.unit
def test_cmip6_nonroot_experiment_returns_validated_parent_attributes():
    parent_info = {
        "parent_experiment_id": "piControl",
        "parent_activity_id": "CMIP",
        "parent_mip_era": "CMIP6",
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 0.0,
        "branch_method": "standard",
    }
    vocab = object.__new__(CMIP6Vocabulary)
    vocab.experiment_id = "historical"
    vocab.experiment = {"parent_experiment_id": ["piControl"]}
    vocab.user_defined_parents = parent_info
    vocab.vocab = {
        "experiment_id": {"piControl": {}},
        "activity_id": {"CMIP": {}},
        "source_id": {"ACCESS-ESM1-6": {}},
    }

    assert vocab.get_parent_experiment_attrs() == parent_info


@pytest.mark.unit
def test_cmip7_root_experiment_warns_when_parent_attributes_are_supplied():
    """piControl-spinup is the true CMIP7 root (empty parent_experiment_id in
    the CV); stray parent attributes supplied for it should be dropped with
    a warning."""
    parent_info = {"parent_experiment_id": "piControl-spinup"}
    vocab = object.__new__(CMIP7Vocabulary)
    vocab.experiment_id = "piControl-spinup"
    vocab.experiment = {"parent_experiment": []}
    vocab.user_defined_parents = parent_info

    with pytest.warns(UserWarning, match="has no published parent.*will be removed"):
        assert vocab.get_parent_experiment_attrs() == {}


@pytest.mark.unit
def test_cmip7_root_experiment_without_parent_attributes_is_silent():
    vocab = object.__new__(CMIP7Vocabulary)
    vocab.experiment_id = "piControl-spinup"
    vocab.experiment = {"parent_experiment": []}
    vocab.user_defined_parents = {}

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert vocab.get_parent_experiment_attrs() == {}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("parent_experiment", "expected"),
    [
        ("piControl", True),
        (["piControl"], True),
        ("none", False),
        ([], False),
        # piControl itself is NOT a root in the real CMIP7 CV: it declares
        # parent_experiment_id -> ["piControl-spinup"], so it must require
        # parent metadata rather than being special-cased as a root.
        (["piControl-spinup"], True),
    ],
)
def test_cmip7_parent_requirement_handles_cv_shapes(parent_experiment, expected):
    vocab = object.__new__(CMIP7Vocabulary)
    vocab.experiment_id = "piControl"
    vocab.experiment = {"parent_experiment": parent_experiment}

    assert vocab.requires_parent_information() is expected


@pytest.mark.unit
def test_remove_parent_attributes_scrubs_complete_parent_metadata():
    attrs = {key: "supplied" for key in _PARENT_ATTRIBUTE_KEYS}
    attrs["experiment_id"] = "piControl"

    result = _remove_parent_attributes(attrs)

    assert _PARENT_ATTRIBUTE_KEYS.isdisjoint(result)
    assert result["experiment_id"] == "piControl"


@pytest.mark.unit
def test_cmip6_global_attributes_scrub_supplemental_parent_metadata(
    vocabulary_instance,
):
    vocab = vocabulary_instance
    vocab.variable["modeling_realm"] = "atmos"
    vocab.supplemental_global_attributes = {
        key: "supplied" for key in _PARENT_ATTRIBUTE_KEYS
    }

    with patch.multiple(
        vocab,
        _resolve_activity_id=lambda: "CMIP",
        _get_further_info_url=lambda: "https://example.com",
        _get_institution=lambda: "CSIRO",
        _get_license=lambda: "CC BY 4.0",
        _get_nominal_resolution=lambda **kwargs: "250 km",
        _format_source_string=lambda: "ACCESS-ESM1-6",
        _get_source_type=lambda: "AOGCM",
        _get_sub_experiment=lambda: "none",
        _get_sub_experiment_id=lambda: "none",
        _get_external_variables=lambda: None,
    ):
        attrs = vocab.get_required_global_attributes()

        with (
            patch.object(vocab, "requires_parent_information", return_value=True),
            patch.object(vocab, "get_parent_experiment_attrs", return_value={}),
        ):
            nonroot_attrs = vocab.get_required_global_attributes()

    assert _PARENT_ATTRIBUTE_KEYS.isdisjoint(attrs)
    assert nonroot_attrs["parent_experiment_id"] == "supplied"


@pytest.mark.unit
def test_cmip7_global_attributes_scrub_supplemental_parent_metadata(
    cmip7_vocab_instance,
):
    vocab = cmip7_vocab_instance
    vocab.experiment_id = "piControl-spinup"
    vocab.experiment = {"parent_experiment": []}
    vocab.variable["modeling_realm"] = "atmos"
    vocab.supplemental_global_attributes = {
        key: "supplied" for key in _PARENT_ATTRIBUTE_KEYS
    }

    with patch.multiple(
        vocab,
        get_variant_components=lambda: {
            "realization_index": 1,
            "initialization_index": 1,
            "physics_index": 1,
            "forcing_index": 1,
        },
        _resolve_activity_id=lambda: "CMIP",
        _get_area_label=lambda: "glb",
        _get_branding_suffix=lambda: "tavg-h2m-hxy-u",
        _get_data_specs_version=lambda: "1.0",
        _get_drs_specs=lambda: "MIP-DRS",
        _get_horizontal_label=lambda: "hxy",
        _get_institution_name=lambda: "ACCESS Consortium",
        _get_license_id=lambda: "CC-BY-4.0",
        _get_nominal_resolution=lambda: "250 km",
        _get_validated_region=lambda: "glb",
        _get_temporal_label=lambda: "tavg",
        _get_vertical_label=lambda: "u",
        _get_external_variables=lambda: None,
    ):
        attrs = vocab.get_required_global_attributes()

        with (
            patch.object(vocab, "requires_parent_information", return_value=True),
            patch.object(vocab, "get_parent_experiment_attrs", return_value={}),
        ):
            nonroot_attrs = vocab.get_required_global_attributes()

    assert _PARENT_ATTRIBUTE_KEYS.isdisjoint(attrs)
    assert nonroot_attrs["parent_experiment_id"] == "supplied"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("experiment_metadata", "expected"),
    [
        (
            {
                "activity": ["CMIP"],
                "experiment": "Pre-industrial control simulation.",
            },
            "Pre-industrial control simulation.",
        ),
        ({"activity": ["CMIP"]}, None),
    ],
)
def test_cmip7_global_attributes_include_optional_experiment(
    cmip7_vocab_instance, experiment_metadata, expected
):
    cmip7_vocab_instance.experiment = experiment_metadata
    cmip7_vocab_instance.variable["modeling_realm"] = "atmos"

    with patch.object(
        cmip7_vocab_instance,
        "requires_parent_information",
        return_value=False,
    ):
        attrs = cmip7_vocab_instance.get_required_global_attributes()

    if expected is None:
        assert "experiment" not in attrs
    else:
        assert attrs["experiment"] == expected


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
            source_id="ACCESS-ESM1-6",
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
def test_standardize_missing_values_casts_markers_to_data_dtype(vocabulary_instance):
    da = xr.DataArray(
        np.array([1.0, np.nan, 3.0], dtype=np.float32),
        dims=["x"],
        attrs={"units": "K"},
    )

    result = vocabulary_instance.standardize_missing_values(da, convert_existing=False)

    assert np.asarray(result.attrs["missing_value"]).dtype == np.float32
    assert np.asarray(result.attrs["_FillValue"]).dtype == np.float32


@pytest.mark.unit
def test_standardize_missing_values_casts_markers_integer_dtype(vocabulary_instance):
    """Test casting to float64 (standard data type for climate variables)."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0], dtype=np.float64),
        dims=["x"],
        attrs={"units": "K"},
    )

    result = vocabulary_instance.standardize_missing_values(da, convert_existing=False)

    # Check that missing values are cast to float64
    assert np.asarray(result.attrs["missing_value"]).dtype == np.float64
    assert np.asarray(result.attrs["_FillValue"]).dtype == np.float64


@pytest.mark.unit
def test_standardize_missing_values_casts_float16_data(vocabulary_instance):
    """Test that less common float types are handled."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0], dtype=np.float16),
        dims=["x"],
        attrs={"units": "K"},
    )

    result = vocabulary_instance.standardize_missing_values(da, convert_existing=False)

    # For float16 data, missing values should be cast to float16
    assert np.asarray(result.attrs["missing_value"]).dtype == np.float16


@pytest.mark.unit
def test_standardize_missing_values_integer_dtype_upcasts_to_float32(
    vocabulary_instance,
):
    """Integer arrays (e.g. sftof from mask*100) must be upcast to float32.

    The CMIP fill value 1e20 cannot be stored in any integer dtype
    (int64 max ≈ 9.2e18), so standardize_missing_values must cast the array
    to float32 before applying the fill value.  This is the regression test
    for the OverflowError reported in issue #517.
    """
    # Simulate land_ocean_mask * 100 — integer dtype, valid range 0–100
    da = xr.DataArray(
        np.array([0, 100, 50], dtype=np.int32),
        dims=["x"],
        attrs={"units": "%"},
    )

    # Must not raise OverflowError
    result = vocabulary_instance.standardize_missing_values(da, convert_existing=False)

    # Array must have been upcast to float
    assert np.issubdtype(
        result.dtype, np.floating
    ), f"Expected floating dtype after upcast, got {result.dtype}"
    # Fill-value attributes must be representable in that dtype
    assert "missing_value" in result.attrs
    assert "missing_value" not in (None,)


@pytest.mark.unit
def test_cast_missing_value_overflow_falls_through():
    """OverflowError when casting 1e20 to an integer dtype must fall through.

    The OverflowError branch in _cast_missing_value_to_data_dtype is a
    defensive guard for callers that bypass standardize_missing_values; it
    must return the original float value unchanged rather than crashing.
    """
    da = xr.DataArray(np.array([0, 1], dtype=np.int32), dims=["x"])
    result = _cast_missing_value_to_data_dtype(1e20, da)
    # Must not raise and must return the original float value
    assert result == 1e20


@pytest.mark.unit
def test_standardize_missing_values_fallback_on_cast_failure(vocabulary_instance):
    """Test that the casting helper returns original value on TypeError."""
    # Create a DataArray with a complex dtype that might cause issues
    da = xr.DataArray(
        np.array([1 + 2j, 3 + 4j], dtype=np.complex64),
        dims=["x"],
        attrs={"units": "1"},
    )

    result = vocabulary_instance.standardize_missing_values(da, convert_existing=False)

    # The casting should fall back gracefully since complex types aren't floating/integer
    # The result should have missing_value and _FillValue as floats since they're the defaults
    assert "missing_value" in result.attrs or "_FillValue" in result.attrs


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
def test_generate_filename_yearly_year_only(vocabulary_instance):
    """Yearly tables (Oyr) format the time range as YYYY-YYYY, not YYYYMM."""
    cf_time = xr.cftime_range("2020-01-01", periods=2, freq="YS", calendar="gregorian")
    ds = xr.Dataset(
        {
            "no3": xr.DataArray(
                np.array([1.0, 2.0]),
                dims=["time"],
                coords={"time": cf_time},
            )
        }
    )
    attrs = {**_FILENAME_ATTRS, "variable_id": "no3", "table_id": "Oyr"}

    with patch.object(
        CMIP6Vocabulary, "_load_drs_templates", return_value=_TIME_RANGE_TEMPLATE
    ):
        filename = vocabulary_instance.generate_filename(attrs, ds, "no3", "Oyr.no3")

    assert "2020-2021" in filename
    assert "202001" not in filename  # no month component


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


@pytest.mark.unit
@pytest.mark.parametrize(
    ("compound_name", "expected"),
    [
        ("atmos.tas.tavg-h2m-hxy-u.mon.glb", "area: areacella"),
        ("aerosol.bry.tavg-p39-hy-air.mon.glb", None),
        ("atmos.unknown.tavg-u-hxy-u.mon.glb", None),
    ],
)
def test_cmip7_variable_entry_uses_compound_cell_measures(compound_name, expected):
    vocab = object.__new__(CMIP7Vocabulary)
    vocab.compound_name = compound_name
    vocab.cmor_name = compound_name.split(".")[1]
    table = {"variable_entry": {vocab.cmor_name: {"units": "K"}}}

    with patch.object(vocab, "_load_table", return_value=table):
        variable = vocab._get_variable_entry()

    assert variable.get("cell_measures") == expected
    assert "cell_measures" not in table["variable_entry"][vocab.cmor_name]


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

    assert filename.endswith("_202001-202002.nc")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("frequency", "times", "expected_suffix"),
    [
        ("yr", ["2020-01-01", "2021-01-01"], "_2020-2021.nc"),
        ("mon", ["2020-01-01", "2020-02-01"], "_202001-202002.nc"),
        ("day", ["2020-01-01", "2020-01-02"], "_20200101-20200102.nc"),
        (
            "1hr",
            ["2020-01-01 00:00", "2020-01-01 01:00"],
            "_202001010000-202001010100.nc",
        ),
        (
            "3hrPt",
            ["2020-01-01 00:00", "2020-01-01 03:00"],
            "_202001010000-202001010300.nc",
        ),
        (
            "6hr",
            ["2020-01-01 00:00", "2020-01-01 06:00"],
            "_202001010000-202001010600.nc",
        ),
        (
            "subhrPt",
            ["2020-01-01 00:00:01", "2020-01-01 00:30:02"],
            "_20200101000001-20200101003002.nc",
        ),
    ],
)
def test_cmip7_generate_filename_time_precision(
    cmip7_vocab_instance, frequency, times, expected_suffix
):
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 281.0]),
                dims=["time"],
                coords={"time": pd.to_datetime(times)},
            )
        }
    )

    filename = cmip7_vocab_instance.generate_filename(
        {**_CMIP7_ATTRS, "frequency": frequency}, ds, "tas", "Amon.tas"
    )

    assert filename.endswith(expected_suffix)


@pytest.mark.unit
def test_cmip7_generate_filename_fx_omits_time_range(cmip7_vocab_instance):
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0]),
                dims=["time"],
                coords={"time": pd.to_datetime(["2020-01-01"])},
            )
        }
    )

    filename = cmip7_vocab_instance.generate_filename(
        {**_CMIP7_ATTRS, "frequency": "fx"}, ds, "tas", "fx.tas"
    )

    assert filename.endswith("_r1i1p1f1.nc")
    assert "2020" not in filename


@pytest.mark.unit
def test_cmip7_standardize_missing_values_casts_markers_to_data_dtype(
    cmip7_vocab_instance,
):
    da = xr.DataArray(
        np.array([1.0, np.nan, 3.0], dtype=np.float32),
        dims=["x"],
        attrs={"units": "K"},
    )

    result = cmip7_vocab_instance.standardize_missing_values(da, convert_existing=False)

    assert np.asarray(result.attrs["missing_value"]).dtype == np.float32
    assert np.asarray(result.attrs["_FillValue"]).dtype == np.float32


@pytest.mark.unit
def test_cmip7_standardize_missing_values_integer_dtype_upcasts_to_float32(
    cmip7_vocab_instance,
):
    """CMIP7: integer arrays must be upcast to float32 (mirrors CMIP6 fix, issue #517)."""
    da = xr.DataArray(
        np.array([0, 100, 50], dtype=np.int32),
        dims=["x"],
        attrs={"units": "%"},
    )

    result = cmip7_vocab_instance.standardize_missing_values(da, convert_existing=False)

    assert np.issubdtype(
        result.dtype, np.floating
    ), f"Expected floating dtype after upcast, got {result.dtype}"
    assert "missing_value" in result.attrs


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


@pytest.mark.unit
def test_cmip7_parent_source_validation_accepts_temporary_access_entry():
    """CMIP7 parent_source_id validation reuses the temporary ACCESS source override."""
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
    parent_info = {
        "parent_experiment_id": "esm-piControl",
        "parent_activity_id": "CMIP",
        "parent_mip_era": "CMIP7",
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01 00:00:00",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 0.0,
        "branch_method": "standard",
    }
    with (
        patch.object(
            CMIP7Vocabulary,
            "_get_experiment",
            return_value={"activity": ["CMIP"], "parent_experiment": ["esm-piControl"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
            parent_info=parent_info,
        )

    assert vocab.get_parent_experiment_attrs()["parent_source_id"] == "ACCESS-ESM1-6"


@pytest.mark.unit
@pytest.mark.parametrize(
    "experiment_id",
    ["piControl", "piControl-spinup", "esm-piControl", "esm-piControl-spinup"],
)
def test_cmip7_experiment_lookup_matches_real_cv_exactly(experiment_id):
    """CMIP7 experiment_id lookups require the CV's exact casing.

    ``piControl`` (concentration-driven) and ``esm-piControl``
    (emission-driven) are distinct experiments in the real CMIP7 CV, so
    lookups must not fuzzily conflate them.
    """
    metadata = CMIP7Vocabulary._load_experiment_metadata(None, experiment_id)
    assert metadata["experiment_id"] == experiment_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "experiment_id",
    ["picontrol", "picontrol-spinup", "esm-picontrol", "esm-picontrol-spinup"],
)
def test_cmip7_experiment_lookup_rejects_mismatched_case(experiment_id):
    """A wrongly-cased experiment_id must be rejected, not silently aliased."""
    with pytest.raises(FileNotFoundError):
        CMIP7Vocabulary._load_experiment_metadata(None, experiment_id)


@pytest.mark.unit
def test_cmip7_parent_experiment_id_rejects_mismatched_case():
    """CMIP7 parent_experiment_id validation requires the CV's exact casing."""
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
    parent_info = {
        "parent_experiment_id": "picontrol-spinup",
        "parent_activity_id": "CMIP",
        "parent_mip_era": "CMIP7",
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01 00:00:00",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 0.0,
        "branch_method": "standard",
    }

    with (
        patch.object(
            CMIP7Vocabulary,
            "_get_experiment",
            return_value={"activity": ["CMIP"], "parent_experiment": ["esm-piControl"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
            parent_info=parent_info,
        )

        with pytest.raises(ValueError, match="Invalid parent_experiment_id"):
            vocab.get_parent_experiment_attrs()


@pytest.mark.unit
def test_cmip7_load_project_cv_supports_flat_layout():
    """CMIP7 project CV loader supports the current flat bundled CV layout."""
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
            return_value={"activity": ["CMIP"], "parent_experiment": ["none"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    area_cv = vocab._load_project_cv("area_label")
    assert "area_label" in area_cv


@pytest.mark.unit
def test_cmip7_load_cv_term_list_returns_dict_keys_from_cmor_cvs():
    """CMIP7 _load_cv_term_list reads from cmor-cvs.json and returns dict keys."""
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
            return_value={"activity": ["CMIP"], "parent_experiment": ["none"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    mock_cv = {
        "temporal_label": {"tavg": "mean", "tminavg": "min mean", "tclm": "climatology"}
    }
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs", return_value=mock_cv
    ):
        result = vocab._load_cv_term_list("temporal_label")
        assert set(result) == {"tavg", "tminavg", "tclm"}


@pytest.mark.unit
def test_cmip7_load_cv_term_list_returns_list_from_cmor_cvs():
    """CMIP7 _load_cv_term_list returns list elements when the CV section is a list."""
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
            return_value={"activity": ["CMIP"], "parent_experiment": ["none"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    mock_cv = {"some_list_cv": ["alpha", "beta", "gamma"]}
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs", return_value=mock_cv
    ):
        result = vocab._load_cv_term_list("some_list_cv")
        assert result == ["alpha", "beta", "gamma"]


@pytest.mark.unit
def test_cmip7_extracts_labels_from_branding_suffix_template_order():
    """CMIP7 label extraction follows the branding_suffix template positions."""
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
            return_value={"activity": ["CMIP"], "parent_experiment": ["none"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_source",
            return_value={"institution_id": ["CSIRO"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas.tavg-h2m-hxy-u.mon.glb",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    mock_cv = {
        "branding_suffix": "<temporal_label><vertical_label><horizontal_label><area_label>",
        "temporal_label": {"tavg": ""},
        "vertical_label": {"h2m": ""},
        "horizontal_label": {"hxy": ""},
        "area_label": {"u": ""},
    }
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs", return_value=mock_cv
    ):
        assert vocab._get_temporal_label() == "tavg"
        assert vocab._get_vertical_label() == "h2m"
        assert vocab._get_horizontal_label() == "hxy"
        assert vocab._get_area_label() == "u"


@pytest.mark.unit
def test_cmip7_extracts_labels_from_branding_suffix_with_invalid_token():
    """CMIP7 returns None for invalid branding label components."""
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
            return_value={"activity": ["CMIP"], "parent_experiment": ["none"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_source",
            return_value={"institution_id": ["CSIRO"]},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value=mock_table["variable_entry"]["tas"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas.tavg-bad-hxy-u.mon.glb",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    mock_cv = {
        "branding_suffix": "<temporal_label><vertical_label><horizontal_label><area_label>",
        "temporal_label": {"tavg": ""},
        "vertical_label": {"h2m": ""},
        "horizontal_label": {"hxy": ""},
        "area_label": {"u": ""},
    }
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs", return_value=mock_cv
    ):
        assert vocab._get_temporal_label() == "tavg"
        assert vocab._get_vertical_label() is None
        assert vocab._get_horizontal_label() == "hxy"
        assert vocab._get_area_label() == "u"


@pytest.mark.unit
def test_cmip7_vocabulary_exposes_mip_era():
    """CMIP7Vocabulary defines mip_era so shared logging uses 'CMIP7'."""
    assert CMIP7Vocabulary.mip_era == "CMIP7"


def _make_cmip6_vocab(
    mock_vocab_data, mock_table_data, modeling_realm, source_components
):
    vocab_data = dict(mock_vocab_data)
    vocab_data["source_id"] = {
        "ACCESS-ESM1-6": {
            **vocab_data["source_id"]["ACCESS-ESM1-6"],
            "model_component": source_components,
        }
    }
    table_data = {
        "Header": mock_table_data["Header"],
        "variable_entry": {
            "tas": {
                **mock_table_data["variable_entry"]["tas"],
                "modeling_realm": modeling_realm,
            }
        },
    }
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=table_data),
    ):
        return CMIP6Vocabulary(
            compound_name="Amon.tas",
            experiment_id="piControl",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )


@pytest.mark.unit
def test_get_nominal_resolution_single_realm(mock_vocab_data, mock_table_data):
    """Single realm: returns native_nominal_resolution without needing target_realm."""
    vocab = _make_cmip6_vocab(
        mock_vocab_data,
        mock_table_data,
        modeling_realm="atmos",
        source_components={"atmos": {"native_nominal_resolution": "100 km"}},
    )
    assert vocab._get_nominal_resolution() == "100 km"


@pytest.mark.unit
def test_get_nominal_resolution_single_realm_missing_key(
    mock_vocab_data, mock_table_data
):
    """Single realm with no native_nominal_resolution key returns None."""
    vocab = _make_cmip6_vocab(
        mock_vocab_data,
        mock_table_data,
        modeling_realm="atmos",
        source_components={"atmos": {"description": "no resolution here"}},
    )
    assert vocab._get_nominal_resolution() is None


@pytest.mark.unit
def test_get_nominal_resolution_multiple_realms_no_target_raises(
    mock_vocab_data, mock_table_data
):
    """Multiple modeling realms without target_realm raises ValueError."""
    vocab = _make_cmip6_vocab(
        mock_vocab_data,
        mock_table_data,
        modeling_realm="atmos ocean",
        source_components={
            "atmos": {"native_nominal_resolution": "100 km"},
            "ocean": {"native_nominal_resolution": "50 km"},
        },
    )
    with pytest.warns(UserWarning, match="multiple modeling realms"):
        result = vocab._get_nominal_resolution()
    assert result == "100 km"


@pytest.mark.unit
def test_get_nominal_resolution_multiple_realms_invalid_target_raises(
    mock_vocab_data, mock_table_data
):
    """target_realm not in the variable's realms raises ValueError."""
    vocab = _make_cmip6_vocab(
        mock_vocab_data,
        mock_table_data,
        modeling_realm="atmos ocean",
        source_components={
            "atmos": {"native_nominal_resolution": "100 km"},
            "ocean": {"native_nominal_resolution": "50 km"},
        },
    )
    with pytest.raises(ValueError, match="not found in variable's modeling realms"):
        vocab._get_nominal_resolution(target_realm="land")


@pytest.mark.unit
def test_get_nominal_resolution_multiple_realms_valid_target(
    mock_vocab_data, mock_table_data
):
    """With a valid target_realm, returns resolution for the specified realm."""
    vocab = _make_cmip6_vocab(
        mock_vocab_data,
        mock_table_data,
        modeling_realm="atmos ocean",
        source_components={
            "atmos": {"native_nominal_resolution": "100 km"},
            "ocean": {"native_nominal_resolution": "50 km"},
        },
    )
    assert vocab._get_nominal_resolution(target_realm="atmos") == "100 km"
    assert vocab._get_nominal_resolution(target_realm="ocean") == "50 km"


@pytest.mark.unit
def test_get_nominal_resolution_multiple_realms_target_missing_key(
    mock_vocab_data, mock_table_data
):
    """Valid target_realm but no native_nominal_resolution in that component returns None."""
    vocab = _make_cmip6_vocab(
        mock_vocab_data,
        mock_table_data,
        modeling_realm="atmos ocean",
        source_components={
            "atmos": {"description": "no resolution"},
            "ocean": {"native_nominal_resolution": "50 km"},
        },
    )
    assert vocab._get_nominal_resolution(target_realm="atmos") is None


@pytest.mark.unit
def test_cmip7_get_nominal_resolution_multiple_realms_valid_target():
    """CMIP7 supports selecting native nominal resolution by target realm."""
    mock_cv = {
        "experiment_id": {
            "historical": {
                "experiment": "historical",
                "activity": ["CMIP"],
            }
        },
        "source_id": {
            "ACCESS-ESM1-6": {
                "institution_id": ["CSIRO"],
                "license_info": {"id": "CC BY 4.0"},
                "release_year": "2021",
                "model_component": {
                    "atmos": {"native_nominal_resolution": "100 km"},
                    "ocean": {"native_nominal_resolution": "50 km"},
                },
            }
        },
    }
    mock_table = {
        "Header": {"table_id": "ocean"},
        "variable_entry": {
            "tos": {
                "frequency": "mon",
                "modeling_realm": "atmos ocean",
                "units": "degC",
                "type": "real",
                "dimensions": ["longitude", "latitude", "time"],
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
            return_value=mock_table["variable_entry"]["tos"],
        ),
        patch.object(CMIP7Vocabulary, "_load_table", return_value=mock_table),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="ocean.tos",
            experiment_id="historical",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    assert vocab._get_nominal_resolution(target_realm="ocean") == "50 km"


# ---------------------------------------------------------------------------
# Error message context: _get_experiment / _get_source / _load_table
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_experiment_error_lists_available(mock_vocab_data, mock_table_data):
    """Unknown experiment_id error must list available experiments from the CV."""
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
        pytest.raises(
            ValueError, match="Experiment 'unknownExp' not found"
        ) as exc_info,
    ):
        CMIP6Vocabulary(
            compound_name="Amon.tas",
            experiment_id="unknownExp",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )
    msg = str(exc_info.value)
    assert "Available experiments" in msg
    assert "piControl" in msg


@pytest.mark.unit
def test_get_source_error_lists_available(mock_vocab_data, mock_table_data):
    """Unknown source_id error must list available source_ids from the CV."""
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
        pytest.raises(ValueError, match="Source 'UNKNOWN-MODEL' not found") as exc_info,
    ):
        CMIP6Vocabulary(
            compound_name="Amon.tas",
            experiment_id="piControl",
            source_id="UNKNOWN-MODEL",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )
    msg = str(exc_info.value)
    assert "Available source_ids" in msg
    assert "ACCESS-ESM1-6" in msg


@pytest.mark.unit
def test_load_table_error_includes_filename_and_directory(
    mock_vocab_data, mock_table_data
):
    """Missing table file raises FileNotFoundError with the searched filename and table name."""
    # Build a vocab where loading succeeds, then call _load_table for a non-existent table
    with (
        patch.object(
            CMIP6Vocabulary, "_load_controlled_vocab", return_value=mock_vocab_data
        ),
        patch.object(CMIP6Vocabulary, "_load_table", return_value=mock_table_data),
    ):
        vocab = CMIP6Vocabulary(
            compound_name="Amon.tas",
            experiment_id="piControl",
            source_id="ACCESS-ESM1-6",
            variant_label="r1i1p1f1",
            grid_label="gn",
        )

    # Force a load failure for a non-existent table
    vocab.table = "NonexistentTable12345"
    with pytest.raises(FileNotFoundError, match="NonexistentTable12345") as exc_info:
        vocab._load_table()
    msg = str(exc_info.value)
    assert "looked for" in msg
    assert str(vocab.table_dir) in msg


@pytest.mark.unit
def test_load_cmor_cvs_reads_real_file():
    """_load_cmor_cvs() must load the real cmor-cvs.json without mocking.

    This guards against broken resource paths (e.g. hyphens in package names
    passed to importlib.resources.files()) that only fail at runtime, not in
    tests that mock the function.
    """
    import access_moppy.vocabulary_processors as _vp

    # Reset the cache so the real file I/O path is exercised.
    original_cache = _vp._CMOR_CVS_CACHE
    _vp._CMOR_CVS_CACHE = None
    try:
        cv = _load_cmor_cvs()
    finally:
        _vp._CMOR_CVS_CACHE = original_cache

    assert isinstance(cv, dict), "cmor-cvs.json CV section must be a dict"
    assert len(cv) > 0, "cmor-cvs.json CV section must not be empty"


# ---------------------------------------------------------------------------
# CMIP7Vocabulary._load_source_metadata supplement logic
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_source_metadata_supplements_missing_fields():
    """_load_source_metadata merges supplement fields absent from the official CV."""
    official = {"source_id": "ACCESS-ESM1-6", "source": "some string"}
    supplement = {"model_component": {"atmos": {"native_nominal_resolution": "250 km"}}}

    with (
        patch(
            "access_moppy.vocabulary_processors._load_cmor_cvs",
            return_value={"source_id": {"ACCESS-ESM1-6": official}},
        ),
        patch(
            "access_moppy.vocabulary_processors._CMIP7_SOURCE_SUPPLEMENTS",
            {"ACCESS-ESM1-6": supplement},
        ),
    ):
        result = CMIP7Vocabulary._load_source_metadata(None, "ACCESS-ESM1-6")

    assert result["model_component"] == supplement["model_component"]
    assert result["source_id"] == "ACCESS-ESM1-6"


@pytest.mark.unit
def test_load_source_metadata_no_supplement_needed():
    """_load_source_metadata returns source unchanged when no fields are missing."""
    official = {
        "source_id": "SOME-MODEL",
        "model_component": {"atmos": {"native_nominal_resolution": "100 km"}},
    }
    supplement = {"model_component": {"atmos": {"native_nominal_resolution": "999 km"}}}

    with (
        patch(
            "access_moppy.vocabulary_processors._load_cmor_cvs",
            return_value={"source_id": {"SOME-MODEL": official}},
        ),
        patch(
            "access_moppy.vocabulary_processors._CMIP7_SOURCE_SUPPLEMENTS",
            {"SOME-MODEL": supplement},
        ),
    ):
        result = CMIP7Vocabulary._load_source_metadata(None, "SOME-MODEL")

    # Existing model_component must not be overwritten
    assert result["model_component"]["atmos"]["native_nominal_resolution"] == "100 km"


@pytest.mark.unit
def test_load_source_metadata_unknown_source_raises():
    """_load_source_metadata raises FileNotFoundError for unknown source_id."""
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs",
        return_value={"source_id": {}},
    ):
        with pytest.raises(FileNotFoundError):
            CMIP7Vocabulary._load_source_metadata(None, "UNKNOWN-MODEL")


# ---------------------------------------------------------------------------
# CMIP7Vocabulary institution_id default and _get_institution_name
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cmip7_vocabulary_institution_id_default(cmip7_vocab_instance):
    """institution_id defaults to 'ACCESS-Consortium' when not supplied."""
    assert cmip7_vocab_instance.institution_id == "ACCESS-Consortium"


@pytest.mark.unit
def test_cmip7_vocabulary_institution_id_explicit():
    """institution_id is stored as-is when explicitly supplied."""
    mock_source = {
        "institution_id": ["CSIRO"],
        "model_component": {},
        "label": "TEST",
        "release_year": "2025",
    }
    with (
        patch.object(
            CMIP7Vocabulary,
            "_get_experiment",
            return_value={
                "activity_id": ["CMIP"],
                "experiment": "test",
                "activity": ["CMIP"],
            },
        ),
        patch.object(CMIP7Vocabulary, "_get_source", return_value=mock_source),
        patch.object(
            CMIP7Vocabulary,
            "_get_variable_entry",
            return_value={"frequency": "mon", "dimensions": [], "type": "real"},
        ),
        patch.object(
            CMIP7Vocabulary,
            "_load_table",
            return_value={"Header": {}, "variable_entry": {}},
        ),
    ):
        vocab = CMIP7Vocabulary(
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="TEST-MODEL",
            variant_label="r1i1p1f1",
            grid_label="gn",
            institution_id="CSIRO",
        )
    assert vocab.institution_id == "CSIRO"


@pytest.mark.unit
def test_get_institution_name_found_in_cv(cmip7_vocab_instance):
    """_get_institution_name returns the CV name when the institution_id is registered."""
    cmip7_vocab_instance.institution_id = "ACCESS-Consortium"
    mock_cv = {"institution_id": {"ACCESS-Consortium": "ACCESS-NRI Consortium"}}
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs", return_value=mock_cv
    ):
        name = cmip7_vocab_instance._get_institution_name()
    assert name == "ACCESS-NRI Consortium"


@pytest.mark.unit
def test_get_institution_name_fallback_to_id(cmip7_vocab_instance):
    """_get_institution_name falls back to institution_id when not in CV."""
    cmip7_vocab_instance.institution_id = "UNKNOWN-ORG"
    with patch(
        "access_moppy.vocabulary_processors._load_cmor_cvs",
        return_value={"institution_id": {}},
    ):
        name = cmip7_vocab_instance._get_institution_name()
    assert name == "UNKNOWN-ORG"


@pytest.mark.unit
def test_load_source_metadata_source_not_in_supplements():
    """_load_source_metadata returns official source unchanged when not in supplements."""
    official = {"source_id": "SOME-OTHER-MODEL", "source": "a model"}
    with (
        patch(
            "access_moppy.vocabulary_processors._load_cmor_cvs",
            return_value={"source_id": {"SOME-OTHER-MODEL": official}},
        ),
        patch(
            "access_moppy.vocabulary_processors._CMIP7_SOURCE_SUPPLEMENTS",
            {},  # no entry for SOME-OTHER-MODEL
        ),
    ):
        result = CMIP7Vocabulary._load_source_metadata(None, "SOME-OTHER-MODEL")
    assert result is official


@pytest.mark.unit
def test_cmip7_get_license_uses_institution_id(cmip7_vocab_instance):
    """_get_license uses self.institution_id in the license string."""
    cmip7_vocab_instance.institution_id = "TEST-ORG"
    license_str = cmip7_vocab_instance._get_license()
    assert "TEST-ORG" in license_str
