"""Unit tests for mapping and ocean file discovery utilities."""

from unittest.mock import patch

import pytest

from access_moppy.utilities import (
    _get_cmip7_to_cmip6_mapping,
    get_monthly_ocean_files,
    load_model_mappings,
)


@pytest.mark.unit
def test_get_cmip7_to_cmip6_mapping_exact_match_case_insensitive():
    result = _get_cmip7_to_cmip6_mapping("ATMOS.AREACELLA.TI-U-HXY-U.FX.GLB")
    assert result == "fx.areacella"


@pytest.mark.unit
def test_get_cmip7_to_cmip6_mapping_single_regex_match():
    result = _get_cmip7_to_cmip6_mapping(r"^atmos\.areacella\.ti-u-hxy-u\.fx\.GLB$")
    assert result == "fx.areacella"


@pytest.mark.unit
def test_get_cmip7_to_cmip6_mapping_regex_multiple_matches_returns_none():
    result = _get_cmip7_to_cmip6_mapping(r"^aerosol\.od550.*")
    assert result is None


@pytest.mark.unit
def test_get_cmip7_to_cmip6_mapping_invalid_regex_returns_none():
    result = _get_cmip7_to_cmip6_mapping("[")
    assert result is None


@pytest.mark.unit
def test_get_cmip7_to_cmip6_mapping_unknown_returns_none():
    result = _get_cmip7_to_cmip6_mapping("not.a.real.cmip7.variable")
    assert result is None


@pytest.mark.unit
def test_load_model_mappings_default_model_success():
    result = load_model_mappings("Amon.tas")
    assert "tas" in result
    assert "model_variables" in result["tas"]


@pytest.mark.unit
def test_load_model_mappings_unknown_variable_returns_empty():
    result = load_model_mappings("Amon.thisdoesnotexist")
    assert result == {}


@pytest.mark.unit
def test_load_model_mappings_unknown_model_returns_empty():
    result = load_model_mappings("Amon.tas", model_id="ACCESS-DOES-NOT-EXIST")
    assert result == {}


@pytest.mark.unit
def test_get_monthly_ocean_files_invalid_compound_name():
    with pytest.raises(ValueError, match="Invalid compound_name format"):
        get_monthly_ocean_files("badname")


@pytest.mark.unit
def test_get_monthly_ocean_files_missing_root(tmp_path):
    missing_root = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError, match="Root folder does not exist"):
        get_monthly_ocean_files("Omon.so", root_folder=str(missing_root))


@pytest.mark.unit
def test_get_monthly_ocean_files_mapping_exception_warns_and_returns_empty(tmp_path):
    with patch(
        "access_moppy.utilities.load_model_mappings",
        side_effect=RuntimeError("boom"),
    ):
        with pytest.warns(UserWarning, match="Could not load mapping"):
            result = get_monthly_ocean_files("Omon.so", root_folder=str(tmp_path))
    assert result == []


@pytest.mark.unit
def test_get_monthly_ocean_files_empty_mapping_warns_and_returns_empty(tmp_path):
    with patch("access_moppy.utilities.load_model_mappings", return_value={}):
        with pytest.warns(UserWarning, match="No mapping found"):
            result = get_monthly_ocean_files("Omon.so", root_folder=str(tmp_path))
    assert result == []


@pytest.mark.unit
def test_get_monthly_ocean_files_missing_model_variables_warns_and_returns_empty(
    tmp_path,
):
    with patch(
        "access_moppy.utilities.load_model_mappings",
        return_value={"so": {"model_variables": []}},
    ):
        with pytest.warns(UserWarning, match="No model variables found"):
            result = get_monthly_ocean_files("Omon.so", root_folder=str(tmp_path))
    assert result == []


@pytest.mark.unit
def test_get_monthly_ocean_files_monthly_pattern_finds_files(tmp_path):
    ocean_dir = tmp_path / "output401" / "ocean"
    ocean_dir.mkdir(parents=True)
    file_a = ocean_dir / "run-temp-1monthly-mean-200001.nc"
    file_b = ocean_dir / "run-salt-1monthly-mean-200001.nc"
    file_a.write_text("a")
    file_b.write_text("b")

    with patch(
        "access_moppy.utilities.load_model_mappings",
        return_value={"so": {"model_variables": ["temp", "salt"]}},
    ):
        result = get_monthly_ocean_files("Omon.so", root_folder=str(tmp_path))

    assert result == sorted([str(file_a), str(file_b)])


@pytest.mark.unit
def test_get_monthly_ocean_files_ofx_patterns_deduplicate(tmp_path):
    ocean_dir = tmp_path / "output401" / "ocean"
    ocean_dir.mkdir(parents=True)
    file_a = ocean_dir / "ocean-2d-area_t.nc"
    file_b = ocean_dir / "prefix-area_t-suffix.nc"
    file_a.write_text("a")
    file_b.write_text("b")

    with patch(
        "access_moppy.utilities.load_model_mappings",
        return_value={"areacello": {"model_variables": ["area_t"]}},
    ):
        result = get_monthly_ocean_files("Ofx.areacello", root_folder=str(tmp_path))

    assert result == sorted([str(file_a), str(file_b)])


@pytest.mark.unit
def test_get_monthly_ocean_files_no_files_warns(tmp_path):
    ocean_dir = tmp_path / "output401" / "ocean"
    ocean_dir.mkdir(parents=True)

    with patch(
        "access_moppy.utilities.load_model_mappings",
        return_value={"so": {"model_variables": ["temp"]}},
    ):
        with pytest.warns(UserWarning, match="No ocean files found"):
            result = get_monthly_ocean_files("Omon.so", root_folder=str(tmp_path))

    assert result == []
