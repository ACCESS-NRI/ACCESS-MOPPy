"""Unit tests for mapping and ocean file discovery utilities."""

import json
from unittest.mock import MagicMock, patch

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
def test_load_model_mappings_esm1_6_success():
    result = load_model_mappings("Amon.tas", model_id="ACCESS-ESM1-6")
    assert "tas" in result
    assert "model_variables" in result["tas"]


@pytest.mark.unit
def test_load_model_mappings_oyr_osalttend_uses_yearly_salt_tendency_expl():
    result = load_model_mappings("Oyr.osalttend", model_id="ACCESS-ESM1-6")
    assert "osalttend" in result
    assert result["osalttend"]["model_variables"] == ["salt_tendency_expl"]
    assert result["osalttend"]["calculation"]["type"] == "direct"
    assert result["osalttend"]["calculation"]["formula"] == "salt_tendency_expl"


@pytest.mark.unit
def test_load_model_mappings_unknown_variable_returns_empty():
    result = load_model_mappings("Amon.thisdoesnotexist", model_id="ACCESS-ESM1-6")
    assert result == {}


@pytest.mark.unit
def test_load_model_mappings_unknown_model_returns_empty():
    result = load_model_mappings("Amon.tas", model_id="ACCESS-DOES-NOT-EXIST")
    assert result == {}


def _make_mock_entry(name: str, content: dict):
    """Return a mock Traversable-like entry for a mapping file."""
    entry = MagicMock()
    entry.name = name
    entry.read_text.return_value = json.dumps(content)
    return entry


@pytest.mark.unit
def test_load_model_mappings_component_organised_structure():
    """Variable found in a component sub-dict (e.g. 'atmosphere') is returned."""
    mapping_content = {
        "atmosphere": {"tas": {"model_variables": ["temp"], "units": "K"}}
    }
    mock_entry = _make_mock_entry("MY-MODEL_mappings.json", mapping_content)
    mock_dir = MagicMock()
    mock_dir.__truediv__.return_value = mock_entry

    with patch("access_moppy.utilities.files", return_value=mock_dir):
        result = load_model_mappings("Amon.tas", model_id="MY-MODEL")

    assert result == {"tas": {"model_variables": ["temp"], "units": "K"}}


@pytest.mark.unit
def test_load_model_mappings_flat_variables_fallback():
    """Variable found in the legacy flat 'variables' dict is returned."""
    mapping_content = {
        "variables": {"tas": {"model_variables": ["temp"], "units": "K"}}
    }
    mock_entry = _make_mock_entry("MY-MODEL_mappings.json", mapping_content)
    mock_dir = MagicMock()
    mock_dir.__truediv__.return_value = mock_entry

    with patch("access_moppy.utilities.files", return_value=mock_dir):
        result = load_model_mappings("Amon.tas", model_id="MY-MODEL")

    assert result == {"tas": {"model_variables": ["temp"], "units": "K"}}


@pytest.mark.unit
def test_load_model_mappings_variable_absent_in_found_file_returns_empty():
    """File exists but requested variable is not present → empty dict."""
    mapping_content = {"atmosphere": {"pr": {"model_variables": ["precip"]}}}
    mock_entry = _make_mock_entry("MY-MODEL_mappings.json", mapping_content)
    mock_dir = MagicMock()
    mock_dir.__truediv__.return_value = mock_entry

    with patch("access_moppy.utilities.files", return_value=mock_dir):
        result = load_model_mappings("Amon.tas", model_id="MY-MODEL")

    assert result == {}


@pytest.mark.unit
def test_get_monthly_ocean_files_invalid_compound_name_raises_value_error():
    with pytest.raises(ValueError, match="Invalid compound_name format"):
        get_monthly_ocean_files("badname", model_id="ACCESS-ESM1-6")


@pytest.mark.unit
def test_get_monthly_ocean_files_missing_root(tmp_path):
    missing_root = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError, match="Root folder does not exist"):
        get_monthly_ocean_files(
            "Omon.so", model_id="ACCESS-ESM1-6", root_folder=str(missing_root)
        )


@pytest.mark.unit
def test_get_monthly_ocean_files_mapping_exception_warns_and_returns_empty(tmp_path):
    with patch(
        "access_moppy.utilities.load_model_mappings",
        side_effect=RuntimeError("boom"),
    ):
        with pytest.warns(UserWarning, match="Could not load mapping"):
            result = get_monthly_ocean_files(
                "Omon.so", model_id="ACCESS-ESM1-6", root_folder=str(tmp_path)
            )
    assert result == []


@pytest.mark.unit
def test_get_monthly_ocean_files_empty_mapping_warns_and_returns_empty(tmp_path):
    with patch("access_moppy.utilities.load_model_mappings", return_value={}):
        with pytest.warns(UserWarning, match="No mapping found"):
            result = get_monthly_ocean_files(
                "Omon.so", model_id="ACCESS-ESM1-6", root_folder=str(tmp_path)
            )
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
            result = get_monthly_ocean_files(
                "Omon.so", model_id="ACCESS-ESM1-6", root_folder=str(tmp_path)
            )
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
        result = get_monthly_ocean_files(
            "Omon.so", model_id="ACCESS-ESM1-6", root_folder=str(tmp_path)
        )

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
        result = get_monthly_ocean_files(
            "Ofx.areacello", model_id="ACCESS-ESM1-6", root_folder=str(tmp_path)
        )

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
            result = get_monthly_ocean_files(
                "Omon.so", model_id="ACCESS-ESM1-6", root_folder=str(tmp_path)
            )

    assert result == []


# ---------------------------------------------------------------------------
# Mapping invariants for variables that exist in both Ofx and a time-bearing
# table (Omon / Odec). The same mapping entry must serve every table; per-table
# differences are resolved at runtime by Ocean_CMORiser._align_main_var_dims_with_vocab.
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "cmor_name,source_var",
    [
        ("masscello", "rho_dzt"),
        ("thkcello", "dzt"),
    ],
)
def test_dual_table_mapping_uses_direct_calculation_and_keeps_time(
    cmor_name, source_var
):
    """Mapping must describe the time-aware form so it works for both Ofx and Omon.

    The historic shape (calculation.operation == 'drop_time_axis' and no 'time'
    key in 'dimensions') would silently break the Omon variant of these
    variables again; lock the new shape in.
    """
    mapping = load_model_mappings(f"Omon.{cmor_name}", model_id="ACCESS-ESM1-6")
    assert mapping, f"No mapping found for Omon.{cmor_name}"
    entry = mapping[cmor_name]

    # Calculation must be a plain rename, not a time-stripping formula.
    calc = entry["calculation"]
    assert calc["type"] == "direct", (
        f"{cmor_name} calculation type should be 'direct' so the time axis is "
        f"preserved by default; got {calc!r}"
    )
    assert calc["formula"] == source_var

    # dimensions must include time so the rename map sees it for Omon/Odec.
    dims = entry["dimensions"]
    assert "time" in dims and dims["time"] == "time", (
        f"{cmor_name} mapping must declare 'time': 'time' in dimensions; "
        f"got {dims!r}"
    )
    # Spatial dims must still be present.
    for d in ("st_ocean", "yt_ocean", "xt_ocean"):
        assert d in dims, f"{cmor_name} mapping missing spatial dim '{d}'"
