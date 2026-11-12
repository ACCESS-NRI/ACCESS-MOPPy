"""The CM3 monthly atmosphere stress mapping names its actual UM archive field."""

from access_moppy.utilities import load_model_mappings


def test_cm3_tauu_direct_mapping_uses_same_archived_field_for_load_and_formula():
    mapping = load_model_mappings("Amon.tauu", "ACCESS-CM3")
    entry = mapping["tauu"]
    assert entry["model_variables"] == ["fld_s03i392"]
    assert entry["calculation"] == {"type": "direct", "formula": "fld_s03i392"}
    assert entry["dimensions"] == {"time": "time", "lat": "lat", "lon_u": "lon"}
