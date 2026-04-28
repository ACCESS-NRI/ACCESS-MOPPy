"""
Unit tests for access_moppy.esmval.variable_mapper
"""

from __future__ import annotations

import json
from pathlib import Path

from access_moppy.esmval.variable_mapper import (
    MappingEntry,
    VariableIndex,
    _infer_mip,
)

# ---------------------------------------------------------------------------
# Fixtures: minimal mapping JSON for testing from_json()
# ---------------------------------------------------------------------------


def _write_mapping(tmp_path: Path, content: dict) -> Path:
    p = tmp_path / "custom_mappings.json"
    p.write_text(json.dumps(content))
    return p


MINIMAL_MAPPING = {
    "atmosphere": {
        "tas": {
            "model_variables": ["fld_s03i236"],
            "calculation": {"type": "direct"},
            "dimensions": ["longitude", "latitude", "time"],
        },
        "tasmax": {
            "model_variables": ["fld_s03i236"],
            "calculation": {"type": "direct"},
        },
    },
    "ocean": {
        "tos": {
            "model_variables": ["temp"],
            "calculation": {"type": "direct"},
        },
        "areacello": {
            "model_variables": [],
            "calculation": {"type": "dataset_function"},
            "ressource_file": "areacello.nc",
        },
    },
    "sea_ice": {
        "siconc": {
            "model_variables": ["aice_m"],
            "calculation": {"type": "direct"},
        },
    },
}


class TestMappingEntry:
    def test_is_named_tuple(self):
        e = MappingEntry("Amon.tas", ["fld_s03i236"], "atmosphere", "direct", None)
        assert e.compound_name == "Amon.tas"
        assert e.model_variables == ["fld_s03i236"]
        assert e.component == "atmosphere"
        assert e.calculation_type == "direct"
        assert e.resource_file is None

    def test_resource_file_optional(self):
        e = MappingEntry(
            "Ofx.areacello", [], "ocean", "dataset_function", "areacello.nc"
        )
        assert e.resource_file == "areacello.nc"


class TestInferMip:
    def test_atmosphere_default(self):
        assert _infer_mip("pr", "atmosphere") == "Amon"

    def test_ocean_default(self):
        assert _infer_mip("tos", "ocean") == "Omon"

    def test_sea_ice_default(self):
        assert _infer_mip("siconc", "sea_ice") == "SImon"

    def test_known_override_tasmax(self):
        assert _infer_mip("tasmax", "atmosphere") == "day"

    def test_known_override_areacella(self):
        assert _infer_mip("areacella", "atmosphere") == "fx"

    def test_known_override_areacello(self):
        assert _infer_mip("areacello", "ocean") == "Ofx"

    def test_unknown_component_defaults_to_amon(self):
        assert _infer_mip("somevar", "unknowncomp") == "Amon"


class TestVariableIndexFromJson:
    def test_get_atmosphere_variable(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        entry = idx.get("Amon", "tas")
        assert entry is not None
        assert entry.compound_name == "Amon.tas"
        assert "fld_s03i236" in entry.model_variables
        assert entry.component == "atmosphere"

    def test_get_returns_none_for_unknown(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        assert idx.get("Amon", "notavar") is None

    def test_is_supported_true(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        assert idx.is_supported("Amon", "tas") is True

    def test_is_supported_false(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        assert idx.is_supported("Amon", "notavar") is False

    def test_ocean_variable(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        entry = idx.get("Omon", "tos")
        assert entry is not None
        assert entry.component == "ocean"

    def test_table_override_daily(self, tmp_path):
        """tasmax should map to MIP 'day' not 'Amon'."""
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        entry = idx.get("day", "tasmax")
        assert entry is not None
        assert entry.compound_name == "day.tasmax"

    def test_resource_file_variable(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        entry = idx.get("Ofx", "areacello")
        assert entry is not None
        assert entry.resource_file == "areacello.nc"

    def test_unsupported_filters(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        pairs = [("Amon", "tas"), ("Amon", "nope"), ("SImon", "siconc")]
        result = idx.unsupported(pairs)
        assert ("Amon", "nope") in result
        assert ("Amon", "tas") not in result
        assert ("SImon", "siconc") not in result

    def test_all_compound_names(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file)
        names = idx.all_compound_names()
        assert "Amon.tas" in names
        assert "Omon.tos" in names
        assert isinstance(names, list)

    def test_empty_mapping(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, {})
        idx = VariableIndex.from_json(mapping_file)
        assert idx.is_supported("Amon", "tas") is False
        assert idx.all_compound_names() == []

    def test_model_id_attribute(self, tmp_path):
        mapping_file = _write_mapping(tmp_path, MINIMAL_MAPPING)
        idx = VariableIndex.from_json(mapping_file, model_id="MY_MODEL")
        assert idx.model_id == "MY_MODEL"


class TestVariableIndexDefaultModel:
    """Tests that use the real bundled mapping (requires access_moppy installed)."""

    def test_well_known_atmosphere_variable(self):
        idx = VariableIndex("ACCESS-ESM1.6")
        assert idx.is_supported("Amon", "tas")

    def test_unsupported_nonexistent_variable(self):
        idx = VariableIndex("ACCESS-ESM1.6")
        assert not idx.is_supported("Amon", "this_is_not_a_real_variable_xyz")

    def test_index_is_cached(self):
        idx1 = VariableIndex("ACCESS-ESM1.6")
        idx2 = VariableIndex("ACCESS-ESM1.6")
        # Both must share the same underlying dict (class-level cache)
        assert idx1._index is idx2._index

    def test_unknown_model_returns_empty(self):
        idx = VariableIndex("NONEXISTENT_MODEL_XYZ")
        assert idx.all_compound_names() == []
