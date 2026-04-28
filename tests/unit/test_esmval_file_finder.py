"""
Unit tests for access_moppy.esmval.file_finder
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from access_moppy.esmval.file_finder import (
    RawFileFinder,
    _extract_year_from_path,
    _parse_timerange,
    _path_in_timerange,
    _split_compound,
)
from access_moppy.esmval.variable_mapper import VariableIndex

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_index(tmp_path: Path, component: str = "atmosphere") -> VariableIndex:
    """Build a minimal VariableIndex from a JSON fixture."""
    mapping = {
        component: {
            "tas": {
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
    }
    p = tmp_path / "mapping.json"
    p.write_text(json.dumps(mapping))
    return VariableIndex.from_json(p)


def _make_atmos_file(root: Path, output_dir: str, name: str) -> Path:
    """Create a dummy atmosphere netCDF file at the expected path."""
    d = root / output_dir / "atmosphere" / "netCDF"
    d.mkdir(parents=True, exist_ok=True)
    f = d / name
    f.touch()
    return f


# ---------------------------------------------------------------------------
# _split_compound
# ---------------------------------------------------------------------------


class TestSplitCompound:
    def test_standard(self):
        assert _split_compound("Amon.tas") == ("Amon", "tas")

    def test_ocean(self):
        assert _split_compound("Omon.tos") == ("Omon", "tos")

    def test_invalid_no_dot(self):
        with pytest.raises(ValueError, match="Invalid compound_name"):
            _split_compound("Amontas")


# ---------------------------------------------------------------------------
# _parse_timerange
# ---------------------------------------------------------------------------


class TestParseTimerange:
    def test_standard_year_only(self):
        assert _parse_timerange("1850/2014") == (1850, 2014)

    def test_iso_dates(self):
        start, end = _parse_timerange("1850-01-01/2014-12-31")
        assert start == 1850
        assert end == 2014

    def test_empty_string(self):
        assert _parse_timerange("") == (None, None)

    def test_single_part_returns_none(self):
        assert _parse_timerange("1850") == (None, None)

    def test_unparseable_returns_none(self):
        assert _parse_timerange("notayear/alsonotayear") == (None, None)


# ---------------------------------------------------------------------------
# _extract_year_from_path
# ---------------------------------------------------------------------------


class TestExtractYearFromPath:
    def test_atmosphere_file(self):
        p = Path("aiihca.pa-200001_mon.nc")
        assert _extract_year_from_path(p) == 2000

    def test_ocean_file(self):
        p = Path("ocean-2d-surface_temp-1monthly-mean-ym_2000_01.nc")
        assert _extract_year_from_path(p) == 2000

    def test_ice_file(self):
        p = Path("iceh-1monthly-mean_200001-31.nc")
        assert _extract_year_from_path(p) == 2000

    def test_no_year_returns_none(self):
        p = Path("no_year_here.nc")
        assert _extract_year_from_path(p) is None


# ---------------------------------------------------------------------------
# _path_in_timerange
# ---------------------------------------------------------------------------


class TestPathInTimerange:
    def _atmos(self, year: int) -> Path:
        month = "01"
        return Path(f"aiihca.pa-{year}{month}_mon.nc")

    def test_no_bounds_accepts_all(self):
        assert _path_in_timerange(self._atmos(2000), None, None) is True

    def test_within_range(self):
        assert _path_in_timerange(self._atmos(2000), 1990, 2005) is True

    def test_before_start(self):
        assert _path_in_timerange(self._atmos(1989), 1990, 2005) is False

    def test_after_end(self):
        assert _path_in_timerange(self._atmos(2006), 1990, 2005) is False

    def test_on_boundary_start(self):
        assert _path_in_timerange(self._atmos(1990), 1990, 2005) is True

    def test_on_boundary_end(self):
        assert _path_in_timerange(self._atmos(2005), 1990, 2005) is True

    def test_no_year_in_filename_is_included(self):
        """Files whose year cannot be determined are conservatively included."""
        p = Path("no_year_info.nc")
        assert _path_in_timerange(p, 1990, 2005) is True

    def test_only_start_bound(self):
        assert _path_in_timerange(self._atmos(1995), 1990, None) is True
        assert _path_in_timerange(self._atmos(1985), 1990, None) is False

    def test_only_end_bound(self):
        assert _path_in_timerange(self._atmos(2000), None, 2005) is True
        assert _path_in_timerange(self._atmos(2010), None, 2005) is False


# ---------------------------------------------------------------------------
# RawFileFinder
# ---------------------------------------------------------------------------


class TestRawFileFinder:
    def test_returns_empty_for_unknown_compound(self, tmp_path):
        idx = _fake_index(tmp_path)
        finder = RawFileFinder(tmp_path, variable_index=idx)
        result = finder.find("Amon.notavar")
        assert result == []

    def test_returns_empty_for_resource_file_variable(self, tmp_path):
        """Variables backed by a resource file need no raw input."""
        idx = _fake_index(tmp_path)
        finder = RawFileFinder(tmp_path, variable_index=idx)
        result = finder.find("Ofx.areacello")
        assert result == []

    def test_finds_atmosphere_files(self, tmp_path):
        idx = _fake_index(tmp_path)
        root = tmp_path / "archive"
        # Create fake files in default pattern location
        _make_atmos_file(root, "output001", "aiihca.pa-200001_mon.nc")
        _make_atmos_file(root, "output001", "aiihca.pa-200101_mon.nc")

        finder = RawFileFinder(root, variable_index=idx)
        result = finder.find("Amon.tas")
        assert len(result) == 2

    def test_timerange_filters_files(self, tmp_path):
        idx = _fake_index(tmp_path)
        root = tmp_path / "archive"
        _make_atmos_file(root, "output001", "aiihca.pa-199901_mon.nc")
        _make_atmos_file(root, "output001", "aiihca.pa-200001_mon.nc")
        _make_atmos_file(root, "output001", "aiihca.pa-200601_mon.nc")

        finder = RawFileFinder(root, variable_index=idx)
        result = finder.find("Amon.tas", timerange="2000/2005")
        names = [f.name for f in result]
        assert "aiihca.pa-200001_mon.nc" in names
        assert "aiihca.pa-199901_mon.nc" not in names
        assert "aiihca.pa-200601_mon.nc" not in names

    def test_pattern_override_is_used(self, tmp_path):
        idx = _fake_index(tmp_path)
        root = tmp_path / "archive"
        # Create a file that only matches the custom pattern
        custom_dir = root / "custom_output" / "atm"
        custom_dir.mkdir(parents=True, exist_ok=True)
        custom_file = custom_dir / "aiihca.pa-200001_mon.nc"
        custom_file.touch()

        finder = RawFileFinder(
            root,
            variable_index=idx,
            pattern_overrides={"Amon.tas": "custom_output/atm/*mon.nc"},
        )
        result = finder.find("Amon.tas")
        assert len(result) == 1
        assert result[0] == custom_file

    def test_find_many_returns_dict(self, tmp_path):
        idx = _fake_index(tmp_path)
        root = tmp_path / "archive"
        _make_atmos_file(root, "output001", "aiihca.pa-200001_mon.nc")

        finder = RawFileFinder(root, variable_index=idx)
        result = finder.find_many(["Amon.tas", "Ofx.areacello"])
        assert isinstance(result, dict)
        assert "Amon.tas" in result
        assert "Ofx.areacello" in result
        assert result["Ofx.areacello"] == []

    def test_input_root_property(self, tmp_path):
        idx = _fake_index(tmp_path)
        finder = RawFileFinder(tmp_path / "archive", variable_index=idx)
        assert finder.input_root == tmp_path / "archive"

    def test_returns_sorted_paths(self, tmp_path):
        idx = _fake_index(tmp_path)
        root = tmp_path / "archive"
        _make_atmos_file(root, "output001", "aiihca.pa-200301_mon.nc")
        _make_atmos_file(root, "output001", "aiihca.pa-200101_mon.nc")
        _make_atmos_file(root, "output001", "aiihca.pa-200201_mon.nc")

        finder = RawFileFinder(root, variable_index=idx)
        result = finder.find("Amon.tas")
        assert result == sorted(result)
