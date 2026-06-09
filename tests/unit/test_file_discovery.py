"""Unit tests for access_moppy.file_discovery."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from access_moppy.file_discovery import (
    FileDiscoveryError,
    _TABLE_TO_FREQ,
    _build_patterns,
    _extract_year_from_path,
    _find_variable_entry,
    _load_full_mappings,
    discover_files,
)


# ---------------------------------------------------------------------------
# _extract_year_from_path
# ---------------------------------------------------------------------------


class TestExtractYearFromPath:
    @pytest.mark.parametrize(
        "filename, expected",
        [
            # Ocean annual (legacy)
            ("ocean-2d-surface_temp-1mon-mean-y_1850.nc", 1850),
            ("ocean-3d-temp-1yr-mean-y_2014.nc", 2014),
            # Ocean fixed (no year) — returns None
            ("ocean-2d-area_t-fx.nc", None),
            # Ice monthly (legacy)
            ("iceh-1monthly-mean_1850-01.nc", 1850),
            ("iceh-1daily-mean_2010-12.nc", 2010),
            # Atmosphere embedded YYYYMM (legacy)
            ("aiihca.pa-185001_mon.nc", 1850),
            ("aiihca.pe-201512_dai.nc", 2015),
            ("aiihca.pi-185006_3hr.nc", 1850),
            # Proposed unified YYYYMM-YYYYMM range (new naming scheme)
            ("tos_mean_ocean_1mon_185001-185012.nc", 1850),
            ("wt_mean_ocean_1yr_234501-234512.nc", 2345),
            ("tas_mean_atm_1mon_200101-200112.nc", 2001),
        ],
    )
    def test_known_patterns(self, filename, expected):
        assert _extract_year_from_path(Path(filename)) == expected

    def test_unrecognised_returns_none(self):
        assert _extract_year_from_path(Path("no_year_here.nc")) is None


# ---------------------------------------------------------------------------
# _TABLE_TO_FREQ
# ---------------------------------------------------------------------------


class TestTableToFreq:
    def test_common_tables_present(self):
        for table in ("Amon", "Omon", "SImon", "day", "Ofx", "fx", "Oyr"):
            assert table in _TABLE_TO_FREQ, f"Missing table: {table}"

    def test_monthly_tables_map_to_mon(self):
        for table in ("Amon", "Lmon", "Omon", "SImon", "AERmon"):
            assert _TABLE_TO_FREQ[table] == "mon"


# ---------------------------------------------------------------------------
# _load_full_mappings
# ---------------------------------------------------------------------------


class TestLoadFullMappings:
    def test_loads_esm16(self):
        mappings = _load_full_mappings("ACCESS-ESM1.6")
        assert "model_info" in mappings
        assert "ocean" in mappings
        assert "atmosphere" in mappings

    def test_file_discovery_block_present(self):
        mappings = _load_full_mappings("ACCESS-ESM1.6")
        fd = mappings["model_info"]["file_discovery"]
        assert "output_dir_pattern" in fd
        assert "components" in fd
        assert "ocean" in fd["components"]
        assert "atmosphere" in fd["components"]
        assert "sea_ice" in fd["components"]

    def test_missing_model_raises(self):
        with pytest.raises(FileDiscoveryError, match="No mapping file found"):
            _load_full_mappings("NONEXISTENT-MODEL-XYZ")


# ---------------------------------------------------------------------------
# _find_variable_entry
# ---------------------------------------------------------------------------


class TestFindVariableEntry:
    def setup_method(self):
        self.mappings = _load_full_mappings("ACCESS-ESM1.6")

    def test_ocean_variable_found(self):
        component, entry = _find_variable_entry(self.mappings, "tos")
        assert component == "ocean"
        assert "model_variables" in entry

    def test_atmosphere_variable_found(self):
        component, entry = _find_variable_entry(self.mappings, "tas")
        assert component == "atmosphere"

    def test_sea_ice_variable_found(self):
        component, entry = _find_variable_entry(self.mappings, "siconc")
        assert component == "sea_ice"

    def test_unknown_variable_returns_none(self):
        assert _find_variable_entry(self.mappings, "totally_unknown_var_xyz") is None


# ---------------------------------------------------------------------------
# _build_patterns
# ---------------------------------------------------------------------------


class TestBuildPatterns:
    def setup_method(self):
        mappings = _load_full_mappings("ACCESS-ESM1.6")
        self.fd_cfg = mappings["model_info"]["file_discovery"]

    def test_atmosphere_monthly_no_model_var(self):
        # Atmosphere: all variables packed in one file; no {model_var}
        var_entry = {"model_variables": ["fld_s30i297"]}
        patterns = _build_patterns(var_entry, "atmosphere", "mon", self.fd_cfg)
        assert len(patterns) == 1
        assert "{model_var}" not in patterns[0]
        assert "*.pa-*_mon.nc" in patterns[0]

    def test_ocean_monthly_one_model_var(self):
        var_entry = {"model_variables": ["surface_temp"]}
        patterns = _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)
        assert len(patterns) == 1
        assert "surface_temp" in patterns[0]
        assert "{model_var}" not in patterns[0]

    def test_ocean_multi_model_vars_produces_multiple_patterns(self):
        var_entry = {"model_variables": ["ty_trans_rho", "ty_trans_rho_gm"]}
        patterns = _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)
        assert len(patterns) == 2
        assert any("ty_trans_rho-1mon" in p or "ty_trans_rho" in p for p in patterns)

    def test_per_variable_file_pattern_overrides_component_config(self):
        var_entry = {
            "model_variables": ["surface_temp"],
            "file_pattern": "output*/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc",
        }
        patterns = _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)
        assert patterns == [
            "output*/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc"
        ]

    def test_unknown_component_raises(self):
        with pytest.raises(FileDiscoveryError, match="No file_discovery config"):
            _build_patterns({}, "nonexistent_component", "mon", self.fd_cfg)

    def test_unknown_freq_raises(self):
        with pytest.raises(FileDiscoveryError, match="No pattern for frequency"):
            _build_patterns({}, "atmosphere", "subhr_nonexistent", self.fd_cfg)

    def test_ocean_missing_model_variables_raises(self):
        var_entry = {"model_variables": []}
        with pytest.raises(FileDiscoveryError, match="no 'model_variables'"):
            _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)

    def test_sea_ice_monthly_no_model_var(self):
        var_entry = {"model_variables": ["aice"]}
        patterns = _build_patterns(var_entry, "sea_ice", "mon", self.fd_cfg)
        assert len(patterns) == 1
        assert "iceh-1monthly-mean" in patterns[0]


# ---------------------------------------------------------------------------
# discover_files (integration-style, using a fake directory tree)
# ---------------------------------------------------------------------------


class TestDiscoverFiles:
    """Tests for the top-level discover_files function.

    A temporary directory is used as a fake archive root; discover_files must
    return only the files that actually exist and match the pattern.
    """

    def _make_archive(self, tmp_path: Path, filenames: list[tuple[str, str]]) -> Path:
        """Create a fake archive tree.  ``filenames`` is a list of (subdir, name) tuples."""
        for subdir, name in filenames:
            d = tmp_path / subdir
            d.mkdir(parents=True, exist_ok=True)
            (d / name).touch()
        return tmp_path

    def test_atmosphere_monthly(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/atmosphere/netCDF", "aiihca.pa-185001_mon.nc"),
                ("output000/atmosphere/netCDF", "aiihca.pa-185002_mon.nc"),
                ("output001/atmosphere/netCDF", "aiihca.pa-185101_mon.nc"),
                # Should NOT match a daily file when asking for monthly
                ("output000/atmosphere/netCDF", "aiihca.pe-185001_dai.nc"),
            ],
        )
        result = discover_files(archive, "Amon.tas")
        names = [p.name for p in result]
        assert "aiihca.pa-185001_mon.nc" in names
        assert "aiihca.pa-185002_mon.nc" in names
        assert "aiihca.pa-185101_mon.nc" in names
        # Daily file must NOT appear in a monthly query
        assert "aiihca.pe-185001_dai.nc" not in names

    def test_ocean_monthly_per_variable(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1851.nc"),
                ("output000/ocean", "ocean-2d-eta_t-1mon-mean-y_1850.nc"),  # different var
            ],
        )
        result = discover_files(archive, "Omon.tos")
        names = [p.name for p in result]
        assert "ocean-2d-surface_temp-1mon-mean-y_1850.nc" in names
        assert "ocean-2d-surface_temp-1mon-mean-y_1851.nc" in names
        # Different model variable should NOT be included
        assert "ocean-2d-eta_t-1mon-mean-y_1850.nc" not in names

    def test_sea_ice_monthly(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ice", "iceh-1monthly-mean_1850-01.nc"),
                ("output000/ice", "iceh-1monthly-mean_1850-02.nc"),
                ("output000/ice", "iceh-1daily-mean_1850-01.nc"),  # daily, should NOT appear
            ],
        )
        result = discover_files(archive, "SImon.siconc")
        names = [p.name for p in result]
        assert "iceh-1monthly-mean_1850-01.nc" in names
        assert "iceh-1monthly-mean_1850-02.nc" in names
        assert "iceh-1daily-mean_1850-01.nc" not in names

    def test_year_filtering_start(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1900.nc"),
                ("output002/ocean", "ocean-2d-surface_temp-1mon-mean-y_1950.nc"),
            ],
        )
        result = discover_files(archive, "Omon.tos", start_year=1900)
        years = {_extract_year_from_path(p) for p in result}
        assert 1850 not in years
        assert 1900 in years
        assert 1950 in years

    def test_year_filtering_end(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1900.nc"),
                ("output002/ocean", "ocean-2d-surface_temp-1mon-mean-y_1950.nc"),
            ],
        )
        result = discover_files(archive, "Omon.tos", end_year=1900)
        years = {_extract_year_from_path(p) for p in result}
        assert 1850 in years
        assert 1900 in years
        assert 1950 not in years

    def test_year_filtering_range(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1900.nc"),
                ("output002/ocean", "ocean-2d-surface_temp-1mon-mean-y_1950.nc"),
            ],
        )
        result = discover_files(archive, "Omon.tos", start_year=1900, end_year=1900)
        assert len(result) == 1
        assert _extract_year_from_path(result[0]) == 1900

    def test_no_files_found_returns_empty(self, tmp_path):
        result = discover_files(tmp_path, "Omon.tos")
        assert result == []

    def test_invalid_compound_name_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid compound_name"):
            discover_files(tmp_path, "no_dot_here")

    def test_unknown_variable_raises(self, tmp_path):
        with pytest.raises(FileDiscoveryError, match="not found in mappings"):
            discover_files(tmp_path, "Omon.total_nonexistent_variable_xyz")

    def test_unknown_table_raises(self, tmp_path):
        with pytest.raises(FileDiscoveryError, match="Unknown CMIP table"):
            discover_files(tmp_path, "FAKEX.tos")

    def test_explicit_file_pattern_in_mapping(self, tmp_path):
        """A per-variable file_pattern in the mapping entry overrides auto-discovery."""
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
            ],
        )
        fake_entry = {
            "model_variables": ["surface_temp"],
            "file_pattern": "output[0-9][0-9][0-9]/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc",
        }
        mappings = _load_full_mappings("ACCESS-ESM1.6")
        fd_cfg = mappings["model_info"]["file_discovery"]
        patterns = _build_patterns(fake_entry, "ocean", "mon", fd_cfg)
        # Should use the explicit pattern, not the auto one
        assert patterns == [
            "output[0-9][0-9][0-9]/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc"
        ]

    def test_result_is_sorted_and_deduplicated(self, tmp_path):
        # Multi-var ocean mapping: if two model_vars happen to match the same
        # physical file (unlikely but guard against duplicates in output)
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1851.nc"),
            ],
        )
        result = discover_files(archive, "Omon.tos")
        assert result == sorted(set(result))
