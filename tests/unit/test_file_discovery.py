"""Unit tests for access_moppy.file_discovery."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from access_moppy.file_discovery import (
    _TABLE_TO_FREQ,
    FileDiscoveryError,
    _build_patterns,
    _diagnose_no_files,
    _extract_year_from_path,
    _find_variable_entry,
    _load_full_mappings,
    discover_files,
    discover_year_range,
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
        # The ocean "mon" frequency lists two naming conventions (legacy
        # "1mon-mean-y_" and newer "1monthly-mean-ym_"), so one model variable
        # yields one pattern per convention.
        var_entry = {"model_variables": ["surface_temp"]}
        patterns = _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)
        assert len(patterns) == 2
        assert all("surface_temp" in p for p in patterns)
        assert all("{model_var}" not in p for p in patterns)

    def test_ocean_multi_model_vars_produces_multiple_patterns(self):
        # Two model variables × two naming conventions per frequency.
        var_entry = {"model_variables": ["ty_trans_rho", "ty_trans_rho_gm"]}
        patterns = _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)
        assert len(patterns) == 4
        assert all("{model_var}" not in p for p in patterns)
        assert any("ty_trans_rho_gm" in p for p in patterns)

    def test_per_variable_file_pattern_overrides_component_config(self):
        var_entry = {
            "model_variables": ["surface_temp"],
            "file_pattern": "output*/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc",
        }
        patterns = _build_patterns(var_entry, "ocean", "mon", self.fd_cfg)
        assert patterns == ["output*/ocean/ocean-2d-surface_temp-1mon-mean-y_*.nc"]

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
                (
                    "output000/ocean",
                    "ocean-2d-eta_t-1mon-mean-y_1850.nc",
                ),  # different var
            ],
        )
        result = discover_files(archive, "Omon.tos")
        names = [p.name for p in result]
        assert "ocean-2d-surface_temp-1mon-mean-y_1850.nc" in names
        assert "ocean-2d-surface_temp-1mon-mean-y_1851.nc" in names
        # Different model variable should NOT be included
        assert "ocean-2d-eta_t-1mon-mean-y_1850.nc" not in names

    def test_ocean_monthly_newer_naming_convention(self, tmp_path):
        # Newer experiments name ocean output "1monthly-mean-ym_YYYY_MM"
        # instead of the legacy "1mon-mean-y_YYYY".  Discovery must handle both
        # (including the "-max-" statistic variant) without a per-variable
        # file_pattern.
        archive = self._make_archive(
            tmp_path,
            [
                (
                    "output000/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc",
                ),
                (
                    "output000/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0001_02.nc",
                ),
                # other variable — must not be included
                ("output000/ocean", "ocean-2d-eta_t-1monthly-mean-ym_0001_01.nc"),
                # daily — must not appear in a monthly query
                ("output000/ocean", "ocean-2d-surface_temp-1daily-mean-ym_0001_01.nc"),
            ],
        )
        result = discover_files(archive, "Omon.tos")
        names = [p.name for p in result]
        assert "ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc" in names
        assert "ocean-2d-surface_temp-1monthly-mean-ym_0001_02.nc" in names
        assert "ocean-2d-eta_t-1monthly-mean-ym_0001_01.nc" not in names
        assert "ocean-2d-surface_temp-1daily-mean-ym_0001_01.nc" not in names

    def test_ocean_mixed_conventions_both_discovered(self, tmp_path):
        # Legacy and newer-named files for the same variable are both returned.
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                (
                    "output001/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc",
                ),
            ],
        )
        names = [p.name for p in discover_files(archive, "Omon.tos")]
        assert "ocean-2d-surface_temp-1mon-mean-y_1850.nc" in names
        assert "ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc" in names

    def test_ocean_fx_both_conventions_no_overmatch(self, tmp_path):
        # Fixed fields are "-fx.nc" (legacy) or plain ".nc" (newer).  Both must
        # match, but a time-varying file for the same diagnostic must NOT be
        # pulled into an fx query by the plain ".nc" pattern.
        #
        # Built from a synthetic mapping entry rather than a real CMOR variable:
        # standard ocean fx fields (e.g. areacello) are formula-derived with an
        # empty model_variables list, so they are not auto-discovered.
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-area_t.nc"),  # newer fx
                ("output000/ocean", "ocean-2d-area_t-fx.nc"),  # legacy fx
                # time-varying — must NOT be treated as fx
                ("output000/ocean", "ocean-2d-area_t-1monthly-mean-ym_0001_01.nc"),
            ],
        )
        mappings = _load_full_mappings("ACCESS-ESM1.6")
        fd_cfg = mappings["model_info"]["file_discovery"]
        patterns = _build_patterns(
            {"model_variables": ["area_t"]}, "ocean", "fx", fd_cfg
        )
        names = set()
        for pat in patterns:
            names.update(p.name for p in archive.glob(pat))
        assert "ocean-2d-area_t.nc" in names
        assert "ocean-2d-area_t-fx.nc" in names
        assert "ocean-2d-area_t-1monthly-mean-ym_0001_01.nc" not in names

    def test_year_extraction_newer_convention(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                (
                    "output000/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc",
                ),
                (
                    "output005/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0005_01.nc",
                ),
                (
                    "output010/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0010_01.nc",
                ),
            ],
        )
        result = discover_files(archive, "Omon.tos", start_year=5, end_year=5)
        assert len(result) == 1
        assert _extract_year_from_path(result[0]) == 5

    def test_zero_padded_string_year_args(self, tmp_path):
        # pi-control archives often label years as "0001", "0100" etc.
        # discover_files must accept these zero-padded strings in start_year /
        # end_year and treat them identically to the equivalent integer.
        archive = self._make_archive(
            tmp_path,
            [
                (
                    "output000/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc",
                ),
                (
                    "output050/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0050_01.nc",
                ),
                (
                    "output100/ocean",
                    "ocean-2d-surface_temp-1monthly-mean-ym_0100_01.nc",
                ),
            ],
        )
        # String args "0001" and "0100" must behave identically to int 1 and 100
        result_str = discover_files(
            archive, "Omon.tos", start_year="0001", end_year="0050"
        )
        result_int = discover_files(archive, "Omon.tos", start_year=1, end_year=50)
        assert sorted(result_str) == sorted(result_int)
        assert len(result_str) == 2

    def test_invalid_year_arg_raises(self, tmp_path):
        with pytest.raises(ValueError, match="start_year"):
            discover_files(tmp_path, "Omon.tos", start_year="not_a_year")

    def test_sea_ice_monthly(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ice", "iceh-1monthly-mean_1850-01.nc"),
                ("output000/ice", "iceh-1monthly-mean_1850-02.nc"),
                (
                    "output000/ice",
                    "iceh-1daily-mean_1850-01.nc",
                ),  # daily, should NOT appear
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
        self._make_archive(
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


# ---------------------------------------------------------------------------
# _diagnose_no_files
# ---------------------------------------------------------------------------


class TestDiagnoseNoFiles:
    """Tests for the three diagnostic branches of _diagnose_no_files."""

    @staticmethod
    def _make_archive(tmp_path, files):
        for subdir, name in files:
            p = tmp_path / subdir
            p.mkdir(parents=True, exist_ok=True)
            (p / name).touch()
        return tmp_path

    def test_year_filter_excluded_all_files(self, tmp_path):
        # Files exist (years 1850–1851) but the requested range is 1900–1950.
        # _diagnose_no_files should report the available years and the
        # requested range, not claim there are no files at all.
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1851.nc"),
            ],
        )
        msg = _diagnose_no_files(archive, "Omon.tos", "ACCESS-ESM1.6", 1900, 1950)
        assert "1850" in msg
        assert "1851" in msg
        assert "1900" in msg
        assert "1950" in msg

    def test_no_matching_files_reports_patterns(self, tmp_path):
        # output directories exist but contain no files matching the pattern.
        archive = self._make_archive(
            tmp_path,
            [("output000/ocean", "unexpected_file_format.nc")],
        )
        msg = _diagnose_no_files(archive, "Omon.tos", "ACCESS-ESM1.6", None, None)
        assert "output000" in msg
        assert msg  # non-empty

    def test_no_output_dirs_reports_missing_dirs(self, tmp_path):
        # Completely empty archive — no output* directories at all.
        msg = _diagnose_no_files(tmp_path, "Omon.tos", "ACCESS-ESM1.6", None, None)
        assert "output" in msg.lower()
        assert str(tmp_path) in msg


# ---------------------------------------------------------------------------
# discover_year_range
# ---------------------------------------------------------------------------


class TestDiscoverYearRange:
    @staticmethod
    def _make_archive(tmp_path, entries):
        for subdir, name in entries:
            p = tmp_path / subdir
            p.mkdir(parents=True, exist_ok=True)
            (p / name).touch()
        return tmp_path

    def test_returns_min_max_years(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1900.nc"),
                ("output002/ocean", "ocean-2d-surface_temp-1mon-mean-y_1950.nc"),
            ],
        )
        result = discover_year_range(archive, "Omon.tos")
        assert result == (1850, 1950)

    def test_single_file_returns_same_year_twice(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc")],
        )
        result = discover_year_range(archive, "Omon.tos")
        assert result == (1850, 1850)

    def test_no_files_returns_none(self, tmp_path):
        # Empty archive — no files at all.
        result = discover_year_range(tmp_path, "Omon.tos")
        assert result is None

    def test_files_without_parseable_year_returns_none(self, tmp_path):
        # Files exist but no year can be extracted from their names.
        archive = self._make_archive(
            tmp_path,
            [("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_.nc")],
        )
        # discover_files will return the file (can't filter), but
        # discover_year_range should return None because no year is parseable.
        result = discover_year_range(archive, "Omon.tos")
        assert result is None

    def test_atmosphere_variable(self, tmp_path):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/atmosphere/netCDF", "aiihca.pa-185001_mon.nc"),
                ("output001/atmosphere/netCDF", "aiihca.pa-185101_mon.nc"),
            ],
        )
        result = discover_year_range(archive, "Amon.tas")
        assert result == (1850, 1851)

    def test_propagates_file_discovery_error(self, tmp_path):
        with pytest.raises(FileDiscoveryError):
            discover_year_range(tmp_path, "Omon.totally_nonexistent_variable_xyz")


# ---------------------------------------------------------------------------
# discover_files logging
# ---------------------------------------------------------------------------


class TestDiscoverFilesLogging:
    @staticmethod
    def _make_archive(tmp_path, entries):
        for subdir, name in entries:
            p = tmp_path / subdir
            p.mkdir(parents=True, exist_ok=True)
            (p / name).touch()
        return tmp_path

    def test_logs_file_count_and_year_range(self, tmp_path, caplog):
        archive = self._make_archive(
            tmp_path,
            [
                ("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_1850.nc"),
                ("output001/ocean", "ocean-2d-surface_temp-1mon-mean-y_1900.nc"),
            ],
        )
        with caplog.at_level(logging.INFO, logger="access_moppy.file_discovery"):
            discover_files(archive, "Omon.tos")
        assert any("2" in r.message and "1850" in r.message for r in caplog.records)

    def test_logs_year_range_unknown_when_no_year_parseable(self, tmp_path, caplog):
        # Files that match the glob but have no parseable year in their name.
        archive = self._make_archive(
            tmp_path,
            [("output000/ocean", "ocean-2d-surface_temp-1mon-mean-y_.nc")],
        )
        with caplog.at_level(logging.INFO, logger="access_moppy.file_discovery"):
            discover_files(archive, "Omon.tos")
        assert any("year range unknown" in r.message for r in caplog.records)

    def test_logs_no_files_found(self, tmp_path, caplog):
        with caplog.at_level(logging.INFO, logger="access_moppy.file_discovery"):
            discover_files(tmp_path, "Omon.tos")
        assert any("no files found" in r.message for r in caplog.records)
