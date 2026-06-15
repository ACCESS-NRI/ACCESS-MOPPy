"""
Tests for the mip-cmor-tables backend and CMIP6PlusMIPVocabulary.

Covers:
* MIPCMORTablesBackend._table_filename / _load_table routing
* CMIP6PlusMIPVocabulary instantiation with APmon / OPmon / LPmon / APday tables
* Variable metadata retrieval for tas, pr, psl, ua, va, zg, hur
* parse_mip_table_frequency for the new table name scheme
* parse_cmip6_table_frequency fall-through to MIP names
* _MONTHLY_TABLE_IDS includes new MIP table names
* Driver CMIP6Plus auto-selection of CMIP6PlusMIPVocabulary
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from access_moppy.vocabulary_processors import (
    CMIP6PlusMIPVocabulary,
    MIPCMORTablesBackend,
    parse_mip_table_frequency,
)
from access_moppy.utilities import (
    _MONTHLY_TABLE_IDS,
    parse_cmip6_table_frequency,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_MOCK_VOCAB = {
    "experiment_id": {
        "historical": {
            "experiment": "historical",
            "activity_id": ["CMIP"],
            "required_model_components": ["AOGCM"],
            "parent_experiment_id": ["piControl"],
        }
    },
    "activity_id": {"CMIP": "CMIP DECK"},
    "source_id": {
        "ACCESS-CM2": {
            "label": "ACCESS-CM2",
            "institution_id": ["CSIRO-ARCCSS"],
            "license_info": {
                "id": "CC BY 4.0",
                "url": "https://creativecommons.org/licenses/by/4.0/",
            },
            "release_year": "2019",
            "model_component": {
                "atmos": {
                    "description": "mock atmosphere",
                    "native_nominal_resolution": "250 km",
                },
                "ocean": {
                    "description": "mock ocean",
                    "native_nominal_resolution": "100 km",
                },
            },
        }
    },
    "institution_id": {"CSIRO-ARCCSS": "CSIRO-ARCCSS"},
}

_MOCK_PARENT_INFO = {
    "parent_experiment_id": "piControl",
    "parent_activity_id": "CMIP",
    "parent_mip_era": "CMIP6Plus",
    "parent_source_id": "ACCESS-CM2",
    "parent_variant_label": "r1i1p1f1",
    "parent_time_units": "days since 0001-01-01 00:00:00",
    "branch_time_in_child": 0.0,
    "branch_time_in_parent": 0.0,
    "branch_method": "standard",
}


def _make_table(variable_entries: dict) -> dict:
    """Helper to build a minimal MIP table dict around variable entries."""
    return {
        "Header": {
            "Conventions": "CF-1.7 CMIP-6.5",
            "data_specs_version": "6.5.0.0",
            "product": "model-output",
            "missing_value": "1e20",
            "int_missing_value": "-999",
            "table_id": "APmon",
        },
        "variable_entry": variable_entries,
    }


_ATMOS_VARS = {
    "tas": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "K",
        "type": "real",
        "dimensions": ["longitude", "latitude", "time", "height2m"],
        "long_name": "Near-Surface Air Temperature",
        "standard_name": "air_temperature",
        "cell_methods": "area: time: mean",
        "cell_measures": "area: areacella",
        "out_name": "tas",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
    "pr": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "kg m-2 s-1",
        "type": "real",
        "dimensions": ["longitude", "latitude", "time"],
        "long_name": "Precipitation",
        "standard_name": "precipitation_flux",
        "cell_methods": "area: time: mean",
        "cell_measures": "area: areacella",
        "out_name": "pr",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
    "psl": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "Pa",
        "type": "real",
        "dimensions": ["longitude", "latitude", "time"],
        "long_name": "Sea Level Pressure",
        "standard_name": "air_pressure_at_mean_sea_level",
        "cell_methods": "area: time: mean",
        "cell_measures": "area: areacella",
        "out_name": "psl",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
    "ua": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "m s-1",
        "type": "real",
        "dimensions": ["longitude", "latitude", "plev19", "time"],
        "long_name": "Eastward Wind",
        "standard_name": "eastward_wind",
        "cell_methods": "time: mean",
        "cell_measures": "area: areacella",
        "out_name": "ua",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
    "va": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "m s-1",
        "type": "real",
        "dimensions": ["longitude", "latitude", "plev19", "time"],
        "long_name": "Northward Wind",
        "standard_name": "northward_wind",
        "cell_methods": "time: mean",
        "cell_measures": "area: areacella",
        "out_name": "va",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
    "zg": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "m",
        "type": "real",
        "dimensions": ["longitude", "latitude", "plev19", "time"],
        "long_name": "Geopotential Height",
        "standard_name": "geopotential_height",
        "cell_methods": "time: mean",
        "cell_measures": "area: areacella",
        "out_name": "zg",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
    "hur": {
        "frequency": "mon",
        "modeling_realm": "atmos",
        "units": "%",
        "type": "real",
        "dimensions": ["longitude", "latitude", "plev19", "time"],
        "long_name": "Relative Humidity",
        "standard_name": "relative_humidity",
        "cell_methods": "time: mean",
        "cell_measures": "area: areacella",
        "out_name": "hur",
        "positive": "",
        "valid_max": "",
        "valid_min": "",
    },
}

_MOCK_APMON_TABLE = _make_table(_ATMOS_VARS)


def _make_vocab(compound_name: str, *, mock_table=None) -> CMIP6PlusMIPVocabulary:
    """Return a fully-mocked CMIP6PlusMIPVocabulary instance."""
    table = mock_table or _MOCK_APMON_TABLE
    with (
        patch.object(
            CMIP6PlusMIPVocabulary,
            "_load_controlled_vocab",
            return_value=_MOCK_VOCAB,
        ),
        patch.object(CMIP6PlusMIPVocabulary, "_load_table", return_value=table),
    ):
        return CMIP6PlusMIPVocabulary(
            compound_name=compound_name,
            experiment_id="historical",
            source_id="ACCESS-CM2",
            variant_label="r1i1p1f1",
            grid_label="gn",
            activity_id="CMIP",
            parent_info=_MOCK_PARENT_INFO,
        )


# ---------------------------------------------------------------------------
# MIPCMORTablesBackend unit tests
# ---------------------------------------------------------------------------


class TestMIPCMORTablesBackend:
    @pytest.mark.unit
    def test_table_filename_uses_mip_prefix(self):
        """_table_filename returns MIP_<TableID>.json."""

        class _Stub(MIPCMORTablesBackend):
            pass

        stub = _Stub()
        assert stub._table_filename("APmon") == "MIP_APmon.json"
        assert stub._table_filename("OPmon") == "MIP_OPmon.json"
        assert stub._table_filename("LPmon") == "MIP_LPmon.json"
        assert stub._table_filename("SImon") == "MIP_SImon.json"
        assert stub._table_filename("coordinate") == "MIP_coordinate.json"

    @pytest.mark.unit
    def test_load_table_raises_for_unknown_table(self):
        """_load_table raises FileNotFoundError for a nonexistent table."""
        vocab = _make_vocab("APmon.tas")
        vocab.table = "Amon"  # legacy name – should not exist in MIP tables
        with pytest.raises(FileNotFoundError, match="MIP CMOR table file not found"):
            # Bypass the mock so it hits the real filesystem lookup
            with patch.object(
                CMIP6PlusMIPVocabulary, "_load_table", wraps=MIPCMORTablesBackend._load_table
            ):
                MIPCMORTablesBackend._load_table(vocab)


# ---------------------------------------------------------------------------
# CMIP6PlusMIPVocabulary – instantiation and CV inheritance
# ---------------------------------------------------------------------------


class TestCMIP6PlusMIPVocabularyInstantiation:
    @pytest.mark.unit
    def test_inherits_cmip6plus_cv_dir(self):
        """cv_dir still points at CMIP6Plus_CVs (not CMIP6_CVs)."""
        vocab = _make_vocab("APmon.tas")
        assert "CMIP6Plus" in vocab.cv_dir

    @pytest.mark.unit
    def test_mip_era_is_cmip6plus(self):
        vocab = _make_vocab("APmon.tas")
        assert vocab.mip_era == "CMIP6Plus"

    @pytest.mark.unit
    def test_table_parsed_correctly(self):
        vocab = _make_vocab("APmon.tas")
        assert vocab.table == "APmon"
        assert vocab.cmor_name == "tas"

    @pytest.mark.unit
    def test_ocean_table_parses(self):
        ocean_table = _make_table(
            {
                "thetao": {
                    "frequency": "mon",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "type": "real",
                    "dimensions": ["longitude", "latitude", "olevel", "time"],
                    "long_name": "Sea Water Potential Temperature",
                    "standard_name": "sea_water_potential_temperature",
                    "cell_methods": "time: mean",
                    "cell_measures": "area: areacello volume: volcello",
                    "out_name": "thetao",
                    "positive": "",
                    "valid_max": "",
                    "valid_min": "",
                }
            }
        )
        vocab = _make_vocab("OPmon.thetao", mock_table=ocean_table)
        assert vocab.table == "OPmon"
        assert vocab.cmor_name == "thetao"

    @pytest.mark.unit
    def test_repr_identifies_class(self):
        vocab = _make_vocab("APmon.tas")
        assert "CMIP6PlusMIPVocabulary" in repr(vocab)
        assert "APmon" in repr(vocab)
        assert "tas" in repr(vocab)


# ---------------------------------------------------------------------------
# Variable metadata for the target variable set
# ---------------------------------------------------------------------------


class TestCMIP6PlusMIPVariableMetadata:
    @pytest.mark.unit
    @pytest.mark.parametrize("varname", ["tas", "pr", "psl", "ua", "va", "zg", "hur"])
    def test_variable_entry_loaded(self, varname):
        """variable entry is populated for all target variables."""
        vocab = _make_vocab(f"APmon.{varname}")
        assert vocab.variable is not None
        assert vocab.variable["units"] == _ATMOS_VARS[varname]["units"]
        assert vocab.variable["standard_name"] == _ATMOS_VARS[varname]["standard_name"]

    @pytest.mark.unit
    @pytest.mark.parametrize("varname", ["tas", "pr", "psl", "ua", "va", "zg", "hur"])
    def test_fill_value_defaults_set(self, varname):
        vocab = _make_vocab(f"APmon.{varname}")
        assert vocab.get_cmip_missing_value() == pytest.approx(1e20)
        assert vocab.get_cmip_fill_value() == pytest.approx(1e20)

    @pytest.mark.unit
    def test_variable_not_found_raises_with_suggestions(self):
        vocab = _make_vocab("APmon.tas")
        # Patch _get_variable_suggestions on MIPCMORTablesBackend so it returns a canned list
        with patch.object(
            CMIP6PlusMIPVocabulary,
            "_get_variable_suggestions",
            return_value=["Try APmon.pr"],
        ):
            from access_moppy.vocabulary_processors import VariableNotFoundError

            # Force re-evaluation of _get_variable_entry with an unknown name
            vocab.cmor_name = "nonexistent_var_xyz"
            with pytest.raises(VariableNotFoundError, match="nonexistent_var_xyz"):
                vocab._get_variable_entry()


# ---------------------------------------------------------------------------
# Global attributes
# ---------------------------------------------------------------------------


class TestCMIP6PlusMIPGlobalAttributes:
    @pytest.mark.unit
    def test_mip_era_attribute(self):
        vocab = _make_vocab("APmon.tas")
        with patch.object(CMIP6PlusMIPVocabulary, "get_parent_experiment_attrs", return_value={}):
            attrs = vocab.get_required_global_attributes()
        assert attrs["mip_era"] == "CMIP6Plus"

    @pytest.mark.unit
    def test_table_id_attribute_uses_mip_table(self):
        vocab = _make_vocab("APmon.tas")
        with patch.object(CMIP6PlusMIPVocabulary, "get_parent_experiment_attrs", return_value={}):
            attrs = vocab.get_required_global_attributes()
        assert attrs["table_id"] == "APmon"

    @pytest.mark.unit
    def test_variable_id_attribute(self):
        vocab = _make_vocab("APmon.tas")
        with patch.object(CMIP6PlusMIPVocabulary, "get_parent_experiment_attrs", return_value={}):
            attrs = vocab.get_required_global_attributes()
        assert attrs["variable_id"] == "tas"

    @pytest.mark.unit
    def test_license_mentions_cmip6plus(self):
        vocab = _make_vocab("APmon.tas")
        with patch.object(CMIP6PlusMIPVocabulary, "get_parent_experiment_attrs", return_value={}):
            attrs = vocab.get_required_global_attributes()
        assert "CMIP6Plus" in attrs["license"]


# ---------------------------------------------------------------------------
# parse_mip_table_frequency
# ---------------------------------------------------------------------------


class TestParseMIPTableFrequency:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "compound_name, expected_days",
        [
            ("APmon.tas", 30),
            ("APday.tasmax", 1),
            ("OPmon.thetao", 30),
            ("OPday.tos", 1),
            ("LPmon.mrso", 30),
            ("LPday.mrso", 1),
            ("SImon.siconc", 30),
            ("SIday.siconc", 1),
            ("LImon.snw", 30),
            ("OBmon.dissic", 30),
            ("APfx.orog", 0),
            ("OPfx.deptho", 0),
        ],
    )
    def test_known_mip_tables(self, compound_name, expected_days):
        import pandas as pd

        result = parse_mip_table_frequency(compound_name)
        assert result == pd.Timedelta(days=expected_days)

    @pytest.mark.unit
    def test_subdaily_tables(self):
        import pandas as pd

        assert parse_mip_table_frequency("AP3hr.pr") == pd.Timedelta(hours=3)
        assert parse_mip_table_frequency("AP6hr.ua") == pd.Timedelta(hours=6)
        assert parse_mip_table_frequency("AP1hr.pr") == pd.Timedelta(hours=1)

    @pytest.mark.unit
    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid compound name"):
            parse_mip_table_frequency("APmon")

    @pytest.mark.unit
    def test_unknown_mip_table_raises_value_error(self):
        """Legacy CMIP6 names like Amon are not in the MIP map – should raise."""
        with pytest.raises(ValueError, match="Unknown MIP table ID"):
            parse_mip_table_frequency("Amon.tas")


# ---------------------------------------------------------------------------
# parse_cmip6_table_frequency fall-through
# ---------------------------------------------------------------------------


class TestParseCMIP6TableFrequencyFallThrough:
    @pytest.mark.unit
    def test_legacy_names_still_work(self):
        import pandas as pd

        assert parse_cmip6_table_frequency("Amon.tas") == pd.Timedelta(days=30)
        assert parse_cmip6_table_frequency("Omon.thetao") == pd.Timedelta(days=30)
        assert parse_cmip6_table_frequency("day.pr") == pd.Timedelta(days=1)

    @pytest.mark.unit
    def test_new_mip_names_work_via_fallthrough(self):
        import pandas as pd

        assert parse_cmip6_table_frequency("APmon.tas") == pd.Timedelta(days=30)
        assert parse_cmip6_table_frequency("OPmon.thetao") == pd.Timedelta(days=30)
        assert parse_cmip6_table_frequency("LPday.mrso") == pd.Timedelta(days=1)

    @pytest.mark.unit
    def test_completely_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown table ID"):
            parse_cmip6_table_frequency("XYZUNKNOWN.foo")


# ---------------------------------------------------------------------------
# _MONTHLY_TABLE_IDS
# ---------------------------------------------------------------------------


class TestMonthlyTableIDs:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "table_id",
        [
            # Legacy CMIP6 names must still be present
            "Amon", "Lmon", "Omon", "SImon", "CFmon",
            # New MIP names
            "APmon", "APmonLev", "APmonZ",
            "AEmon", "OPmon", "LPmon", "LImon", "OBmon",
            "GIAmon", "GIGmon",
        ],
    )
    def test_monthly_ids_present(self, table_id):
        assert table_id in _MONTHLY_TABLE_IDS

    @pytest.mark.unit
    @pytest.mark.parametrize("table_id", ["APday", "OPday", "LPday", "AP3hr"])
    def test_non_monthly_ids_absent(self, table_id):
        assert table_id not in _MONTHLY_TABLE_IDS


# ---------------------------------------------------------------------------
# Driver auto-selection
# ---------------------------------------------------------------------------


class TestDriverAutoSelectsMIPVocabulary:
    @pytest.fixture
    def mock_vocab_attrs(self):
        """Minimal mock vocab instance attributes used by the driver post-init."""
        v = type("MockVocab", (), {})()
        v.table = "APmon"
        v.cmor_name = "tas"
        v.variable = {
            "frequency": "mon",
            "modeling_realm": "atmos",
            "dimensions": "longitude latitude time height2m",
            "units": "K",
            "type": "real",
            "missing_value": 1e20,
            "_FillValue": 1e20,
        }
        v.cmip_table = {
            "Header": {
                "Conventions": "CF-1.7",
                "data_specs_version": "6.5.0.0",
                "product": "model-output",
            }
        }
        return v

    @pytest.mark.unit
    def test_apmon_selects_mip_vocabulary(self, mock_vocab_attrs, tmp_path):
        """Driver picks CMIP6PlusMIPVocabulary for APmon compound names."""
        from access_moppy.driver import ACCESS_ESM_CMORiser

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6PlusMIPVocabulary") as mock_mip,
            patch("access_moppy.driver.CMIP6PlusVocabulary") as mock_legacy,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_mip.return_value = mock_vocab_attrs

            try:
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="APmon.tas",
                    cmip_version="CMIP6Plus",
                    experiment_id="historical",
                    source_id="ACCESS-CM2",
                    variant_label="r1i1p1f1",
                    grid_label="gn",
                    output_path=str(tmp_path),
                )
            except Exception:
                pass

            mock_mip.assert_called_once()
            mock_legacy.assert_not_called()

    @pytest.mark.unit
    def test_amon_selects_legacy_vocabulary(self, mock_vocab_attrs, tmp_path):
        """Driver picks CMIP6PlusVocabulary for legacy Amon compound names."""
        from access_moppy.driver import ACCESS_ESM_CMORiser

        mock_vocab_attrs.table = "Amon"
        mock_vocab_attrs.cmip_table["Header"]["data_specs_version"] = "01.00.33"

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6PlusMIPVocabulary") as mock_mip,
            patch("access_moppy.driver.CMIP6PlusVocabulary") as mock_legacy,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_legacy.return_value = mock_vocab_attrs

            try:
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    cmip_version="CMIP6Plus",
                    experiment_id="historical",
                    source_id="ACCESS-CM2",
                    variant_label="r1i1p1f1",
                    grid_label="gn",
                    output_path=str(tmp_path),
                )
            except Exception:
                pass

            mock_legacy.assert_called_once()
            mock_mip.assert_not_called()
