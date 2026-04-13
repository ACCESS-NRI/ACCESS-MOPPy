from pathlib import Path
from unittest.mock import patch

import pytest
import xarray as xr

from access_moppy.vocabulary_processors import CMIP6PlusVocabulary


class TestCMIP6PlusVocabulary:
    @pytest.fixture
    def mock_vocab_data(self):
        return {
            "experiment_id": {
                "historical": {
                    "experiment": "historical",
                    "activity_id": ["CMIP"],
                    "required_model_components": ["AOGCM"],
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
                        }
                    },
                }
            },
        }

    @pytest.fixture
    def mock_table_data(self):
        return {
            "Header": {
                "Conventions": "CF-1.7 CMIP-6.2",
                "data_specs_version": "01.00.33",
                "product": "model-output",
            },
            "variable_entry": {
                "tas": {
                    "frequency": "mon",
                    "modeling_realm": "atmos",
                    "units": "K",
                    "type": "real",
                    "dimensions": "longitude latitude time",
                }
            },
        }

    @pytest.fixture
    def vocabulary_instance(self, mock_vocab_data, mock_table_data):
        parent_info = {
            "parent_experiment_id": "historical",
            "parent_activity_id": "CMIP",
            "parent_mip_era": "CMIP6Plus",
            "parent_source_id": "ACCESS-CM2",
            "parent_variant_label": "r1i1p1f1",
            "parent_time_units": "days since 0001-01-01 00:00:00",
            "branch_time_in_child": 0.0,
            "branch_time_in_parent": 0.0,
            "branch_method": "standard",
        }
        with (
            patch.object(
                CMIP6PlusVocabulary,
                "_load_controlled_vocab",
                return_value=mock_vocab_data,
            ),
            patch.object(
                CMIP6PlusVocabulary, "_load_table", return_value=mock_table_data
            ),
        ):
            return CMIP6PlusVocabulary(
                compound_name="Amon.tas",
                experiment_id="historical",
                source_id="ACCESS-CM2",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                parent_info=parent_info,
            )

    @pytest.mark.unit
    def test_required_global_attributes_use_cmip6plus_values(self, vocabulary_instance):
        attrs = vocabulary_instance.get_required_global_attributes()

        assert attrs["mip_era"] == "CMIP6Plus"
        assert attrs["activity_id"] == "CMIP"
        assert attrs["institution"] == "CSIRO-ARCCSS"
        assert attrs["institution_id"] == "CSIRO-ARCCSS"
        assert attrs["license"].startswith("CMIP6Plus model data produced by")

    @pytest.mark.unit
    def test_generate_filename_uses_template(self, vocabulary_instance):
        ds = xr.Dataset({"tas": xr.DataArray([[280.0]], dims=["x", "y"])})
        attrs = {
            "variable_id": "tas",
            "table_id": "Amon",
            "source_id": "ACCESS-CM2",
            "experiment_id": "historical",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
        }

        with patch.object(
            CMIP6PlusVocabulary,
            "_load_drs_templates",
            return_value={
                "filename_template": "<variable_id>_<table_id>_<source_id>_<experiment_id>_<member_id>_<grid_label>"
            },
        ):
            filename = vocabulary_instance.generate_filename(
                attrs=attrs,
                ds=ds,
                cmor_name="tas",
                compound_name="Amon.tas",
            )

        assert filename == "tas_Amon_ACCESS-CM2_historical_r1i1p1f1_gn.nc"

    @pytest.mark.unit
    def test_generate_filename_compact_template_is_normalized(
        self, vocabulary_instance
    ):
        ds = xr.Dataset({"rsds": xr.DataArray([[280.0]], dims=["x", "y"])})
        attrs = {
            "variable_id": "rsds",
            "table_id": "Amon",
            "source_id": "ACCESS-ESM1-5",
            "experiment_id": "piControl-spinup",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
        }

        with patch.object(
            CMIP6PlusVocabulary,
            "_load_drs_templates",
            return_value={
                "filename_template": "<variable_id><table_id><source_id><experiment_id><member_id><grid_label>"
            },
        ):
            filename = vocabulary_instance.generate_filename(
                attrs=attrs,
                ds=ds,
                cmor_name="rsds",
                compound_name="Amon.rsds",
            )

        assert filename == "rsds_Amon_ACCESS-ESM1-5_piControl-spinup_r1i1p1f1_gn.nc"

    @pytest.mark.unit
    def test_build_drs_path_uses_cmip6plus_mip_era(self, vocabulary_instance):
        with patch.object(
            CMIP6PlusVocabulary,
            "_load_drs_templates",
            return_value={
                "directory_path_template": "<mip_era><activity_id><institution_id><source_id><experiment_id><member_id><table_id><variable_id><grid_label><version>"
            },
        ):
            drs_path = vocabulary_instance.build_drs_path(Path("/tmp/out"), "20260318")

        expected = Path(
            "/tmp/out/CMIP6Plus/CMIP/CSIRO-ARCCSS/ACCESS-CM2/historical/r1i1p1f1/Amon/tas/gn/v20260318"
        )
        assert drs_path == expected
