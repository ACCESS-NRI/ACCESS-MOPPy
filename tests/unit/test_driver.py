"""
Unit tests for the ACCESS_ESM_CMORiser driver class.

These tests focus on the initialization and configuration of the main
CMORiser interface without requiring actual data processing.
"""

import tempfile
import types
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser


class TestACCESSESMCMORiser:
    """Unit tests for ACCESS_ESM_CMORiser driver class."""

    @pytest.fixture
    def valid_config(self):
        """Valid configuration for CMORiser initialization."""
        return {
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-5",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
            "activity_id": "CMIP",
        }

    @pytest.mark.unit
    def test_init_with_minimal_params(self, valid_config, temp_dir):
        """Test initialization with minimal required parameters."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.input_paths == ["test.nc"]
            assert cmoriser.compound_name == "Amon.tas"
            assert cmoriser.output_path == Path(temp_dir)
            assert cmoriser.experiment_id == "historical"
            assert cmoriser.source_id == "ACCESS-ESM1-5"

    @pytest.mark.unit
    def test_init_with_multiple_input_paths(self, valid_config, temp_dir):
        """Test initialization with multiple input files."""
        input_files = ["file1.nc", "file2.nc", "file3.nc"]

        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=input_files,
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.input_paths == input_files

    @pytest.mark.unit
    def test_loads_global_attributes_from_experiment_metadata(
        self, valid_config, temp_dir
    ):
        """Selected raw experiment metadata is included in output attributes."""
        experiment_root = temp_dir / "experiment"
        output_folder = experiment_root / "output000" / "atmos"
        output_folder.mkdir(parents=True)
        input_file = output_folder / "history.nc"
        (experiment_root / "metadata.yaml").write_text(
            "experiment_uuid: 135fafe6-5457-4300-a3d9-56138a932b99\n"
            "parent_experiment: cfcbc3a9-6f5a-4fa5-b963-24bd050e546b\n"
            "name: esm-historical-1.1-r1i1p1f1\n"
            "description: not copied to global attributes\n",
            encoding="utf-8",
        )

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.discover_files", return_value=[input_file]),
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            cmoriser = ACCESS_ESM_CMORiser(
                input_folder=experiment_root,
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

        attrs = cmoriser.vocab.get_required_global_attributes()
        assert attrs["access_experiment_uuid"] == "135fafe6-5457-4300-a3d9-56138a932b99"
        assert (
            attrs["access_parent_experiment_uuid"]
            == "cfcbc3a9-6f5a-4fa5-b963-24bd050e546b"
        )
        assert attrs["access_name"] == "esm-historical-1.1-r1i1p1f1"
        assert "parent_experiment" not in attrs
        assert "description" not in attrs

    @pytest.mark.unit
    def test_init_with_parent_info(self, valid_config, temp_dir):
        """Test initialization with parent experiment information."""
        parent_info = {
            "parent_experiment_id": "piControl",
            "parent_activity_id": "CMIP",
            "parent_source_id": "ACCESS-ESM1-5",
            "parent_variant_label": "r1i1p1f1",
            "parent_time_units": "days since 0001-01-01 00:00:00",
            "parent_mip_era": "CMIP6",
            "branch_time_in_child": 0.0,
            "branch_time_in_parent": 54786.0,
            "branch_method": "standard",
        }

        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                parent_info=parent_info,
                **valid_config,
            )

            # Should use provided parent info instead of defaults
            assert cmoriser.parent_info == parent_info

    @pytest.mark.unit
    def test_init_with_drs_root(self, valid_config, temp_dir):
        """Test initialization with DRS root specification."""
        drs_root = temp_dir / "drs_structure"

        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                drs_root=str(drs_root),
                **valid_config,
            )

            assert cmoriser.drs_root == Path(drs_root)

    @pytest.mark.unit
    def test_compound_name_parsing(self, valid_config, temp_dir):
        """Test that compound names are parsed correctly."""
        test_cases = [
            ("Amon.tas", "Amon", "tas"),
            ("Omon.tos", "Omon", "tos"),
            ("Lmon.mrso", "Lmon", "mrso"),
            ("day.pr", "day", "pr"),
        ]

        for compound_name, expected_table, expected_var in test_cases:
            with patch("access_moppy.driver.load_model_mappings") as mock_load:
                mock_load.return_value = {expected_var: {"units": "K"}}

                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name=compound_name,
                    output_path=temp_dir,
                    **valid_config,
                )

                # Check that the compound name is stored correctly
                assert cmoriser.compound_name == compound_name
                # Check that mappings were loaded for the correct compound name
                mock_load.assert_called_with(compound_name, model_id="ACCESS-ESM1-5")

    @pytest.mark.unit
    def test_output_path_conversion(self, valid_config):
        """Test that output path is properly converted to Path object."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            # Test with string path using secure temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                test_output_path = str(Path(temp_dir) / "test_output")
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=test_output_path,
                    **valid_config,
                )

                assert isinstance(cmoriser.output_path, Path)
                assert cmoriser.output_path == Path(test_output_path)

    @pytest.mark.unit
    def test_default_parent_info_used(self, valid_config, temp_dir):
        """Test that default parent info is used when none provided."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            # Should use default parent info when none provided
            assert "parent_experiment_id" in cmoriser.parent_info
            assert cmoriser.parent_info["parent_experiment_id"] == "piControl"

    @pytest.mark.unit
    def test_root_experiment_does_not_use_default_parent_info(self, temp_dir):
        """Root experiments omit parent metadata without emitting a default warning."""
        config = {
            "experiment_id": "piControl",
            "source_id": "ACCESS-ESM1-5",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
            "activity_id": "CMIP",
        }
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}
            with warnings.catch_warnings(record=True) as caught:
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    **config,
                )

        assert cmoriser.parent_info == {}
        assert not any("No parent_info provided" in str(item.message) for item in caught)

    @pytest.mark.unit
    def test_variable_mapping_loaded(self, valid_config, temp_dir):
        """Test that variable mapping is loaded correctly."""
        mock_mapping = {
            "tas": {
                "CF standard Name": "air_temperature",
                "units": "K",
                "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            }
        }

        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = mock_mapping

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.variable_mapping.mapping == mock_mapping
            mock_load.assert_called_once_with("Amon.tas", model_id="ACCESS-ESM1-5")

    @pytest.mark.unit
    def test_missing_required_params(self, temp_dir):
        """Test that missing required parameters raise appropriate errors."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            # Test missing experiment_id parameter - should raise TypeError
            with pytest.raises(TypeError, match="experiment_id"):
                # Intentionally omit experiment_id to test error handling
                # This is expected to fail with TypeError
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    source_id="ACCESS-ESM1-5",
                    variant_label="r1i1p1f1",
                    grid_label="gn",
                    activity_id="CMIP",
                    # experiment_id intentionally omitted for testing
                )

    @pytest.mark.unit
    def test_drs_root_path_conversion(self, valid_config, temp_dir):
        """Test DRS root path conversion from string to Path."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            # Use secure temporary directory for DRS root path
            with tempfile.TemporaryDirectory() as drs_temp_dir:
                test_drs_path = str(Path(drs_temp_dir) / "drs")
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    drs_root=test_drs_path,
                    **valid_config,
                )

                assert isinstance(cmoriser.drs_root, Path)
                assert cmoriser.drs_root == Path(test_drs_path)

    @pytest.mark.unit
    def test_model_id_parameter(self, valid_config, temp_dir):
        """Test initialization with model_id parameter for model-specific mappings."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_mapping = {
                "tas": {
                    "CF standard Name": "air_temperature",
                    "units": "K",
                }
            }
            mock_load.return_value = mock_mapping

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                model_id="ACCESS-ESM1-6",
                **valid_config,
            )

            # Verify the model_id is stored
            assert cmoriser.model_id == "ACCESS-ESM1-6"

            # Verify load_model_mappings was called with model_id
            mock_load.assert_called_once_with("Amon.tas", model_id="ACCESS-ESM1-6")

            # Verify the mapping was loaded correctly
            assert cmoriser.variable_mapping.mapping == mock_mapping

    @pytest.mark.unit
    def test_model_id_none_fallback(self, valid_config, temp_dir):
        """Test that omitting model_id resolves it from source_id."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                model_id=None,
                **valid_config,
            )

            # model_id=None should resolve to the source_id's mapping identifier
            assert cmoriser.model_id == "ACCESS-ESM1-5"

            # Verify load_model_mappings was called with the resolved model_id
            mock_load.assert_called_once_with("Amon.tas", model_id="ACCESS-ESM1-5")

    @pytest.mark.unit
    def test_init_with_cmip6plus_version(self, valid_config, temp_dir):
        """Test CMIP6Plus selects CMIP6PlusVocabulary class."""
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6PlusVocabulary") as mock_vocab,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                cmip_version="CMIP6Plus",
                **valid_config,
            )

            mock_vocab.assert_called_once()

    @pytest.mark.unit
    def test_invalid_cmip_version_error(self, valid_config, temp_dir):
        """Test invalid cmip_version returns clear accepted values."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            with pytest.raises(
                ValueError,
                match="cmip_version must be 'CMIP6', 'CMIP6Plus', or 'CMIP7'",
            ):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    cmip_version="CMIP8",
                    **valid_config,
                )

    @pytest.mark.unit
    def test_input_paths_and_input_data_together_raises(self, valid_config, temp_dir):
        with pytest.raises(
            ValueError, match="Cannot specify both 'input_data' and 'input_paths'"
        ):
            ACCESS_ESM_CMORiser(
                input_data=["a.nc"],
                input_paths=["b.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

    @pytest.mark.unit
    def test_missing_input_data_for_non_internal_calculation_raises(
        self, valid_config, temp_dir
    ):
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            with pytest.raises(
                ValueError, match="Must specify either 'input_data' or 'input_paths'"
            ):
                ACCESS_ESM_CMORiser(
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_missing_input_data_allowed_for_internal_calculation(
        self, valid_config, temp_dir
    ):
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {
                "tas": {"calculation": {"type": "internal"}, "units": "K"}
            }
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.input_paths == []
            mock_atmos.assert_called_once()

    @pytest.mark.unit
    def test_ressource_file_used_when_no_input_data(self, valid_config, temp_dir):
        """When no input_data is supplied but the mapping has a ressource_file,
        the bundled resource path is resolved and used as input_data."""
        fake_nc_path = "/fake/path/to/fx.zfull_ACCESS-ESM.nc"

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.get_bundled_resource_path") as mock_get_path,
            patch("access_moppy.driver.as_file") as mock_as_file,
            patch("access_moppy.driver.CMIP6Vocabulary") as mock_vocab,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {
                "zfull": {"ressource_file": "fx.zfull_ACCESS-ESM.nc", "units": "m"}
            }
            mock_resource = MagicMock()
            mock_get_path.return_value = mock_resource
            mock_as_file.return_value.__enter__.return_value = Path(fake_nc_path)
            mock_vocab.return_value = MagicMock()
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                compound_name="fx.zfull",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.input_paths == [fake_nc_path]
            mock_get_path.assert_called_once_with("fx.zfull_ACCESS-ESM.nc")
            mock_as_file.assert_called_once_with(mock_resource)

    @pytest.mark.unit
    def test_empty_model_variables_allows_no_input_data(self, valid_config, temp_dir):
        """When model_variables is [] no input_data is required; input_paths stays empty."""
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6Vocabulary") as mock_vocab,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {
                "mrsofc": {
                    "model_variables": [],
                    "calculation": {
                        "type": "formula",
                        "operation": "load_ressource_data",
                        "args": [
                            {"literal": "fx.mrsofc_ACCESS-ESM.nc"},
                            {"literal": "mrsofc"},
                        ],
                    },
                    "units": "kg m-2",
                }
            }
            mock_vocab.return_value = MagicMock()
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                compound_name="Lmon.mrsofc",
                output_path=temp_dir,
                **valid_config,
            )

            # No input paths — the calculation is self-contained
            assert cmoriser.input_paths == []

    @pytest.mark.unit
    def test_xarray_input_dataarray_converts_to_dataset_and_disables_validation(
        self, valid_config, temp_dir
    ):
        da = xr.DataArray([1.0, 2.0], dims=["time"], name="tas")

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            with pytest.warns(UserWarning, match="Disabling frequency validation"):
                cmoriser = ACCESS_ESM_CMORiser(
                    input_data=da,
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    validate_frequency=True,
                    write_prefetch=7,
                    **valid_config,
                )

            assert cmoriser.input_is_xarray is True
            assert cmoriser.validate_frequency is False
            assert cmoriser.input_paths == []
            assert isinstance(cmoriser.input_dataset, xr.Dataset)
            called_kwargs = mock_atmos.call_args.kwargs
            assert isinstance(called_kwargs["input_data"], xr.Dataset)
            assert called_kwargs["write_prefetch"] == 7

    @pytest.mark.unit
    def test_cmip7_uses_mapping_and_cmip7_vocabulary(self, valid_config, temp_dir):
        with (
            patch("access_moppy.driver._get_cmip7_to_cmip6_mapping") as mock_map,
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP7Vocabulary") as mock_vocab7,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_map.return_value = "Amon.tas"
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_vocab7.return_value = MagicMock()
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="atmos.tas.some.cmip7.name",
                cmip_version="CMIP7",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.cmip6_compound_name == "Amon.tas"
            assert cmoriser.cmip7_compound_name == "atmos.tas.some.cmip7.name"
            mock_map.assert_called_once_with("atmos.tas.some.cmip7.name")
            mock_load.assert_called_once_with("Amon.tas", model_id="ACCESS-ESM1-5")
            mock_vocab7.assert_called_once()

    @pytest.mark.unit
    def test_cmip7_g999_uses_model_grid_mapping_for_siu(self, temp_dir):
        grid_labels = {
            "ocean": {"U": "g116"},
            "sea_ice": {"U": "g116", "default": "g118"},
        }
        with (
            patch(
                "access_moppy.driver._get_cmip7_to_cmip6_mapping",
                return_value="SImon.siu",
            ),
            patch(
                "access_moppy.driver.load_model_mappings",
                return_value={"siu": {"units": "m s-1"}},
            ),
            patch(
                "access_moppy.driver.load_model_info",
                return_value={"cmip7_grid_labels": grid_labels},
            ),
            patch("access_moppy.driver.CMIP7Vocabulary"),
            patch("access_moppy.driver.SeaIce_CMORiser") as mock_seaice,
        ):
            mock_seaice.return_value.ds = xr.Dataset()

            ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="seaIce.siu.tavg-u-hxy-si.mon.GLB",
                experiment_id="historical",
                source_id="ACCESS-ESM1-6",
                variant_label="r1i1p1f1",
                grid_label="g999",
                cmip_version="CMIP7",
                activity_id="CMIP",
                output_path=temp_dir,
            )

            assert mock_seaice.call_args.kwargs["cmip7_grid_labels"] == grid_labels

    @pytest.mark.unit
    def test_cmip7_accepts_cmip6_compound_name_fallback(self, valid_config, temp_dir):
        with (
            patch(
                "access_moppy.driver._get_cmip7_to_cmip6_mapping", return_value=None
            ) as mock_map,
            patch(
                "access_moppy.driver.load_cmip6_to_cmip7_mapping"
            ) as mock_reverse_map,
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP7Vocabulary") as mock_vocab7,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_reverse_map.return_value = {
                "Amon.rsdt": "atmos.rsdt.tavg-u-hxy-u.mon.GLB"
            }
            mock_load.return_value = {"rsdt": {"units": "W m-2"}}
            mock_vocab7.return_value = MagicMock()
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.rsdt",
                cmip_version="CMIP7",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser.cmip6_compound_name == "Amon.rsdt"
            assert cmoriser.cmip7_compound_name == "atmos.rsdt.tavg-u-hxy-u.mon.GLB"
            mock_map.assert_called_once_with("Amon.rsdt")
            mock_reverse_map.assert_called_once_with()
            mock_load.assert_called_once_with("Amon.rsdt", model_id="ACCESS-ESM1-5")
            mock_vocab7.assert_called_once()

    @pytest.mark.unit
    def test_cmip7_invalid_compound_name_raises_clear_error(
        self, valid_config, temp_dir
    ):
        with patch(
            "access_moppy.driver._get_cmip7_to_cmip6_mapping", return_value=None
        ):
            with pytest.raises(ValueError, match="Could not map CMIP7 compound name"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="bad.cmip7.name.format",
                    cmip_version="CMIP7",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_cmip7_missing_region_suffix_suggests_glb(self, valid_config, temp_dir):
        with patch(
            "access_moppy.driver._get_cmip7_to_cmip6_mapping", return_value=None
        ):
            with pytest.raises(
                ValueError, match=r"atmos\.rsdt\.tavg-u-hxy-u\.mon\.GLB"
            ):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="atmos.rsdt.tavg-u-hxy-u.mon",
                    cmip_version="CMIP7",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_ocean_table_selects_om3_for_access_om3_source(
        self, valid_config, temp_dir
    ):
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6Vocabulary") as mock_vocab,
            patch("access_moppy.driver.Ocean_CMORiser_OM3") as mock_om3,
        ):
            mock_load.return_value = {"tos": {"units": "K"}}
            mock_vocab.return_value = MagicMock()
            mock_om3_instance = MagicMock()
            mock_om3_instance.ds = xr.Dataset()
            mock_om3.return_value = mock_om3_instance

            ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Omon.tos",
                source_id="ACCESS-OM3",
                output_path=temp_dir,
                experiment_id=valid_config["experiment_id"],
                variant_label=valid_config["variant_label"],
                grid_label=valid_config["grid_label"],
                activity_id=valid_config["activity_id"],
            )

            mock_om3.assert_called_once()

    @pytest.mark.unit
    def test_ocean_table_selects_om2_for_non_om3_source(self, valid_config, temp_dir):
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Ocean_CMORiser_OM2") as mock_om2,
        ):
            mock_load.return_value = {"tos": {"units": "K"}}
            mock_om2_instance = MagicMock()
            mock_om2_instance.ds = xr.Dataset()
            mock_om2.return_value = mock_om2_instance

            ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Omon.tos",
                source_id="ACCESS-ESM1-5",
                output_path=temp_dir,
                experiment_id=valid_config["experiment_id"],
                variant_label=valid_config["variant_label"],
                grid_label=valid_config["grid_label"],
                activity_id=valid_config["activity_id"],
            )

            mock_om2.assert_called_once()

    @pytest.mark.unit
    def test_unsupported_table_raises_value_error_with_supported_list(
        self, valid_config, temp_dir
    ):
        """Unknown CMIP table raises ValueError listing atmosphere/ocean/sea-ice tables."""
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6Vocabulary"),
        ):
            mock_load.return_value = {"foo": {"units": "1"}}

            with pytest.raises(ValueError, match="Unsupported CMIP table 'XXXmon'"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="XXXmon.foo",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_unsupported_table_error_lists_known_tables(self, valid_config, temp_dir):
        """The ValueError message for an unknown table must mention known table groups."""
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6Vocabulary"),
        ):
            mock_load.return_value = {"foo": {"units": "1"}}

            with pytest.raises(ValueError, match="atmosphere:") as exc_info:
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="XXXmon.foo",
                    output_path=temp_dir,
                    **valid_config,
                )

            msg = str(exc_info.value)
            assert "ocean:" in msg
            assert "sea-ice:" in msg
            assert "Amon" in msg
            assert "Omon" in msg
            assert "SImon" in msg

    @pytest.mark.unit
    def test_sea_ice_table_selects_seaice_cmoriser(self, valid_config, temp_dir):
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.SeaIce_CMORiser") as mock_seaice,
        ):
            mock_load.return_value = {"siconc": {"units": "1"}}
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_seaice.return_value = mock_instance

            ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="SImon.siconc",
                output_path=temp_dir,
                **valid_config,
            )

            mock_seaice.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("compound_name", "source_id", "component_name"),
        [
            ("Amon.tas", "ACCESS-ESM1-5", "Atmosphere_CMORiser"),
            ("Omon.tos", "ACCESS-ESM1-5", "Ocean_CMORiser_OM2"),
            ("Omon.tos", "ACCESS-OM3", "Ocean_CMORiser_OM3"),
            ("SImon.siconc", "ACCESS-ESM1-5", "SeaIce_CMORiser"),
        ],
    )
    def test_chunk_settings_reach_component_cmoriser(
        self,
        valid_config,
        temp_dir,
        compound_name,
        source_id,
        component_name,
    ):
        variable = compound_name.split(".", 1)[1]
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch(f"access_moppy.driver.{component_name}") as mock_component,
        ):
            mock_load.return_value = {variable: {"units": "1"}}
            mock_component.return_value.ds = xr.Dataset()

            ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name=compound_name,
                source_id=source_id,
                output_path=temp_dir,
                experiment_id=valid_config["experiment_id"],
                variant_label=valid_config["variant_label"],
                grid_label=valid_config["grid_label"],
                activity_id=valid_config["activity_id"],
                enable_chunking=True,
                chunk_size_mb=8,
                max_chunk_size_mb=64,
            )

            kwargs = mock_component.call_args.kwargs
            assert kwargs["enable_chunking"] is True
            assert kwargs["chunk_size_mb"] == 8
            assert kwargs["max_chunk_size_mb"] == 64

    @pytest.mark.unit
    def test_dataset_delegation_and_run_write_methods(self, valid_config, temp_dir):
        dataset = xr.Dataset({"tas": ("time", [280.0, 281.0])})

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}

            mock_instance = MagicMock()
            mock_instance.ds = dataset
            mock_instance.cmor_name = "tas"
            mock_instance.sizes = dataset.sizes
            mock_instance.written_files = []
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            assert cmoriser["tas"].values[0] == 280.0
            cmoriser["tas"] = xr.DataArray([282.0, 283.0], dims=["time"])
            assert cmoriser.to_dataset()["tas"].values[0] == 282.0
            assert repr(cmoriser) == repr(dataset)
            assert cmoriser.sizes["time"] == 2
            # written_files is exposed via the wrapped CMORiser, not its ds
            # (regression test for the ACCESS-MOPPy AttributeError bug).
            assert cmoriser.written_files == []

            cmoriser.run(write_output=False)
            mock_instance.run.assert_called_once()
            mock_instance.write.assert_not_called()

            cmoriser.run(write_output=True)
            assert mock_instance.write.call_count == 1

            cmoriser.write()
            assert mock_instance.write.call_count == 2

    @pytest.mark.unit
    def test_to_iris_raises_when_main_cube_not_found(self, valid_config, temp_dir):
        dataset = xr.Dataset({"tas": ("time", [280.0, 281.0])})

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}

            mock_instance = MagicMock()
            mock_instance.ds = dataset
            mock_instance.cmor_name = "tas"
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            fake_module = types.ModuleType("ncdata.iris_xarray")

            class _Cube:
                def __init__(self, var_name):
                    self.var_name = var_name
                    self.data = [1.0]

            def _fake_cubes_from_xarray(_ds):
                return [_Cube("not_tas")]

            fake_module.cubes_from_xarray = _fake_cubes_from_xarray

            with patch.dict("sys.modules", {"ncdata.iris_xarray": fake_module}):
                with pytest.raises(
                    ValueError, match="Could not find cube for variable 'tas'"
                ):
                    cmoriser.to_iris()

    @pytest.mark.unit
    def test_input_folder_discovers_files(self, valid_config, temp_dir):
        """input_folder triggers auto-discovery and populates input_paths."""
        discovered = [Path("/archive/output000/atm/aiihca.pa-185001_mon.nc")]

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
            patch(
                "access_moppy.driver.discover_files", return_value=discovered
            ) as mock_disc,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            cmoriser = ACCESS_ESM_CMORiser(
                input_folder="/archive",
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )

            mock_disc.assert_called_once_with(
                "/archive",
                "Amon.tas",
                model_id="ACCESS-ESM1-5",
                start_year=None,
                end_year=None,
            )
            assert cmoriser.input_paths == [str(discovered[0])]

    @pytest.mark.unit
    def test_input_folder_passes_year_filters(self, valid_config, temp_dir):
        """start_year and end_year are forwarded to discover_files."""
        discovered = [Path("/archive/output000/atm/file.nc")]

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
            patch(
                "access_moppy.driver.discover_files", return_value=discovered
            ) as mock_disc,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            ACCESS_ESM_CMORiser(
                input_folder="/archive",
                compound_name="Amon.tas",
                output_path=temp_dir,
                start_year=1900,
                end_year=1950,
                **valid_config,
            )

            mock_disc.assert_called_once_with(
                "/archive",
                "Amon.tas",
                model_id="ACCESS-ESM1-5",
                start_year=1900,
                end_year=1950,
            )

    @pytest.mark.unit
    def test_input_folder_and_input_data_together_raises(self, valid_config, temp_dir):
        """Providing both input_folder and input_data must raise ValueError."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            with pytest.raises(
                ValueError, match="Cannot specify both 'input_folder' and 'input_data'"
            ):
                ACCESS_ESM_CMORiser(
                    input_data=["a.nc"],
                    input_folder="/archive",
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_input_folder_no_files_found_raises(self, valid_config, temp_dir):
        """Empty discovery result raises a clear ValueError."""
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.discover_files", return_value=[]),
        ):
            mock_load.return_value = {"tas": {"units": "K"}}

            with pytest.raises(ValueError, match="No files found for"):
                ACCESS_ESM_CMORiser(
                    input_folder="/archive",
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_input_folder_discovery_error_raises_value_error(
        self, valid_config, temp_dir
    ):
        """A FileDiscoveryError from discover_files is re-raised as ValueError."""
        from access_moppy.file_discovery import FileDiscoveryError

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch(
                "access_moppy.driver.discover_files",
                side_effect=FileDiscoveryError("no pattern"),
            ),
        ):
            mock_load.return_value = {"tas": {"units": "K"}}

            with pytest.raises(ValueError, match="Could not auto-discover files"):
                ACCESS_ESM_CMORiser(
                    input_folder="/archive",
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_input_folder_uses_model_id_for_discovery(self, valid_config, temp_dir):
        """model_id is forwarded to discover_files instead of the default."""
        discovered = [Path("/archive/output000/atm/file.nc")]

        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
            patch(
                "access_moppy.driver.discover_files", return_value=discovered
            ) as mock_disc,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            ACCESS_ESM_CMORiser(
                input_folder="/archive",
                compound_name="Amon.tas",
                output_path=temp_dir,
                model_id="ACCESS-CM2",
                **valid_config,
            )

            mock_disc.assert_called_once_with(
                "/archive",
                "Amon.tas",
                model_id="ACCESS-CM2",
                start_year=None,
                end_year=None,
            )


def _make_driver_without_init(dataset, cmor_name="tas"):
    """
    Build an ACCESS_ESM_CMORiser instance bypassing __init__ entirely.

    __init__ requires CMIP vocabulary files that are not available in the
    unit-test environment.  For to_iris() tests we only need the two
    attributes that the method reads from self.cmoriser:
      - self.cmoriser.ds
      - self.cmoriser.cmor_name
    Using __new__ lets us inject those without any external dependencies.
    """
    driver = ACCESS_ESM_CMORiser.__new__(ACCESS_ESM_CMORiser)
    inner = MagicMock()
    inner.ds = dataset
    inner.cmor_name = cmor_name
    driver.cmoriser = inner
    return driver


def _fake_ncdata_module(cmor_name="tas"):
    """
    Return a fake ncdata.iris_xarray module whose cubes_from_xarray
    produces a single cube with var_name=cmor_name.

    The cube's data is taken directly from the xarray dataset passed into
    cubes_from_xarray so that fill-value replacements applied before the
    call (ds[cmor_name].where(...)) are reflected in the cube data.
    """
    fake_module = types.ModuleType("ncdata.iris_xarray")

    class _FakeCube:
        def __init__(self, data):
            self.var_name = cmor_name
            self.data = data

    def _cubes_from_xarray(ds):
        return [_FakeCube(ds[cmor_name].values.copy())]

    fake_module.cubes_from_xarray = _cubes_from_xarray
    return fake_module


class TestToIrisFillValueExceptionHandling:
    """
    Tests for the except (TypeError, ValueError): pass branch inside
    to_iris() when float(fill_val) fails.

    driver.py lines 348-353:
        if fill_val is not None:
            try:
                fill_val = float(fill_val)
                ds[cmor_name] = ds[cmor_name].where(ds[cmor_name] != fill_val)
            except (TypeError, ValueError):
                pass

    All tests bypass __init__ via __new__ to avoid needing CMIP vocabulary
    files that are not available in the unit-test environment.
    """

    @pytest.mark.unit
    def test_non_numeric_string_fill_value_raises_value_error_and_is_silenced(self):
        """
        When _FillValue is a non-numeric string (e.g. 'N/A'), float() raises
        ValueError.  to_iris() must catch it silently and still return a cube.

        Covers the ValueError arm of except (TypeError, ValueError): pass.
        """
        data = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        dataset = xr.Dataset(
            {"tas": ("time", data, {"_FillValue": "N/A"})},
            coords={"time": np.arange(3, dtype=float)},
        )

        driver = _make_driver_without_init(dataset)

        with patch.dict("sys.modules", {"ncdata.iris_xarray": _fake_ncdata_module()}):
            cube = driver.to_iris()

        assert cube is not None
        assert cube.var_name == "tas"

    @pytest.mark.unit
    def test_non_convertible_object_fill_value_raises_type_error_and_is_silenced(self):
        """
        When _FillValue is an object for which float() raises TypeError
        (e.g. a list), to_iris() must catch it silently and still return a cube.

        Covers the TypeError arm of except (TypeError, ValueError): pass.
        """
        data = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        dataset = xr.Dataset(
            {"tas": ("time", data, {"_FillValue": [1, 2, 3]})},
            coords={"time": np.arange(3, dtype=float)},
        )

        driver = _make_driver_without_init(dataset)

        with patch.dict("sys.modules", {"ncdata.iris_xarray": _fake_ncdata_module()}):
            cube = driver.to_iris()

        assert cube is not None
        assert cube.var_name == "tas"

    @pytest.mark.unit
    def test_valid_numeric_fill_value_masks_matching_values(self):
        """
        Sanity check for the happy path: when _FillValue is a valid numeric
        string (e.g. '1e20'), float() succeeds, matching values are replaced
        with NaN in the dataset, cubes_from_xarray sees NaN, and the final
        cube has that position masked.

        Ensures the exception branches do not interfere with the normal path.
        """
        fill = 1e20
        data = np.array([1.0, fill, 3.0], dtype=np.float64)
        dataset = xr.Dataset(
            {"tas": ("time", data, {"_FillValue": str(fill)})},
            coords={"time": np.arange(3, dtype=float)},
        )

        driver = _make_driver_without_init(dataset)
        # _fake_ncdata_module reads data from the dataset passed to it, so it
        # will see NaN at index 1 after to_iris() replaces the fill value.
        with patch.dict("sys.modules", {"ncdata.iris_xarray": _fake_ncdata_module()}):
            cube = driver.to_iris()

        assert cube is not None
        assert np.ma.is_masked(
            cube.data[1]
        ), "Value equal to _FillValue must be masked after to_iris()"

    @pytest.mark.unit
    def test_raises_import_error_when_ncdata_unavailable(self):
        """
        When ncdata.iris_xarray cannot be imported, to_iris() must raise
        ImportError with a message naming ncdata and iris.
        """
        data = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        dataset = xr.Dataset(
            {"tas": ("time", data)},
            coords={"time": np.arange(3, dtype=float)},
        )

        driver = _make_driver_without_init(dataset)

        # Remove ncdata.iris_xarray from sys.modules so the import fails
        with patch.dict("sys.modules", {"ncdata.iris_xarray": None}):
            with pytest.raises(ImportError, match="ncdata and iris are required"):
                driver.to_iris()

    @pytest.mark.unit
    def test_aux_vars_promoted_to_coordinates(self):
        """
        When latitude/longitude/vertices_* are present as data variables,
        to_iris() must promote them to coordinates before conversion so that
        cubes_from_xarray treats them as auxiliary coordinates rather than
        separate data cubes.
        """
        nt, nlat, nlon = 3, 2, 2
        data = np.ones((nt, nlat, nlon), dtype=np.float32)
        lat_2d = np.array([[10.0, 10.0], [20.0, 20.0]])
        lon_2d = np.array([[100.0, 110.0], [100.0, 110.0]])

        # latitude and longitude are data variables, not coordinates
        dataset = xr.Dataset(
            {
                "tas": (["time", "y", "x"], data),
                "latitude": (["y", "x"], lat_2d),
                "longitude": (["y", "x"], lon_2d),
            },
            coords={"time": np.arange(nt, dtype=float)},
        )
        assert "latitude" in dataset.data_vars
        assert "longitude" in dataset.data_vars

        promoted_vars = []

        driver = _make_driver_without_init(dataset, cmor_name="tas")

        # Intercept the dataset that reaches cubes_from_xarray
        fake_module = types.ModuleType("ncdata.iris_xarray")

        class _FakeCube:
            var_name = "tas"
            data = np.ones(nt)

        def _cubes_from_xarray(ds):
            promoted_vars.extend(list(ds.coords))
            return [_FakeCube()]

        fake_module.cubes_from_xarray = _cubes_from_xarray

        with patch.dict("sys.modules", {"ncdata.iris_xarray": fake_module}):
            driver.to_iris()

        assert (
            "latitude" in promoted_vars
        ), "latitude must have been promoted to a coordinate"
        assert (
            "longitude" in promoted_vars
        ), "longitude must have been promoted to a coordinate"

    @pytest.mark.unit
    def test_vertices_promoted_to_coordinates(self):
        """
        vertices_latitude and vertices_longitude (ocean curvilinear bounds)
        must also be promoted from data variables to coordinates.
        """
        nt, nlat, nlon = 2, 2, 2
        nv = 4  # vertices per cell
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        dataset = xr.Dataset(
            {
                "tos": (["time", "y", "x"], data),
                "vertices_latitude": (["y", "x", "nv"], np.zeros((nlat, nlon, nv))),
                "vertices_longitude": (["y", "x", "nv"], np.zeros((nlat, nlon, nv))),
            },
            coords={"time": np.arange(nt, dtype=float)},
        )

        promoted_vars = []
        driver = _make_driver_without_init(dataset, cmor_name="tos")

        fake_module = types.ModuleType("ncdata.iris_xarray")

        class _FakeCube:
            var_name = "tos"
            data = np.ones(nt)

        def _cubes_from_xarray(ds):
            promoted_vars.extend(list(ds.coords))
            return [_FakeCube()]

        fake_module.cubes_from_xarray = _cubes_from_xarray

        with patch.dict("sys.modules", {"ncdata.iris_xarray": fake_module}):
            driver.to_iris()

        assert "vertices_latitude" in promoted_vars
        assert "vertices_longitude" in promoted_vars

    @pytest.mark.unit
    def test_no_aux_vars_skips_set_coords(self):
        """
        When none of the aux variables (latitude, longitude, vertices_*)
        appear as data variables, the set_coords branch must be skipped
        and to_iris() must still succeed.
        """
        data = np.array([1.0, 2.0], dtype=np.float32)
        dataset = xr.Dataset(
            {"tas": ("time", data)},
            coords={"time": np.arange(2, dtype=float)},
        )

        driver = _make_driver_without_init(dataset)

        with patch.dict("sys.modules", {"ncdata.iris_xarray": _fake_ncdata_module()}):
            cube = driver.to_iris()

        assert cube is not None
        assert cube.var_name == "tas"


class TestACCESSESMCMORiserContextManager:
    """Tests for close(), __enter__(), and __exit__() on ACCESS_ESM_CMORiser."""

    @pytest.fixture
    def valid_config(self):
        return {
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-5",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
            "activity_id": "CMIP",
        }

    @pytest.mark.unit
    def test_close_releases_resource_stack(self, valid_config, temp_dir):
        """close() drains the ExitStack without raising."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            )
            cmoriser.close()  # must not raise

    @pytest.mark.unit
    def test_getattr_cmoriser_missing_raises_attribute_error_not_recursion(self):
        """Accessing 'cmoriser' on an instance where it was never set must raise
        AttributeError, not RecursionError."""
        driver = ACCESS_ESM_CMORiser.__new__(ACCESS_ESM_CMORiser)
        with pytest.raises(AttributeError, match="table may not be supported"):
            _ = driver.cmoriser

    @pytest.mark.unit
    def test_getattr_other_attr_when_cmoriser_missing_raises_attribute_error(self):
        """Accessing any delegated attribute when cmoriser is unset must raise
        AttributeError rather than triggering infinite recursion."""
        driver = ACCESS_ESM_CMORiser.__new__(ACCESS_ESM_CMORiser)
        with pytest.raises(AttributeError):
            _ = driver.ds

    @pytest.mark.unit
    def test_context_manager_enter_and_exit(self, valid_config, temp_dir):
        """__enter__ returns self; __exit__ calls close() without error."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}
            with ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                **valid_config,
            ) as cmoriser:
                assert cmoriser is not None
                assert hasattr(cmoriser, "_resource_stack")
            # After the with block __exit__ was called; no exception raised


class TestMappingNotFoundWarning:
    """Tests for MappingNotFoundWarning emitted by ACCESS_ESM_CMORiser.__init__."""

    @pytest.fixture
    def valid_config(self):
        return {
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-5",
            "variant_label": "r1i1p1f1",
            "grid_label": "gn",
            "activity_id": "CMIP",
        }

    @pytest.mark.unit
    def test_no_warning_when_mapping_found(self, valid_config, temp_dir):
        """No MappingNotFoundWarning is emitted when a mapping is successfully loaded."""
        with (
            patch("access_moppy.driver.load_model_mappings") as mock_load,
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_load.return_value = {"tas": {"units": "K"}}
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            import warnings as _warnings

            with _warnings.catch_warnings(record=True) as recorded:
                _warnings.simplefilter("always")
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    **valid_config,
                )

            from access_moppy.utilities import MappingNotFoundWarning

            mapping_warns = [
                w for w in recorded if issubclass(w.category, MappingNotFoundWarning)
            ]
            assert mapping_warns == []

    @pytest.mark.unit
    def test_warns_when_model_file_missing(self, valid_config, temp_dir):
        """MappingNotFoundWarning mentions unsupported model when no mapping file exists."""
        with (
            patch("access_moppy.driver.load_model_mappings", return_value={}),
            patch("access_moppy.driver._model_mapping_file_exists", return_value=False),
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            from access_moppy.utilities import MappingNotFoundWarning

            with pytest.warns(MappingNotFoundWarning, match="not yet supported"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    model_id="UNSUPPORTED-MODEL",
                    **valid_config,
                )

    @pytest.mark.unit
    def test_warns_with_contribute_url_when_model_file_missing(
        self, valid_config, temp_dir
    ):
        """Warning message includes the contribution URL when model file is absent."""
        with (
            patch("access_moppy.driver.load_model_mappings", return_value={}),
            patch("access_moppy.driver._model_mapping_file_exists", return_value=False),
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            from access_moppy.utilities import MappingNotFoundWarning

            with pytest.warns(MappingNotFoundWarning, match="ACCESS-NRI/ACCESS-MOPPy"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    model_id="UNSUPPORTED-MODEL",
                    **valid_config,
                )

    @pytest.mark.unit
    def test_warns_when_variable_missing_from_existing_model_file(
        self, valid_config, temp_dir
    ):
        """MappingNotFoundWarning mentions the missing variable when the model file
        exists but does not contain a mapping for the requested variable."""
        with (
            patch("access_moppy.driver.load_model_mappings", return_value={}),
            patch("access_moppy.driver._model_mapping_file_exists", return_value=True),
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            from access_moppy.utilities import MappingNotFoundWarning

            with pytest.warns(MappingNotFoundWarning, match="tas"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    model_id="ACCESS-ESM1-6",
                    **valid_config,
                )

    @pytest.mark.unit
    def test_warns_with_contribute_url_when_variable_missing(
        self, valid_config, temp_dir
    ):
        """Warning message includes the contribution URL when the variable is unmapped."""
        with (
            patch("access_moppy.driver.load_model_mappings", return_value={}),
            patch("access_moppy.driver._model_mapping_file_exists", return_value=True),
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            from access_moppy.utilities import MappingNotFoundWarning

            with pytest.warns(MappingNotFoundWarning, match="ACCESS-NRI/ACCESS-MOPPy"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    model_id="ACCESS-ESM1-6",
                    **valid_config,
                )

    @pytest.mark.unit
    def test_resolved_model_id_used_in_warning_when_none_supplied(
        self, valid_config, temp_dir
    ):
        """When model_id=None the warning references the model resolved from source_id."""
        with (
            patch("access_moppy.driver.load_model_mappings", return_value={}),
            patch("access_moppy.driver._model_mapping_file_exists", return_value=True),
            patch("access_moppy.driver.CMIP6Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            from access_moppy.utilities import MappingNotFoundWarning

            # valid_config uses source_id="ACCESS-ESM1-5", so the resolved model_id
            # should also be "ACCESS-ESM1-5"
            with pytest.warns(MappingNotFoundWarning, match="ACCESS-ESM1-5"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    model_id=None,
                    **valid_config,
                )

    @pytest.mark.unit
    def test_cmip7_path_warns_when_mapping_missing(self, valid_config, temp_dir):
        """MappingNotFoundWarning is also emitted when using the CMIP7 code path."""
        with (
            patch(
                "access_moppy.driver._get_cmip7_to_cmip6_mapping",
                return_value="Amon.tas",
            ),
            patch("access_moppy.driver.load_model_mappings", return_value={}),
            patch("access_moppy.driver._model_mapping_file_exists", return_value=True),
            patch("access_moppy.driver.CMIP7Vocabulary"),
            patch("access_moppy.driver.Atmosphere_CMORiser") as mock_atmos,
        ):
            mock_instance = MagicMock()
            mock_instance.ds = xr.Dataset()
            mock_atmos.return_value = mock_instance

            from access_moppy.utilities import MappingNotFoundWarning

            with pytest.warns(MappingNotFoundWarning, match="tas"):
                ACCESS_ESM_CMORiser(
                    input_paths=["test.nc"],
                    compound_name="atmos.tas.some.cmip7.name",
                    cmip_version="CMIP7",
                    output_path=temp_dir,
                    **valid_config,
                )
