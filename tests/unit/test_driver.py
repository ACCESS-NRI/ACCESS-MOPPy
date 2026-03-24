"""
Unit tests for the ACCESS_ESM_CMORiser driver class.

These tests focus on the initialization and configuration of the main
CMORiser interface without requiring actual data processing.
"""

import tempfile
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

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
                # Check that mappings were loaded for the correct compound name with None model_id
                mock_load.assert_called_with(compound_name, model_id=None)

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
            mock_load.assert_called_once_with("Amon.tas", model_id=None)

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
                model_id="ACCESS-ESM1.6",
                **valid_config,
            )

            # Verify the model_id is stored
            assert cmoriser.model_id == "ACCESS-ESM1.6"

            # Verify load_model_mappings was called with model_id
            mock_load.assert_called_once_with("Amon.tas", model_id="ACCESS-ESM1.6")

            # Verify the mapping was loaded correctly
            assert cmoriser.variable_mapping.mapping == mock_mapping

    @pytest.mark.unit
    def test_model_id_none_fallback(self, valid_config, temp_dir):
        """Test that None model_id falls back to generic mappings."""
        with patch("access_moppy.driver.load_model_mappings") as mock_load:
            mock_load.return_value = {"tas": {"units": "K"}}

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["test.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                model_id=None,
                **valid_config,
            )

            # Verify the model_id is None
            assert cmoriser.model_id is None

            # Verify load_model_mappings was called with None model_id
            mock_load.assert_called_once_with("Amon.tas", model_id=None)

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
                    **valid_config,
                )

            assert cmoriser.input_is_xarray is True
            assert cmoriser.validate_frequency is False
            assert cmoriser.input_paths == []
            assert isinstance(cmoriser.input_dataset, xr.Dataset)
            called_kwargs = mock_atmos.call_args.kwargs
            assert isinstance(called_kwargs["input_data"], xr.Dataset)

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
            mock_load.assert_called_once_with("Amon.tas", model_id=None)
            mock_vocab7.assert_called_once()

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
