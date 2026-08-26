from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.sea_ice import SeaIce_CMORiser


class TestSeaIceCMORiser:
    """Unit tests for sea-ice CMORisation."""

    @pytest.fixture
    def mock_vocab(self):
        """Mock vocabulary for sea-ice tests."""
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1-6"
        vocab.variable = {"units": "1", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab._get_axes = Mock(return_value=({}, {}))
        vocab._get_required_bounds_variables = Mock(return_value=({}, {}))
        return vocab

    @pytest.fixture
    def mock_mapping(self):
        """Mock variable mapping for sea-ice tests."""
        return {
            "siconc": {
                "model_variables": ["ice_conc"],
                "calculation": {"type": "direct"},
            }
        }

    @pytest.fixture
    def mock_seaice_dataset(self):
        """Create a mock sea-ice dataset with time not in first position."""
        time = pd.date_range("2000-01-01", periods=3, freq="ME")
        nj = np.arange(2)
        ni = np.arange(4)

        return xr.Dataset(
            data_vars={
                "ice_conc": (
                    ["nj", "time", "ni"],
                    np.random.random((2, 3, 4)),
                    {"coordinates": "ULON ULAT", "units": "1"},
                )
            },
            coords={
                "time": ("time", time),
                "nj": ("nj", nj),
                "ni": ("ni", ni),
            },
        )

    @pytest.mark.unit
    def test_chunk_settings_reach_base_cmoriser(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
                enable_chunking=True,
                chunk_size_mb=8,
                max_chunk_size_mb=64,
            )

        assert cmoriser.enable_chunking is True
        assert cmoriser.chunker.target_chunk_size_mb == 8
        assert cmoriser.chunker.max_chunk_size_mb == 64

    @pytest.mark.unit
    def test_select_and_process_variables_moves_time_to_first_dimension(
        self, mock_vocab, mock_mapping, mock_seaice_dataset, temp_dir
    ):
        """Ensure the processed sea-ice variable has time as the leading dimension."""
        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = mock_seaice_dataset

                cmoriser.select_and_process_variables()

                assert cmoriser.ds[cmoriser.cmor_name].dims == ("time", "j", "i")

    @pytest.mark.unit
    def test_select_and_process_missing_model_var_in_formula_raises_key_error(
        self, mock_vocab, temp_dir
    ):
        """Formula calc with a missing model variable raises KeyError with context."""
        import pandas as pd

        mapping = {
            "siconc": {
                "model_variables": ["missing_var"],
                "calculation": {"type": "formula", "operation": "missing_var * 0.01"},
            }
        }
        time = pd.date_range("2000-01-01", periods=2, freq="ME")
        ds = xr.Dataset(
            {"other_var": (["time"], [1.0, 2.0])},
            coords={"time": time},
        )

        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mapping,
                )
                cmoriser.ds = ds

                with pytest.raises(KeyError, match="missing_var"):
                    cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_select_and_process_empty_required_vars_direct_raises(
        self, mock_vocab, temp_dir
    ):
        """direct calc type with empty model_variables raises ValueError."""
        mapping = {
            "siconc": {
                "model_variables": [],
                "calculation": {"type": "direct"},
            }
        }

        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mapping,
                )
                cmoriser.ds = xr.Dataset()

                with pytest.raises(
                    ValueError, match="requires at least one model_variable"
                ):
                    cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_infer_grid_type_lists_data_var_coord_attrs(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """infer_grid_type error must list each data_var with its 'coordinates' attr."""
        ds = xr.Dataset(
            data_vars={
                "ice_conc": (
                    ["nj", "ni"],
                    np.zeros((2, 2)),
                    {"coordinates": "XLON XLAT"},
                ),
                "ice_thick": (["nj", "ni"], np.zeros((2, 2))),
            }
        )

        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=mock_vocab,
                variable_mapping=mock_mapping,
            )
            cmoriser.ds = ds

            with pytest.raises(
                ValueError, match="Could not infer grid type"
            ) as exc_info:
                cmoriser.infer_grid_type()

            msg = str(exc_info.value)
            assert "ice_conc" in msg
            assert "XLON XLAT" in msg
            assert "ice_thick" in msg
            assert "<missing>" in msg

    @pytest.mark.unit
    def test_get_dim_rename_unsupported_source_id_mentions_access_prefix(
        self, mock_mapping, temp_dir
    ):
        """_get_dim_rename raises ValueError naming the required ACCESS- prefix."""
        vocab = Mock()
        vocab.source_id = "OTHER-MODEL"
        vocab._get_nominal_resolution = Mock(return_value="1deg")

        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=vocab,
                variable_mapping=mock_mapping,
            )

            with pytest.raises(
                ValueError, match="Unsupported source_id 'OTHER-MODEL'"
            ) as exc_info:
                cmoriser._get_dim_rename()

            msg = str(exc_info.value)
            assert "SeaIce_CMORiser" in msg
            assert "ACCESS-" in msg

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("variable", "grid_type", "expected_grid_label"),
        [("siu", "U", "g116"), ("sithick", "T", "g118")],
    )
    def test_update_attributes_sets_cmip7_sea_ice_grid_label(
        self, temp_dir, variable, grid_type, expected_grid_label
    ):
        """The inferred sea-ice grid label must reach the output metadata."""
        ny, nx, nt = 2, 4, 3
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1-6"
        vocab.grid_label = "g999"
        vocab.variable = {"units": "1", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(
            side_effect=lambda: {"grid_label": vocab.grid_label}
        )
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "long_name": "time",
                "axis": "T",
            }
        }
        mapping = {
            variable: {
                "model_variables": [variable],
                "calculation": {"type": "direct"},
            }
        }
        ds = xr.Dataset(
            {variable: (["time", "j", "i"], np.ones((nt, ny, nx), dtype=np.float32))},
            coords={
                "time": ("time", pd.date_range("2000-01-01", periods=nt, freq="ME")),
                "i": ("i", np.arange(nx)),
                "j": ("j", np.arange(ny)),
            },
        )
        grid_info = {
            "i": np.arange(nx),
            "j": np.arange(ny),
            "vertices": np.arange(4),
            "latitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "longitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "vertices_latitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
            "vertices_longitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
        }
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name=f"SImon.{variable}",
                vocab=vocab,
                variable_mapping=mapping,
                cmip7_grid_labels={
                    "sea_ice": {"U": "g116", "T": "g118", "default": "g118"}
                },
            )
        cmoriser.ds = ds
        cmoriser.grid_type = grid_type
        cmoriser.symmetric = None
        cmoriser.supergrid = Mock()
        cmoriser.supergrid.extract_grid.return_value = grid_info

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time"].attrs["standard_name"] == "time"
        assert cmoriser.ds["time"].attrs["axis"] == "T"
        assert vocab.grid_label == expected_grid_label
        assert cmoriser.ds.attrs["grid_label"] == expected_grid_label

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("placeholder", "expected"),
        [
            # The table left the measure to the modelling centre, and the model
            # config names the one it publishes for the B-grid corner.
            ("--MODEL", "area: areacellu"),
            # The table named a real measure: the config must not override it.
            (None, "area: areacello"),
        ],
    )
    def test_update_attributes_answers_placeholder_cell_measures(
        self, temp_dir, placeholder, expected
    ):
        """A "--MODEL" cell_measures is answered from the model config."""
        ny, nx, nt = 2, 4, 3
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1-6"
        vocab.grid_label = "g999"
        vocab.cell_measures_placeholder = placeholder
        vocab.variable = {"units": "m s-1", "type": "real"}
        if placeholder is None:
            vocab.variable["cell_measures"] = "area: areacello"
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={})
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "long_name": "time",
                "axis": "T",
            }
        }
        mapping = {
            "siu": {"model_variables": ["siu"], "calculation": {"type": "direct"}}
        }
        ds = xr.Dataset(
            {"siu": (["time", "j", "i"], np.ones((nt, ny, nx), dtype=np.float32))},
            coords={
                "time": ("time", pd.date_range("2000-01-01", periods=nt, freq="ME")),
                "i": ("i", np.arange(nx)),
                "j": ("j", np.arange(ny)),
            },
        )
        grid_info = {
            "i": np.arange(nx),
            "j": np.arange(ny),
            "vertices": np.arange(4),
            "latitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "longitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "vertices_latitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
            "vertices_longitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
        }
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siu",
                vocab=vocab,
                variable_mapping=mapping,
                cell_measures_overrides={"sea_ice": {"U": "area: areacellu"}},
            )
        cmoriser.ds = ds
        cmoriser.grid_type = "U"
        cmoriser.symmetric = None
        cmoriser.supergrid = Mock()
        cmoriser.supergrid.extract_grid.return_value = grid_info

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["siu"].attrs["cell_measures"] == expected

    def test_update_attributes_upcasts_time_bnds_to_match_time(self, temp_dir):
        """A float32 time_bnds must be upcast to double alongside the time
        coordinate (observed real-world mismatch: time written as double,
        time_bnds as float, in the same sea-ice output file)."""
        ny, nx, nt = 2, 4, 3
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1-6"
        vocab.variable = {"units": "1", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={})
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "long_name": "time",
                "axis": "T",
            }
        }
        mapping = {
            "siconc": {
                "model_variables": ["ice_conc"],
                "calculation": {"type": "direct"},
            }
        }
        ds = xr.Dataset(
            {
                "siconc": (
                    ["time", "j", "i"],
                    np.ones((nt, ny, nx), dtype=np.float32),
                ),
                "time_bnds": (
                    ["time", "bnds"],
                    np.zeros((nt, 2), dtype=np.float32),
                ),
            },
            coords={
                "time": (
                    "time",
                    pd.date_range("2000-01-01", periods=nt, freq="ME"),
                    {"bounds": "time_bnds"},
                ),
                "i": ("i", np.arange(nx)),
                "j": ("j", np.arange(ny)),
            },
        )
        grid_info = {
            "i": np.arange(nx),
            "j": np.arange(ny),
            "vertices": np.arange(4),
            "latitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "longitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "vertices_latitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
            "vertices_longitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
        }
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=vocab,
                variable_mapping=mapping,
            )
        cmoriser.ds = ds
        cmoriser.grid_type = "T"
        cmoriser.symmetric = None
        cmoriser.supergrid = Mock()
        cmoriser.supergrid.extract_grid.return_value = grid_info

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time_bnds"].dtype == np.float64

    def _make_update_attributes_cmoriser(self, temp_dir, var_type):
        """Build a SeaIce_CMORiser for update_attributes() dtype/fill-value
        tests, with the CMOR table's 'type' either set (var_type is a string)
        or absent (var_type is None), mirroring CMIP7 tables that carry no
        per-variable 'type'."""
        ny, nx, nt = 2, 4, 3
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1-6"
        vocab.variable = {"units": "1"}
        if var_type is not None:
            vocab.variable["type"] = var_type
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={})
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "long_name": "time",
                "axis": "T",
            }
        }
        mapping = {
            "siconc": {
                "model_variables": ["ice_conc"],
                "calculation": {"type": "direct"},
            }
        }
        ds = xr.Dataset(
            {"siconc": (["time", "j", "i"], np.ones((nt, ny, nx), dtype=np.float32))},
            coords={
                "time": ("time", pd.date_range("2000-01-01", periods=nt, freq="ME")),
                "i": ("i", np.arange(nx)),
                "j": ("j", np.arange(ny)),
            },
        )
        grid_info = {
            "i": np.arange(nx),
            "j": np.arange(ny),
            "vertices": np.arange(4),
            "latitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "longitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "vertices_latitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
            "vertices_longitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
        }
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=vocab,
                variable_mapping=mapping,
            )
        cmoriser.ds = ds
        cmoriser.grid_type = "T"
        cmoriser.symmetric = None
        cmoriser.supergrid = Mock()
        cmoriser.supergrid.extract_grid.return_value = grid_info
        return cmoriser

    @pytest.mark.unit
    def test_missing_table_type_preserves_source_dtype(self, temp_dir):
        """No 'type' in the CMOR table entry (as in CMIP7 tables): keep the
        source dtype (float32) instead of falling back to float64."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type=None)
        assert cmoriser.ds["siconc"].dtype == np.float32

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["siconc"].dtype == np.float32

    @pytest.mark.unit
    def test_explicit_double_type_upcasts_and_recasts_fill_value(self, temp_dir):
        """When the table does specify 'double', the data is upcast as
        before, and _FillValue/missing_value are re-cast to float64 too so
        they stay bit-consistent with the now-float64 data."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type="double")
        cmoriser.ds["siconc"].attrs["_FillValue"] = np.float32(1e20)
        cmoriser.ds["siconc"].attrs["missing_value"] = np.float32(1e20)

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["siconc"].dtype == np.float64
        assert isinstance(cmoriser.ds["siconc"].attrs["_FillValue"], np.float64)
        assert isinstance(cmoriser.ds["siconc"].attrs["missing_value"], np.float64)
        assert cmoriser.ds["siconc"].attrs["_FillValue"] == np.float64(np.float32(1e20))

    @pytest.mark.unit
    def test_source_range_attributes_dropped_unless_table_declares_them(self, temp_dir):
        """valid_range is never a CMOR attribute, and valid_min/valid_max are
        the table's to declare, so both must be stripped when they were only
        inherited from the source variable."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type=None)
        cmoriser.ds["siconc"].attrs["valid_range"] = np.array(
            [-1e20, 1e20], dtype=np.float32
        )
        cmoriser.ds["siconc"].attrs["valid_min"] = np.float32(-1e20)
        cmoriser.ds["siconc"].attrs["valid_max"] = np.float32(1e20)

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        attrs = cmoriser.ds["siconc"].attrs
        assert "valid_range" not in attrs
        assert "valid_min" not in attrs
        assert "valid_max" not in attrs

    @pytest.mark.unit
    def test_time_gets_leap_seconds_units_metadata(self, temp_dir):
        """CF-1.11 §4.4 asks a time axis on a real-world calendar to say
        whether leap seconds are counted; model time never counts them.
        SeaIce_CMORiser overrides update_attributes, hence its own coverage."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type=None)
        cmoriser.vocab.get_required_global_attributes.return_value = {
            "Conventions": "CF-1.12"
        }
        cmoriser.ds["time"].attrs["calendar"] = "proleptic_gregorian"

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert cmoriser.ds["time"].attrs["units_metadata"] == "leap_seconds: none"
        # siconc is a fraction — nothing ambiguous about its units.
        assert "units_metadata" not in cmoriser.ds["siconc"].attrs

    @pytest.mark.unit
    def test_vertices_is_pure_dimension_not_coordinate(self, temp_dir):
        """`vertices` must be a bare dimension, not an attribute-less int
        coordinate variable (matches the ocean path; a variable fails CF §3.3)."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type=None)

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "vertices" not in cmoriser.ds.coords
        assert "vertices" in cmoriser.ds.dims

    @pytest.mark.unit
    def test_dropping_vertices_keeps_bounds_and_leaves_no_file_variable(self, temp_dir):
        """Dropping the `vertices` coordinate must remove only that variable:
        the bounds using the dimension keep their shape, and the written file
        carries no `vertices` variable for the compliance checker to reject."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type=None)

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "vertices" not in cmoriser.ds.variables
        for v in ("vertices_latitude", "vertices_longitude"):
            assert cmoriser.ds[v].dims == ("j", "i", "vertices")
            assert cmoriser.ds[v].sizes["vertices"] == 4

        written = temp_dir / "vertices_roundtrip.nc"
        cmoriser.ds.to_netcdf(written)
        with xr.open_dataset(written, decode_cf=False) as reopened:
            assert "vertices" not in reopened.variables
            assert reopened["vertices_latitude"].shape[-1] == 4

    @pytest.mark.unit
    def test_vertices_bounds_have_no_attributes(self, temp_dir):
        """vertices_latitude/longitude inherit units and standard_name from
        latitude/longitude and must repeat neither (CF §7.1; as in ocean)."""
        cmoriser = self._make_update_attributes_cmoriser(temp_dir, var_type=None)

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        for v in ("vertices_latitude", "vertices_longitude"):
            assert cmoriser.ds[v].attrs == {}
        assert cmoriser.ds["latitude"].attrs.get("units") == "degrees_north"
        assert cmoriser.ds["longitude"].attrs.get("units") == "degrees_east"

    @pytest.mark.unit
    def test_pure_dimension_grid_is_renamed_to_i_j(
        self, mock_vocab, mock_mapping, temp_dir
    ):
        """ni/nj as *pure dimensions* (no coordinate variable) must still be
        renamed to i/j, so the data variable aligns with the supergrid.

        Reproduces the real-data case where the `if k in self.ds` filter dropped
        the dimension rename and left siconc on (nj, ni).
        """
        # No coords for nj/ni -> they are pure dimensions, unlike the other
        # fixture which makes them coordinate variables.
        ds = xr.Dataset(
            data_vars={
                "ice_conc": (
                    ["nj", "time", "ni"],
                    np.random.random((2, 3, 4)),
                    {"coordinates": "TLON TLAT", "units": "1"},
                )
            },
            coords={
                "time": ("time", pd.date_range("2000-01-01", periods=3, freq="ME"))
            },
        )
        assert "nj" not in ds.variables  # pure dimension

        with patch("access_moppy.sea_ice.Supergrid"):
            with patch("access_moppy.ocean.CMORiser.load_dataset", return_value=None):
                cmoriser = SeaIce_CMORiser(
                    input_paths=["test.nc"],
                    output_path=str(temp_dir),
                    compound_name="SImon.siconc",
                    vocab=mock_vocab,
                    variable_mapping=mock_mapping,
                )
                cmoriser.ds = ds
                cmoriser.select_and_process_variables()

        assert cmoriser.ds["siconc"].dims == ("time", "j", "i")
        assert "nj" not in cmoriser.ds.dims and "ni" not in cmoriser.ds.dims

    @pytest.mark.unit
    def test_update_attributes_drops_model_lat_lon_and_sets_coordinates(self, temp_dir):
        """Model lat/lon (renamed from TLAT/TLON) are dropped in favour of the
        supergrid latitude/longitude, and the data variable's stale coordinates
        attribute is replaced with 'latitude longitude'."""
        ny, nx, nt = 2, 4, 3
        vocab = Mock()
        vocab.source_id = "ACCESS-ESM1-6"
        vocab.variable = {"units": "1", "type": "real"}
        vocab._get_nominal_resolution = Mock(return_value="1deg")
        vocab.get_required_global_attributes = Mock(return_value={})
        vocab.axes = {
            "time": {"out_name": "time", "standard_name": "time", "axis": "T"}
        }
        mapping = {
            "siconc": {
                "model_variables": ["ice_conc"],
                "calculation": {"type": "direct"},
            }
        }
        ds = xr.Dataset(
            {
                "siconc": (
                    ["time", "j", "i"],
                    np.ones((nt, ny, nx), dtype=np.float32),
                    {"coordinates": "TLON TLAT time"},  # stale, dangling
                ),
                # leftover model coords (renamed from TLAT/TLON), no standard_name
                "lat": (["j", "i"], np.zeros((ny, nx)), {"units": "degrees_north"}),
                "lon": (["j", "i"], np.zeros((ny, nx)), {"units": "degrees_east"}),
            },
            coords={
                "time": ("time", pd.date_range("2000-01-01", periods=nt, freq="ME")),
                "i": ("i", np.arange(nx)),
                "j": ("j", np.arange(ny)),
            },
        )
        grid_info = {
            "i": np.arange(nx),
            "j": np.arange(ny),
            "vertices": np.arange(4),
            "latitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "longitude": xr.DataArray(np.ones((ny, nx)), dims=("j", "i")),
            "vertices_latitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
            "vertices_longitude": xr.DataArray(
                np.ones((ny, nx, 4)), dims=("j", "i", "vertices")
            ),
        }
        with patch("access_moppy.sea_ice.Supergrid"):
            cmoriser = SeaIce_CMORiser(
                input_paths=["test.nc"],
                output_path=str(temp_dir),
                compound_name="SImon.siconc",
                vocab=vocab,
                variable_mapping=mapping,
            )
        cmoriser.ds = ds
        cmoriser.grid_type = "T"
        cmoriser.symmetric = None
        cmoriser.supergrid = Mock()
        cmoriser.supergrid.extract_grid.return_value = grid_info

        with patch.object(cmoriser, "_check_calendar"):
            cmoriser.update_attributes()

        assert "lat" not in cmoriser.ds and "lon" not in cmoriser.ds
        assert "latitude" in cmoriser.ds and "longitude" in cmoriser.ds
        assert cmoriser.ds["siconc"].attrs["coordinates"] == "latitude longitude"


class TestNormaliseTimeBounds:
    """Unit tests for SeaIce_CMORiser._normalise_time_bounds (CICE time bounds)."""

    @staticmethod
    def _bare():
        return object.__new__(SeaIce_CMORiser)

    @pytest.mark.unit
    def test_renames_cice_time_bounds_and_dim(self):
        """CICE 'time_bounds' on 'd2' becomes 'time_bnds' on 'bnds'."""
        ds = xr.Dataset(
            {"time_bounds": (["time", "d2"], np.zeros((3, 2)))},
            coords={"time": ("time", np.arange(3, dtype=float))},
        )
        ds["time"].attrs["bounds"] = "time_bounds"
        cm = self._bare()
        cm.ds = ds
        cm._normalise_time_bounds()

        assert "time_bnds" in cm.ds and "time_bounds" not in cm.ds
        assert cm.ds["time_bnds"].dims == ("time", "bnds")
        assert cm.ds["time"].attrs["bounds"] == "time_bnds"

    @pytest.mark.unit
    def test_noop_when_already_canonical(self):
        """Already-canonical time_bnds/bnds is left unchanged (no error)."""
        ds = xr.Dataset(
            {"time_bnds": (["time", "bnds"], np.zeros((2, 2)))},
            coords={"time": ("time", np.arange(2, dtype=float))},
        )
        ds["time"].attrs["bounds"] = "time_bnds"
        cm = self._bare()
        cm.ds = ds
        cm._normalise_time_bounds()

        assert cm.ds["time_bnds"].dims == ("time", "bnds")
        assert cm.ds["time"].attrs["bounds"] == "time_bnds"

    @pytest.mark.unit
    def test_noop_when_bounds_attr_absent_or_missing_var(self):
        """No time:bounds attr, or it points at a missing variable -> no-op."""
        # No bounds attr at all.
        ds = xr.Dataset(coords={"time": ("time", np.arange(2, dtype=float))})
        cm = self._bare()
        cm.ds = ds
        cm._normalise_time_bounds()  # must not raise
        assert "bounds" not in cm.ds["time"].attrs

        # bounds attr points at a variable that is not present.
        ds2 = xr.Dataset(coords={"time": ("time", np.arange(2, dtype=float))})
        ds2["time"].attrs["bounds"] = "time_bounds"
        cm2 = self._bare()
        cm2.ds = ds2
        cm2._normalise_time_bounds()  # must not raise
        assert "time_bnds" not in cm2.ds
