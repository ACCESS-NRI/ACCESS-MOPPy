"""
Unit tests for Atmosphere_CMORiser.

Covers two bug fixes in select_and_process_variables():
  1. time_0 non-singleton dimension is dropped before transpose
     (land variables such as cVeg/cSoil acquire a time_0 dimension > 1 when
     xarray broadcasts multiple model variables with differing dim orders)
  2. Inherited units attribute is cleared after formula calculations
     (raw PP variables carry units="1"; the formula converts to kg m-2 but
     xarray does not update the attribute, causing _check_units to fail)
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.atmosphere import Atmosphere_CMORiser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_vocab(dimensions="time lat lon", units="kg m-2"):
    """Return a minimal mock vocabulary object."""
    vocab = MagicMock()
    vocab.variable = {"dimensions": dimensions, "units": units, "type": "double"}
    vocab.axes = {
        "lat": {"out_name": "lat"},
        "lon": {"out_name": "lon"},
        "time": {"out_name": "time"},
    }
    # _get_axes returns (required_axes, rename_map)
    vocab._get_axes.return_value = ([], {})
    # _get_required_bounds_variables returns (required_bounds, rename_map)
    vocab._get_required_bounds_variables.return_value = ({}, {})
    return vocab


def _make_cmoriser(
    ds, cmor_name, dimensions="time lat lon", units="kg m-2", tmp_path=None
):
    """
    Instantiate an Atmosphere_CMORiser with a pre-loaded xarray Dataset.
    The instance's internal ds is replaced after init so we can inject
    arbitrary test data without going through load_dataset().
    """
    import tempfile

    out = str(tmp_path or tempfile.mkdtemp())
    vocab = _make_vocab(dimensions=dimensions, units=units)

    mapping = {
        cmor_name: {
            "model_variables": [cmor_name],
            "calculation": {"type": "direct", "formula": cmor_name},
        }
    }

    cmoriser = Atmosphere_CMORiser(
        input_data=ds,
        output_path=out,
        vocab=vocab,
        variable_mapping=mapping,
        compound_name=f"Lmon.{cmor_name}",
        validate_frequency=False,
        enable_chunking=False,
        enable_compression=False,
    )
    return cmoriser


# ---------------------------------------------------------------------------
# Tests for Fix 1: time_0 dimension removal before transpose
# ---------------------------------------------------------------------------


class TestTime0DimensionHandling:
    """
    Ensure that a time_0 dimension that is absent from the CMOR transpose order
    is dropped regardless of its size, so that transpose() does not raise a
    ValueError.
    """

    @pytest.mark.unit
    def test_time0_nonsingleton_dropped_before_transpose(self, tmp_path):
        """
        time_0 with size > 1 must be dropped before transpose.

        This reproduces the original crash:
          ValueError: ('time', 'lon', 'lat') must be a permuted list of
          ('time', 'time_0', 'lat', 'lon'), unless `...` is included
        """
        nt, n0, nlat, nlon = 12, 12, 5, 5
        data = np.ones((nt, n0, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"cVeg": (["time", "time_0", "lat", "lon"], data, {"units": "kg m-2"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "time_0": np.arange(n0, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "cVeg", dimensions="time lat lon", tmp_path=tmp_path
        )

        # Bypass full load_dataset; inject ds directly and run only the
        # transpose/squeeze portion via select_and_process_variables.
        # We patch load_dataset so the pre-injected ds is preserved.
        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # vocab._get_axes / _get_required_bounds_variables already mocked
            cmoriser.select_and_process_variables()

        assert (
            "time_0" not in cmoriser.ds["cVeg"].dims
        ), "time_0 should have been dropped before transpose"

    @pytest.mark.unit
    def test_time0_singleton_still_dropped(self, tmp_path):
        """
        time_0 with size == 1 must also be removed (handled by squeeze).
        Ensures the pre-existing squeeze logic still works alongside the new fix.
        """
        nt, nlat, nlon = 12, 5, 5
        data = np.ones((nt, 1, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"cVeg": (["time", "time_0", "lat", "lon"], data, {"units": "kg m-2"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "time_0": [0.0],
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "cVeg", dimensions="time lat lon", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "time_0" not in cmoriser.ds["cVeg"].dims

    @pytest.mark.unit
    def test_no_time0_unaffected(self, tmp_path):
        """
        Variables without time_0 (e.g. Amon.tas) must pass through unchanged.
        """
        nt, nlat, nlon = 12, 5, 5
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], data, {"units": "K"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "time_0" not in cmoriser.ds["tas"].dims
        assert cmoriser.ds["tas"].dims == ("time", "lat", "lon")

    @pytest.mark.unit
    def test_time0_not_in_cmor_dims_is_dropped(self, tmp_path):
        """
        time_0 present as outer dim (fld_s03i236 style) should also be dropped.
        """
        nt, n0, nlat, nlon = 12, 12, 5, 5
        data = np.ones((n0, nt, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"tas": (["time_0", "time", "lat", "lon"], data, {"units": "K"})},
            coords={
                "time_0": np.arange(n0, dtype=float),
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "time_0" not in cmoriser.ds["tas"].dims


# ---------------------------------------------------------------------------
# Tests for Fix 2: units attribute cleared after formula calculation
# ---------------------------------------------------------------------------


class TestFormulaUnitsClearing:
    """
    After evaluate_expression(), the result inherits the raw model variable's
    units attribute (e.g. "1").  This must be cleared so that update_attributes()
    can write the correct CMOR units without _check_units raising a ValueError.
    """

    @pytest.mark.unit
    def test_formula_result_units_cleared(self, tmp_path):
        """
        After a formula-type calculation, the result variable must have
        no units attribute so that the CMOR units can be applied cleanly.
        """

        nt, nlat, nlon = 3, 5, 5
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        # Two input variables with different (wrong) units inherited from PP
        ds = xr.Dataset(
            {
                "var_a": (["time", "lat", "lon"], data, {"units": "1"}),
                "var_b": (["time", "lat", "lon"], data, {"units": "1"}),
            },
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        vocab = _make_vocab(dimensions="time lat lon", units="kg m-2")
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})

        mapping = {
            "cVeg": {
                "model_variables": ["var_a", "var_b"],
                "calculation": {
                    "type": "formula",
                    "operation": "add",
                    "operands": ["var_a", "var_b"],
                },
            }
        }

        import tempfile

        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path or tempfile.mkdtemp()),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="Lmon.cVeg",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        # units attribute must be absent (cleared) after formula calculation
        assert "units" not in cmoriser.ds["cVeg"].attrs, (
            "Inherited units attribute should have been cleared after formula "
            "calculation so update_attributes() can apply the correct CMOR units"
        )

    @pytest.mark.unit
    def test_direct_calc_preserves_units(self, tmp_path):
        """
        Direct (rename-only) calculations must NOT have their units cleared —
        the attribute check in _check_units should be able to validate them.
        """
        nt, nlat, nlon = 3, 5, 5
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"fld_tas": (["time", "lat", "lon"], data, {"units": "K"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "fld_tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
        )
        # Patch the mapping to direct-rename fld_tas → tas
        cmoriser.mapping = {
            "tas": {
                "model_variables": ["fld_tas"],
                "calculation": {"type": "direct", "formula": "fld_tas"},
            }
        }
        cmoriser.cmor_name = "tas"

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        # For direct calc, units attribute should still be present
        assert "units" in cmoriser.ds["tas"].attrs
        assert cmoriser.ds["tas"].attrs["units"] == "K"

    @pytest.mark.unit
    def test_formula_with_inherited_wrong_units_no_longer_raises(self, tmp_path):
        """
        Before the fix, a formula result with units="1" would cause
        _check_units to raise ValueError("Mismatch units for cVeg: 1 != kg m-2").
        After the fix, clearing the attribute allows the full run() to complete.
        """
        nt, nlat, nlon = 3, 5, 5
        data = np.random.rand(nt, nlat, nlon).astype(np.float32)

        ds = xr.Dataset(
            {
                "var_a": (["time", "lat", "lon"], data, {"units": "1"}),
                "var_b": (
                    ["time", "lat", "lon"],
                    np.ones((nt, nlat, nlon), np.float32),
                    {"units": "1"},
                ),
            },
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        vocab = _make_vocab(dimensions="time lat lon", units="kg m-2")
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})

        mapping = {
            "cVeg": {
                "model_variables": ["var_a", "var_b"],
                "calculation": {
                    "type": "formula",
                    "operation": "add",
                    "operands": ["var_a", "var_b"],
                },
            }
        }

        import tempfile

        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path or tempfile.mkdtemp()),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="Lmon.cVeg",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # select_and_process_variables must not raise due to units mismatch
            cmoriser.select_and_process_variables()


class TestSoilDepthDimension:
    """
    Ensure that tsl (soil temperature) gets its soil_model_level_number
    dimension replaced with actual depth values in metres via calc_tsl,
    so that the CMIP6 sdepth unit check (m) and the transpose both succeed.

    Regression test for two cascading errors when running Lmon.tsl:
      1. ValueError: Dimensions {'depth'} do not exist. Expected one or more of
         ('time', 'soil_model_level_number', 'lat', 'lon')
      2. ValueError: Mismatch units for depth: 1 != m
    """

    @pytest.mark.unit
    def test_calc_tsl_replaces_level_with_depth_metres(self, tmp_path):
        """
        calc_tsl must swap soil_model_level_number for a 'depth' coordinate
        whose values are the CABLE layer mid-point depths in metres.

        This is the unit test for the calc_tsl function itself.
        """
        from access_moppy.derivations.calc_land import calc_tsl

        try:
            import xarray as xr
        except ImportError:
            pytest.skip("xarray not available")

        nz = 6
        # Build a minimal DataArray with soil_model_level_number as a dim
        da = xr.DataArray(
            [[float(i)] for i in range(1, nz + 1)],
            dims=["soil_model_level_number", "x"],
            coords={"soil_model_level_number": list(range(1, nz + 1))},
        )

        result = calc_tsl(da)

        assert "depth" in result.dims, "calc_tsl must produce a 'depth' dimension"
        assert (
            "soil_model_level_number" not in result.dims
        ), "soil_model_level_number must be dropped"
        expected_depths = [
            0.0109999999403954,
            0.0509999990463257,
            0.157000005245209,
            0.438499987125397,
            1.18550002574921,
            2.87199997901917,
        ]
        import math

        for got, exp in zip(result["depth"].values, expected_depths):
            assert math.isclose(
                got, exp, rel_tol=1e-6
            ), f"depth value {got} does not match expected {exp}"

    @pytest.mark.unit
    def test_tsl_select_and_process_produces_depth_dim(self, tmp_path):
        """
        select_and_process_variables for tsl must produce a 'depth' dimension
        (not soil_model_level_number) when the formula path calls calc_tsl.

        This reproduces both crashes:
          - ValueError: Dimensions {'depth'} do not exist
          - ValueError: Mismatch units for depth: 1 != m
        """
        import tempfile

        nt, nz, nlat, nlon = 3, 6, 5, 5
        data = np.ones((nt, nz, nlat, nlon), dtype=np.float32) * 280.0

        ds = xr.Dataset(
            {
                "fld_s08i225": (
                    ["time", "soil_model_level_number", "lat", "lon"],
                    data,
                    {"units": "K"},
                )
            },
            coords={
                "time": np.arange(nt, dtype=float),
                "soil_model_level_number": np.arange(1, nz + 1, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        vocab = MagicMock()
        vocab.variable = {
            "dimensions": "longitude latitude sdepth time",
            "units": "K",
            "type": "double",
        }
        vocab.axes = {
            "longitude": {"out_name": "lon"},
            "latitude": {"out_name": "lat"},
            "sdepth": {"out_name": "depth"},
            "time": {"out_name": "time"},
        }
        # Formula path: calc_tsl already produces 'depth', rename map is a no-op
        vocab._get_axes.return_value = (
            {"sdepth": {"out_name": "depth"}},
            {"depth": "depth"},
        )
        vocab._get_required_bounds_variables.return_value = ({}, {})

        mapping = {
            "tsl": {
                "model_variables": ["fld_s08i225"],
                "calculation": {
                    "type": "formula",
                    "operation": "calc_tsl",
                    "args": ["fld_s08i225"],
                },
                "dimensions": {
                    "time": "time",
                    "depth": "depth",
                    "lat": "lat",
                    "lon": "lon",
                },
            }
        }

        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path or tempfile.mkdtemp()),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="Lmon.tsl",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # Must not raise ValueError about missing 'depth' dim or units mismatch
            cmoriser.select_and_process_variables()

        assert (
            "depth" in cmoriser.ds["tsl"].dims
        ), "tsl must have 'depth' as a dimension after processing"
        assert (
            "soil_model_level_number" not in cmoriser.ds["tsl"].dims
        ), "soil_model_level_number must have been replaced by depth"
        # depth coordinate values must be in metres (not integer level indices)
        depth_vals = cmoriser.ds["tsl"]["depth"].values
        assert all(
            v < 10.0 for v in depth_vals
        ), "depth values must be in metres (< 10 m), not level indices"
